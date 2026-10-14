/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.jdbc;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import org.h2.Driver;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

/**
 * A split or a page sink holds its connection across many calls, and the engine may run those calls
 * -- and the close -- on different threads, with other splits running on the same thread in between.
 * Those connections go through {@link ConnectionFactory#openConnection(JdbcIdentity)} and must never
 * be reused; only metadata calls, through the session, are.
 */
@Test(singleThreaded = true)
public class TestConnectionReuseIsolation
{
    private static final AtomicInteger DATABASES = new AtomicInteger();

    private final ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
    private final JdbcIdentity identity = JdbcIdentity.from(session);
    private DriverConnectionFactory factory;
    private ExecutorService threadA;
    private ExecutorService threadB;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        threadA = Executors.newSingleThreadExecutor();
        threadB = Executors.newSingleThreadExecutor();
        BaseJdbcConfig config = new BaseJdbcConfig()
                .setConnectionUrl("jdbc:h2:mem:isolation" + DATABASES.incrementAndGet() + ";DB_CLOSE_DELAY=-1")
                .setConnectionReuseEnabled(true)
                .setConnectionReuseRetention(new Duration(60, SECONDS));
        factory = new DriverConnectionFactory(new Driver(), config);
        try (Connection connection = factory.openConnection(identity); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE rows_written (writer VARCHAR, n INT)");
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        factory.close();
        threadA.shutdownNow();
        threadB.shutdownNow();
    }

    @Test
    public void testSplitConnectionsAreNeverShared()
            throws Exception
    {
        Connection first = on(threadA, () -> factory.openConnection(identity));
        Connection second = on(threadA, () -> factory.openConnection(identity));
        try {
            assertNotSame(physical(first), physical(second));
        }
        finally {
            first.close();
            second.close();
        }
    }

    @Test
    public void testSplitConnectionIsNotKeptForTheNextCaller()
            throws Exception
    {
        Connection split = factory.openConnection(identity);
        split.close();
        assertTrue(split.isClosed());
    }

    @Test
    public void testMetadataConnectionIsNotHandedToASplit()
            throws Exception
    {
        try (Connection metadata = factory.openConnection(session, identity);
                Connection split = factory.openConnection(identity)) {
            assertNotSame(physical(split), physical(metadata));
        }
    }

    @Test
    public void testRollbackOfOneWriterDoesNotDiscardAnother()
            throws Exception
    {
        // Two page sinks of the same user whose calls interleave on one thread, one closed elsewhere.
        Connection sink1 = on(threadA, () -> beginWriter(factory.openConnection(identity)));
        Connection sink2 = on(threadA, () -> beginWriter(factory.openConnection(identity)));
        on(threadA, () -> insert(sink1, "sink1"));
        on(threadA, () -> insert(sink2, "sink2"));
        on(threadB, () -> {
            sink2.commit();
            sink2.close();
            return null;
        });
        on(threadA, () -> {
            sink1.rollback();
            sink1.close();
            return null;
        });

        assertEquals(count("sink2"), 1, "rows committed by sink2");
        assertEquals(count("sink1"), 0, "rows rolled back by sink1");
    }

    @Test
    public void testAbortOfOneSplitDoesNotKillAnother()
            throws Exception
    {
        Connection split1 = on(threadA, () -> factory.openConnection(identity));
        Connection split2 = on(threadA, () -> factory.openConnection(identity));
        // What MySqlClient.abortReadConnection does when a query is cancelled.
        split1.abort(Runnable::run);
        split1.close();
        assertFalse(split2.isClosed(), "split2 was closed by the abort of split1");
        try (Statement statement = split2.createStatement()) {
            statement.execute("SELECT 1");
        }
        split2.close();
    }

    @Test
    public void testMetadataReuseStaysWithinTheQuery()
            throws Exception
    {
        ConnectorSession otherQuery = new TestingConnectorSession(ImmutableList.of());
        Connection first = factory.openConnection(session, identity);
        Connection physical = physical(first);
        first.close();

        try (Connection sameQuery = factory.openConnection(session, identity)) {
            assertSame(physical(sameQuery), physical);
        }
        try (Connection nextQuery = factory.openConnection(otherQuery, JdbcIdentity.from(otherQuery))) {
            assertNotSame(physical(nextQuery), physical);
        }
    }

    @Test
    public void testQueryEndClosesTheKeptConnection()
            throws Exception
    {
        Connection metadata = factory.openConnection(session, identity);
        Connection physical = physical(metadata);
        metadata.close();
        assertFalse(physical.isClosed());
        factory.cleanupQuery(session);
        assertTrue(physical.isClosed());
    }

    private static Connection beginWriter(Connection connection)
            throws SQLException
    {
        connection.setAutoCommit(false);
        return connection;
    }

    private static Void insert(Connection connection, String writer)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO rows_written VALUES ('" + writer + "', 1)");
        }
        return null;
    }

    private long count(String writer)
            throws SQLException
    {
        try (Connection connection = factory.openConnection(identity);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM rows_written WHERE writer = '" + writer + "'")) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private static Connection physical(Connection connection)
            throws SQLException
    {
        return connection.unwrap(Connection.class);
    }

    private static <T> T on(ExecutorService thread, Callable<T> task)
            throws Exception
    {
        return thread.submit(task).get(30, SECONDS);
    }
}
