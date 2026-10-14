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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestConnectionLeases
{
    private static final AtomicInteger DATABASES = new AtomicInteger();
    private static final JdbcIdentity ALICE = new JdbcIdentity("alice", ImmutableMap.of());
    private static final JdbcIdentity BOB = new JdbcIdentity("bob", ImmutableMap.of());
    private static final Properties PROPERTIES = new Properties();

    private OpenedConnections opened;
    private ExecutorService otherThread;

    @BeforeMethod
    public void setUp()
    {
        opened = new OpenedConnections("jdbc:h2:mem:leases" + DATABASES.incrementAndGet() + ";DB_CLOSE_DELAY=-1");
        otherThread = Executors.newSingleThreadExecutor();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        otherThread.shutdownNow();
    }

    @Test
    public void testNestedCallSharesTheConnection()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 10);
        try (Connection outer = leases.openConnection("q1", ALICE, PROPERTIES);
                Connection inner = leases.openConnection("q1", ALICE, PROPERTIES)) {
            assertSame(physical(inner), physical(outer));
        }
        finally {
            leases.close();
        }
        assertEquals(opened.connections.size(), 1);
    }

    @Test
    public void testNextCallOfTheQueryReusesTheConnection()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 10);
        try {
            leases.openConnection("q1", ALICE, PROPERTIES).close();
            leases.openConnection("q1", ALICE, PROPERTIES).close();
            assertEquals(opened.connections.size(), 1);
        }
        finally {
            leases.close();
        }
        assertEquals(opened.countOpen(), 0);
    }

    @Test
    public void testNoReuseAcrossQueries()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 10);
        try {
            leases.openConnection("q1", ALICE, PROPERTIES).close();
            leases.openConnection("q2", ALICE, PROPERTIES).close();
            assertEquals(opened.connections.size(), 2);
        }
        finally {
            leases.close();
        }
    }

    @Test
    public void testNoReuseAcrossIdentities()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 10);
        try (Connection alice = leases.openConnection("q1", ALICE, PROPERTIES);
                Connection bob = leases.openConnection("q1", BOB, PROPERTIES)) {
            assertNotSame(physical(alice), physical(bob));
        }
        finally {
            leases.close();
        }
    }

    @Test
    public void testKeptConnectionMovesToAnotherThreadOfTheQuery()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 10);
        try {
            Connection first = leases.openConnection("q1", ALICE, PROPERTIES);
            Connection physical = physical(first);
            first.close();
            Connection second = on(otherThread, () -> leases.openConnection("q1", ALICE, PROPERTIES));
            assertSame(physical(second), physical);
            second.close();
        }
        finally {
            leases.close();
        }
    }

    @Test
    public void testHeldConnectionIsNotSharedWithAnotherThread()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 10);
        try (Connection held = leases.openConnection("q1", ALICE, PROPERTIES)) {
            Connection other = on(otherThread, () -> leases.openConnection("q1", ALICE, PROPERTIES));
            assertNotSame(physical(other), physical(held));
            on(otherThread, () -> {
                other.close();
                return null;
            });
        }
        finally {
            leases.close();
        }
    }

    @Test
    public void testQueryEndClosesKeptConnections()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 10);
        try {
            leases.openConnection("q1", ALICE, PROPERTIES).close();
            leases.openConnection("q2", ALICE, PROPERTIES).close();
            assertEquals(leases.retainedCount(), 2);

            leases.cleanupQuery("q1");
            assertEquals(leases.retainedCount(), 1);
            assertTrue(opened.connections.get(0).isClosed());
            assertFalse(opened.connections.get(1).isClosed());
        }
        finally {
            leases.close();
        }
    }

    @Test
    public void testConnectionReleasedAfterQueryEndIsClosed()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 10);
        try {
            Connection straggler = leases.openConnection("q1", ALICE, PROPERTIES);
            leases.cleanupQuery("q1");
            straggler.close();
            assertEquals(leases.retainedCount(), 0);
            assertEquals(opened.countOpen(), 0);
        }
        finally {
            leases.close();
        }
    }

    @Test
    public void testConnectionWhoseSessionChangedIsClosedNotKept()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 10);
        try {
            Connection readOnly = leases.openConnection("q1", ALICE, PROPERTIES);
            readOnly.setReadOnly(true);
            readOnly.close();

            Connection transaction = leases.openConnection("q1", ALICE, PROPERTIES);
            transaction.setAutoCommit(false);
            transaction.close();

            Connection schema = leases.openConnection("q1", ALICE, PROPERTIES);
            schema.setSchema("INFORMATION_SCHEMA");
            schema.close();

            assertEquals(opened.connections.size(), 3);
            assertEquals(leases.retainedCount(), 0);
            assertEquals(opened.countOpen(), 0);
        }
        finally {
            leases.close();
        }
    }

    @Test
    public void testNestedChangeClosesTheSharedConnectionAtTheEnd()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 10);
        try {
            try (Connection outer = leases.openConnection("q1", ALICE, PROPERTIES)) {
                try (Connection inner = leases.openConnection("q1", ALICE, PROPERTIES)) {
                    inner.setAutoCommit(false);
                    inner.setAutoCommit(true);
                }
                try (Statement statement = outer.createStatement()) {
                    statement.execute("SELECT 1");
                }
            }
            assertEquals(leases.retainedCount(), 0);
            assertEquals(opened.countOpen(), 0);
        }
        finally {
            leases.close();
        }
    }

    @Test
    public void testClosedHandleCannotReachTheConnection()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 10);
        try {
            Connection first = leases.openConnection("q1", ALICE, PROPERTIES);
            Connection physical = physical(first);
            first.close();
            // A second close must not release the connection that the next caller adopts.
            Connection second = leases.openConnection("q1", ALICE, PROPERTIES);
            first.close();
            assertSame(physical(second), physical);
            assertTrue(first.isClosed());
            assertFalse(first.isValid(1));
            assertThrows(SQLException.class, first::createStatement);
            try (Statement statement = second.createStatement()) {
                statement.execute("SELECT 1");
            }
            second.close();
        }
        finally {
            leases.close();
        }
    }

    @Test
    public void testNestedDoubleCloseKeepsTheOuterConnection()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 10);
        try (Connection outer = leases.openConnection("q1", ALICE, PROPERTIES)) {
            Connection inner = leases.openConnection("q1", ALICE, PROPERTIES);
            inner.close();
            inner.close();
            assertFalse(outer.isClosed());
            try (Statement statement = outer.createStatement()) {
                statement.execute("SELECT 1");
            }
        }
        finally {
            leases.close();
        }
    }

    @Test
    public void testClosedConnectionIsNotKept()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 10);
        try {
            Connection connection = leases.openConnection("q1", ALICE, PROPERTIES);
            physical(connection).close();
            connection.close();
            assertEquals(leases.retainedCount(), 0);
        }
        finally {
            leases.close();
        }
    }

    @Test
    public void testKeptConnectionsAreCapped()
            throws Exception
    {
        ConnectionLeases leases = leases(60_000, 2);
        try {
            for (int query = 0; query < 5; query++) {
                leases.openConnection("q" + query, ALICE, PROPERTIES).close();
            }
            assertEquals(opened.connections.size(), 5);
            assertEquals(opened.countOpen(), 2);
            assertFalse(opened.connections.get(4).isClosed());
            assertFalse(opened.connections.get(3).isClosed());
        }
        finally {
            leases.close();
        }
        assertEquals(opened.countOpen(), 0);
    }

    @Test
    public void testZeroRetentionKeepsNothingButStillShares()
            throws Exception
    {
        ConnectionLeases leases = leases(0, 10);
        try {
            try (Connection outer = leases.openConnection("q1", ALICE, PROPERTIES);
                    Connection inner = leases.openConnection("q1", ALICE, PROPERTIES)) {
                assertSame(physical(inner), physical(outer));
            }
            assertEquals(opened.countOpen(), 0);
            leases.openConnection("q1", ALICE, PROPERTIES).close();
            assertEquals(opened.connections.size(), 2);
        }
        finally {
            leases.close();
        }
    }

    @Test
    public void testExpiredConnectionIsClosedWithoutFurtherCalls()
            throws Exception
    {
        ConnectionLeases leases = leases(100, 10);
        try {
            leases.openConnection("q1", ALICE, PROPERTIES).close();
            assertEquals(opened.countOpen(), 1);
            long deadline = System.nanoTime() + SECONDS.toNanos(10);
            while (opened.countOpen() > 0 && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(opened.countOpen(), 0);
        }
        finally {
            leases.close();
        }
    }

    private ConnectionLeases leases(long retentionMillis, int maxRetained)
    {
        return new ConnectionLeases(opened, retentionMillis, maxRetained);
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

    private static class OpenedConnections
            implements ConnectionLeases.ConnectionSource
    {
        private final String url;
        private final List<Connection> connections = new CopyOnWriteArrayList<>();

        private OpenedConnections(String url)
        {
            this.url = url;
        }

        @Override
        public Connection openConnection(Properties connectionProperties)
                throws SQLException
        {
            Connection connection = DriverManager.getConnection(url);
            connections.add(connection);
            return connection;
        }

        private long countOpen()
                throws SQLException
        {
            long open = 0;
            for (Connection connection : connections) {
                if (!connection.isClosed()) {
                    open++;
                }
            }
            return open;
        }
    }
}
