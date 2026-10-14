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
import com.facebook.airlift.units.MinDuration;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.inject.ConfigurationException;
import org.h2.Driver;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.airlift.testing.ValidationAssertions.assertFailsValidation;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class TestPoolingDriverConnectionFactory
{
    private static final AtomicInteger DATABASES = new AtomicInteger();
    private static final ConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of());

    @Test
    public void testSplitDoesNotWaitForAnExhaustedPool()
            throws Exception
    {
        PoolingDriverConnectionFactory pool = pool(config().setConnectionPoolMaxSize(1).setConnectionPoolMaxWait(new Duration(30, SECONDS)));
        try (Connection held = split(pool, credentials(1))) {
            long start = System.nanoTime();
            try (Connection overflow = split(pool, credentials(1)); Statement statement = overflow.createStatement()) {
                statement.execute("SELECT 1");
            }
            assertTrue(System.nanoTime() - start < SECONDS.toNanos(5), "a split waited for the pool");
            assertEquals(pool.getOverflowConnections(), 1);
        }
        finally {
            pool.close();
        }
    }

    @Test
    public void testMetadataCallWaitsForTheExhaustedPoolAndTimesOut()
            throws Exception
    {
        PoolingDriverConnectionFactory pool = pool(config().setConnectionPoolMaxSize(1).setConnectionPoolMaxWait(new Duration(200, MILLISECONDS)));
        try (Connection held = pool.openConnection(credentials(1), true)) {
            assertThrows(SQLException.class, () -> pool.openConnection(credentials(1), true));
            assertEquals(pool.getOverflowConnections(), 0);
        }
        finally {
            pool.close();
        }
    }

    @Test
    public void testCallerWaitingOnAnEvictedPoolIsServedByItsReplacement()
            throws Exception
    {
        PoolingDriverConnectionFactory pool = pool(config()
                .setConnectionPoolMaxSize(1)
                .setConnectionPoolMaxCredentialSets(1)
                .setConnectionPoolMaxWait(new Duration(10, SECONDS)));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Connection held = pool.openConnection(credentials(1), true);
            Future<Connection> waiter = executor.submit(() -> pool.openConnection(credentials(1), true));
            Thread.sleep(300);

            // A second set of credentials evicts the pool the waiter is queued on.
            pool.openConnection(credentials(2), true).close();

            try (Connection served = waiter.get(10, SECONDS); Statement statement = served.createStatement()) {
                statement.execute("SELECT 1");
            }
            // The connection borrowed before the eviction still works and can be returned.
            try (Statement statement = held.createStatement()) {
                statement.execute("SELECT 1");
            }
            held.close();
        }
        finally {
            executor.shutdownNow();
            pool.close();
        }
    }

    @Test
    public void testConnectionThatDiedWhileIdleIsNotHandedOut()
            throws Exception
    {
        CountingDriver driver = new CountingDriver();
        BaseJdbcConfig config = config().setConnectionPoolMaxSize(1);
        PoolingDriverConnectionFactory pool = new PoolingDriverConnectionFactory(driver, config.getConnectionUrl(), config);
        try {
            Connection first = split(pool, credentials(1));
            Connection physical = driver.lastConnection();
            first.close();

            // The link dropping is not something the pool is told about, so the connection it holds
            // is dead while it still looks idle and reusable.
            physical.close();
            Thread.sleep(600);

            Connection second = split(pool, credentials(1));
            try (Statement statement = second.createStatement()) {
                statement.execute("SELECT 1");
            }
            second.close();
        }
        finally {
            pool.close();
        }
    }

    @Test
    public void testBorrowWithinTheBypassWindowSkipsValidation()
            throws Exception
    {
        CountingDriver driver = new CountingDriver();
        BaseJdbcConfig config = config()
                .setConnectionPoolMaxSize(1)
                .setConnectionPoolValidationBypassWindow(new Duration(1, SECONDS));
        PoolingDriverConnectionFactory pool = new PoolingDriverConnectionFactory(driver, config.getConnectionUrl(), config);
        try {
            borrowAndReturn(pool, 4);
            assertEquals(driver.validations(), 0, "a connection in use moments ago was checked");

            Thread.sleep(1200);
            borrowAndReturn(pool, 1);
            assertEquals(driver.validations(), 1, "a connection left idle past the window was not checked");
        }
        finally {
            pool.close();
        }
    }

    @Test
    public void testBypassWindowOfZeroValidatesEveryBorrow()
            throws Exception
    {
        CountingDriver driver = new CountingDriver();
        BaseJdbcConfig config = config()
                .setConnectionPoolMaxSize(1)
                .setConnectionPoolValidationBypassWindow(new Duration(0, MILLISECONDS));
        PoolingDriverConnectionFactory pool = new PoolingDriverConnectionFactory(driver, config.getConnectionUrl(), config);
        try {
            borrowAndReturn(pool, 4);
            assertEquals(driver.validations(), 4);
        }
        finally {
            pool.close();
        }
    }

    @Test
    public void testConnectorThatRefusesPoolingRejectsTheProperty()
    {
        BaseJdbcConfig config = config();
        ConfigurationException e = expectThrows(ConfigurationException.class,
                () -> DriverConnectionFactory.withoutPooling("example", new Driver(), config));
        assertTrue(
                e.getMessage().contains("connection-pool.enabled is not supported by the example connector"),
                "message does not name the property and the connector: " + e.getMessage());
    }

    @Test
    public void testConnectorThatRefusesPoolingStillLoadsWithoutIt()
            throws Exception
    {
        try (DriverConnectionFactory factory = DriverConnectionFactory.withoutPooling("example", new Driver(), config().setConnectionPoolEnabled(false))) {
            try (Connection connection = factory.openConnection(JdbcIdentity.from(SESSION))) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("SELECT 1");
                }
            }
        }
    }

    @Test
    public void testConnectorPropertiesDoNotDisablePooling()
            throws Exception
    {
        Properties properties = new Properties();
        properties.setProperty("user", "sa");
        properties.setProperty("password", "");
        BaseJdbcConfig config = config().setConnectionPoolMaxSize(1);
        try (DriverConnectionFactory factory = new DriverConnectionFactory(new Driver(), config, properties)) {
            Connection first = factory.openConnection(JdbcIdentity.from(SESSION));
            first.close();
            // A pool of one hands the same connection back, which a factory without a pool cannot do.
            Connection second = factory.openConnection(JdbcIdentity.from(SESSION));
            try (Statement statement = second.createStatement()) {
                statement.execute("SELECT 1");
            }
            second.close();
        }
    }

    @Test
    public void testIdleTimeMustBePositive()
    {
        assertFailsValidation(
                new BaseJdbcConfig().setConnectionUrl("jdbc:h2:mem:x").setConnectionPoolMaxIdleTime(new Duration(0, SECONDS)),
                "connectionPoolMaxIdleTime",
                "must be greater than or equal to 1s",
                MinDuration.class);
    }

    private static Connection split(PoolingDriverConnectionFactory pool, Properties credentials)
            throws SQLException
    {
        return pool.openConnection(credentials, false);
    }

    private static void borrowAndReturn(PoolingDriverConnectionFactory pool, int times)
            throws SQLException
    {
        for (int i = 0; i < times; i++) {
            Connection connection = split(pool, credentials(1));
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT 1");
            }
            connection.close();
        }
    }

    /**
     * Counts the checks the pool makes on a connection, and keeps the physical connection so a test
     * can close it behind the pool's back.
     */
    private static class CountingDriver
            extends Driver
    {
        private final AtomicInteger validations = new AtomicInteger();
        private volatile Connection lastConnection;

        @Override
        public Connection connect(String url, Properties info)
                throws SQLException
        {
            Connection connection = super.connect(url, info);
            if (connection == null) {
                return null;
            }
            Connection counting = new ForwardingConnection()
            {
                @Override
                protected Connection delegate()
                {
                    return connection;
                }

                @Override
                public boolean isValid(int timeout)
                        throws SQLException
                {
                    validations.incrementAndGet();
                    return connection.isValid(timeout);
                }
            };
            lastConnection = connection;
            return counting;
        }

        int validations()
        {
            return validations.get();
        }

        Connection lastConnection()
        {
            return lastConnection;
        }
    }

    private static BaseJdbcConfig config()
    {
        return new BaseJdbcConfig()
                .setConnectionUrl("jdbc:h2:mem:pool" + DATABASES.incrementAndGet() + ";DB_CLOSE_DELAY=-1")
                .setConnectionPoolEnabled(true);
    }

    private static PoolingDriverConnectionFactory pool(BaseJdbcConfig config)
    {
        return new PoolingDriverConnectionFactory(new Driver(), config.getConnectionUrl(), config);
    }

    // Two credential sets the H2 driver accepts: they differ only in a connection setting.
    private static Properties credentials(int set)
    {
        Properties properties = new Properties();
        properties.setProperty("user", "sa");
        properties.setProperty("password", "");
        properties.setProperty("LOCK_TIMEOUT", String.valueOf(1000 * set));
        return properties;
    }
}
