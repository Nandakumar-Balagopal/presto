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

import com.facebook.airlift.log.Logger;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Hands out connections from a pool instead of dialling the database for every operation. A
 * connection is reused only for the credentials it was opened with, so credential pass-through
 * cannot leak one user's connection to another.
 */
public class PoolingDriverConnectionFactory
        implements AutoCloseable
{
    private static final Logger log = Logger.get(PoolingDriverConnectionFactory.class);
    private static final int MAX_BORROW_ATTEMPTS = 3;
    private static final Duration VALIDATION_TIMEOUT = Duration.ofSeconds(5);

    private final Driver driver;
    private final String connectionUrl;
    private final int maxSize;
    private final Duration maxWait;
    private final Duration maxIdleTime;
    private final Duration validationBypassWindow;
    private final Duration maxLifetime;
    private final Optional<String> resetSql;
    private final AtomicLong poolsCreated = new AtomicLong();
    private final AtomicLong connectionsBorrowed = new AtomicLong();
    private final AtomicLong borrowWaitMillis = new AtomicLong();
    private final AtomicLong overflowConnections = new AtomicLong();
    private final AtomicLong overflowRefused = new AtomicLong();
    private final Semaphore overflowPermits;
    private final Cache<Properties, Pool> pools;

    public PoolingDriverConnectionFactory(Driver driver, String connectionUrl, BaseJdbcConfig config)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        requireNonNull(config, "config is null");
        this.maxSize = config.getConnectionPoolMaxSize();
        this.maxWait = Duration.ofMillis(config.getConnectionPoolMaxWait().toMillis());
        this.maxIdleTime = Duration.ofMillis(config.getConnectionPoolMaxIdleTime().toMillis());
        this.validationBypassWindow = Duration.ofMillis(config.getConnectionPoolValidationBypassWindow().toMillis());
        this.maxLifetime = Duration.ofMillis(config.getConnectionPoolMaxLifetime().toMillis());
        this.resetSql = Optional.ofNullable(config.getConnectionPoolResetSql());
        // Without a ceiling here a node opens as many connections as it has callers, so max-size
        // bounds what the pool keeps and nothing bounds what reaches the database. Exhausting the
        // database's own limit fails queries for every other client too, not just this catalog.
        this.overflowPermits = new Semaphore(config.getConnectionPoolMaxOverflow());
        // One pool is kept per set of credentials, so with credential pass-through the number of
        // pools follows the number of users. Bound it, and close a pool once its credentials have
        // gone unused, so neither the pools nor their idle connections accumulate.
        this.pools = CacheBuilder.newBuilder()
                .maximumSize(config.getConnectionPoolMaxCredentialSets())
                .expireAfterAccess(maxIdleTime)
                .removalListener((RemovalListener<Properties, Pool>) notification -> close(notification.getValue().dataSource))
                .build();
    }

    /**
     * @param waitForPool whether to wait for a connection when the pool is exhausted. A caller that
     * holds its connection while it waits for other operators -- a split or a page sink -- must not:
     * a query that joins two tables of the catalog, or reads from it while writing to it, holds one
     * connection and needs another, so with enough such queries at once every connection is held by
     * a caller waiting for a second, and the waiting callers occupy the threads that would release
     * them. Such a caller takes an idle connection if there is one and otherwise opens its own, so
     * the pool bounds the connections it keeps, not the connections a query may use.
     */
    public Connection openConnection(Properties connectionProperties, boolean waitForPool)
            throws SQLException
    {
        Properties key = new Properties();
        key.putAll(connectionProperties);

        // A pool is closed when its credentials are evicted, which may happen between looking it up
        // and borrowing from it, or while a caller waits on it; the evicted pool's replacement serves
        // the caller instead. Connections already borrowed from a closed pool keep working.
        for (int attempt = 1; ; attempt++) {
            Pool pool = poolFor(key);
            try {
                return borrow(pool, key, waitForPool);
            }
            catch (SQLException | RuntimeException e) {
                if (!pool.objectPool.isClosed() || attempt == MAX_BORROW_ATTEMPTS) {
                    throw e;
                }
                pools.asMap().remove(key, pool);
            }
        }
    }

    private Connection borrow(Pool pool, Properties key, boolean waitForPool)
            throws SQLException
    {
        if (!waitForPool) {
            try {
                Connection connection = pool.objectPool.borrowObject(Duration.ZERO);
                connectionsBorrowed.incrementAndGet();
                return connection;
            }
            catch (NoSuchElementException e) {
                if (pool.objectPool.isClosed()) {
                    throw e;
                }
                if (!overflowPermits.tryAcquire()) {
                    // The node is already at max-size + max-overflow, so wait for the pool rather
                    // than add to the load on a database that may be near its own limit.
                    overflowRefused.incrementAndGet();
                    return borrowAndWait(pool);
                }
                boolean opened = false;
                try {
                    Connection connection = driver.connect(connectionUrl, key);
                    if (connection == null) {
                        throw new SQLException("Driver returned null connection");
                    }
                    overflowConnections.incrementAndGet();
                    opened = true;
                    return new OverflowConnection(connection);
                }
                finally {
                    if (!opened) {
                        overflowPermits.release();
                    }
                }
            }
            catch (SQLException | RuntimeException e) {
                throw e;
            }
            catch (Exception e) {
                throw new SQLException("Failed to borrow a pooled connection: " + e.getMessage(), e);
            }
        }

        return borrowAndWait(pool);
    }

    private Connection borrowAndWait(Pool pool)
            throws SQLException
    {
        long start = System.nanoTime();
        Connection connection = pool.dataSource.getConnection();
        borrowWaitMillis.addAndGet((System.nanoTime() - start) / 1_000_000);
        connectionsBorrowed.incrementAndGet();

        return connection;
    }

    private Pool poolFor(Properties key)
            throws SQLException
    {
        try {
            return pools.get(key, () -> {
                // Check the reset statement on a connection of our own first. Run inside the pool it
                // would surface as "Unable to activate object" on an unrelated query, with the pool
                // swallowing the reason.
                if (resetSql.isPresent()) {
                    try (Connection connection = driver.connect(connectionUrl, key);
                            java.sql.Statement statement = connection.createStatement()) {
                        statement.execute(resetSql.get());
                    }
                    catch (SQLException e) {
                        throw new SQLException(format("The database rejected the statement configured as connection-pool.reset-sql (%s): %s",
                                resetSql.get(), e.getMessage()), e);
                    }
                }
                return createPool(key);
            });
        }
        catch (ExecutionException e) {
            throw new SQLException("Failed to create a connection pool: " + e.getCause().getMessage(), e.getCause());
        }
    }

    private Pool createPool(Properties connectionProperties)
    {
        org.apache.commons.dbcp2.DriverConnectionFactory connectionFactory =
                new org.apache.commons.dbcp2.DriverConnectionFactory(driver, connectionUrl, connectionProperties);

        poolsCreated.incrementAndGet();
        PoolableConnectionFactory poolableConnectionFactory = new ResettingConnectionFactory(connectionFactory, resetSql, validationBypassWindow);
        GenericObjectPoolConfig<PoolableConnection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(maxSize);
        poolConfig.setMaxIdle(maxSize);
        poolConfig.setMaxWait(maxWait);
        poolConfig.setMinEvictableIdleDuration(maxIdleTime);
        poolConfig.setTimeBetweenEvictionRuns(maxIdleTime);
        // Handing out a connection without checking it is not an option worth offering: a link that
        // drops without closing leaves every connection in the pool dead, and each one then fails a
        // query. Recovery is not one failure either, since a dead connection that is handed out and
        // returned stays in the pool, so failures keep landing for as long as the pool holds one.
        // What the check costs is avoided instead, by skipping it for a connection that was in use
        // moments ago -- see ResettingConnectionFactory.
        poolConfig.setTestOnBorrow(true);
        // A connection that died while idle is dropped by the evictor rather than handed out, which
        // is the cheaper half of validating: it costs a round trip per idle connection, not per borrow.
        poolConfig.setTestWhileIdle(true);
        // A connection whose driver has already reported a connection-level failure fails validation
        // without a round trip. Without this, each dead connection in a pool burns the validation
        // timeout before being replaced, so a dropped link stalls queries for max-size * 5s.
        poolableConnectionFactory.setFastFailValidation(true);

        // A pooled connection is an authenticated session, so a password change, a locked account or
        // a revoked grant at the database is not noticed while the connection is reused. Retiring a
        // connection by age bounds that window; validation does not, since it does not re-authenticate.
        poolableConnectionFactory.setMaxConn(maxLifetime.isZero() ? Duration.ofMillis(-1) : maxLifetime);
        // Without a timeout the check is Connection.isValid(0), which waits forever on a database that
        // has stopped answering without closing the socket -- the case validation is there to catch --
        // and max-wait does not bound it. The same check runs for idle connections on a timer thread
        // that every pool in the JVM shares, so one unresponsive database would stall eviction for all.
        poolableConnectionFactory.setValidationQueryTimeout(VALIDATION_TIMEOUT);

        GenericObjectPool<PoolableConnection> pool = new GenericObjectPool<>(poolableConnectionFactory, poolConfig);
        poolableConnectionFactory.setPool(pool);
        return new Pool(pool, new PoolingDataSource<>(pool));
    }

    private static final class Pool
    {
        private final GenericObjectPool<PoolableConnection> objectPool;
        private final PoolingDataSource<PoolableConnection> dataSource;

        private Pool(GenericObjectPool<PoolableConnection> objectPool, PoolingDataSource<PoolableConnection> dataSource)
        {
            this.objectPool = objectPool;
            this.dataSource = dataSource;
        }
    }

    @Override
    public void close()
    {
        pools.invalidateAll();
        pools.cleanUp();
    }

    public long getPoolsCreated()
    {
        return poolsCreated.get();
    }

    public long getConnectionsBorrowed()
    {
        return connectionsBorrowed.get();
    }

    public long getBorrowWaitMillis()
    {
        return borrowWaitMillis.get();
    }

    public long getOverflowRefused()
    {
        return overflowRefused.get();
    }

    /**
     * Returns its permit when closed, so a node's connections stay within max-size + max-overflow.
     */
    private final class OverflowConnection
            extends ForwardingConnection
    {
        private final Connection delegate;
        private boolean released;

        private OverflowConnection(Connection delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        protected Connection delegate()
        {
            return delegate;
        }

        @Override
        public void close()
                throws SQLException
        {
            try {
                delegate.close();
            }
            finally {
                if (!released) {
                    released = true;
                    overflowPermits.release();
                }
            }
        }
    }

    public long getOverflowConnections()
    {
        return overflowConnections.get();
    }

    /**
     * Runs a statement before a connection leaves the pool. A database keeps state on a session that
     * survives a return to the pool -- a query band, a temporary table, a SET -- and the next caller
     * would otherwise inherit it. Also decides when checking a connection is worth its round trip.
     */
    private static class ResettingConnectionFactory
            extends PoolableConnectionFactory
    {
        private final Optional<String> resetSql;
        private final Duration validationBypassWindow;

        ResettingConnectionFactory(org.apache.commons.dbcp2.ConnectionFactory connectionFactory, Optional<String> resetSql, Duration validationBypassWindow)
        {
            super(connectionFactory, null);
            this.resetSql = requireNonNull(resetSql, "resetSql is null");
            this.validationBypassWindow = requireNonNull(validationBypassWindow, "validationBypassWindow is null");
            setRollbackOnReturn(true);
            setAutoCommitOnReturn(true);
            // BaseJdbcClient marks a connection read-only while it reads, and a caller may leave
            // auto-commit off. Neither is restored on return unless a default is set here, so a
            // connection that has served a query would otherwise refuse the next write with
            // "cannot execute INSERT in a read-only transaction".
            setDefaultReadOnly(false);
            setDefaultAutoCommit(true);
        }

        /**
         * Skips the check for a connection that was in use within the bypass window. What makes a
         * pooled connection stale is time spent idle, so a connection handed back moments ago is
         * about as likely to be alive as a brand new one, and under load that is almost every
         * borrow. The cost this avoids is not uniform: PostgreSQL answers isValid in 1-3 ms, while
         * Teradata takes 260-300 ms, which a query pays once per connection it borrows.
         */
        @Override
        public boolean validateObject(org.apache.commons.pool2.PooledObject<PoolableConnection> pooledObject)
        {
            if (!validationBypassWindow.isZero()
                    && Duration.between(pooledObject.getLastReturnInstant(), Instant.now()).compareTo(validationBypassWindow) < 0) {
                // A driver that has given up on a connection reports it closed from local state, so
                // the one failure this window could let through still costs nothing to rule out.
                try {
                    return !pooledObject.getObject().isClosed();
                }
                catch (SQLException e) {
                    return false;
                }
            }
            return super.validateObject(pooledObject);
        }

        @Override
        public void activateObject(org.apache.commons.pool2.PooledObject<PoolableConnection> pooledObject)
                throws SQLException
        {
            super.activateObject(pooledObject);
            if (resetSql.isPresent()) {
                try (java.sql.Statement statement = pooledObject.getObject().createStatement()) {
                    statement.execute(resetSql.get());
                }
                catch (SQLException e) {
                    // Left unwrapped this surfaces as "Unable to activate object" on every borrow,
                    // and not every database accepts its reset as a statement: MySQL resets a
                    // connection through the protocol rather than through SQL.
                    throw new SQLException(format("Statement configured as connection-pool.reset-sql failed: %s", resetSql.get()), e);
                }
            }
        }
    }

    private static void close(PoolingDataSource<PoolableConnection> dataSource)
    {
        try {
            dataSource.close();
        }
        catch (Exception e) {
            log.warn(e, "Failed to close a connection pool");
        }
    }
}
