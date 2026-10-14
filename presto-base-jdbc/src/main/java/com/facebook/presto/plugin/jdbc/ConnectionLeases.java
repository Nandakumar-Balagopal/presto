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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;

import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Reuses connections for the metadata calls of one query. A query's metadata is read with many
 * short calls, each of which would otherwise open and close a connection, and some of which open a
 * second connection while they hold the first.
 *
 * <ul>
 * <li>A call nested inside another on the same thread is handed the connection its caller holds.</li>
 * <li>A connection is kept briefly after its caller closes it, and the next call of the same query,
 * on any thread, takes it. Only one caller holds a kept connection at a time.</li>
 * <li>Nothing is reused across queries: kept connections are keyed by query and closed when the
 * query ends, or after the retention time if the engine does not report the end.</li>
 * <li>A connection whose session settings a caller changed is closed rather than kept, so no reset
 * is needed and none can fail.</li>
 * </ul>
 */
class ConnectionLeases
{
    interface ConnectionSource
    {
        Connection openConnection(Properties connectionProperties)
                throws SQLException;
    }

    private final ConnectionSource source;
    private final long retentionMillis;
    // A nested call runs on the thread of the call it is nested in, so only that thread shares a
    // lease. The map is concurrent because a lease may be released from another thread.
    private final ThreadLocal<Map<ReuseKey, Lease>> held = ThreadLocal.withInitial(ConcurrentHashMap::new);
    private final Cache<ReuseKey, Connection> retained;
    // Queries that have ended, so that a connection released after the end is closed, not kept.
    private final Cache<String, Boolean> endedQueries;
    private final ScheduledExecutorService sweeper;

    ConnectionLeases(ConnectionSource source, long retentionMillis, int maxRetained)
    {
        this.source = requireNonNull(source, "source is null");
        this.retentionMillis = retentionMillis;
        // A connection that expires or is displaced is closed; one that is taken for reuse is not. A
        // single segment keeps the bound exact, where Guava would divide a small bound between segments.
        this.retained = CacheBuilder.newBuilder()
                .concurrencyLevel(1)
                .maximumSize(maxRetained)
                .expireAfterWrite(max(retentionMillis, 0), TimeUnit.MILLISECONDS)
                .removalListener((RemovalListener<ReuseKey, Connection>) notification -> {
                    if (notification.getCause() != RemovalCause.EXPLICIT) {
                        closeQuietly(notification.getValue());
                    }
                })
                .build();
        this.endedQueries = CacheBuilder.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(max(retentionMillis, 1000) * 10, TimeUnit.MILLISECONDS)
                .build();
        if (retentionMillis > 0) {
            this.sweeper = Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("jdbc-connection-reuse-%s"));
            long period = max(retentionMillis, 500);
            sweeper.scheduleWithFixedDelay(retained::cleanUp, period, period, TimeUnit.MILLISECONDS);
        }
        else {
            this.sweeper = null;
        }
    }

    Connection openConnection(String queryId, JdbcIdentity identity, Properties connectionProperties)
            throws SQLException
    {
        ReuseKey key = new ReuseKey(queryId, identity, connectionProperties);

        Map<ReuseKey, Lease> leases = held.get();
        Lease lease = leases.get(key);
        if (lease != null && lease.tryAcquire()) {
            return lease.newHandle();
        }

        lease = new Lease(key, adoptOrOpen(key), leases);
        leases.put(key, lease);
        return lease.newHandle();
    }

    void cleanupQuery(String queryId)
    {
        endedQueries.put(queryId, true);
        retained.asMap().forEach((key, connection) -> {
            if (key.queryId.equals(queryId) && retained.asMap().remove(key, connection)) {
                closeQuietly(connection);
            }
        });
    }

    private Connection adoptOrOpen(ReuseKey key)
            throws SQLException
    {
        Connection candidate = retained.asMap().remove(key);
        if (candidate != null) {
            // A kept connection is not validated: it was in use by the same query moments ago, and a
            // new query never inherits one, so it starts from the pool, which validates on borrow.
            try {
                if (!candidate.isClosed()) {
                    return candidate;
                }
            }
            catch (SQLException e) {
                // A connection that cannot be checked is not worth failing the caller for.
            }
            closeQuietly(candidate);
        }
        return source.openConnection(key.connectionProperties);
    }

    private void keepOrClose(ReuseKey key, Connection connection, boolean dirty)
    {
        try {
            if (dirty || retentionMillis <= 0 || connection.isClosed() || endedQueries.getIfPresent(key.queryId) != null) {
                closeQuietly(connection);
                return;
            }
        }
        catch (SQLException e) {
            closeQuietly(connection);
            return;
        }
        // Replacing a kept connection of the same key closes the one replaced.
        retained.put(key, connection);
    }

    void close()
    {
        if (sweeper != null) {
            sweeper.shutdownNow();
        }
        retained.asMap().values().forEach(ConnectionLeases::closeQuietly);
        retained.invalidateAll();
    }

    int retainedCount()
    {
        retained.cleanUp();
        return (int) retained.size();
    }

    private static void closeQuietly(Connection connection)
    {
        try {
            connection.close();
        }
        catch (SQLException ignored) {
        }
    }

    private static final class ReuseKey
    {
        private final String queryId;
        // A connection is only handed to the identity and the credentials that opened it. A query has
        // one identity, but the identity stays part of the key so that this does not depend on it.
        private final JdbcIdentity identity;
        private final Properties connectionProperties;

        private ReuseKey(String queryId, JdbcIdentity identity, Properties connectionProperties)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.identity = requireNonNull(identity, "identity is null");
            this.connectionProperties = new Properties();
            this.connectionProperties.putAll(connectionProperties);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof ReuseKey)) {
                return false;
            }
            ReuseKey that = (ReuseKey) other;
            return queryId.equals(that.queryId) && identity.equals(that.identity) && connectionProperties.equals(that.connectionProperties);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(queryId, identity, connectionProperties);
        }
    }

    /**
     * One physical connection and the callers holding it. Each caller gets a handle of its own, so a
     * caller that closes twice, or keeps using a handle after closing it, cannot release or reach a
     * connection that another caller now holds.
     */
    private final class Lease
    {
        private final ReuseKey key;
        private final Connection connection;
        private final Map<ReuseKey, Lease> owner;
        private int holders = 1;
        private boolean released;
        private volatile boolean dirty;

        private Lease(ReuseKey key, Connection connection, Map<ReuseKey, Lease> owner)
        {
            this.key = key;
            this.connection = connection;
            this.owner = owner;
        }

        private synchronized boolean tryAcquire()
        {
            if (released) {
                return false;
            }
            holders++;
            return true;
        }

        private Connection newHandle()
        {
            return new Handle();
        }

        private void release()
        {
            synchronized (this) {
                holders--;
                if (holders > 0) {
                    return;
                }
                released = true;
            }
            owner.remove(key, this);
            keepOrClose(key, connection, dirty);
        }

        private final class Handle
                extends ForwardingConnection
        {
            private volatile boolean closed;

            @Override
            protected Connection delegate()
                    throws SQLException
            {
                if (closed) {
                    throw new SQLException("Connection is closed");
                }
                return connection;
            }

            @Override
            public synchronized void close()
            {
                if (!closed) {
                    closed = true;
                    release();
                }
            }

            @Override
            public boolean isClosed()
                    throws SQLException
            {
                return closed || connection.isClosed();
            }

            @Override
            public boolean isValid(int timeout)
                    throws SQLException
            {
                return !closed && connection.isValid(timeout);
            }

            @Override
            public void abort(Executor executor)
                    throws SQLException
            {
                dirty = true;
                try {
                    super.abort(executor);
                }
                finally {
                    close();
                }
            }

            // Any change to the session makes the connection unfit for the next caller.

            @Override
            public void setAutoCommit(boolean autoCommit)
                    throws SQLException
            {
                dirty = true;
                super.setAutoCommit(autoCommit);
            }

            @Override
            public void setReadOnly(boolean readOnly)
                    throws SQLException
            {
                dirty = true;
                super.setReadOnly(readOnly);
            }

            @Override
            public void setCatalog(String catalog)
                    throws SQLException
            {
                dirty = true;
                super.setCatalog(catalog);
            }

            @Override
            public void setSchema(String schema)
                    throws SQLException
            {
                dirty = true;
                super.setSchema(schema);
            }

            @Override
            public void setTransactionIsolation(int level)
                    throws SQLException
            {
                dirty = true;
                super.setTransactionIsolation(level);
            }

            @Override
            public void setHoldability(int holdability)
                    throws SQLException
            {
                dirty = true;
                super.setHoldability(holdability);
            }

            @Override
            public void setTypeMap(Map<String, Class<?>> map)
                    throws SQLException
            {
                dirty = true;
                super.setTypeMap(map);
            }

            @Override
            public void setClientInfo(String name, String value)
                    throws SQLClientInfoException
            {
                dirty = true;
                super.setClientInfo(name, value);
            }

            @Override
            public void setClientInfo(Properties properties)
                    throws SQLClientInfoException
            {
                dirty = true;
                super.setClientInfo(properties);
            }

            @Override
            public void setNetworkTimeout(Executor executor, int milliseconds)
                    throws SQLException
            {
                dirty = true;
                super.setNetworkTimeout(executor, milliseconds);
            }

            @Override
            public Savepoint setSavepoint()
                    throws SQLException
            {
                dirty = true;
                return super.setSavepoint();
            }

            @Override
            public Savepoint setSavepoint(String name)
                    throws SQLException
            {
                dirty = true;
                return super.setSavepoint(name);
            }

            @Override
            public String toString()
            {
                return "Connection lease on " + connection;
            }
        }
    }
}
