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

import com.facebook.presto.spi.ConnectorSession;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface ConnectionFactory
        extends AutoCloseable
{
    Connection openConnection(JdbcIdentity identity)
            throws SQLException;

    /**
     * Opens a connection for a metadata call of the query in the session. The connection may be one
     * that another metadata call of the same query used or is still using on this thread, so callers
     * must close it before returning. A split or a page sink, which keeps its connection across calls,
     * uses {@link #openConnection(JdbcIdentity)} instead.
     */
    default Connection openConnection(ConnectorSession session, JdbcIdentity identity)
            throws SQLException
    {
        return openConnection(identity);
    }

    /**
     * Called when the query in the session has ended, to release anything kept for it.
     */
    default void cleanupQuery(ConnectorSession session) {}

    @Override
    default void close()
            throws SQLException
    {}
}
