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
import com.google.common.collect.ImmutableList;
import com.google.inject.ConfigurationException;
import com.google.inject.spi.Message;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DriverConnectionFactory
        implements ConnectionFactory
{
    private final Driver driver;
    private final String connectionUrl;
    private final Properties connectionProperties;
    private final Optional<String> userCredentialName;
    private final Optional<String> passwordCredentialName;
    private final Optional<PoolingDriverConnectionFactory> pool;
    private final Optional<ConnectionLeases> leases;

    public DriverConnectionFactory(Driver driver, BaseJdbcConfig config)
    {
        this(driver, config.getConnectionUrl(), config, basicConnectionProperties(config), true);
    }

    /**
     * For a connector that sets connection properties of its own. The catalog's reuse and pool
     * settings apply, which the constructor without a config cannot do: it has no config to read
     * them from, so a catalog using that path gets neither feature however it is configured.
     */
    public DriverConnectionFactory(Driver driver, BaseJdbcConfig config, Properties connectionProperties)
    {
        this(driver, config.getConnectionUrl(), config, connectionProperties, true);
    }

    /**
     * For a connector whose backend should never be pooled, because it pools inside its driver or
     * expects a session per request. A catalog of such a connector that asks for a pool fails to
     * load rather than silently going unpooled. Reuse within a query is unaffected, since it holds
     * no connection past the query that opened it.
     */
    public static DriverConnectionFactory withoutPooling(String connectorName, Driver driver, BaseJdbcConfig config)
    {
        return withoutPooling(connectorName, driver, config, basicConnectionProperties(config));
    }

    public static DriverConnectionFactory withoutPooling(String connectorName, Driver driver, BaseJdbcConfig config, Properties connectionProperties)
    {
        requireNonNull(connectorName, "connectorName is null");
        if (config.isConnectionPoolEnabled()) {
            throw new ConfigurationException(ImmutableList.of(new Message(
                    format("Catalog property connection-pool.enabled is not supported by the %s connector", connectorName))));
        }
        return new DriverConnectionFactory(driver, config.getConnectionUrl(), config, connectionProperties, false);
    }

    private DriverConnectionFactory(Driver driver, String connectionUrl, BaseJdbcConfig config, Properties connectionProperties, boolean poolingSupported)
    {
        this(
                driver,
                connectionUrl,
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties,
                poolingSupported && config.isConnectionPoolEnabled() ?
                        Optional.of(new PoolingDriverConnectionFactory(driver, connectionUrl, config)) :
                        Optional.empty(),
                config.isConnectionReuseEnabled() ? config.getConnectionReuseRetention().toMillis() : -1,
                config.getConnectionReuseMaxRetained());
    }

    public static Properties basicConnectionProperties(BaseJdbcConfig config)
    {
        Properties connectionProperties = new Properties();
        if (config.getConnectionUser() != null) {
            connectionProperties.setProperty("user", config.getConnectionUser());
        }
        if (config.getConnectionPassword() != null) {
            connectionProperties.setProperty("password", config.getConnectionPassword());
        }
        return connectionProperties;
    }

    public DriverConnectionFactory(Driver driver, String connectionUrl, Optional<String> userCredentialName, Optional<String> passwordCredentialName, Properties connectionProperties)
    {
        this(driver, connectionUrl, userCredentialName, passwordCredentialName, connectionProperties, Optional.empty(), -1, 0);
    }

    private DriverConnectionFactory(
            Driver driver,
            String connectionUrl,
            Optional<String> userCredentialName,
            Optional<String> passwordCredentialName,
            Properties connectionProperties,
            Optional<PoolingDriverConnectionFactory> pool,
            long reuseRetentionMillis,
            int reuseMaxRetained)
    {
        this.pool = requireNonNull(pool, "pool is null");
        // Reuse sits above the pool when both are on, so a nested call is served from the lease and
        // the pool sees one borrow per operation rather than one per call.
        this.leases = reuseRetentionMillis >= 0 ?
                Optional.of(new ConnectionLeases(properties -> openNewConnection(properties, true), reuseRetentionMillis, reuseMaxRetained)) :
                Optional.empty();
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "connectionProperties is null"));
        this.userCredentialName = requireNonNull(userCredentialName, "userCredentialName is null");
        this.passwordCredentialName = requireNonNull(passwordCredentialName, "passwordCredentialName is null");
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        // A split or a page sink holds its connection while other operators run, so it never waits
        // for the pool; see PoolingDriverConnectionFactory.
        return openNewConnection(connectionPropertiesFor(identity), false);
    }

    @Override
    public Connection openConnection(ConnectorSession session, JdbcIdentity identity)
            throws SQLException
    {
        Properties properties = connectionPropertiesFor(identity);
        if (leases.isPresent()) {
            return leases.get().openConnection(session.getQueryId(), identity, properties);
        }
        return openNewConnection(properties, true);
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        leases.ifPresent(value -> value.cleanupQuery(session.getQueryId()));
    }

    private Properties connectionPropertiesFor(JdbcIdentity identity)
    {
        Properties updatedConnectionProperties;
        if (userCredentialName.isPresent() || passwordCredentialName.isPresent()) {
            updatedConnectionProperties = new Properties();
            updatedConnectionProperties.putAll(connectionProperties);
            userCredentialName.ifPresent(credentialName -> setConnectionProperty(updatedConnectionProperties, identity.getExtraCredentials(), credentialName, "user"));
            passwordCredentialName.ifPresent(credentialName -> setConnectionProperty(updatedConnectionProperties, identity.getExtraCredentials(), credentialName, "password"));
        }
        else {
            updatedConnectionProperties = connectionProperties;
        }

        return updatedConnectionProperties;
    }

    private Connection openNewConnection(Properties connectionProperties, boolean waitForPool)
            throws SQLException
    {
        if (pool.isPresent()) {
            return pool.get().openConnection(connectionProperties, waitForPool);
        }

        Connection connection = driver.connect(connectionUrl, connectionProperties);
        checkState(connection != null, "Driver returned null connection");
        return connection;
    }

    @Override
    public void close()
    {
        leases.ifPresent(ConnectionLeases::close);
        pool.ifPresent(PoolingDriverConnectionFactory::close);
    }

    private static void setConnectionProperty(Properties connectionProperties, Map<String, String> extraCredentials, String credentialName, String propertyName)
    {
        String value = extraCredentials.get(credentialName);
        if (value != null) {
            connectionProperties.setProperty(propertyName, value);
        }
    }
}
