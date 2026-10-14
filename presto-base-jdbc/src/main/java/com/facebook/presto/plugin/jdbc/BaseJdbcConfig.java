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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.ConfigurationException;
import com.google.inject.spi.Message;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Set;

import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Base configuration class for JDBC connectors.
 *
 * This class is provided for convenience and contains common configuration properties
 * that many JDBC connectors may need. However, core JDBC functionality should not
 * depend on this class, as JDBC connectors may choose to use their own mechanisms
 * for connection management, authentication, and other configuration needs.
 *
 * Connectors are free to implement their own configuration classes and connection
 * strategies without extending or using this base configuration.
 */
public class BaseJdbcConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private String userCredentialName;
    private String passwordCredentialName;
    private boolean caseInsensitiveNameMatching;
    private Duration caseInsensitiveNameMatchingCacheTtl = new Duration(1, MINUTES);
    private Set<String> listSchemasIgnoredSchemas = ImmutableSet.of("information_schema");
    private boolean caseSensitiveNameMatchingEnabled;
    @Min(1)
    private int fetchSize = 20000;
    private boolean connectionReuseEnabled;
    private Duration connectionReuseRetention = new Duration(2, SECONDS);
    private int connectionReuseMaxRetained = 10;
    private boolean connectionPoolEnabled;
    private int connectionPoolMaxSize = 10;
    private Duration connectionPoolMaxWait = new Duration(30, SECONDS);
    private Duration connectionPoolMaxIdleTime = new Duration(5, MINUTES);
    private int connectionPoolMaxCredentialSets = 100;
    private Duration connectionPoolValidationBypassWindow = new Duration(500, MILLISECONDS);
    private int connectionPoolMaxOverflow = 10;
    private Duration connectionPoolMaxLifetime = new Duration(30, MINUTES);
    private String connectionPoolResetSql;

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("connection-url")
    public BaseJdbcConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @Nullable
    public String getConnectionUser()
    {
        return connectionUser;
    }

    @Config("connection-user")
    public BaseJdbcConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    @Nullable
    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @Config("connection-password")
    @ConfigSecuritySensitive
    public BaseJdbcConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    @Nullable
    public String getUserCredentialName()
    {
        return userCredentialName;
    }

    @Config("user-credential-name")
    public BaseJdbcConfig setUserCredentialName(String userCredentialName)
    {
        this.userCredentialName = userCredentialName;
        return this;
    }

    @Nullable
    public String getPasswordCredentialName()
    {
        return passwordCredentialName;
    }

    @Config("password-credential-name")
    public BaseJdbcConfig setPasswordCredentialName(String passwordCredentialName)
    {
        this.passwordCredentialName = passwordCredentialName;
        return this;
    }

    @Deprecated
    public boolean isCaseInsensitiveNameMatching()
    {
        return caseInsensitiveNameMatching;
    }

    @Deprecated
    @Config("case-insensitive-name-matching")
    @ConfigDescription("Deprecated: This will be removed in future releases. Use 'case-sensitive-name-matching=true' instead for mysql. " +
            "This configuration setting converts all schema/table names to lowercase. " +
            "If your source database contains names differing only by case (e.g., 'Testdb' and 'testdb'), " +
            "this setting can lead to conflicts and query failures.")
    public BaseJdbcConfig setCaseInsensitiveNameMatching(boolean caseInsensitiveNameMatching)
    {
        this.caseInsensitiveNameMatching = caseInsensitiveNameMatching;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getCaseInsensitiveNameMatchingCacheTtl()
    {
        return caseInsensitiveNameMatchingCacheTtl;
    }

    @Config("case-insensitive-name-matching.cache-ttl")
    public BaseJdbcConfig setCaseInsensitiveNameMatchingCacheTtl(Duration caseInsensitiveNameMatchingCacheTtl)
    {
        this.caseInsensitiveNameMatchingCacheTtl = caseInsensitiveNameMatchingCacheTtl;
        return this;
    }

    public Set<String> getlistSchemasIgnoredSchemas()
    {
        return listSchemasIgnoredSchemas;
    }

    @Config("list-schemas-ignored-schemas")
    public BaseJdbcConfig setlistSchemasIgnoredSchemas(String listSchemasIgnoredSchemas)
    {
        this.listSchemasIgnoredSchemas = ImmutableSet.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().split(listSchemasIgnoredSchemas.toLowerCase(ENGLISH)));
        return this;
    }

    public boolean isCaseSensitiveNameMatching()
    {
        return caseSensitiveNameMatchingEnabled;
    }

    @Config("case-sensitive-name-matching")
    @ConfigDescription("Enable case-sensitive matching of schema, table names across the connector. " +
            "When disabled, names are matched case-insensitively using lowercase normalization.")
    public BaseJdbcConfig setCaseSensitiveNameMatching(boolean caseSensitiveNameMatchingEnabled)
    {
        this.caseSensitiveNameMatchingEnabled = caseSensitiveNameMatchingEnabled;
        return this;
    }

    @PostConstruct
    public void validateConfig()
    {
        if (isCaseInsensitiveNameMatching() && isCaseSensitiveNameMatching()) {
            throw new ConfigurationException(ImmutableList.of(new Message("Only one of 'case-insensitive-name-matching=true' or 'case-sensitive-name-matching=true' can be set. " +
                    "These options are mutually exclusive.")));
        }

        if (connectionUrl == null) {
            throw new ConfigurationException(ImmutableList.of(new Message("connection-url is required but was not provided")));
        }
    }

    public int getFetchSize()
    {
        return fetchSize;
    }

    @Config("jdbc-fetch-size")
    @ConfigDescription("Number of rows to fetch from the database at a time")
    public BaseJdbcConfig setFetchSize(int fetchSize)
    {
        this.fetchSize = fetchSize;
        return this;
    }

    public boolean isConnectionPoolEnabled()
    {
        return connectionPoolEnabled;
    }

    @Config("connection-pool.enabled")
    @ConfigDescription("Reuse connections to the database through a pool instead of opening one per operation")
    public BaseJdbcConfig setConnectionPoolEnabled(boolean connectionPoolEnabled)
    {
        this.connectionPoolEnabled = connectionPoolEnabled;
        return this;
    }

    @Min(1)
    public int getConnectionPoolMaxSize()
    {
        return connectionPoolMaxSize;
    }

    @Config("connection-pool.max-size")
    @ConfigDescription("Maximum number of connections the pool holds for a single set of credentials")
    public BaseJdbcConfig setConnectionPoolMaxSize(int connectionPoolMaxSize)
    {
        this.connectionPoolMaxSize = connectionPoolMaxSize;
        return this;
    }

    @MinDuration("0s")
    public Duration getConnectionPoolMaxWait()
    {
        return connectionPoolMaxWait;
    }

    @Config("connection-pool.max-wait")
    @ConfigDescription("How long to wait for a connection from an exhausted pool before failing")
    public BaseJdbcConfig setConnectionPoolMaxWait(Duration connectionPoolMaxWait)
    {
        this.connectionPoolMaxWait = connectionPoolMaxWait;
        return this;
    }

    @MinDuration("1s")
    public Duration getConnectionPoolMaxIdleTime()
    {
        return connectionPoolMaxIdleTime;
    }

    @Config("connection-pool.max-idle-time")
    @ConfigDescription("How long an unused connection is kept in the pool before it is evicted, and a pool whose credentials go unused before it is closed")
    public BaseJdbcConfig setConnectionPoolMaxIdleTime(Duration connectionPoolMaxIdleTime)
    {
        this.connectionPoolMaxIdleTime = connectionPoolMaxIdleTime;
        return this;
    }

    @Min(1)
    public int getConnectionPoolMaxCredentialSets()
    {
        return connectionPoolMaxCredentialSets;
    }

    @Config("connection-pool.max-credential-sets")
    @ConfigDescription("Maximum number of credential sets a pool is kept for, which bounds the pools created by credential pass-through")
    public BaseJdbcConfig setConnectionPoolMaxCredentialSets(int connectionPoolMaxCredentialSets)
    {
        this.connectionPoolMaxCredentialSets = connectionPoolMaxCredentialSets;
        return this;
    }

    @MinDuration("0ms")
    public Duration getConnectionPoolValidationBypassWindow()
    {
        return connectionPoolValidationBypassWindow;
    }

    @Config("connection-pool.validation-bypass-window")
    @ConfigDescription("Hand out a pooled connection without checking it if it was last used within this window, which skips the check for most borrows under load; 0ms checks every borrow")
    public BaseJdbcConfig setConnectionPoolValidationBypassWindow(Duration connectionPoolValidationBypassWindow)
    {
        this.connectionPoolValidationBypassWindow = connectionPoolValidationBypassWindow;
        return this;
    }

    @Nullable
    public String getConnectionPoolResetSql()
    {
        return connectionPoolResetSql;
    }

    @Config("connection-pool.reset-sql")
    @ConfigDescription("Statement run before a pooled connection is handed out, to clear session state left by its previous user")
    public BaseJdbcConfig setConnectionPoolResetSql(String connectionPoolResetSql)
    {
        this.connectionPoolResetSql = connectionPoolResetSql;
        return this;
    }

    @MinDuration("0s")
    public Duration getConnectionPoolMaxLifetime()
    {
        return connectionPoolMaxLifetime;
    }

    @Config("connection-pool.max-lifetime")
    @ConfigDescription("How long a pooled connection may live before it is replaced, which bounds how long an authenticated session outlives a credential change at the database. 0 keeps connections indefinitely")
    public BaseJdbcConfig setConnectionPoolMaxLifetime(Duration connectionPoolMaxLifetime)
    {
        this.connectionPoolMaxLifetime = connectionPoolMaxLifetime;
        return this;
    }

    public boolean isConnectionReuseEnabled()
    {
        return connectionReuseEnabled;
    }

    @Config("connection-reuse.enabled")
    @ConfigDescription("Reuse a connection for the metadata calls of one query instead of opening one per call")
    public BaseJdbcConfig setConnectionReuseEnabled(boolean connectionReuseEnabled)
    {
        this.connectionReuseEnabled = connectionReuseEnabled;
        return this;
    }

    @MinDuration("0s")
    public Duration getConnectionReuseRetention()
    {
        return connectionReuseRetention;
    }

    @Config("connection-reuse.retention")
    @ConfigDescription("How long a connection is kept after a metadata call closes it, so that the next metadata call of the same query can reuse it. It is closed sooner when the query ends. 0 closes it immediately")
    public BaseJdbcConfig setConnectionReuseRetention(Duration connectionReuseRetention)
    {
        this.connectionReuseRetention = connectionReuseRetention;
        return this;
    }

    @Min(0)
    public int getConnectionReuseMaxRetained()
    {
        return connectionReuseMaxRetained;
    }

    @Config("connection-reuse.max-retained")
    @ConfigDescription("Maximum number of connections kept open after their callers close them, across all queries. The oldest is closed when another is kept")
    public BaseJdbcConfig setConnectionReuseMaxRetained(int connectionReuseMaxRetained)
    {
        this.connectionReuseMaxRetained = connectionReuseMaxRetained;
        return this;
    }

    @Min(0)
    public int getConnectionPoolMaxOverflow()
    {
        return connectionPoolMaxOverflow;
    }

    @Config("connection-pool.max-overflow")
    @ConfigDescription("Connections a node may open beyond the pool when it is exhausted, so that splits do not wait. A node opens at most max-size + max-overflow connections")
    public BaseJdbcConfig setConnectionPoolMaxOverflow(int connectionPoolMaxOverflow)
    {
        this.connectionPoolMaxOverflow = connectionPoolMaxOverflow;
        return this;
    }
}
