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
package com.facebook.presto.connector.system;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ChangeKindPageSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.table.AbstractConnectorTableFunction;
import com.facebook.presto.spi.function.table.Argument;
import com.facebook.presto.spi.function.table.ConnectorTableFunction;
import com.facebook.presto.spi.function.table.ConnectorTableFunctionHandle;
import com.facebook.presto.spi.function.table.Descriptor;
import com.facebook.presto.spi.function.table.GenericTableReturnTypeSpecification;
import com.facebook.presto.spi.function.table.ScalarArgument;
import com.facebook.presto.spi.function.table.ScalarArgumentSpecification;
import com.facebook.presto.spi.function.table.TableFunctionAnalysis;
import com.facebook.presto.spi.function.table.TableFunctionProcessorProvider;
import com.facebook.presto.spi.function.table.TableFunctionProcessorState;
import com.facebook.presto.spi.function.table.TableFunctionSplitProcessor;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.type.ChangeKindEnumType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import jakarta.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.connector.ConnectorTableVersion.VersionOperator.EQUAL;
import static com.facebook.presto.spi.connector.ConnectorTableVersion.VersionType.VERSION;
import static com.facebook.presto.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static com.facebook.presto.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static com.facebook.presto.spi.function.table.TableFunctionProcessorState.Processed.usedInputAndProduced;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static java.util.Objects.requireNonNull;

/**
 * Coordinator-only access to a connector's row-level change set.
 *
 * <p>The table argument is a fully qualified {@code catalog.schema.table} name,
 * followed by the recorded and target snapshot versions. This is an engine
 * integration primitive; it deliberately does not distribute a connector page
 * source to workers.</p>
 */
public class Changes
        extends AbstractConnectorTableFunction
        implements ConnectorTableFunction
{
    public static final String NAME = "changes";
    private static final String TABLE_ARGUMENT = "TABLE";
    private static final String FROM_ARGUMENT = "FROM_VERSION";
    private static final String TO_ARGUMENT = "TO_VERSION";
    private static final String INCLUDE_ROW_ID_ARGUMENT = "INCLUDE_ROW_ID";

    private final Metadata metadata;
    private final InternalNodeManager nodeManager;
    private final AccessControl accessControl;

    @Inject
    public Changes(Metadata metadata, InternalNodeManager nodeManager, AccessControl accessControl)
    {
        super(
                "builtin",
                NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder().name(TABLE_ARGUMENT).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(FROM_ARGUMENT).type(BigintType.BIGINT).build(),
                        ScalarArgumentSpecification.builder().name(TO_ARGUMENT).type(BigintType.BIGINT).build(),
                        ScalarArgumentSpecification.builder().name(INCLUDE_ROW_ID_ARGUMENT).type(BooleanType.BOOLEAN).defaultValue(false).build()),
                GenericTableReturnTypeSpecification.GENERIC_TABLE);
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
    {
        String tableName = requireVarcharArgument(arguments, TABLE_ARGUMENT);
        long fromVersion = requireBigintArgument(arguments, FROM_ARGUMENT);
        long toVersion = requireBigintArgument(arguments, TO_ARGUMENT);
        boolean includeRowId = requireBooleanArgument(arguments, INCLUDE_ROW_ID_ARGUMENT);
        if (fromVersion > toVersion) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "FROM_VERSION must not be greater than TO_VERSION");
        }

        Session engineSession = SystemConnectorSessionUtil.toSession(transaction, session);
        TableHandle tableHandle;
        try {
            tableHandle = metadata.getMetadataResolver(engineSession)
                    .getTableHandle(QualifiedObjectName.valueOf(tableName))
                    .orElseThrow(() -> new PrestoException(INVALID_FUNCTION_ARGUMENT, "Table does not exist: " + tableName));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "TABLE must be a fully qualified catalog.schema.table name: " + tableName, e);
        }

        List<ColumnMetadata> tableColumns = metadata.getMetadataResolver(engineSession).getColumns(tableHandle);
        accessControl.checkCanSelectFromColumns(
                engineSession.getRequiredTransactionId(),
                engineSession.getIdentity(),
                engineSession.getAccessControlContext(),
                QualifiedObjectName.valueOf(tableName),
                tableColumns.stream()
                        .map(column -> new Subfield(column.getName(), ImmutableList.of()))
                        .collect(ImmutableSet.toImmutableSet()));
        Map<String, ColumnHandle> columnHandles = metadata.getMetadataResolver(engineSession).getColumnHandles(tableHandle);
        List<ColumnHandle> projectedColumns = new ArrayList<>();
        List<Descriptor.Field> outputColumns = new ArrayList<>();
        for (ColumnMetadata column : tableColumns) {
            if (column.getName().equalsIgnoreCase("change_kind")) {
                throw new PrestoException(NOT_SUPPORTED, "TABLE contains a column named change_kind, which is reserved by system.changes");
            }
            ColumnHandle columnHandle = columnHandles.get(column.getName());
            if (columnHandle == null) {
                throw new PrestoException(NOT_SUPPORTED, "TABLE column is not readable by system.changes: " + column.getName());
            }
            projectedColumns.add(columnHandle);
            outputColumns.add(new Descriptor.Field(column.getName(), Optional.of(column.getType())));
        }
        if (includeRowId) {
            ColumnHandle rowIdHandle = columnHandles.get("$row_id");
            if (rowIdHandle == null) {
                throw new PrestoException(NOT_SUPPORTED, "TABLE does not expose a row lineage column");
            }
            accessControl.checkCanSelectFromColumns(
                    engineSession.getRequiredTransactionId(),
                    engineSession.getIdentity(),
                    engineSession.getAccessControlContext(),
                    QualifiedObjectName.valueOf(tableName),
                    ImmutableSet.of(new Subfield("$row_id", ImmutableList.of())));
            projectedColumns.add(rowIdHandle);
            outputColumns.add(new Descriptor.Field("$row_id", Optional.of(metadata.getColumnMetadata(engineSession, tableHandle, rowIdHandle).getType())));
        }
        outputColumns.add(new Descriptor.Field("change_kind", Optional.of(ChangeKindEnumType.CHANGE_KIND)));

        ConnectorTableVersion from = new ConnectorTableVersion(VERSION, EQUAL, BigintType.BIGINT, fromVersion);
        ConnectorTableVersion to = new ConnectorTableVersion(VERSION, EQUAL, BigintType.BIGINT, toVersion);
        return TableFunctionAnalysis.builder()
                .returnedType(new Descriptor(outputColumns))
                .handle(new ChangesFunctionHandle(
                        tableHandle,
                        from,
                        to,
                        projectedColumns,
                        new ChangesSession(
                                ((GlobalSystemTransactionHandle) transaction).getTransactionId(),
                                session.getQueryId(),
                                session.getIdentity().getUser(),
                                session.getSqlFunctionProperties().getTimeZoneKey().getId(),
                                session.getLocale().toLanguageTag(),
                                session.getStartTime())))
                .build();
    }

    public ConnectorSplitSource getSplitSource(ChangesFunctionHandle handle)
    {
        return new FixedSplitSource(ImmutableList.of(new ChangesFunctionSplit(nodeManager.getCurrentNode().getHostAndPort())));
    }

    public TableFunctionProcessorProvider getProcessorProvider()
    {
        return new TableFunctionProcessorProvider()
        {
            @Override
            public TableFunctionSplitProcessor getSplitProcessor(ConnectorTableFunctionHandle handle)
            {
                return new ChangesSplitProcessor(metadata, (ChangesFunctionHandle) handle);
            }
        };
    }

    private static String requireVarcharArgument(Map<String, Argument> arguments, String name)
    {
        Object value = requireScalarArgument(arguments, name);
        if (value == null) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, name + " is null");
        }
        if (!(value instanceof Slice)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, name + " must be a varchar");
        }
        return ((Slice) value).toStringUtf8();
    }

    private static long requireBigintArgument(Map<String, Argument> arguments, String name)
    {
        Object value = requireScalarArgument(arguments, name);
        if (value == null) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, name + " is null");
        }
        if (!(value instanceof Long)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, name + " must be a bigint");
        }
        return (long) value;
    }

    private static boolean requireBooleanArgument(Map<String, Argument> arguments, String name)
    {
        Object value = requireScalarArgument(arguments, name);
        if (value == null) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, name + " is null");
        }
        if (!(value instanceof Boolean)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, name + " must be a boolean");
        }
        return (boolean) value;
    }

    private static Object requireScalarArgument(Map<String, Argument> arguments, String name)
    {
        Argument argument = arguments.get(name);
        if (!(argument instanceof ScalarArgument)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, name + " must be a scalar argument");
        }
        return ((ScalarArgument) argument).getValue();
    }

    public static class ChangesFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final TableHandle tableHandle;
        private final ConnectorTableVersion from;
        private final ConnectorTableVersion to;
        private final List<ColumnHandle> projectedColumns;
        private final ChangesSession session;

        @JsonCreator
        public ChangesFunctionHandle(
                @JsonProperty("tableHandle") TableHandle tableHandle,
                @JsonProperty("from") ConnectorTableVersion from,
                @JsonProperty("to") ConnectorTableVersion to,
                @JsonProperty("projectedColumns") List<ColumnHandle> projectedColumns,
                @JsonProperty("session") ChangesSession session)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.from = requireNonNull(from, "from is null");
            this.to = requireNonNull(to, "to is null");
            this.projectedColumns = ImmutableList.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
            this.session = requireNonNull(session, "session is null");
        }

        @JsonProperty
        public TableHandle getTableHandle()
        {
            return tableHandle;
        }

        @JsonProperty
        public ConnectorTableVersion getFrom()
        {
            return from;
        }

        @JsonProperty
        public ConnectorTableVersion getTo()
        {
            return to;
        }

        @JsonProperty
        public List<ColumnHandle> getProjectedColumns()
        {
            return projectedColumns;
        }

        @JsonProperty
        public ChangesSession getSession()
        {
            return session;
        }
    }

    public static class ChangesFunctionSplit
            implements ConnectorSplit
    {
        private final HostAddress coordinator;

        @JsonCreator
        public ChangesFunctionSplit(@JsonProperty("coordinator") HostAddress coordinator)
        {
            this.coordinator = requireNonNull(coordinator, "coordinator is null");
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return HARD_AFFINITY;
        }

        @Override
        public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
        {
            return ImmutableList.of(coordinator);
        }

        @Override
        public Object getInfo()
        {
            return this;
        }
    }

    public static class ChangesSession
    {
        private final TransactionId transactionId;
        private final String queryId;
        private final String user;
        private final String timeZoneId;
        private final String locale;
        private final long startTime;

        @JsonCreator
        public ChangesSession(
                @JsonProperty("transactionId") TransactionId transactionId,
                @JsonProperty("queryId") String queryId,
                @JsonProperty("user") String user,
                @JsonProperty("timeZoneId") String timeZoneId,
                @JsonProperty("locale") String locale,
                @JsonProperty("startTime") long startTime)
        {
            this.transactionId = requireNonNull(transactionId, "transactionId is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.user = requireNonNull(user, "user is null");
            this.timeZoneId = requireNonNull(timeZoneId, "timeZoneId is null");
            this.locale = requireNonNull(locale, "locale is null");
            this.startTime = startTime;
        }

        @JsonProperty
        public TransactionId getTransactionId()
        {
            return transactionId;
        }

        @JsonProperty
        public String getQueryId()
        {
            return queryId;
        }

        @JsonProperty
        public String getUser()
        {
            return user;
        }

        @JsonProperty
        public String getTimeZoneId()
        {
            return timeZoneId;
        }

        @JsonProperty
        public String getLocale()
        {
            return locale;
        }

        @JsonProperty
        public long getStartTime()
        {
            return startTime;
        }
    }

    private static class ChangesSplitProcessor
            implements TableFunctionSplitProcessor
    {
        private final Metadata metadata;
        private final ChangesFunctionHandle handle;
        private ChangeKindPageSource pageSource;
        private boolean splitUsed;

        private ChangesSplitProcessor(Metadata metadata, ChangesFunctionHandle handle)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.handle = requireNonNull(handle, "handle is null");
        }

        @Override
        public TableFunctionProcessorState process(ConnectorSplit split)
        {
            if (pageSource == null) {
                if (split == null) {
                    return FINISHED;
                }
                pageSource = metadata.getChangeSet(
                        toSession(handle.getSession()),
                        handle.getTableHandle(),
                        handle.getFrom(),
                        handle.getTo(),
                        handle.getProjectedColumns(),
                        TupleDomain.all());
            }

            Page page = pageSource.getNextPage();
            if (page != null) {
                if (!splitUsed) {
                    splitUsed = true;
                    return usedInputAndProduced(page);
                }
                return produced(page);
            }
            if (!pageSource.isFinished()) {
                CompletableFuture<Void> blocked = pageSource.isBlocked().thenApply(ignored -> null);
                return TableFunctionProcessorState.Blocked.blocked(blocked);
            }
            closePageSource();
            return FINISHED;
        }

        private void closePageSource()
        {
            try {
                pageSource.close();
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unable to close system.changes page source", e);
            }
        }
    }

    private static Session toSession(ChangesSession changesSession)
    {
        return Session.builder(createTestingSessionPropertyManager(new SystemSessionProperties()))
                .setQueryId(new QueryId(changesSession.getQueryId()))
                .setTransactionId(changesSession.getTransactionId())
                .setCatalog("system")
                .setSchema("runtime")
                .setIdentity(new Identity(changesSession.getUser(), Optional.empty()))
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(changesSession.getTimeZoneId()))
                .setLocale(Locale.forLanguageTag(changesSession.getLocale()))
                .setStartTime(changesSession.getStartTime())
                .build();
    }
}
