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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HiveTransactionHandle;
import com.facebook.presto.iceberg.changelog.ChangelogOperation;
import com.facebook.presto.iceberg.changelog.ChangelogSplitSource;
import com.facebook.presto.spi.ChangeKindPageSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.Table;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.ConnectorSplitSource.ConnectorSplitBatch;
import static com.facebook.presto.spi.SplitContext.NON_CACHEABLE;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.util.Objects.requireNonNull;

/**
 * Reads an Iceberg incremental changelog as base-table columns followed by an
 * engine-owned change-kind value. ChangelogSplitSource remains the single
 * authority for translating Iceberg changelog tasks into readable data splits.
 */
class IcebergChangeSetPageSource
        implements ChangeKindPageSource
{
    private final ChangelogSplitSource splitSource;
    private final IcebergPageSourceProvider pageSourceProvider;
    private final ConnectorSession session;
    private final IcebergTableLayoutHandle layout;
    private final List<IcebergColumnHandle> projectedColumns;
    private final RuntimeStats runtimeStats;

    private ConnectorPageSource delegate;
    private ChangelogOperation changeKind;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;
    private boolean finished;

    public IcebergChangeSetPageSource(
            ConnectorSession session,
            TypeManager typeManager,
            IcebergPageSourceProvider pageSourceProvider,
            Table table,
            IncrementalChangelogScan scan,
            IcebergTableLayoutHandle layout,
            List<IcebergColumnHandle> projectedColumns)
    {
        this.splitSource = new ChangelogSplitSource(
                requireNonNull(session, "session is null"),
                requireNonNull(typeManager, "typeManager is null"),
                requireNonNull(table, "table is null"),
                requireNonNull(scan, "scan is null"));
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.session = session;
        this.layout = requireNonNull(layout, "layout is null");
        this.projectedColumns = ImmutableList.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        this.runtimeStats = new RuntimeStats();
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes + (delegate == null ? 0 : delegate.getCompletedBytes());
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions + (delegate == null ? 0 : delegate.getCompletedPositions());
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos + (delegate == null ? 0 : delegate.getReadTimeNanos());
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        while (!finished) {
            if (delegate == null && !advanceSplit()) {
                return null;
            }

            Page page = delegate.getNextPage();
            if (page != null) {
                return appendChangeKind(page, changeKind);
            }
            if (!delegate.isFinished()) {
                return null;
            }
            closeDelegate();
        }
        return null;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate == null ? 0 : delegate.getSystemMemoryUsage();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return delegate == null ? NOT_BLOCKED : delegate.isBlocked();
    }

    @Override
    public RuntimeStats getRuntimeStats()
    {
        return runtimeStats;
    }

    @Override
    public void close()
            throws IOException
    {
        IOException failure = null;
        try {
            closeDelegate();
        }
        catch (PrestoException e) {
            failure = new IOException(e);
        }
        finally {
            splitSource.close();
            finished = true;
        }
        if (failure != null) {
            throw failure;
        }
    }

    private boolean advanceSplit()
    {
        ConnectorSplitBatch batch = splitSource.getNextBatch(NOT_PARTITIONED, 1).join();
        if (batch.getSplits().isEmpty()) {
            finished = batch.isNoMoreSplits();
            return false;
        }

        ConnectorSplit connectorSplit = batch.getSplits().get(0);
        IcebergSplit changelogSplit = (IcebergSplit) connectorSplit;
        changeKind = changelogSplit.getChangelogSplitInfo()
                .orElseThrow(() -> new PrestoException(GENERIC_INTERNAL_ERROR, "Changelog split is missing change kind"))
                .getOperation();
        delegate = pageSourceProvider.createPageSource(
                new HiveTransactionHandle(),
                session,
                withoutChangelogInfo(changelogSplit),
                layout,
                (List<ColumnHandle>) (List<?>) projectedColumns,
                NON_CACHEABLE,
                runtimeStats);
        return true;
    }

    private void closeDelegate()
    {
        if (delegate == null) {
            return;
        }
        completedBytes += delegate.getCompletedBytes();
        completedPositions += delegate.getCompletedPositions();
        readTimeNanos += delegate.getReadTimeNanos();
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, e);
        }
        finally {
            delegate = null;
        }
    }

    private static Page appendChangeKind(Page page, ChangelogOperation changeKind)
    {
        Block[] blocks = new Block[page.getChannelCount() + 1];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            blocks[channel] = page.getBlock(channel);
        }
        blocks[page.getChannelCount()] = RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice(changeKind.name()), page.getPositionCount());
        return new Page(page.getPositionCount(), blocks);
    }

    private static IcebergSplit withoutChangelogInfo(IcebergSplit split)
    {
        return new IcebergSplit(
                split.getPath(),
                split.getStart(),
                split.getLength(),
                split.getFileFormat(),
                split.getAddresses(),
                split.getPartitionKeys(),
                split.getPartitionSpecAsJson(),
                split.getPartitionDataJson(),
                split.getNodeSelectionStrategy(),
                split.getSplitWeight(),
                split.getDeletes(),
                java.util.Optional.empty(),
                split.getDataSequenceNumber(),
                split.getFirstRowId(),
                split.getAffinitySchedulingFileSectionSize());
    }
}
