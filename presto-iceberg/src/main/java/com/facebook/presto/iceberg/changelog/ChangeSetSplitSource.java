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
package com.facebook.presto.iceberg.changelog;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergSplit;
import com.facebook.presto.iceberg.PartitionData;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.SnapshotUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.hive.HiveCommonSessionProperties.getAffinitySchedulingFileSectionSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getNodeSelectionStrategy;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getDataSequenceNumber;
import static com.facebook.presto.iceberg.IcebergUtil.getFirstRowId;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionKeys;
import static com.facebook.presto.iceberg.IcebergUtil.partitionDataFromStructLike;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.limit;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Plans the rows a snapshot range removed with a delete file.
 * <p>
 * Iceberg's incremental changelog scan cannot do this. It refuses any range whose snapshots carry
 * delete manifests -- "Delete files are currently not supported in changelog scans" -- and while
 * its API declares a task type for the case, nothing ever constructs one. So the splits are
 * planned here instead.
 * <p>
 * The rows are recoverable because a delete file does not erase anything: it records which
 * positions of a data file are no longer live, and the data file keeps them. Reading a data file
 * through a delete file, keeping the marked positions instead of discarding them, therefore
 * reproduces rows that exist nowhere in the current table.
 * <p>
 * One split per delete file a snapshot added, over the single data file that delete file names.
 * The delete files already in effect at that point are attached in the ordinary, excluding sense,
 * so that a row some earlier snapshot had already removed is not reported again here -- it was
 * reported when it went. This is the shape Iceberg's own contract for the case specifies: the rows
 * removed are those the newly added delete files mark, minus those the delete files already in
 * effect had removed.
 */
public class ChangeSetSplitSource
        implements ConnectorSplitSource
{
    private final Iterator<IcebergSplit> iterator;
    private final int splitCount;

    public ChangeSetSplitSource(
            ConnectorSession session,
            TypeManager typeManager,
            Table table,
            long fromSnapshotId,
            long toSnapshotId)
    {
        requireNonNull(session, "session is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(table, "table is null");

        List<IcebergSplit> splits = buildSplits(
                table,
                fromSnapshotId,
                toSnapshotId,
                getColumns(table.schema(), table.spec(), typeManager),
                getNodeSelectionStrategy(session),
                getAffinitySchedulingFileSectionSize(session).toBytes());
        this.splitCount = splits.size();
        this.iterator = splits.iterator();
    }

    private static List<IcebergSplit> buildSplits(
            Table table,
            long fromSnapshotId,
            long toSnapshotId,
            List<IcebergColumnHandle> columnHandles,
            NodeSelectionStrategy nodeSelectionStrategy,
            long affinitySchedulingSectionSize)
    {
        if (toSnapshotId == 0 || fromSnapshotId == toSnapshotId) {
            return ImmutableList.of();
        }

        // Oldest first, so ordinals ascend in commit order the way a changelog numbers its tasks.
        List<Snapshot> snapshots = new ArrayList<>(ImmutableList.copyOf(
                SnapshotUtil.ancestorsBetween(table, toSnapshotId, fromSnapshotId)));
        Collections.reverse(snapshots);

        List<IcebergSplit> splits = new ArrayList<>();
        long ordinal = 0;
        for (Snapshot snapshot : snapshots) {
            // A compaction rewrites files without changing a row, and V3 row lineage survives the
            // rewrite, so it contributes nothing. Reporting its removals and additions would make
            // every row of a compacted table look touched.
            if (DataOperations.REPLACE.equals(snapshot.operation())) {
                ordinal++;
                continue;
            }

            List<DeleteFile> added = ImmutableList.copyOf(snapshot.addedDeleteFiles(table.io()));
            List<DataFile> addedData = ImmutableList.copyOf(snapshot.addedDataFiles(table.io()));
            List<DataFile> removedData = ImmutableList.copyOf(snapshot.removedDataFiles(table.io()));
            if (added.isEmpty() && addedData.isEmpty() && removedData.isEmpty()) {
                ordinal++;
                continue;
            }

            // The data files as they stood at this snapshot, which is where a delete file's target
            // is still live.
            Map<String, FileScanTask> dataFiles = dataFilesAt(table, snapshot.snapshotId());
            // And as they stood immediately before it, which is what says which rows had already
            // gone. That cannot be read from this snapshot: replacing a deletion vector removes
            // the old one in the same commit, so by the time this snapshot exists the vector whose
            // rows were already reported is no longer attached to anything.
            Map<String, FileScanTask> dataFilesBefore = snapshot.parentId() == null
                    ? ImmutableMap.of()
                    : dataFilesAt(table, snapshot.parentId());

            // A data file this snapshot added contributes all of its rows as insertions. Rows a
            // later snapshot in the range then removes are reported as removals at that snapshot,
            // so both events appear, in order, as they did.
            for (DataFile addedFile : addedData) {
                FileScanTask task = dataFiles.get(addedFile.path().toString());
                if (task == null) {
                    continue;
                }
                splits.add(toSplit(
                        task,
                        ImmutableList.of(),
                        ImmutableList.of(),
                        ChangelogOperation.INSERT,
                        ordinal,
                        snapshot.snapshotId(),
                        columnHandles,
                        nodeSelectionStrategy,
                        affinitySchedulingSectionSize));
            }

            // A data file dropped outright takes its live rows with it, which is how a delete that
            // lines up with whole files is recorded -- there is no delete file to read. Its own
            // deletes still apply, so rows already gone before this snapshot are not reported a
            // second time. The file's metadata comes from the snapshot before this one, where it
            // was still live.
            if (!removedData.isEmpty()) {
                for (DataFile removedFile : removedData) {
                    FileScanTask task = dataFilesBefore.get(removedFile.path().toString());
                    if (task == null) {
                        continue;
                    }
                    splits.add(toSplit(
                            task,
                            task.deletes().stream()
                                    .map(com.facebook.presto.iceberg.delete.DeleteFile::fromIceberg)
                                    .collect(toImmutableList()),
                            ImmutableList.of(),
                            ChangelogOperation.DELETE,
                            ordinal,
                            snapshot.snapshotId(),
                            columnHandles,
                            nodeSelectionStrategy,
                            affinitySchedulingSectionSize));
                }
            }
            for (DeleteFile delete : added) {
                if (delete.content() != FileContent.POSITION_DELETES) {
                    // An equality delete names values rather than positions, so which rows it
                    // removed cannot be located without evaluating it against the data as it
                    // then stood.
                    throw new PrestoException(NOT_SUPPORTED, format(
                            "Recovering the rows removed by %s is not supported", delete.content()));
                }
                String referenced = delete.referencedDataFile();
                if (referenced == null) {
                    // A position delete file covering several data files does not record which,
                    // and pairing it with every candidate would read data files it never mentions.
                    throw new PrestoException(NOT_SUPPORTED,
                            "Recovering removed rows requires a delete file scoped to a single data file");
                }
                FileScanTask task = dataFiles.get(referenced);
                if (task == null) {
                    // The delete names a data file that is not live at its own snapshot, which
                    // means a later snapshot in this range rewrote or dropped it. Those rows are
                    // accounted for by that snapshot instead.
                    continue;
                }

                // Whatever was already deleting rows of this data file the moment before, taken
                // from the previous snapshot. Those rows left at that snapshot and were reported
                // there, so excluding them here is what keeps a removal from being reported twice.
                FileScanTask before = dataFilesBefore.get(referenced);
                List<com.facebook.presto.iceberg.delete.DeleteFile> existing = before == null
                        ? ImmutableList.of()
                        : before.deletes().stream()
                                .map(com.facebook.presto.iceberg.delete.DeleteFile::fromIceberg)
                                .collect(toImmutableList());

                splits.add(toSplit(
                        task,
                        existing,
                        ImmutableList.of(com.facebook.presto.iceberg.delete.DeleteFile.fromIceberg(delete)),
                        ChangelogOperation.DELETE,
                        ordinal,
                        snapshot.snapshotId(),
                        columnHandles,
                        nodeSelectionStrategy,
                        affinitySchedulingSectionSize));
            }
            ordinal++;
        }
        return ImmutableList.copyOf(splits);
    }

    private static Map<String, FileScanTask> dataFilesAt(Table table, long snapshotId)
    {
        Map<String, FileScanTask> tasks = new LinkedHashMap<>();
        try (CloseableIterable<FileScanTask> planned = table.newScan().useSnapshot(snapshotId).planFiles()) {
            for (FileScanTask task : planned) {
                tasks.put(task.file().path().toString(), task);
            }
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_CANNOT_OPEN_SPLIT, format("Cannot enumerate the data files of snapshot %s", snapshotId), e);
        }
        return tasks;
    }

    private static IcebergSplit toSplit(
            FileScanTask task,
            List<com.facebook.presto.iceberg.delete.DeleteFile> deletes,
            List<com.facebook.presto.iceberg.delete.DeleteFile> retainedDeletes,
            ChangelogOperation operation,
            long ordinal,
            long snapshotId,
            List<IcebergColumnHandle> columnHandles,
            NodeSelectionStrategy nodeSelectionStrategy,
            long affinitySchedulingSectionSize)
    {
        Optional<PartitionData> partitionData = partitionDataFromStructLike(task.spec(), task.file().partition());
        // The whole data file is one split. Delete positions are relative to the file, so a split
        // covering part of it would need those positions rebased onto the fragment. The path must
        // be the delete file's referenced path exactly, since the reader checks the two agree.
        return new IcebergSplit(
                task.file().path().toString(),
                0,
                task.file().fileSizeInBytes(),
                FileFormat.fromIcebergFileFormat(task.file().format()),
                ImmutableList.of(),
                getPartitionKeys(task.spec(), task.file().partition()),
                PartitionSpecParser.toJson(task.spec()),
                partitionData.map(PartitionData::toJson),
                nodeSelectionStrategy,
                SplitWeight.standard(),
                deletes,
                Optional.of(new ChangelogSplitInfo(
                        operation,
                        ordinal,
                        snapshotId,
                        columnHandles)),
                getDataSequenceNumber(task.file()),
                getFirstRowId(task.file()),
                affinitySchedulingSectionSize,
                retainedDeletes);
    }

    @Override
    public boolean isFinished()
    {
        return !iterator.hasNext();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        List<ConnectorSplit> batch = new ArrayList<>();
        Iterator<IcebergSplit> batchIterator = limit(iterator, maxSize);
        while (batchIterator.hasNext()) {
            batch.add(batchIterator.next());
        }
        return completedFuture(new ConnectorSplitBatch(batch, isFinished()));
    }

    @Override
    public void close() {}

    /**
     * How many splits the range produces, for callers sizing the work before running it.
     */
    public int getSplitCount()
    {
        return splitCount;
    }
}
