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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.delete.DeleteFile;
import com.facebook.presto.iceberg.delete.IcebergDeletePageSink;
import com.facebook.presto.iceberg.delete.IcebergDeletionVectorPageSink;
import com.facebook.presto.spi.ConnectorMergeSink;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.MergePage;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.LocationProvider;
import org.roaringbitmap.longlong.ImmutableLongBitmapDataProvider;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.iceberg.IcebergUtil.MIN_FORMAT_VERSION_FOR_DELETION_VECTORS;
import static com.facebook.presto.iceberg.IcebergUtil.readDeletionVectorBlob;
import static com.facebook.presto.iceberg.delete.DeletionVectors.applyDeletionVector;
import static com.facebook.presto.plugin.base.util.Closables.closeAllSuppress;
import static com.facebook.presto.spi.connector.MergePage.createDeleteAndInsertPages;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

public class IcebergMergeSink
        implements ConnectorMergeSink
{
    private final LocationProvider locationProvider;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final FileFormat fileFormat;
    private final Map<Integer, PartitionSpec> partitionsSpecs;
    private final ConnectorPageSink insertPageSink;
    private final int columnCount;
    private final int formatVersion;
    private final Map<String, DeleteFile> deletionVectorsInEffect;
    private final Map<Slice, FileDeletion> fileDeletions = new HashMap<>();

    public IcebergMergeSink(
            LocationProvider locationProvider,
            IcebergFileWriterFactory fileWriterFactory,
            HdfsEnvironment hdfsEnvironment,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            FileFormat fileFormat,
            Map<Integer, PartitionSpec> partitionsSpecs,
            ConnectorPageSink insertPageSink,
            int columnCount,
            int formatVersion,
            Map<String, DeleteFile> deletionVectorsInEffect)
    {
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.session = requireNonNull(session, "session is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.partitionsSpecs = requireNonNull(partitionsSpecs, "partitionsSpecs is null");
        this.insertPageSink = requireNonNull(insertPageSink, "insertPageSink is null");
        this.columnCount = columnCount;
        this.formatVersion = formatVersion;
        this.deletionVectorsInEffect = requireNonNull(deletionVectorsInEffect, "deletionVectorsInEffect is null");
    }

    /**
     * @param page It has N + 2 channels/blocks, where N is the number of columns in the source table. <br>
     *             1: Source table column 1.<br>
     *             2: Source table column 2.<br>
     *             N: Source table column N.<br>
     *             N + 1: Operation: INSERT(1), DELETE(2), UPDATE(3). More info: {@link ConnectorMergeSink}<br>
     *             N + 2: Target Table Row ID (_file:varchar, _pos:bigint, partition_spec_id:integer, partition_data:varchar).
     */
    @Override
    public void storeMergedRows(Page page)
    {
        MergePage mergePage = createDeleteAndInsertPages(page, columnCount);

        mergePage.getInsertionsPage().ifPresent(insertPageSink::appendPage);

        mergePage.getDeletionsPage().ifPresent(deletions -> {
            ColumnarRow rowIdRow = toColumnarRow(deletions.getBlock(deletions.getChannelCount() - 1));

            for (int position = 0; position < rowIdRow.getPositionCount(); position++) {
                Slice filePath = VarcharType.VARCHAR.getSlice(rowIdRow.getField(0), position);
                long rowPosition = BIGINT.getLong(rowIdRow.getField(1), position);

                int index = position;
                FileDeletion deletion = fileDeletions.computeIfAbsent(filePath, ignored -> {
                    int partitionSpecId = toIntExact(INTEGER.getLong(rowIdRow.getField(2), index));
                    String partitionData = VarcharType.VARCHAR.getSlice(rowIdRow.getField(3), index).toStringUtf8();
                    return new FileDeletion(partitionSpecId, partitionData);
                });

                deletion.rowsToDelete().addLong(rowPosition);
            }
        });
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return insertPageSink.finish().thenCompose(insertFragments -> {
            List<Slice> fragments = new ArrayList<>(insertFragments);

            try {
                fileDeletions.forEach((dataFilePath, deletion) -> {
                    ConnectorPageSink sink = createPositionDeletePageSink(
                            dataFilePath.toStringUtf8(),
                            partitionsSpecs.get(deletion.partitionSpecId()),
                            deletion.partitionDataJson());
                    fragments.addAll(writePositionDeletes(sink, deletion.rowsToDelete()));
                });
                return completedFuture(fragments);
            }
            catch (Exception e) {
                return failedFuture(e);
            }
        });
    }

    @Override
    public void abort()
    {
        insertPageSink.abort();
    }

    /**
     * From format version 3 Iceberg requires a row-level delete to be recorded as a deletion
     * vector and rejects the commit outright otherwise ("Must use DVs for position deletes in
     * V3"), so the choice of sink is not a preference.
     *
     * <p>A vector replaces the one already masking this data file rather than adding to it, so the
     * new one is seeded with the positions the old one held. Those positions are not otherwise
     * recoverable here: the read side applied the old vector before the merge saw the rows, so the
     * rows it hid never reached {@code storeMergedRows} and are absent from
     * {@code rowsToDelete}. Dropping them would resurrect rows an earlier operation deleted.
     */
    private ConnectorPageSink createPositionDeletePageSink(String dataFilePath, PartitionSpec partitionSpec, String partitionDataJson)
    {
        if (formatVersion >= MIN_FORMAT_VERSION_FOR_DELETION_VECTORS) {
            return new IcebergDeletionVectorPageSink(
                    partitionSpec,
                    Optional.of(partitionDataJson),
                    locationProvider,
                    hdfsEnvironment,
                    new HdfsContext(session),
                    jsonCodec,
                    session,
                    dataFilePath,
                    alreadyDeletedPositions(dataFilePath));
        }
        return new IcebergDeletePageSink(
                partitionSpec,
                Optional.of(partitionDataJson),
                locationProvider,
                fileWriterFactory,
                hdfsEnvironment,
                new HdfsContext(session),
                jsonCodec,
                session,
                dataFilePath,
                fileFormat);
    }

    private Roaring64Bitmap alreadyDeletedPositions(String dataFilePath)
    {
        Roaring64Bitmap positions = new Roaring64Bitmap();
        DeleteFile existing = deletionVectorsInEffect.get(dataFilePath);
        if (existing != null) {
            applyDeletionVector(readDeletionVectorBlob(hdfsEnvironment, session, existing), existing, positions);
        }
        return positions;
    }

    private static Collection<Slice> writePositionDeletes(ConnectorPageSink sink, ImmutableLongBitmapDataProvider rowsToDelete)
    {
        try {
            return doWritePositionDeletes(sink, rowsToDelete);
        }
        catch (Throwable t) {
            closeAllSuppress(t, sink::abort);
            throw t;
        }
    }

    private static Collection<Slice> doWritePositionDeletes(ConnectorPageSink sink, ImmutableLongBitmapDataProvider rowsToDelete)
    {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT));

        rowsToDelete.forEach(rowPosition -> {
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), rowPosition);
            pageBuilder.declarePosition();
            if (pageBuilder.isFull()) {
                sink.appendPage(pageBuilder.build());
                pageBuilder.reset();
            }
        });

        if (!pageBuilder.isEmpty()) {
            sink.appendPage(pageBuilder.build());
        }

        return sink.finish().join();
    }

    private static class FileDeletion
    {
        private final int partitionSpecId;
        private final String partitionDataJson;
        private final LongBitmapDataProvider rowsToDelete = new Roaring64Bitmap();

        public FileDeletion(int partitionSpecId, String partitionDataJson)
        {
            this.partitionSpecId = partitionSpecId;
            this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
        }

        public int partitionSpecId()
        {
            return partitionSpecId;
        }

        public String partitionDataJson()
        {
            return partitionDataJson;
        }

        public LongBitmapDataProvider rowsToDelete()
        {
            return rowsToDelete;
        }
    }
}
