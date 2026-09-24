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
package com.facebook.presto.iceberg.delete;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.CommitTaskData;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.iceberg.MetricsWrapper;
import com.facebook.presto.iceberg.PartitionData;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.iceberg.FileContent.POSITION_DELETES;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergUtil.partitionDataFromJson;
import static com.facebook.presto.iceberg.delete.IcebergDeletionVectorWriterFactory.createPuffinOutputFileFactory;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Records a row-level delete on an Iceberg V3 table as a deletion vector.
 * <p>
 * From V3 on this is the only legal way to delete rows without rewriting their data file:
 * Iceberg refuses a commit carrying a V2-style positional delete file against a V3 table
 * ("Must use DVs for position deletes in V3"). The vector is a Roaring bitmap of deleted
 * positions, written as one blob of a Puffin file by Iceberg's own writer -- which also produces
 * the spec's framing and blob properties, so this sink does not hand-assemble either.
 * <p>
 * Positions are accumulated in memory and written once, in {@link #finish()}. A bitmap is the
 * point of the format: it is far smaller than a row per deleted position, and the deletes for one
 * data file have to land in one blob regardless, since a vector replaces its predecessor rather
 * than adding to it.
 */
public class IcebergDeletionVectorPageSink
        implements ConnectorPageSink
{
    private final PartitionSpec partitionSpec;
    private final Optional<PartitionData> partitionData;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final String dataFile;
    private final LocationProvider locationProvider;

    private final Roaring64Bitmap deletedPositions = new Roaring64Bitmap();

    /**
     * @param alreadyDeleted the positions a vector already in effect for this data file marks. A
     *         vector supersedes its predecessor rather than adding to it -- Iceberg permits only
     *         one per data file and refuses a second -- so the vector written here has to carry
     *         those positions forward, or the earlier deletions come back.
     */
    public IcebergDeletionVectorPageSink(
            PartitionSpec partitionSpec,
            Optional<String> partitionDataAsJson,
            LocationProvider locationProvider,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            String dataFile,
            Roaring64Bitmap alreadyDeleted)
    {
        this(partitionSpec, partitionDataAsJson, locationProvider, hdfsEnvironment, hdfsContext,
                jsonCodec, session, dataFile);
        deletedPositions.or(requireNonNull(alreadyDeleted, "alreadyDeleted is null"));
    }

    public IcebergDeletionVectorPageSink(
            PartitionSpec partitionSpec,
            Optional<String> partitionDataAsJson,
            LocationProvider locationProvider,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            String dataFile)
    {
        this.partitionSpec = requireNonNull(partitionSpec, "partitionSpec is null");
        this.partitionData = partitionDataFromJson(partitionSpec, partitionDataAsJson);
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        requireNonNull(session, "session is null");
        this.dataFile = requireNonNull(dataFile, "dataFile is null");
    }

    @Override
    public long getCompletedBytes()
    {
        // Nothing is written until finish(), so no bytes have reached storage before then.
        return 0;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return deletedPositions.getLongSizeInBytes();
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getChannelCount() != 1) {
            throw new PrestoException(ICEBERG_BAD_DATA,
                    "Expected a single channel of row positions to delete, got " + page.getChannelCount());
        }

        Block positions = page.getBlock(0);
        for (int position = 0; position < positions.getPositionCount(); position++) {
            deletedPositions.addLong(BIGINT.getLong(positions, position));
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (deletedPositions.isEmpty()) {
            // Nothing was deleted, so there is no vector to write. Writing an empty one would
            // attach a delete file that removes nothing to every commit.
            return completedFuture(ImmutableList.of());
        }

        OutputFileFactory fileFactory = createPuffinOutputFileFactory(
                hdfsEnvironment,
                hdfsContext,
                locationProvider,
                partitionSpec,
                UUID.randomUUID().toString(),
                0,
                0L);

        DeleteWriteResult result;
        // The writer holds an open Puffin stream, so it is closed even if a delete throws midway.
        // No previous-vector loader is supplied: this sink always produces a new vector, and
        // merging it with one already in effect is the commit's business, not the writer's.
        try (BaseDVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null)) {
            deletedPositions.forEach((org.roaringbitmap.longlong.LongConsumer) position ->
                    writer.delete(dataFile, position, partitionSpec, partitionData.orElse(null)));
            writer.close();
            result = writer.result();
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "Failed to write the deletion vector file", e);
        }
        catch (UncheckedIOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "Failed to write the deletion vector file", e.getCause());
        }

        ImmutableList.Builder<Slice> commitTasks = ImmutableList.builder();
        for (DeleteFile deleteFile : result.deleteFiles()) {
            commitTasks.add(wrappedBuffer(jsonCodec.toJsonBytes(new CommitTaskData(
                    deleteFile.path().toString(),
                    deleteFile.fileSizeInBytes(),
                    new MetricsWrapper(new Metrics(deleteFile.recordCount(), null, null, null, null)),
                    deleteFile.specId(),
                    partitionData.map(PartitionData::toJson),
                    FileFormat.PUFFIN,
                    deleteFile.referencedDataFile() != null ? deleteFile.referencedDataFile() : dataFile,
                    POSITION_DELETES,
                    // Carried so the reader can fetch this one blob directly. A Puffin file may
                    // hold the vectors of several data files, and applying another file's
                    // deletions to this one would remove rows that are still live.
                    OptionalLong.of(deleteFile.contentOffset()),
                    OptionalLong.of(deleteFile.contentSizeInBytes())))));
        }
        return completedFuture(commitTasks.build());
    }

    @Override
    public void abort()
    {
        // The Puffin file is written once, in finish(), so an abort before that leaves nothing
        // behind and an abort after it is not reached.
    }
}
