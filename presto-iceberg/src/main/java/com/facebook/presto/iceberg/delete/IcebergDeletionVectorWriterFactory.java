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

import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.HdfsInputFile;
import com.facebook.presto.iceberg.HdfsOutputFile;
import com.facebook.presto.iceberg.PrestoIcebergTableForMetricsConfig;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Builds the {@link OutputFileFactory} that Iceberg's deletion-vector writer needs.
 * <p>
 * Iceberg's builder wants a whole {@link org.apache.iceberg.Table} so it can read a partition
 * spec, an encryption manager and a location provider off it. The page-sink path has those three
 * things individually but no loaded table, so a table exposing exactly them is assembled here. The
 * alternative -- loading the real table on the worker while writing -- would be a metadata read
 * per sink for values the sink was already handed.
 */
public final class IcebergDeletionVectorWriterFactory
{
    private IcebergDeletionVectorWriterFactory() {}

    /**
     * @param operationId embedded in generated file names, so two writers in one query cannot
     *         collide on a name
     */
    public static OutputFileFactory createPuffinOutputFileFactory(
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            LocationProvider locationProvider,
            PartitionSpec partitionSpec,
            String operationId,
            int partitionId,
            long taskId)
    {
        requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        requireNonNull(hdfsContext, "hdfsContext is null");
        requireNonNull(locationProvider, "locationProvider is null");
        requireNonNull(partitionSpec, "partitionSpec is null");
        requireNonNull(operationId, "operationId is null");

        FileIO io = new HdfsBackedFileIO(hdfsEnvironment, hdfsContext);
        org.apache.iceberg.Table table = new DeletionVectorTable(partitionSpec, locationProvider, io);
        return OutputFileFactory.builderFor(table, partitionId, taskId)
                .format(org.apache.iceberg.FileFormat.PUFFIN)
                .operationId(operationId)
                .ioSupplier(() -> io)
                .build();
    }

    /**
     * Routes Iceberg's file access through the connector's own HDFS plumbing, which already runs
     * as the session's user. Deliberately not the connector's production {@code HdfsFileIO}: that
     * carries a manifest-file cache this path has no manifests to cache.
     */
    private static final class HdfsBackedFileIO
            implements FileIO
    {
        private final HdfsEnvironment hdfsEnvironment;
        private final HdfsContext hdfsContext;

        HdfsBackedFileIO(HdfsEnvironment hdfsEnvironment, HdfsContext hdfsContext)
        {
            this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
            this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
        }

        @Override
        public InputFile newInputFile(String path)
        {
            return new HdfsInputFile(new Path(path), hdfsEnvironment, hdfsContext);
        }

        @Override
        public OutputFile newOutputFile(String path)
        {
            return new HdfsOutputFile(new Path(path), hdfsEnvironment, hdfsContext);
        }

        @Override
        public void deleteFile(String path)
        {
            // Reached only if a caller abandons a file it wrote. The deletion-vector sink writes
            // its Puffin file once, in finish(), so there is nothing here to abandon -- and a
            // silent no-op would leave a caller believing a file had been removed.
            throw new UnsupportedOperationException("Deletion vector file IO does not delete files");
        }
    }

    /**
     * A table that answers only what {@link OutputFileFactory.Builder} asks of it: the partition
     * spec, the location provider and the encryption manager.
     */
    private static final class DeletionVectorTable
            extends PrestoIcebergTableForMetricsConfig
    {
        private static final Map<String, String> NO_PROPERTIES = ImmutableMap.of();
        private static final Schema NO_SCHEMA = new Schema();
        private static final EncryptionManager PLAINTEXT = PlaintextEncryptionManager.instance();

        private final LocationProvider locationProvider;
        private final FileIO io;

        DeletionVectorTable(PartitionSpec spec, LocationProvider locationProvider, FileIO io)
        {
            super(NO_SCHEMA, spec, NO_PROPERTIES, Optional.empty());
            this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
            this.io = requireNonNull(io, "io is null");
        }

        @Override
        public LocationProvider locationProvider()
        {
            return locationProvider;
        }

        @Override
        public EncryptionManager encryption()
        {
            return PLAINTEXT;
        }

        /**
         * Also exposed here, not only through the builder's supplier, so that an Iceberg version
         * which reaches for {@code table.io()} directly still writes through the connector's
         * plumbing rather than constructing its own.
         */
        @Override
        public FileIO io()
        {
            return io;
        }
    }
}
