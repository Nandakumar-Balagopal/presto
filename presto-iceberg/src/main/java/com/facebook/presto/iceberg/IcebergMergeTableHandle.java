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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.iceberg.delete.DeleteFile;
import com.facebook.presto.spi.ConnectorMergeTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.iceberg.IcebergUtil.MIN_FORMAT_VERSION_FOR_DELETE;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class IcebergMergeTableHandle
        implements ConnectorMergeTableHandle
{
    private final IcebergTableHandle tableHandle;
    private final IcebergInsertTableHandle insertTableHandle;
    private final Map<Integer, PrestoIcebergPartitionSpec> partitionSpecs;
    private final Map<String, DeleteFile> deletionVectorsInEffect;
    private final int formatVersion;

    @JsonCreator
    @ThriftConstructor
    public IcebergMergeTableHandle(
            @JsonProperty("tableHandle") IcebergTableHandle tableHandle,
            @JsonProperty("insertTableHandle") IcebergInsertTableHandle insertTableHandle,
            @JsonProperty("partitionSpecs") Map<Integer, PrestoIcebergPartitionSpec> partitionSpecs,
            @JsonProperty("deletionVectorsInEffect") Map<String, DeleteFile> deletionVectorsInEffect,
            @JsonProperty("formatVersion") int formatVersion)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.insertTableHandle = requireNonNull(insertTableHandle, "insertTableHandle is null");
        this.partitionSpecs = requireNonNull(partitionSpecs, "partitionSpecs is null");
        this.deletionVectorsInEffect = ImmutableMap.copyOf(requireNonNull(deletionVectorsInEffect, "deletionVectorsInEffect is null"));
        this.formatVersion = formatVersion;
    }

    /**
     * Retains the signature that predates deletion vectors, for a table that can have none.
     */
    public IcebergMergeTableHandle(
            IcebergTableHandle tableHandle,
            IcebergInsertTableHandle insertTableHandle,
            Map<Integer, PrestoIcebergPartitionSpec> partitionSpecs)
    {
        this(tableHandle, insertTableHandle, partitionSpecs, ImmutableMap.of(), MIN_FORMAT_VERSION_FOR_DELETE);
    }

    @Override
    @JsonProperty
    @ThriftField(1)
    public IcebergTableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    @ThriftField(2)
    public IcebergInsertTableHandle getInsertTableHandle()
    {
        return insertTableHandle;
    }

    @JsonProperty
    @ThriftField(3)
    public Map<Integer, PrestoIcebergPartitionSpec> getPartitionSpecs()
    {
        return partitionSpecs;
    }

    /**
     * The deletion vector currently masking each data file, keyed by that data file's path, for the
     * files that have one. Empty below format version 3, where deletion vectors do not exist.
     *
     * <p>Carried on the handle because the sink needs it and cannot derive it: a merge's deletes
     * arrive identified only by data file path, with none of the split context that lets the
     * delete path look its predecessor up, and the sink provider has no catalog access to read the
     * table itself. A vector replaces its predecessor rather than adding to it, so a sink without
     * this would write a vector holding only the rows this merge removed and bring back the rows
     * an earlier operation removed.
     */
    @JsonProperty
    @ThriftField(4)
    public Map<String, DeleteFile> getDeletionVectorsInEffect()
    {
        return deletionVectorsInEffect;
    }

    /**
     * The table's format version, which decides whether the merge's deletes are recorded as
     * deletion vectors. Carried here rather than read from the insert handle's storage properties:
     * those come from {@code Table.properties()}, where the format version does not appear -- it
     * lives in the table metadata -- so a sink reading them would see the default of 2 and write a
     * positional delete file that a version 3 commit rejects.
     */
    @JsonProperty
    @ThriftField(5)
    public int getFormatVersion()
    {
        return formatVersion;
    }
}
