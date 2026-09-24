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

import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.iceberg.changelog.ChangelogSplitInfo;
import com.facebook.presto.iceberg.delete.DeleteFile;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.SOFT_AFFINITY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class IcebergSplit
        implements ConnectorSplit
{
    private final String path;
    private final long start;
    private final long length;
    private final FileFormat fileFormat;
    private final List<HostAddress> addresses;
    private final Map<Integer, HivePartitionKey> partitionKeys;
    private final String partitionSpecAsJson;
    private final Optional<String> partitionDataJson;
    private final NodeSelectionStrategy nodeSelectionStrategy;
    private final SplitWeight splitWeight;
    private final List<DeleteFile> deletes;
    private final Optional<ChangelogSplitInfo> changelogSplitInfo;
    private final long dataSequenceNumber;
    private final long firstRowId;
    private final long affinitySchedulingFileSectionSize;
    private final long affinitySchedulingFileSectionIndex;
    private final List<DeleteFile> retainedDeletes;

    /**
     * @param retainedDeletes delete files whose marked rows are what this split is for, rather
     *         than what it must skip. A row is read only if one of these files removed it and none
     *         of {@code deletes} had removed it already.
     *         <p>
     *         Two lists rather than a flag inverting one: Iceberg reports the rows a snapshot
     *         removed from a data file as the rows its newly added delete files mark, minus the
     *         rows the delete files already in effect had removed. Both sets can name the same
     *         data file, so a single list cannot say which sense applies to which file.
     */
    @JsonCreator
    public IcebergSplit(
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("length") long length,
            @JsonProperty("fileFormat") FileFormat fileFormat,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("partitionKeys") Map<Integer, HivePartitionKey> partitionKeys,
            @JsonProperty("partitionSpecAsJson") String partitionSpecAsJson,
            @JsonProperty("partitionDataJson") Optional<String> partitionDataJson,
            @JsonProperty("nodeSelectionStrategy") NodeSelectionStrategy nodeSelectionStrategy,
            @JsonProperty("splitWeight") SplitWeight splitWeight,
            @JsonProperty("deletes") List<DeleteFile> deletes,
            @JsonProperty("changelogSplitInfo") Optional<ChangelogSplitInfo> changelogSplitInfo,
            @JsonProperty("dataSequenceNumber") long dataSequenceNumber,
            @JsonProperty("firstRowId") long firstRowId,
            @JsonProperty("affinitySchedulingSectionSize") long affinitySchedulingFileSectionSize,
            @JsonProperty("retainedDeletes") List<DeleteFile> retainedDeletes)
    {
        requireNonNull(nodeSelectionStrategy, "nodeSelectionStrategy is null");
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.length = length;
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.partitionKeys = Collections.unmodifiableMap(requireNonNull(partitionKeys, "partitionKeys is null"));
        this.partitionSpecAsJson = requireNonNull(partitionSpecAsJson, "partitionSpecAsJson is null");
        this.partitionDataJson = partitionDataJson;
        this.nodeSelectionStrategy = nodeSelectionStrategy;
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
        this.deletes = ImmutableList.copyOf(requireNonNull(deletes, "deletes is null"));
        this.changelogSplitInfo = requireNonNull(changelogSplitInfo, "changelogSplitInfo is null");
        this.dataSequenceNumber = dataSequenceNumber;
        this.firstRowId = firstRowId;
        this.affinitySchedulingFileSectionSize = affinitySchedulingFileSectionSize;
        this.affinitySchedulingFileSectionIndex = start / affinitySchedulingFileSectionSize;
        this.retainedDeletes = ImmutableList.copyOf(requireNonNull(retainedDeletes, "retainedDeletes is null"));
    }

    /**
     * Retains the signature that predates {@code retainedDeletes}, and reads as a split whose
     * delete files only ever exclude rows.
     */
    public IcebergSplit(
            String path,
            long start,
            long length,
            FileFormat fileFormat,
            List<HostAddress> addresses,
            Map<Integer, HivePartitionKey> partitionKeys,
            String partitionSpecAsJson,
            Optional<String> partitionDataJson,
            NodeSelectionStrategy nodeSelectionStrategy,
            SplitWeight splitWeight,
            List<DeleteFile> deletes,
            Optional<ChangelogSplitInfo> changelogSplitInfo,
            long dataSequenceNumber,
            long firstRowId,
            long affinitySchedulingFileSectionSize)
    {
        this(
                path,
                start,
                length,
                fileFormat,
                addresses,
                partitionKeys,
                partitionSpecAsJson,
                partitionDataJson,
                nodeSelectionStrategy,
                splitWeight,
                deletes,
                changelogSplitInfo,
                dataSequenceNumber,
                firstRowId,
                affinitySchedulingFileSectionSize,
                ImmutableList.of());
    }

    @JsonProperty
    public List<DeleteFile> getRetainedDeletes()
    {
        return retainedDeletes;
    }

    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getStart()
    {
        return start;
    }

    @JsonProperty
    public long getLength()
    {
        return length;
    }

    @JsonProperty
    public FileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public Map<Integer, HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    @JsonProperty
    public String getPartitionSpecAsJson()
    {
        return partitionSpecAsJson;
    }

    @JsonProperty
    public Optional<String> getPartitionDataJson()
    {
        return partitionDataJson;
    }

    @JsonProperty
    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return nodeSelectionStrategy;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        if (getNodeSelectionStrategy() == SOFT_AFFINITY) {
            return nodeProvider.get(path + "#" + affinitySchedulingFileSectionIndex);
        }
        return addresses;
    }

    @JsonProperty
    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @JsonProperty
    public List<DeleteFile> getDeletes()
    {
        return deletes;
    }

    @JsonProperty
    public Optional<ChangelogSplitInfo> getChangelogSplitInfo()
    {
        return changelogSplitInfo;
    }

    @JsonProperty
    public long getDataSequenceNumber()
    {
        return dataSequenceNumber;
    }

    @JsonProperty
    public long getFirstRowId()
    {
        return firstRowId;
    }

    @JsonProperty
    public long getAffinitySchedulingFileSectionSize()
    {
        return affinitySchedulingFileSectionSize;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", path)
                .put("start", start)
                .put("length", length)
                .put("nodeSelectionStrategy", nodeSelectionStrategy)
                .put("splitWeight", splitWeight)
                .put("changelogSplitInfo", changelogSplitInfo)
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .addValue(start)
                .addValue(length)
                .addValue(nodeSelectionStrategy)
                .addValue(splitWeight)
                .addValue(changelogSplitInfo)
                .toString();
    }
}
