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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class CommitTaskData
{
    private final String path;
    private final long fileSizeInBytes;
    private final MetricsWrapper metrics;
    private final int partitionSpecId;
    private final Optional<String> partitionDataJson;
    private final FileFormat fileFormat;
    private final Optional<String> referencedDataFile;
    private final FileContent content;
    private final OptionalLong contentOffset;
    private final OptionalLong contentSizeInBytes;

    /**
     * @param contentOffset where a deletion vector's blob begins within the Puffin file named by
     *         {@code path}, and {@code contentSizeInBytes} how long it is. A Puffin file may hold
     *         the vectors of several data files, so a reader given only the file name would have
     *         to parse its footer to find the one blob it wants -- and merging all of them would
     *         apply another data file's deletions to this one. Absent for anything but a vector.
     */
    @JsonCreator
    public CommitTaskData(
            @JsonProperty("path") String path,
            @JsonProperty("fileSizeInBytes") long fileSizeInBytes,
            @JsonProperty("metrics") MetricsWrapper metrics,
            // Named for the key actually on the wire. This read "partitionSpecJson", a key nothing
            // ever writes: the value round-tripped anyway, under the parameter's own name, so the
            // annotation described a format that did not exist rather than breaking the one that
            // did. Corrected so the declaration cannot be read as evidence of either.
            @JsonProperty("partitionSpecId") int partitionSpecId,
            @JsonProperty("partitionDataJson") Optional<String> partitionDataJson,
            @JsonProperty("fileFormat") FileFormat fileFormat,
            @JsonProperty("referencedDataFile") String referencedDataFile,
            @JsonProperty("content") FileContent content,
            @JsonProperty("contentOffset") OptionalLong contentOffset,
            @JsonProperty("contentSizeInBytes") OptionalLong contentSizeInBytes)
    {
        this.path = requireNonNull(path, "path is null");
        this.fileSizeInBytes = fileSizeInBytes;
        this.metrics = requireNonNull(metrics, "metrics is null");
        this.partitionSpecId = partitionSpecId;
        this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.referencedDataFile = Optional.ofNullable(referencedDataFile);
        this.content = requireNonNull(content, "content is null");
        this.contentOffset = requireNonNull(contentOffset, "contentOffset is null");
        this.contentSizeInBytes = requireNonNull(contentSizeInBytes, "contentSizeInBytes is null");
    }

    /**
     * Retains the signature that predates deletion vectors, and reads as a file that is not one.
     */
    public CommitTaskData(
            String path,
            long fileSizeInBytes,
            MetricsWrapper metrics,
            int partitionSpecId,
            Optional<String> partitionDataJson,
            FileFormat fileFormat,
            String referencedDataFile,
            FileContent content)
    {
        this(path, fileSizeInBytes, metrics, partitionSpecId, partitionDataJson, fileFormat,
                referencedDataFile, content, OptionalLong.empty(), OptionalLong.empty());
    }

    @JsonProperty
    public OptionalLong getContentOffset()
    {
        return contentOffset;
    }

    @JsonProperty
    public OptionalLong getContentSizeInBytes()
    {
        return contentSizeInBytes;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public long getFileSizeInBytes()
    {
        return fileSizeInBytes;
    }

    @JsonProperty
    public MetricsWrapper getMetrics()
    {
        return metrics;
    }

    @JsonProperty
    public int getPartitionSpecId()
    {
        return partitionSpecId;
    }

    @JsonProperty
    public Optional<String> getPartitionDataJson()
    {
        return partitionDataJson;
    }

    @JsonProperty
    public FileFormat getFileFormat()
    {
        return fileFormat;
    }

    @JsonProperty
    public Optional<String> getReferencedDataFile()
    {
        return referencedDataFile;
    }

    @JsonProperty
    public FileContent getContent()
    {
        return content;
    }
}
