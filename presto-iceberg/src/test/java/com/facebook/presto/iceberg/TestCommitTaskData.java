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
import org.apache.iceberg.Metrics;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestCommitTaskData
{
    private static final JsonCodec<CommitTaskData> CODEC = jsonCodec(CommitTaskData.class);

    /**
     * A commit task crosses a worker-to-coordinator boundary as JSON, so anything the round trip
     * drops is lost before the commit reads it. The partition spec id is the one that hurts: a
     * table whose spec has evolved would have its delete files built against spec zero, and on an
     * unpartitioned table -- where zero happens to be right -- nothing would look wrong.
     */
    @Test
    public void testRoundTripPreservesThePartitionSpecId()
    {
        CommitTaskData task = new CommitTaskData(
                "/data/file.parquet",
                1024,
                new MetricsWrapper(new Metrics(7L, null, null, null, null)),
                3,
                Optional.of("{\"partitionValues\":[]}"),
                FileFormat.PARQUET,
                "/data/referenced.parquet",
                FileContent.POSITION_DELETES,
                OptionalLong.of(4),
                OptionalLong.of(42));

        CommitTaskData roundTripped = CODEC.fromJson(CODEC.toJson(task));

        assertEquals(roundTripped.getPartitionSpecId(), 3);
        assertEquals(roundTripped.getPath(), "/data/file.parquet");
        assertEquals(roundTripped.getFileSizeInBytes(), 1024);
        assertEquals(roundTripped.getPartitionDataJson(), Optional.of("{\"partitionValues\":[]}"));
        assertEquals(roundTripped.getReferencedDataFile(), Optional.of("/data/referenced.parquet"));
        assertEquals(roundTripped.getContent(), FileContent.POSITION_DELETES);
    }

    /**
     * The blob coordinates a deletion vector is located by. Without them a reader cannot find its
     * own blob within a Puffin file that may hold several.
     */
    @Test
    public void testRoundTripPreservesTheDeletionVectorLocation()
    {
        CommitTaskData task = new CommitTaskData(
                "/data/deletes.puffin",
                512,
                new MetricsWrapper(new Metrics(2L, null, null, null, null)),
                0,
                Optional.empty(),
                FileFormat.PUFFIN,
                "/data/referenced.parquet",
                FileContent.POSITION_DELETES,
                OptionalLong.of(4),
                OptionalLong.of(44));

        CommitTaskData roundTripped = CODEC.fromJson(CODEC.toJson(task));

        assertEquals(roundTripped.getContentOffset(), OptionalLong.of(4));
        assertEquals(roundTripped.getContentSizeInBytes(), OptionalLong.of(44));
        assertEquals(roundTripped.getFileFormat(), FileFormat.PUFFIN);
    }

    /**
     * A file that is not a deletion vector carries no coordinates, and must not acquire any.
     */
    @Test
    public void testRoundTripKeepsANonVectorWithoutCoordinates()
    {
        CommitTaskData task = new CommitTaskData(
                "/data/file.parquet",
                1024,
                new MetricsWrapper(new Metrics(7L, null, null, null, null)),
                1,
                Optional.empty(),
                FileFormat.PARQUET,
                null,
                FileContent.DATA);

        CommitTaskData roundTripped = CODEC.fromJson(CODEC.toJson(task));

        assertEquals(roundTripped.getPartitionSpecId(), 1);
        assertEquals(roundTripped.getContentOffset(), OptionalLong.empty());
        assertEquals(roundTripped.getContentSizeInBytes(), OptionalLong.empty());
        assertEquals(roundTripped.getReferencedDataFile(), Optional.empty());
    }
}
