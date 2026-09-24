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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import io.airlift.slice.Slice;
import jakarta.annotation.Nullable;
import org.roaringbitmap.longlong.ImmutableLongBitmapDataProvider;
import org.roaringbitmap.longlong.LongBitmapDataProvider;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class PositionDeleteFilter
        implements DeleteFilter
{
    private final ImmutableLongBitmapDataProvider deletedRows;
    @Nullable
    private final String deleteFilePath;
    private final boolean retainDeletedRows;

    public PositionDeleteFilter(ImmutableLongBitmapDataProvider deletedRows, @Nullable String deleteFilePath)
    {
        this(deletedRows, deleteFilePath, false);
    }

    /**
     * @param retainDeletedRows keep the marked rows and discard the rest, rather than the other
     *         way round. A delete file records which rows of a data file went away, and the data
     *         file still holds them, so reading it this way reproduces rows that are no longer
     *         anywhere in the table. Both senses are needed in the same read: the rows a snapshot
     *         removed are the ones its own delete files mark and that no earlier delete file had
     *         already removed, which is a retaining filter over the one and an ordinary filter
     *         over the others.
     */
    public PositionDeleteFilter(ImmutableLongBitmapDataProvider deletedRows, @Nullable String deleteFilePath, boolean retainDeletedRows)
    {
        this.deletedRows = requireNonNull(deletedRows, "deletedRows is null");
        this.deleteFilePath = deleteFilePath;
        this.retainDeletedRows = retainDeletedRows;
    }

    @Override
    public RowPredicate createPredicate(List<IcebergColumnHandle> columns)
    {
        int filePosChannel = rowPositionChannel(columns);
        if (retainDeletedRows) {
            return (page, position) -> deletedRows.contains(BIGINT.getLong(page.getBlock(filePosChannel), position));
        }
        return (page, position) -> {
            long filePos = BIGINT.getLong(page.getBlock(filePosChannel), position);
            return !deletedRows.contains(filePos);
        };
    }

    public Optional<String> getDeleteFilePath()
    {
        return Optional.ofNullable(deleteFilePath);
    }

    private static int rowPositionChannel(List<IcebergColumnHandle> columns)
    {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).isRowPositionColumn()) {
                return i;
            }
        }
        throw new IllegalArgumentException("No row position column");
    }

    public static void readPositionDeletes(ConnectorPageSource pageSource, Slice targetPath, LongBitmapDataProvider deletedRows)
    {
        CachingVarcharComparator comparator = new CachingVarcharComparator(targetPath);

        // Use a linear search since we expect most deletion files to only contain
        // entries for a single path. The comparison cost is minimal if the
        // path values are dictionary encoded, since we only do the comparison once.
        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page == null) {
                continue;
            }

            Block pathBlock = page.getBlock(0);
            Block posBlock = page.getBlock(1);

            for (int position = 0; position < page.getPositionCount(); position++) {
                int result = comparator.compare(pathBlock, position);
                if (result > 0) {
                    // deletion files are sorted by path, so we're done
                    return;
                }
                if (result == 0) {
                    deletedRows.addLong(BIGINT.getLong(posBlock, position));
                }
            }
        }
    }

    private static final class CachingVarcharComparator
    {
        private final Slice reference;
        private int result;
        private Slice value;

        public CachingVarcharComparator(Slice reference)
        {
            this.reference = requireNonNull(reference, "reference is null");
        }

        @SuppressWarnings({"ObjectEquality", "ReferenceEquality"})
        public int compare(Block block, int position)
        {
            checkArgument(!block.isNull(position), "position is null");
            Slice next = VARCHAR.getSlice(block, position);
            // The expected case is a dictionary block with many entries for the
            // same path. Only perform a comparison if the object has changed.
            if (value != next) {
                value = next;
                result = value.compareTo(reference);
            }
            return result;
        }
    }
}
