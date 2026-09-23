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

import com.facebook.presto.spi.PrestoException;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.LongBitmapDataProvider;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static java.lang.String.format;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;

/**
 * Decodes an Iceberg V3 deletion vector: a Roaring bitmap of deleted row positions within a
 * single data file, stored as one blob of a Puffin file.
 * <p>
 * A deletion vector replaces the V2 positional-delete data file. Rather than one row per deleted
 * position, it stores the positions as a bitmap, which is both far smaller and cheap to test. The
 * delete file entry in the manifest carries the blob's byte range within the Puffin file
 * ({@code content_offset} / {@code content_size_in_bytes}), so a reader can fetch exactly the one
 * blob it needs without parsing the Puffin footer or touching the vectors of unrelated data files.
 */
public final class DeletionVectors
{
    /**
     * Blob framing, from the Iceberg table spec:
     *
     * <pre>
     *   | length : 4 | magic : 4 | bitmap : length - 4 | crc : 4 |
     * </pre>
     *
     * {@code length} counts the magic number plus the bitmap, and {@code crc} covers that same
     * range. The magic bytes on disk are {@code D1 D3 39 64}, which is why the constant below
     * reads as {@code 0x6439D3D1} once decoded little-endian.
     */
    private static final int LENGTH_SIZE_BYTES = 4;
    private static final int MAGIC_SIZE_BYTES = 4;
    private static final int CRC_SIZE_BYTES = 4;
    private static final int MAGIC_NUMBER = 0x6439D3D1;

    private DeletionVectors() {}

    /**
     * The exact number of bytes of the Puffin file that {@link #applyDeletionVector} needs, being
     * the blob that {@code delete} points at.
     */
    public static long blobOffset(DeleteFile delete)
    {
        return delete.getContentOffset().orElseThrow(() -> new PrestoException(
                ICEBERG_BAD_DATA,
                format("Deletion vector %s is missing content_offset", delete.path())));
    }

    public static long blobLength(DeleteFile delete)
    {
        return delete.getContentSizeInBytes().orElseThrow(() -> new PrestoException(
                ICEBERG_BAD_DATA,
                format("Deletion vector %s is missing content_size_in_bytes", delete.path())));
    }

    /**
     * Adds every position the vector marks deleted to {@code deletedRows}.
     *
     * @param blob the blob bytes, exactly {@link #blobLength} of them read from
     *         {@link #blobOffset} of the Puffin file named by {@code delete}
     */
    public static void applyDeletionVector(byte[] blob, DeleteFile delete, LongBitmapDataProvider deletedRows)
    {
        requireNonNull(blob, "blob is null");
        requireNonNull(delete, "delete is null");
        requireNonNull(deletedRows, "deletedRows is null");
        deserialize(blob, delete.path(), deletedRows);
    }

    /**
     * Separated from the read so the framing can be exercised without a file system. {@code
     * description} only names the source in error messages.
     */
    public static void deserialize(byte[] blob, String description, LongBitmapDataProvider deletedRows)
    {
        int minimumLength = LENGTH_SIZE_BYTES + MAGIC_SIZE_BYTES + CRC_SIZE_BYTES;
        if (blob.length < minimumLength) {
            throw new PrestoException(ICEBERG_BAD_DATA, format(
                    "Deletion vector %s is %s bytes, too short to hold the %s-byte framing",
                    description,
                    blob.length,
                    minimumLength));
        }

        // Length and CRC are big-endian; the magic number and the bitmap itself are little-endian.
        ByteBuffer framing = ByteBuffer.wrap(blob);
        int bitmapDataLength = framing.getInt(0);
        int expectedBitmapDataLength = blob.length - LENGTH_SIZE_BYTES - CRC_SIZE_BYTES;
        if (bitmapDataLength != expectedBitmapDataLength) {
            throw new PrestoException(ICEBERG_BAD_DATA, format(
                    "Deletion vector %s declares %s bytes of bitmap data, but the blob holds %s",
                    description,
                    bitmapDataLength,
                    expectedBitmapDataLength));
        }

        int crc = crc32(blob, LENGTH_SIZE_BYTES, bitmapDataLength);
        int expectedCrc = framing.getInt(LENGTH_SIZE_BYTES + bitmapDataLength);
        if (crc != expectedCrc) {
            throw new PrestoException(ICEBERG_BAD_DATA, format(
                    "Deletion vector %s failed its checksum: computed %s, expected %s",
                    description,
                    crc,
                    expectedCrc));
        }

        ByteBuffer bitmapData = ByteBuffer.wrap(blob, LENGTH_SIZE_BYTES, bitmapDataLength)
                .slice()
                .order(LITTLE_ENDIAN);
        int magic = bitmapData.getInt();
        if (magic != MAGIC_NUMBER) {
            throw new PrestoException(ICEBERG_BAD_DATA, format(
                    "Deletion vector %s has magic number %s, expected %s",
                    description,
                    Integer.toHexString(magic),
                    Integer.toHexString(MAGIC_NUMBER)));
        }
        readPositions(bitmapData, description, deletedRows);
    }

    /**
     * Reads the portable 64-bit Roaring serialization: a count of bitmaps, then that many
     * (key, 32-bit bitmap) pairs in ascending key order. A position is recovered by putting the
     * key in the high 32 bits and the bitmap's value in the low 32.
     * <p>
     * Iceberg's writer emits exactly this, so the layout is fixed by the table spec rather than by
     * the Roaring library: {@code RoaringBitmap.deserialize} is given the buffer directly, and the
     * position has to be advanced by hand afterwards because that method deliberately leaves the
     * buffer where it found it.
     */
    private static void readPositions(ByteBuffer bitmapData, String description, LongBitmapDataProvider deletedRows)
    {
        long bitmapCount = bitmapData.getLong();
        if (bitmapCount < 0 || bitmapCount > Integer.MAX_VALUE) {
            throw new PrestoException(ICEBERG_BAD_DATA, format(
                    "Deletion vector %s declares %s bitmaps", description, bitmapCount));
        }

        long previousKey = -1;
        for (long index = 0; index < bitmapCount; index++) {
            long key = Integer.toUnsignedLong(bitmapData.getInt());
            if (key <= previousKey) {
                throw new PrestoException(ICEBERG_BAD_DATA, format(
                        "Deletion vector %s has key %s after key %s, but keys must ascend",
                        description,
                        key,
                        previousKey));
            }
            previousKey = key;

            RoaringBitmap bitmap = new RoaringBitmap();
            try {
                bitmap.deserialize(bitmapData);
            }
            catch (IOException e) {
                throw new PrestoException(ICEBERG_BAD_DATA, format(
                        "Deletion vector %s holds an unreadable bitmap for key %s", description, key), e);
            }
            bitmapData.position(bitmapData.position() + bitmap.serializedSizeInBytes());

            long positionBase = key << 32;
            bitmap.forEach((org.roaringbitmap.IntConsumer) value ->
                    deletedRows.addLong(positionBase | Integer.toUnsignedLong(value)));
        }
    }

    private static int crc32(byte[] bytes, int offset, int length)
    {
        CRC32 crc = new CRC32();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }
}
