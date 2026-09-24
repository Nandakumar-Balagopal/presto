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

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.iceberg.delete.DeletionVectors;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ChangeKindPageSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.SeekableInputStream;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.iceberg.IcebergUtil.MAX_FORMAT_VERSION_FOR_METADATA_TABLES;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestIcebergV3
        extends AbstractTestQueryFramework
{
    private static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .setFormat(PARQUET)
                .setNodeCount(OptionalInt.of(1))
                .setCreateTpchTables(false)
                .setAddJmxPlugin(false)
                .build().getQueryRunner();
    }

    private void dropTable(String tableName)
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS " + tableName);
    }

    @Test
    public void testCreateV3Table()
    {
        String tableName = "test_create_v3_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
            assertQuery("SELECT * FROM " + tableName, "SELECT * WHERE false");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testCreateUnsupportedFormatVersion()
    {
        String tableName = "test_create_v4_table";
        // Ensure clean state in case a previous run created the table
        dropTable(tableName);

        assertQueryFails(
                "CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '4')",
                ".*Iceberg table format version 4 is not supported.*");
    }

    @Test
    public void testUpgradeV2ToV3()
    {
        String tableName = "test_upgrade_v2_to_v3";
        try {
            // Create v2 table
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '2')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 2);

            // Upgrade to v3
            BaseTable baseTable = (BaseTable) table;
            TableOperations operations = baseTable.operations();
            TableMetadata currentMetadata = operations.current();
            operations.commit(currentMetadata, currentMetadata.upgradeToFormatVersion(3));

            // Verify the upgrade
            table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertIntoV3Table()
    {
        String tableName = "test_insert_v3_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two')", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'one'), (2, 'two')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'three')", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testRowLevelChangeSet()
            throws Exception
    {
        String tableName = "test_row_level_change_set";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one')", 1);

            ConnectorTableVersion recordedVersion;
            TransactionId recordedTransaction = getQueryRunner().getTransactionManager().beginTransaction(false);
            try {
                Session recordedSession = metadataSession(recordedTransaction);
                TableHandle recordedHandle = getTableHandle(tableName, recordedSession);
                recordedVersion = getQueryRunner().getMetadata().getCurrentTableVersion(recordedSession, recordedHandle).get();
            }
            finally {
                getQueryRunner().getTransactionManager().asyncAbort(recordedTransaction);
            }

            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'two')", 1);

            TransactionId refreshTransaction = getQueryRunner().getTransactionManager().beginTransaction(false);
            try {
                Session refreshSession = metadataSession(refreshTransaction);
                Metadata metadata = getQueryRunner().getMetadata();
                TableHandle tableHandle = getTableHandle(tableName, refreshSession);
                ConnectorTableVersion refreshVersion = metadata.getCurrentTableVersion(refreshSession, tableHandle).get();
                Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(refreshSession, tableHandle);
                List<ColumnHandle> columns = ImmutableList.of(columnHandles.get("id"), columnHandles.get("value"));

                OptionalLong estimatedSize = metadata.estimateChangeSetSize(refreshSession, tableHandle, recordedVersion, refreshVersion);
                assertTrue(estimatedSize.isPresent());
                assertTrue(estimatedSize.getAsLong() > 0);

                ChangeKindPageSource pageSource = metadata.getChangeSet(
                        refreshSession,
                        tableHandle,
                        recordedVersion,
                        refreshVersion,
                        columns,
                        TupleDomain.all());
                try {
                    List<Integer> ids = new ArrayList<>();
                    List<String> changeKinds = new ArrayList<>();
                    while (!pageSource.isFinished()) {
                        Page page = pageSource.getNextPage();
                        if (page == null) {
                            pageSource.isBlocked().get();
                            continue;
                        }
                        for (int position = 0; position < page.getPositionCount(); position++) {
                            ids.add((int) INTEGER.getLong(page.getBlock(0), position));
                            changeKinds.add(VARCHAR.getSlice(page.getBlock(page.getChannelCount() - 1), position).toStringUtf8());
                        }
                    }
                    assertEquals(ids, ImmutableList.of(2));
                    assertEquals(changeKinds, ImmutableList.of("INSERT"));
                }
                finally {
                    pageSource.close();
                }
            }
            finally {
                getQueryRunner().getTransactionManager().asyncAbort(refreshTransaction);
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testSystemChanges()
            throws Exception
    {
        String tableName = "test_system_changes";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one')", 1);
            long fromSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'two')", 1);
            long toSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();

            assertQuery(
                    "SELECT id, value, change_kind FROM TABLE(system.builtin.changes('" + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName + "', " + fromSnapshotId + ", " + toSnapshotId + "))",
                    "VALUES (2, 'two', 'INSERT')");
            assertQuery(
                    "SELECT count(*) FROM TABLE(system.builtin.changes('" + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName + "', " + fromSnapshotId + ", " + toSnapshotId + ", true)) WHERE \"$row_id\" IS NOT NULL",
                    "SELECT 1");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * The change set reports the table's own columns and the change kind, and nothing else.
     * <p>
     * A connector may expose metadata about a row -- the file holding it, its position in that
     * file, whether a delete file marks it -- as hidden columns, which {@code SELECT *} does not
     * return. Those are not part of a change set. Projecting them is also not merely untidy: the
     * Iceberg reader answers a read that asks for its delete-marker column by labelling rows
     * rather than removing them, so requesting that column decides whether delete files filter at
     * all.
     * <p>
     * Asserted on the column count, because the cost of getting this wrong is extra columns nobody
     * selected, which every existing test would happily ignore -- they all name the columns they
     * want.
     */
    @Test
    public void testSystemChangesReportsOnlyTableColumnsAndChangeKind()
            throws Exception
    {
        String tableName = "test_system_changes_schema";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one')", 1);
            long fromSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'two')", 1);
            long toSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();

            String allColumns = format(
                    "SELECT * FROM TABLE(system.builtin.changes('%s.%s.%s', %s, %s))",
                    ICEBERG_CATALOG, TEST_SCHEMA, tableName, fromSnapshotId, toSnapshotId);
            assertEquals(computeActual(allColumns).getTypes().size(), 3, "expected id, value and change_kind");

            // The row lineage column is reported only when asked for, and then exactly once.
            String withRowId = format(
                    "SELECT * FROM TABLE(system.builtin.changes('%s.%s.%s', %s, %s, true))",
                    ICEBERG_CATALOG, TEST_SCHEMA, tableName, fromSnapshotId, toSnapshotId);
            assertEquals(computeActual(withRowId).getTypes().size(), 4, "expected id, value, $row_id and change_kind");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testSystemChangesIncludesDeletedRows()
            throws Exception
    {
        String tableName = "test_system_changes_delete";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two')", 2);
            long fromSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();
            assertUpdate("DELETE FROM " + tableName, 2);
            long toSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();

            assertQuery(
                    "SELECT id, value, change_kind FROM TABLE(system.builtin.changes('" + ICEBERG_CATALOG + "." + TEST_SCHEMA + "." + tableName + "', " + fromSnapshotId + ", " + toSnapshotId + ")) ORDER BY id",
                    "VALUES (1, 'one', 'DELETE'), (2, 'two', 'DELETE')");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * The case the change set exists for: rows removed by a deletion vector rather than by
     * dropping whole data files.
     * <p>
     * Those rows are not reachable from the current table. They survive only in the data file the
     * vector points at, and recovering them means reading that file through its own vector,
     * keeping the marked positions rather than discarding them.
     * <p>
     * The row count is asserted separately from the rows. {@code assertQuery} compares ignoring
     * order and prints only the difference, so a result that returned all four rows against an
     * expectation of two would report just the two extras -- which reads exactly like a result of
     * two wrong rows. Asserting the count makes that ambiguity impossible.
     */
    @Test
    public void testSystemChangesRecoversRowsRemovedByDeletionVector()
            throws Exception
    {
        String tableName = "test_system_changes_dv";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')", 4);
            long fromSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();

            Table table = loadTable(tableName);
            List<FileScanTask> tasks = new ArrayList<>();
            try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
                planned.forEach(tasks::add);
            }
            assertEquals(tasks.size(), 1, "expected the insert to produce exactly one data file");
            // Remove 'two' and 'four'.
            deleteWithDeletionVector(table, tasks.get(0), ImmutableList.of(1L, 3L));
            long toSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();

            // The vector is applied on the ordinary read path, so the rows really are gone.
            assertQuery("SELECT id FROM " + tableName + " ORDER BY id", "VALUES 1, 3");

            assertEquals(computeActual(changes(tableName, fromSnapshotId, toSnapshotId)).getRowCount(), 2);
            assertQuery(
                    changes(tableName, fromSnapshotId, toSnapshotId) + " ORDER BY id",
                    "VALUES (2, 'two', 'DELETE'), (4, 'four', 'DELETE')");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * The changelog table is readable over a range holding a deletion vector.
     * <p>
     * It is the same planning problem as the change set, reached by a different door: a scan of
     * {@code table$changelog} went straight to Iceberg's changelog scan, so any range containing a
     * row-level delete, update or merge made the table unreadable rather than merely incomplete.
     */
    @Test
    public void testChangelogTableOverADeletionVectorRange()
            throws Exception
    {
        String tableName = "test_changelog_table_dv";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two'), (3, 'three')", 3);
            long fromSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();

            // Remove 'two'.
            replaceDeletionVector(tableName, ImmutableList.of(1L));
            long toSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();

            assertQuery(
                    format("SELECT operation, rowdata.id, rowdata.value FROM \"%s@%s$changelog@%s\"",
                            tableName, fromSnapshotId, toSnapshotId),
                    "VALUES ('DELETE', 2, 'two')");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * The cost estimate has to answer for a range holding a deletion vector too. It cannot be
     * taken from Iceberg's changelog scan, which throws while planning such a range, and the
     * throw is an {@link UnsupportedOperationException} that the estimator does not catch -- so
     * getting this wrong surfaces as a failed query, not as a missing estimate.
     */
    @Test
    public void testChangeSetSizeIsEstimatedForADeletionVectorRange()
            throws Exception
    {
        String tableName = "test_change_set_size_dv";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two'), (3, 'three')", 3);

            ConnectorTableVersion recordedVersion;
            TransactionId recordedTransaction = getQueryRunner().getTransactionManager().beginTransaction(false);
            try {
                Session recordedSession = metadataSession(recordedTransaction);
                recordedVersion = getQueryRunner().getMetadata()
                        .getCurrentTableVersion(recordedSession, getTableHandle(tableName, recordedSession)).get();
            }
            finally {
                getQueryRunner().getTransactionManager().asyncAbort(recordedTransaction);
            }

            replaceDeletionVector(tableName, ImmutableList.of(1L));

            TransactionId refreshTransaction = getQueryRunner().getTransactionManager().beginTransaction(false);
            try {
                Session refreshSession = metadataSession(refreshTransaction);
                Metadata metadata = getQueryRunner().getMetadata();
                TableHandle tableHandle = getTableHandle(tableName, refreshSession);
                ConnectorTableVersion refreshVersion = metadata.getCurrentTableVersion(refreshSession, tableHandle).get();

                OptionalLong estimate = metadata.estimateChangeSetSize(refreshSession, tableHandle, recordedVersion, refreshVersion);
                assertTrue(estimate.isPresent(), "expected an estimate for a range holding a deletion vector");
                // One position marked, so one row reported.
                assertEquals(estimate.getAsLong(), 1L);
            }
            finally {
                getQueryRunner().getTransactionManager().asyncAbort(refreshTransaction);
            }
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * A second removal in the same range must not re-report what the first one removed. Iceberg
     * defines the rows a snapshot took away as the ones its own delete files mark minus the ones
     * the delete files already in effect had removed, and a reader that merged the two sets would
     * report the earlier rows again at the later snapshot.
     */
    @Test
    public void testSystemChangesDoesNotRepeatAnEarlierRemoval()
            throws Exception
    {
        String tableName = "test_system_changes_dv_twice";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')", 4);

            // 'one' goes before the range starts, so it must not be reported at all.
            replaceDeletionVector(tableName, ImmutableList.of(0L));
            long fromSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();
            // 'three' goes inside the range. The replacing vector still covers 'one', because a
            // vector holds every deleted position of its data file rather than only the new ones.
            replaceDeletionVector(tableName, ImmutableList.of(0L, 2L));
            long toSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();

            assertQuery("SELECT id FROM " + tableName + " ORDER BY id", "VALUES 2, 4");

            assertEquals(computeActual(changes(tableName, fromSnapshotId, toSnapshotId)).getRowCount(), 1);
            assertQuery(
                    changes(tableName, fromSnapshotId, toSnapshotId),
                    "VALUES (3, 'three', 'DELETE')");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * An insertion and a vector-based removal in one range. Both have to be reported, and the
     * removal must not spill onto the newly added file.
     */
    @Test
    public void testSystemChangesOverInsertAndDeletionVector()
            throws Exception
    {
        String tableName = "test_system_changes_mixed";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two')", 2);
            long fromSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();

            Table table = loadTable(tableName);
            List<FileScanTask> tasks = new ArrayList<>();
            try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
                planned.forEach(tasks::add);
            }
            // Remove 'one', at position 0 of the file that already existed.
            deleteWithDeletionVector(table, tasks.get(0), ImmutableList.of(0L));
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'three')", 1);
            long toSnapshotId = loadTable(tableName).currentSnapshot().snapshotId();

            assertQuery("SELECT id FROM " + tableName + " ORDER BY id", "VALUES 2, 3");

            assertEquals(computeActual(changes(tableName, fromSnapshotId, toSnapshotId)).getRowCount(), 2);
            assertQuery(
                    changes(tableName, fromSnapshotId, toSnapshotId) + " ORDER BY id",
                    "VALUES (1, 'one', 'DELETE'), (3, 'three', 'INSERT')");
        }
        finally {
            dropTable(tableName);
        }
    }

    private String changes(String tableName, long fromSnapshotId, long toSnapshotId)
    {
        return format(
                "SELECT id, value, change_kind FROM TABLE(system.builtin.changes('%s.%s.%s', %s, %s))",
                ICEBERG_CATALOG,
                TEST_SCHEMA,
                tableName,
                fromSnapshotId,
                toSnapshotId);
    }

    private Session metadataSession(TransactionId transactionId)
    {
        return getSession().beginTransactionId(transactionId, getQueryRunner().getTransactionManager(), new AllowAllAccessControl());
    }

    private TableHandle getTableHandle(String tableName, Session session)
    {
        MetadataResolver resolver = getQueryRunner().getMetadata().getMetadataResolver(session);
        return resolver.getTableHandle(new QualifiedObjectName(session.getCatalog().get(), session.getSchema().get(), tableName)).get();
    }

    /**
     * Superseded by {@link #testRowLevelDeleteOnV3Table}: a row-level delete on a V3 table is
     * supported now that the connector writes a deletion vector. Kept as the boundary between what
     * the delete path can do and what the update and merge paths still cannot -- their gate is
     * deliberately separate, so that enabling one does not quietly enable the others.
     */
    @Test
    public void testDeleteOnV3TableNotSupported()
    {
        String tableName = "test_v3_delete";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, value DOUBLE) WITH (\"format-version\" = '3', \"write.delete.mode\" = 'merge-on-read')");
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)", 3);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)");
            // A delete is supported; an update of the same table is not.
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 1", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 2");
            assertThatThrownBy(() -> getQueryRunner().execute("UPDATE " + tableName + " SET name = 'x' WHERE id = 2"))
                    .hasMessageContaining("Iceberg table updates for format version 3 are not supported yet");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testTruncateV3Table()
    {
        String tableName = "test_v3_truncate";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, value DOUBLE) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0), (3, 'Charlie', 300.0)", 3);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");

            assertUpdate("DELETE FROM " + tableName, 3);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 0");

            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'Dave', 400.0)", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (4, 'Dave', 400.0)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMetadataDeleteOnV3PartitionedTable()
    {
        String tableName = "test_v3_metadata_delete";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, value DOUBLE, part VARCHAR)"
                    + " WITH (\"format-version\" = '3', partitioning = ARRAY['part'])");
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'Alice', 100.0, 'A'), (2, 'Bob', 200.0, 'A'),"
                    + " (3, 'Charlie', 300.0, 'B'), (4, 'Dave', 400.0, 'C')", 4);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 4");

            assertUpdate("DELETE FROM " + tableName + " WHERE part = 'A'", 2);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 2");
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (3, 'Charlie', 300.0, 'B'), (4, 'Dave', 400.0, 'C')");

            assertUpdate("DELETE FROM " + tableName + " WHERE part = 'B'", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (4, 'Dave', 400.0, 'C')");

            assertUpdate("DELETE FROM " + tableName + " WHERE part = 'C'", 1);
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 0");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testUpdateOnV3TableNotSupported()
    {
        String tableName = "test_v3_update";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, status VARCHAR, score DOUBLE) WITH (\"format-version\" = '3', \"write.update.mode\" = 'merge-on-read')");
            assertUpdate("INSERT INTO " + tableName
                            + " VALUES (1, 'Alice', 'active', 85.5), (2, 'Bob', 'active', 92.0), (3, 'Charlie', 'inactive', 78.3)",
                    3);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 'active', 85.5), (2, 'Bob', 'active', 92.0), (3, 'Charlie', 'inactive', 78.3)");
            assertThatThrownBy(() -> getQueryRunner()
                    .execute("UPDATE " + tableName + " SET status = 'updated', score = 95.0 WHERE id = 1"))
                    .hasMessageContaining("Iceberg table updates for format version 3 are not supported yet");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMergeOnV3TableNotSupported()
    {
        String tableName = "test_v3_merge_target";
        String sourceTable = "test_v3_merge_source";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, value DOUBLE) WITH (\"format-version\" = '3', \"write.update.mode\" = 'merge-on-read')");
            assertUpdate("CREATE TABLE " + sourceTable + " (id INTEGER, name VARCHAR, value DOUBLE)");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)", 2);
            assertUpdate("INSERT INTO " + sourceTable + " VALUES (1, 'Alice Updated', 150.0), (3, 'Charlie', 300.0)",
                    2);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)");
            assertQuery("SELECT * FROM " + sourceTable + " ORDER BY id",
                    "VALUES (1, 'Alice Updated', 150.0), (3, 'Charlie', 300.0)");
            assertThatThrownBy(() -> getQueryRunner().execute(
                    "MERGE INTO " + tableName + " t USING " + sourceTable + " s ON t.id = s.id " +
                            "WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value " +
                            "WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)"))
                    .hasMessageContaining("Iceberg table updates for format version 3 are not supported yet");
        }
        finally {
            dropTable(tableName);
            dropTable(sourceTable);
        }
    }

    @Test
    public void testOptimizeOnV3Table()
    {
        String tableName = "test_v3_optimize";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, category VARCHAR, value DOUBLE) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'A', 100.0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'B', 200.0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'A', 150.0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'C', 300.0)", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'A', 100.0), (2, 'B', 200.0), (3, 'A', 150.0), (4, 'C', 300.0)");

            assertQuerySucceeds(format("CALL system.rewrite_data_files(schema => '%s', table_name => '%s', options => map(array['rewrite-all'], array['true']))", TEST_SCHEMA, tableName));

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'A', 100.0), (2, 'B', 200.0), (3, 'A', 150.0), (4, 'C', 300.0)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testMetadataTablesThrowOnUnsupportedFormatVersion()
    {
        // Tests unsupported format versions throw clear errors instead of silent data loss
        int unsupportedVersion = MAX_FORMAT_VERSION_FOR_METADATA_TABLES + 1;
        String tableName = "test_unsupported_version_table";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, category VARCHAR, value DOUBLE) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'A', 100.0)", 1);
            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'B', 200.0)", 1);
            Table table = loadTable(tableName);
            table.updateProperties().set("format-version", String.valueOf(unsupportedVersion)).commit();
            assertQueryFails("SELECT * FROM \"" + tableName + "$files\"",
                    format("Cannot read Iceberg manifest files for table format version %s \\(max supported: %s\\).*",
                            unsupportedVersion, MAX_FORMAT_VERSION_FOR_METADATA_TABLES));
            assertQueryFails("SELECT * FROM \"" + tableName + "$partitions\"",
                    format("Cannot read Iceberg manifest files for table format version %s \\(max supported: %s\\).*",
                            unsupportedVersion, MAX_FORMAT_VERSION_FOR_METADATA_TABLES));
            assertQueryFails("SELECT * FROM \"" + tableName + "$manifests\"",
                    format("Cannot read Iceberg manifest files for table format version %s \\(max supported: %s\\).*",
                            unsupportedVersion, MAX_FORMAT_VERSION_FOR_METADATA_TABLES));
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * Writes a deletion vector with Iceberg's own writer and reads it back through Presto. Because
     * the vector is produced by {@link BaseDVFileWriter} rather than assembled by the test, this
     * pins Presto's decoder against the format Iceberg actually emits -- the blob framing, the
     * checksum and the little-endian bitmap layout all have to agree, or the deleted rows come
     * back.
     */
    @Test
    public void testPuffinDeletionVectorsAreApplied()
            throws Exception
    {
        String tableName = "test_puffin_deletion_vectors_applied";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')", 5);

            Table table = loadTable(tableName);
            // Positions are file-relative, so the rows to delete are only predictable while the
            // insert landed in a single file.
            List<FileScanTask> tasks = new ArrayList<>();
            try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
                planned.forEach(tasks::add);
            }
            assertEquals(tasks.size(), 1, "expected the insert to produce exactly one data file");
            FileScanTask task = tasks.get(0);

            // Delete 'two' and 'four', at positions 1 and 3.
            deleteWithDeletionVector(table, task, ImmutableList.of(1L, 3L));

            assertQuery("SELECT id, value FROM " + tableName, "VALUES (1, 'one'), (3, 'three'), (5, 'five')");
            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 3");
            // The vector must survive a predicate that pushes down to the reader.
            assertQuery("SELECT value FROM " + tableName + " WHERE id >= 3", "VALUES 'three', 'five'");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * A vector covering every position leaves nothing behind, which is the case where an
     * off-by-one in the framing is least likely to show up as a wrong row and most likely to show
     * up as no deletion at all.
     */
    @Test
    public void testPuffinDeletionVectorDeletingEveryRow()
            throws Exception
    {
        String tableName = "test_puffin_deletion_vector_all_rows";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer, value varchar) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'one'), (2, 'two'), (3, 'three')", 3);

            Table table = loadTable(tableName);
            List<FileScanTask> tasks = new ArrayList<>();
            try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
                planned.forEach(tasks::add);
            }
            assertEquals(tasks.size(), 1, "expected the insert to produce exactly one data file");

            deleteWithDeletionVector(table, tasks.get(0), ImmutableList.of(0L, 1L, 2L));

            assertQuery("SELECT count(*) FROM " + tableName, "VALUES 0");
        }
        finally {
            dropTable(tableName);
        }
    }

    /**
     * A vector whose positions are spread beyond a single 32-bit Roaring key, which is the only
     * case that exercises the multi-bitmap path of the portable 64-bit layout -- one bitmap per
     * high-order key, in ascending key order.
     */
    @Test
    public void testPuffinDeletionVectorSpanningMultipleBitmapKeys()
            throws Exception
    {
        // No table is needed: this asserts the decoder directly, because producing a data file
        // with more than 2^32 rows is not practical.
        LongBitmapDataProvider deletedRows = new Roaring64Bitmap();
        long highKeyPosition = (1L << 32) + 7L;
        byte[] blob = serializeDeletionVector(ImmutableList.of(3L, highKeyPosition));

        DeletionVectors.deserialize(blob, "test", deletedRows);

        assertTrue(deletedRows.contains(3L));
        assertTrue(deletedRows.contains(highKeyPosition));
        assertEquals(deletedRows.getLongCardinality(), 2L);
    }

    @Test
    public void testPuffinDeletionVectorRejectsACorruptedBlob()
            throws Exception
    {
        byte[] blob = serializeDeletionVector(ImmutableList.of(1L, 2L, 3L));
        // Corrupt a byte of the bitmap, leaving the length and the stored checksum intact.
        blob[blob.length - 6] ^= 0x7f;

        assertThatThrownBy(() -> DeletionVectors.deserialize(blob, "corrupt", new Roaring64Bitmap()))
                .hasMessageContaining("failed its checksum");
    }

    /**
     * Produces a deletion-vector blob with Iceberg's writer, then reads back exactly the bytes the
     * manifest says the blob occupies -- the same range Presto's reader uses.
     */
    private byte[] serializeDeletionVector(List<Long> positions)
            throws Exception
    {
        String tableName = "test_dv_blob_source";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id integer) WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);
            Table table = loadTable(tableName);
            List<FileScanTask> tasks = new ArrayList<>();
            try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
                planned.forEach(tasks::add);
            }
            FileScanTask task = tasks.get(0);

            DeleteFile deleteFile = writeDeletionVector(table, task, positions);
            byte[] blob = new byte[toIntExact(deleteFile.contentSizeInBytes())];
            try (SeekableInputStream input = table.io().newInputFile(deleteFile.path().toString()).newStream()) {
                input.seek(deleteFile.contentOffset());
                ByteStreams.readFully(input, blob);
            }
            return blob;
        }
        finally {
            dropTable(tableName);
        }
    }

    private void deleteWithDeletionVector(Table table, FileScanTask task, List<Long> positions)
            throws Exception
    {
        table.newRowDelta()
                .addDeletes(writeDeletionVector(table, task, positions))
                .commit();
    }

    /**
     * Commits a vector listing {@code positions}, replacing any vector already covering the data
     * file. Iceberg permits at most one vector per data file and rejects a second as a concurrent
     * write, so the positions given here are the complete set of deleted positions, not an
     * addition to what a previous vector held.
     */
    private void replaceDeletionVector(String tableName, List<Long> positions)
            throws Exception
    {
        Table table = loadTable(tableName);
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> planned = table.newScan().planFiles()) {
            planned.forEach(tasks::add);
        }
        assertEquals(tasks.size(), 1, "expected exactly one data file");
        FileScanTask task = tasks.get(0);

        DeleteFile replacement = writeDeletionVector(table, task, positions);
        org.apache.iceberg.RowDelta rowDelta = table.newRowDelta();
        // Iceberg rejects adding a vector for a data file that already has one, unless the
        // validation window is bounded: left unbounded it walks the whole history and reads the
        // vector this commit is replacing as a concurrent write. Starting at the current snapshot
        // leaves no room for a concurrent commit, which is true here.
        rowDelta.validateFromSnapshot(table.currentSnapshot().snapshotId());
        task.deletes().forEach(rowDelta::removeDeletes);
        rowDelta.addDeletes(replacement).commit();
    }

    private DeleteFile writeDeletionVector(Table table, FileScanTask task, List<Long> positions)
            throws Exception
    {
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1)
                .format(FileFormat.PUFFIN)
                .build();
        DeleteWriteResult result;
        try (DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null)) {
            for (long position : positions) {
                writer.delete(task.file().path().toString(), position, task.spec(), task.file().partition());
            }
            writer.close();
            result = writer.result();
        }
        assertEquals(result.deleteFiles().size(), 1, "expected a single deletion vector file");
        return result.deleteFiles().get(0);
    }

    @Test
    public void testV3SupportedOperations()
    {
        String tableName = "test_v3_supported";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, name VARCHAR, created_date DATE, amount DECIMAL(10,2)) WITH (\"format-version\" = '3', partitioning = ARRAY['created_date'])");

            assertUpdate("INSERT INTO " + tableName + " VALUES "
                    + "(1, 'Transaction A', DATE '2024-01-01', 100.50), "
                    + "(2, 'Transaction B', DATE '2024-01-02', 250.75), "
                    + "(3, 'Transaction C', DATE '2024-01-01', 175.00)", 3);

            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES "
                            + "(1, 'Transaction A', DATE '2024-01-01', 100.50), "
                            + "(2, 'Transaction B', DATE '2024-01-02', 250.75), "
                            + "(3, 'Transaction C', DATE '2024-01-01', 175.00)");

            assertQuery(
                    "SELECT created_date, count(*), sum(amount) FROM " + tableName
                            + " GROUP BY created_date ORDER BY created_date",
                    "VALUES "
                            + "(DATE '2024-01-01', 2, 275.50), "
                            + "(DATE '2024-01-02', 1, 250.75)");

            assertQuery("SELECT * FROM " + tableName
                            + " WHERE created_date = DATE '2024-01-01' ORDER BY id",
                    "VALUES "
                            + "(1, 'Transaction A', DATE '2024-01-01', 100.50), "
                            + "(3, 'Transaction C', DATE '2024-01-01', 175.00)");

            assertUpdate("INSERT INTO " + tableName + " VALUES (4, 'Transaction D', DATE '2024-01-03', 300.00)", 1);

            assertQuery("SELECT count(*) as total_count FROM " + tableName, "SELECT 4");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testSelectFromV3TableAfterInsert()
    {
        String tableName = "test_select_v3_table";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id integer, name varchar, price decimal(10,2))"
                    + " WITH (\"format-version\" = '3')");
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'apple', 1.50), (2, 'banana', 0.75),"
                    + " (3, 'cherry', 2.00)", 3);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'apple', 1.50), (2, 'banana', 0.75),"
                            + " (3, 'cherry', 2.00)");
            assertQuery("SELECT count(*) FROM " + tableName, "SELECT 3");
            assertQuery("SELECT sum(price) FROM " + tableName, "SELECT 4.25");
            assertQuery("SELECT name FROM " + tableName
                            + " WHERE price > 1.00 ORDER BY name",
                    "VALUES ('apple'), ('cherry')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3TableWithPartitioning()
    {
        String tableName = "test_v3_partitioned_table";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id integer, category varchar, value integer)"
                    + " WITH (\"format-version\" = '3', partitioning = ARRAY['category'])");
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150)", 3);
            assertQuery("SELECT * FROM " + tableName
                            + " WHERE category = 'A' ORDER BY id",
                    "VALUES (1, 'A', 100), (3, 'A', 150)");
            assertQuery("SELECT category, sum(value) FROM " + tableName
                            + " GROUP BY category ORDER BY category",
                    "VALUES ('A', 250), ('B', 200)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testV3TableEncryptionNotSupported()
    {
        String tableName = "test_v3_encrypted";
        try {
            assertUpdate("CREATE TABLE " + tableName
                    + " (id INTEGER, data VARCHAR)"
                    + " WITH (\"format-version\" = '3')");
            // Insert data so the table has a snapshot
            // (validation requires a non-null snapshot)
            assertUpdate("INSERT INTO " + tableName
                    + " VALUES (1, 'unencrypted')", 1);

            // Set encryption property via the Iceberg API
            Table table = loadTable(tableName);
            table.updateProperties()
                    .set("encryption.key-id", "test-key-id")
                    .commit();

            // Both SELECT and INSERT should fail because the validation
            // rejects encryption
            assertThatThrownBy(() -> getQueryRunner().execute(
                    "SELECT * FROM " + tableName))
                    .hasMessageContaining(
                            "Iceberg table encryption is not supported");

            assertThatThrownBy(() -> getQueryRunner().execute(
                    "INSERT INTO " + tableName + " VALUES (2, 'more')"))
                    .hasMessageContaining(
                            "Iceberg table encryption is not supported");
        }
        finally {
            // Use Iceberg API to drop table directly, bypassing Presto's
            // validateTableForPresto
            dropTableViaIceberg(tableName);
        }
    }

    @Test
    public void testAddColumnWithDefaultRequiresV3()
    {
        String tableName = "test_add_column_default_v2";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '2')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 2);
            assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN country VARCHAR DEFAULT 'IN'",
                    "ADD COLUMN with DEFAULT values is only supported with Iceberg format version 3 or higher.*");

            assertQuery("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + TEST_SCHEMA + "' AND table_name = '" + tableName + "' ORDER BY ordinal_position",
                    "VALUES ('id'), ('name')");

            BaseTable baseTable = (BaseTable) table;
            TableOperations operations = baseTable.operations();
            TableMetadata currentMetadata = operations.current();
            operations.commit(currentMetadata, currentMetadata.upgradeToFormatVersion(3));
            table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN country VARCHAR DEFAULT 'IN'");
            assertQuery("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + TEST_SCHEMA + "' AND table_name = '" + tableName + "' ORDER BY ordinal_position",
                    "VALUES ('id'), ('name'), ('country')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testSetColumnDefaultRequiresV3()
    {
        String tableName = "test_set_column_default_v2";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '2')");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 2);
            // Try to set default on V2 table - should fail with V3 requirement error
            assertQueryFails("ALTER TABLE " + tableName + " ALTER COLUMN name SET DEFAULT 'test'",
                    "SET COLUMN DEFAULT is only supported with Iceberg format version 3 or higher.*");

            // Upgrade to V3
            BaseTable baseTable = (BaseTable) table;
            TableOperations operations = baseTable.operations();
            TableMetadata currentMetadata = operations.current();
            operations.commit(currentMetadata, currentMetadata.upgradeToFormatVersion(3));
            table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);

            // Add column with initial-default in V3
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN country VARCHAR DEFAULT 'UK'");
            table = loadTable(tableName);
            assertEquals(table.schema().findField("country").initialDefault(), "UK");
            assertEquals(table.schema().findField("country").writeDefault(), "UK");

            // Now update write-default only (initial-default should remain 'UK')
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN country SET DEFAULT 'US'");
            table = loadTable(tableName);
            assertEquals(table.schema().findField("country").initialDefault(), "UK");
            assertEquals(table.schema().findField("country").writeDefault(), "US");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testSetColumnDefaultToNull()
    {
        String tableName = "test_set_column_default_null";
        try {
            // Create V3 table with a column that has a default value
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3')");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN name VARCHAR DEFAULT 'default_name'");
            Table table = loadTable(tableName);
            assertEquals(((BaseTable) table).operations().current().formatVersion(), 3);
            // Verify initial default is set
            assertEquals(table.schema().findField("name").initialDefault(), "default_name");
            assertEquals(table.schema().findField("name").writeDefault(), "default_name");
            // Set default to NULL - this should not throw NPE and should clear the write-default
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN name SET DEFAULT NULL");
            table = loadTable(tableName);
            // Verify initial-default remains but write-default is now null
            assertEquals(table.schema().findField("name").initialDefault(), "default_name");
            assertNull(table.schema().findField("name").writeDefault());
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertWithWriteDefault()
    {
        String tableName = "test_insert_with_write_default";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3')");
            // Add a column with default value
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN country VARCHAR DEFAULT 'US'");
            // Verify the default is set
            Table table = loadTable(tableName);
            assertEquals(table.schema().findField("country").initialDefault(), "US");
            assertEquals(table.schema().findField("country").writeDefault(), "US");
            // Insert without specifying the country column - should use write-default
            assertUpdate("INSERT INTO " + tableName + " (id, name) VALUES (1, 'Alice')", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'Alice', 'US')");
            // Change the write-default (initial-default remains 'US')
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN country SET DEFAULT 'UK'");
            table = loadTable(tableName);
            assertEquals(table.schema().findField("country").initialDefault(), "US");
            assertEquals(table.schema().findField("country").writeDefault(), "UK");
            // Insert again without specifying country - should use new write-default 'UK'
            assertUpdate("INSERT INTO " + tableName + " (id, name) VALUES (2, 'Bob')", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES (1, 'Alice', 'US'), (2, 'Bob', 'UK')");
            // Insert with explicit value - should override default
            assertUpdate("INSERT INTO " + tableName + " VALUES (3, 'Charlie', 'CA')", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id", "VALUES (1, 'Alice', 'US'), (2, 'Bob', 'UK'), (3, 'Charlie', 'CA')");
            // Set write-default to NULL
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN country SET DEFAULT NULL");
            table = loadTable(tableName);
            assertEquals(table.schema().findField("country").initialDefault(), "US");
            assertNull(table.schema().findField("country").writeDefault());
            // Insert without specifying country - should preserve materialized NULL, not initial-default
            assertUpdate("INSERT INTO " + tableName + " (id, name) VALUES (4, 'Dave')", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'Alice', 'US'), (2, 'Bob', 'UK'), (3, 'Charlie', 'CA'), (4, 'Dave', NULL)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @DataProvider(name = "withPartitioning")
    public String[][] withPartitioning()
    {
        return new String[][] {
                {"PARQUET", ""},
                {"PARQUET", " WITH(partitioning = 'identity')"},
                {"ORC", ""},
                {"ORC", " WITH(partitioning = 'identity')"}
        };
    }
    @Test(dataProvider = "withPartitioning")
    public void testInsertWithPartitionEvolution(String fileFormat, String withPartitioning)
    {
        String tableName = "test_insert_with_write_default_" + fileFormat.toLowerCase() + (withPartitioning.isEmpty() ? "_unpartitioned" : "_partitioned");
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR) WITH (\"format-version\" = '3', format = '" + fileFormat + "')");
            assertUpdate("INSERT INTO " + tableName + " VALUES(1, 'Alice'), (2, 'Bob')", 2);
            // Add a column with default value
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN country VARCHAR DEFAULT 'US'" + withPartitioning);
            // Verify the default is set
            Table table = loadTable(tableName);
            assertEquals(table.schema().findField("country").initialDefault(), "US");
            assertEquals(table.schema().findField("country").writeDefault(), "US");
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN country SET DEFAULT 'UK'");
            // Insert without specifying the country column - should use write-default
            assertUpdate("INSERT INTO " + tableName + " (id, name) VALUES (3, 'Carol')", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES(1, 'Alice', 'US'), (2, 'Bob', 'US'), (3, 'Carol', 'UK')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country = 'US'", "VALUES(1, 'Alice', 'US'), (2, 'Bob', 'US')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country = 'UK'", "VALUES(3, 'Carol', 'UK')");
            assertUpdate("INSERT INTO " + tableName + " (id, name, country) VALUES (4, 'David', NULL), (5, 'Frank', 'FR')", 2);
            assertQuery("SELECT * FROM " + tableName, "VALUES(1, 'Alice', 'US'), (2, 'Bob', 'US'), (3, 'Carol', 'UK'), (4, 'David', NULL), (5, 'Frank', 'FR')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country = 'US'", "VALUES(1, 'Alice', 'US'), (2, 'Bob', 'US')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country <> 'US'", "VALUES(3, 'Carol', 'UK'), (5, 'Frank', 'FR')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country = 'UK'", "VALUES(3, 'Carol', 'UK')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country IS NULL", "VALUES(4, 'David', NULL)");
            assertQuery("SELECT * FROM " + tableName + " WHERE country IS NOT NULL", "VALUES(1, 'Alice', 'US'), (2, 'Bob', 'US'), (3, 'Carol', 'UK'), (5, 'Frank', 'FR')");
            assertQuery("SELECT * FROM " + tableName + " WHERE country in ('US', 'FR', 'CN')", "VALUES(1, 'Alice', 'US'), (2, 'Bob', 'US'), (5, 'Frank', 'FR')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertWithExplicitNullOverridesWriteDefault()
    {
        String tableName = "test_insert_explicit_null_overrides_write_default";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3')");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN status VARCHAR DEFAULT 'ACTIVE'");

            assertUpdate("INSERT INTO " + tableName + " (id, status) VALUES (1, NULL)", 1);
            assertUpdate("INSERT INTO " + tableName + " (id) VALUES (2)", 1);

            assertQuery("SELECT id, status FROM " + tableName + " ORDER BY id",
                    "VALUES (1, NULL), (2, 'ACTIVE')");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertWithMultipleWriteDefaultColumns()
    {
        String tableName = "test_insert_multiple_write_default_columns";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3')");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN country VARCHAR DEFAULT 'US'");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN priority INTEGER DEFAULT 10");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN is_enabled BOOLEAN DEFAULT true");

            assertUpdate("INSERT INTO " + tableName + " (id) VALUES (1)", 1);
            assertUpdate("INSERT INTO " + tableName + " (id, country) VALUES (2, 'UK')", 1);

            assertQuery("SELECT id, country, priority, is_enabled FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'US', 10, true), (2, 'UK', 10, true)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertWithWriteDefaultDifferentDataTypes()
    {
        String tableName = "test_insert_write_default_different_types";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id INTEGER) WITH (\"format-version\" = '3')");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN name VARCHAR DEFAULT 'Unknown'");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN score DOUBLE DEFAULT 0.0");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN count BIGINT DEFAULT 0");
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN active BOOLEAN DEFAULT false");

            assertUpdate("INSERT INTO " + tableName + " (id) VALUES (1)", 1);

            assertQuery("SELECT id, name, score, count, active FROM " + tableName,
                    "VALUES (1, 'Unknown', 0.0, 0, false)");
        }
        finally {
            dropTable(tableName);
        }
    }

    @Test
    public void testInsertWithWriteDefaultOnPartitionedTable()
    {
        String tableName = "test_insert_write_default_partitioned_table";
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, ds DATE) WITH (\"format-version\" = '3', format = 'PARQUET', partitioning = ARRAY['ds'])");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, DATE '2023-01-01')", 1);
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN region VARCHAR DEFAULT 'US'");

            assertUpdate("INSERT INTO " + tableName + " (id, ds) VALUES (2, DATE '2023-01-02')", 1);
            assertUpdate("ALTER TABLE " + tableName + " ALTER COLUMN region SET DEFAULT 'EU'");
            assertUpdate("INSERT INTO " + tableName + " (id, ds) VALUES (3, DATE '2023-01-03')", 1);

            assertQuery("SELECT id, ds, region FROM " + tableName + " ORDER BY id",
                    "VALUES (1, DATE '2023-01-01', 'US'), (2, DATE '2023-01-02', 'US'), (3, DATE '2023-01-03', 'EU')");
        }
        finally {
            dropTable(tableName);
        }
    }

    private Table loadTable(String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(), ICEBERG_CATALOG,
                getProperties(), new Configuration());
        return catalog.loadTable(TableIdentifier.of(TEST_SCHEMA, tableName));
    }

    private Map<String, String> getProperties()
    {
        File metastoreDir = getCatalogDirectory();
        return ImmutableMap.of("warehouse", metastoreDir.toString());
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner()
                .getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(
                dataDirectory, HADOOP.name(),
                new IcebergConfig().getFileFormat(), false);
        return catalogDirectory.toFile();
    }

    private void dropTableViaIceberg(String tableName)
    {
        Catalog catalog = CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(), ICEBERG_CATALOG,
                getProperties(), new Configuration());
        catalog.dropTable(
                TableIdentifier.of(TEST_SCHEMA, tableName), true);
    }
}
