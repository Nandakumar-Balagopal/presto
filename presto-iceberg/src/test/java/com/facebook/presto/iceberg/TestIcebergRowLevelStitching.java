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

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Row-level stitching of a stale aggregating materialized view over an append-only Iceberg V3 base.
 *
 * <p>V3 is required because row-level staleness comes from row lineage, and this connector caps
 * row-level writes at format version 2, so appends are the only base mutation a V3 table can take.
 *
 * <p>Correctness alone does not show the row-level plan was used -- a full recompute also returns
 * the right answer -- so each case asserts the plan shape as well.
 */
@Test(singleThreaded = true)
public class TestIcebergRowLevelStitching
        extends AbstractTestQueryFramework
{
    private File warehouseLocation;
    private TestingHttpServer restServer;
    private String serverUri;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();
        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();
        serverUri = restServer.getBaseUrl().toString();
        super.init();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(restConnectorProperties(serverUri))
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setSchemaName("test_schema")
                .setCreateTpchTables(false)
                .setExtraProperties(ImmutableMap.of(
                        "experimental.legacy-materialized-views", "false",
                        "experimental.allow-legacy-materialized-views-toggle", "true"))
                .build().getQueryRunner();
    }

    /**
     * Stitching only runs when a stale read is told to stitch; the default is to re-run the view
     * query. Row-level additionally defaults to NEVER while the write path is incomplete.
     */
    private Session stitching(String rowLevelStrategy)
    {
        return Session.builder(getSession())
                .setSystemProperty("materialized_view_stale_read_behavior", "USE_STITCHING")
                .setSystemProperty("materialized_view_stitching_strategy", "ALWAYS")
                .setSystemProperty("materialized_view_row_level_incremental_strategy", rowLevelStrategy)
                .build();
    }

    @DataProvider(name = "partitioning")
    public Object[][] partitioning()
    {
        // Row-level staleness needs no partition metadata, so it must work either way. The
        // unpartitioned case is the one that regressed when row-level sat behind the partition gate.
        return new Object[][] {
                {"", "unpartitioned"},
                {", partitioning = ARRAY['region']", "partitioned"},
        };
    }

    @Test(dataProvider = "partitioning")
    public void testRowLevelStitchingOverAppendOnlyBase(String partitioningClause, String label)
    {
        String base = "rls_base_" + label;
        String view = "rls_mv_" + label;
        try {
            assertQuerySucceeds("CREATE TABLE " + base + " (region varchar, amount bigint) "
                    + "WITH (\"format-version\" = '3'" + partitioningClause + ")");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20), ('NA', 30)");

            assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                    + " AS SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            assertQuerySucceeds("REFRESH MATERIALIZED VIEW " + view);

            // Append only: one row into an existing group, one creating a new group.
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 5), ('APAC', 7)");

            // The stitched read must equal a direct aggregate over the base: no duplicated rows from
            // the two branches overlapping, and no rows missed by the fresh branch being over-excluded.
            assertQuery(
                    stitching("ALWAYS"),
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "VALUES ('APAC', 7), ('EU', 20), ('NA', 45)");

            String plan = explainView(stitching("ALWAYS"), view);

            // The fresh branch anti-joins the storage table against affected_identifiers: a left
            // join keyed null-safely, keeping the rows that found no match.
            assertTrue(plan.contains("LeftJoin"), "expected an anti-join over the storage table, got:\n" + plan);
            assertTrue(plan.contains("IS_NULL(affected)"), "expected the unmatched-row filter on the affected marker, got:\n" + plan);
            assertTrue(plan.contains("IS DISTINCT FROM"), "identifiers must be matched null-safely, got:\n" + plan);

            // affected_identifiers is driven by the connector's changed-rows predicate over the row
            // lineage column, which is what makes this row-level rather than partition-level.
            assertTrue(plan.contains("_last_updated_sequence_number"),
                    "expected the row-level changed-rows predicate at the base scan, got:\n" + plan);

            // And it is genuinely a different plan from the partition-level candidate.
            assertFalse(plan.equals(explainView(stitching("NEVER"), view)),
                    "row-level and partition-level produced identical plans, so row-level did not fire");
        }
        finally {
            assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
            assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
        }
    }

    /**
     * The whole feature rests on an appended row being distinguishable from an untouched one. If
     * row lineage did not carry the older rows' sequence number through, the changed-rows predicate
     * would match everything and the row-level delta would silently recompute the whole table.
     */
    @Test
    public void testAppendedRowsCarryAHigherSequenceNumber()
    {
        String table = "rls_lineage";
        try {
            assertQuerySucceeds("CREATE TABLE " + table + " (region varchar, amount bigint) WITH (\"format-version\" = '3')");
            assertQuerySucceeds("INSERT INTO " + table + " VALUES ('NA', 10), ('EU', 20)");

            long recordedSequenceNumber = (long) computeActual(
                    "SELECT max(\"_last_updated_sequence_number\") FROM " + table)
                    .getMaterializedRows().get(0).getField(0);

            assertQuerySucceeds("INSERT INTO " + table + " VALUES ('NA', 5), ('APAC', 7)");

            assertQuery(
                    "SELECT count(*) FROM " + table + " WHERE \"_last_updated_sequence_number\" > " + recordedSequenceNumber,
                    "SELECT 2");
            assertQuery(
                    "SELECT count(*) FROM " + table + " WHERE \"_last_updated_sequence_number\" IS NULL",
                    "SELECT 0");
            // Row identity has to be present for the change set to name an affected row at all.
            assertQuery("SELECT count(DISTINCT \"_row_id\") FROM " + table, "SELECT 4");
        }
        finally {
            assertQuerySucceeds("DROP TABLE IF EXISTS " + table);
        }
    }

    private String explainView(Session session, String view)
    {
        MaterializedResult result = computeActual(session,
                "EXPLAIN (TYPE LOGICAL) SELECT region, total FROM " + view + " ORDER BY region");
        assertEquals(result.getMaterializedRows().size(), 1);
        return (String) result.getMaterializedRows().get(0).getField(0);
    }
}
