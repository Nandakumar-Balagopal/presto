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
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Edge cases for row-level incremental refresh of an aggregating materialized view over an
 * append-only Iceberg V3 base.
 *
 * <p>Every case asserts the same invariant: after refreshing, the view must equal a direct
 * aggregate over the base. That is the property row-level refresh can break in either direction --
 * over-excluding rows from the fresh branch loses data, under-excluding duplicates it -- and it is
 * checked by comparing two Presto queries rather than a literal, so the expected value cannot drift
 * out of step with the data.
 */
@Test(singleThreaded = true)
public class TestIcebergRowLevelRefreshEdgeCases
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

    private Session refreshSession()
    {
        return Session.builder(getSession())
                .setSystemProperty("materialized_view_default_refresh_type", "INCREMENTAL")
                .setSystemProperty("materialized_view_incremental_refresh_strategy", "ALWAYS")
                .setSystemProperty("materialized_view_row_level_incremental_strategy", "ALWAYS")
                .build();
    }

    private Session stitchSession()
    {
        return Session.builder(getSession())
                .setSystemProperty("materialized_view_stale_read_behavior", "USE_STITCHING")
                .setSystemProperty("materialized_view_stitching_strategy", "ALWAYS")
                .setSystemProperty("materialized_view_row_level_incremental_strategy", "ALWAYS")
                .build();
    }

    /**
     * The invariant. Compared as two Presto queries because the expected side of assertQuery runs
     * against H2, which does not hold the Iceberg tables.
     */
    private void assertViewMatchesBase(String viewQuery, String baseQuery)
    {
        assertEquals(
                computeActual(viewQuery).getMaterializedRows(),
                computeActual(baseQuery).getMaterializedRows(),
                "materialized view disagrees with a direct aggregate over the base");
    }

    /**
     * Refreshes the view and returns how the refresh was planned.
     *
     * <p>Results alone cannot show which granularity ran, because a partition-level or full refresh
     * returns the same answer as a row-level one. Every case here therefore pins the granularity as
     * well, so a silent fall back to full recompute fails the test instead of passing it.
     */
    private RefreshOutcome refreshAndReport(String view)
    {
        MaterializedResult explained = computeActual(refreshSession(),
                "EXPLAIN (TYPE LOGICAL) REFRESH MATERIALIZED VIEW " + view);
        String plan = (String) explained.getMaterializedRows().get(0).getField(0);
        String reasons = explained.getWarnings().stream()
                .map(warning -> warning.getMessage())
                .collect(joining("; "));
        computeActual(refreshSession(), "REFRESH MATERIALIZED VIEW " + view);
        return new RefreshOutcome(plan.contains("_last_updated_sequence_number"), reasons);
    }

    private void refreshExpectingRowLevel(String view)
    {
        RefreshOutcome outcome = refreshAndReport(view);
        assertTrue(outcome.rowLevel,
                "expected " + view + " to refresh from the row-level changed-rows predicate. Reported: " + outcome.reasons);
    }

    private void refreshExpectingFallback(String view)
    {
        assertFalse(refreshAndReport(view).rowLevel, "expected " + view + " to decline row-level refresh");
    }

    private static class RefreshOutcome
    {
        private final boolean rowLevel;
        private final String reasons;

        RefreshOutcome(boolean rowLevel, String reasons)
        {
            this.rowLevel = rowLevel;
            this.reasons = reasons;
        }
    }

    // ------------------------------------------------------------------ values

    @Test
    public void testNullGroupingKey()
    {
        // SQL makes NULL its own group, so the anti-join has to treat a null identifier as joinable.
        // An equi-join would leave the stale row for the null group in the fresh branch.
        run("null_key",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, SUM(amount) AS total FROM %s GROUP BY region",
                "VALUES ('NA', 10), (NULL, 20), ('EU', 5)",
                "VALUES (NULL, 7), ('NA', 1)",
                "SELECT region, total FROM %s ORDER BY region",
                "SELECT region, SUM(amount) FROM %s GROUP BY region ORDER BY region");
    }

    @Test
    public void testEmptyStringAndWhitespaceKeys()
    {
        run("blank_key",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, SUM(amount) AS total FROM %s GROUP BY region",
                "VALUES ('', 1), (' ', 2), ('NA', 3)",
                "VALUES ('', 10), ('  ', 20)",
                "SELECT region, total FROM %s ORDER BY region",
                "SELECT region, SUM(amount) FROM %s GROUP BY region ORDER BY region");
    }

    @Test
    public void testBigintGroupingKeyIncludingZeroAndNegative()
    {
        run("int_key",
                "bucket bigint, amount bigint",
                "ARRAY['bucket']",
                "SELECT bucket, SUM(amount) AS total FROM %s GROUP BY bucket",
                "VALUES (0, 1), (-1, 2), (9223372036854775807, 3)",
                "VALUES (0, 10), (-1, 20), (5, 30)",
                "SELECT bucket, total FROM %s ORDER BY bucket",
                "SELECT bucket, SUM(amount) FROM %s GROUP BY bucket ORDER BY bucket");
    }

    @Test
    public void testDateGroupingKey()
    {
        run("date_key",
                "ds date, amount bigint",
                "ARRAY['ds']",
                "SELECT ds, SUM(amount) AS total FROM %s GROUP BY ds",
                "VALUES (DATE '2024-01-01', 1), (DATE '2024-01-02', 2)",
                "VALUES (DATE '2024-01-01', 10), (DATE '2024-02-01', 20)",
                "SELECT ds, total FROM %s ORDER BY ds",
                "SELECT ds, SUM(amount) FROM %s GROUP BY ds ORDER BY ds");
    }

    @Test
    public void testCompositeGroupingKey()
    {
        run("composite_key",
                "region varchar, tier varchar, amount bigint",
                "ARRAY['region', 'tier']",
                "SELECT region, tier, SUM(amount) AS total FROM %s GROUP BY region, tier",
                "VALUES ('NA', 'a', 1), ('NA', 'b', 2), ('EU', 'a', 3)",
                "VALUES ('NA', 'a', 10), ('APAC', 'c', 20)",
                "SELECT region, tier, total FROM %s ORDER BY region, tier",
                "SELECT region, tier, SUM(amount) FROM %s GROUP BY region, tier ORDER BY region, tier");
    }

    // -------------------------------------------------------------- aggregates

    @Test
    public void testCountStarAndMultipleAggregates()
    {
        run("multi_agg",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, COUNT(*) AS n, SUM(amount) AS total, MIN(amount) AS lo, MAX(amount) AS hi FROM %s GROUP BY region",
                "VALUES ('NA', 10), ('NA', 30), ('EU', 20)",
                "VALUES ('NA', 5), ('APAC', 7)",
                "SELECT region, n, total, lo, hi FROM %s ORDER BY region",
                "SELECT region, COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM %s GROUP BY region ORDER BY region");
    }

    @Test
    public void testAggregatesOverNullMeasure()
    {
        // COUNT(col) skips nulls while COUNT(*) does not, so a group whose appended rows are all
        // null must still recompute both correctly.
        run("null_measure",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, COUNT(*) AS n, COUNT(amount) AS n_amount, SUM(amount) AS total FROM %s GROUP BY region",
                "VALUES ('NA', 10), ('EU', NULL)",
                "VALUES ('NA', NULL), ('EU', 5), ('APAC', NULL)",
                "SELECT region, n, n_amount, total FROM %s ORDER BY region",
                "SELECT region, COUNT(*), COUNT(amount), SUM(amount) FROM %s GROUP BY region ORDER BY region");
    }

    @Test
    public void testViewWithFilterBeforeAggregation()
    {
        // The delta recomputes whole groups from the current base, so the view's own filter must be
        // reapplied there and not just at the leaf.
        run("filtered_view",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, SUM(amount) AS total FROM %s WHERE amount > 5 GROUP BY region",
                "VALUES ('NA', 10), ('NA', 1), ('EU', 20)",
                "VALUES ('NA', 2), ('NA', 50), ('APAC', 1)",
                "SELECT region, total FROM %s ORDER BY region",
                "SELECT region, SUM(amount) FROM %s WHERE amount > 5 GROUP BY region ORDER BY region");
    }

    @Test
    public void testHolisticAggregate()
    {
        // Case B claims correctness for any aggregation because it recomputes whole groups.
        run("holistic",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, approx_distinct(amount) AS distinct_amounts FROM %s GROUP BY region",
                "VALUES ('NA', 10), ('NA', 10), ('NA', 20), ('EU', 1)",
                "VALUES ('NA', 30), ('APAC', 2)",
                "SELECT region, distinct_amounts FROM %s ORDER BY region",
                "SELECT region, approx_distinct(amount) FROM %s GROUP BY region ORDER BY region");
    }

    // -------------------------------------------------------------- sequencing

    @Test
    public void testRefreshWithNoChangesIsIdempotent()
    {
        String base = "seq_noop_base";
        String view = "seq_noop_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20)");
            createView(view, base, "ARRAY['region']", "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            // Nothing changes between these, so none of them has any delta to compute.
            refreshExpectingFallback(view);
            refreshExpectingFallback(view);
            refreshExpectingFallback(view);
            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    @Test
    public void testSeveralAppendsThenOneRefresh()
    {
        String base = "seq_multi_base";
        String view = "seq_multi_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20)");
            createView(view, base, "ARRAY['region']", "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            refreshExpectingFallback(view);

            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 1)");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('APAC', 2)");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 3), ('EU', 4)");
            refreshExpectingRowLevel(view);

            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    @Test
    public void testInterleavedStaleReadAndRefresh()
    {
        String base = "seq_inter_base";
        String view = "seq_inter_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20)");
            createView(view, base, "ARRAY['region']", "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            refreshExpectingFallback(view);

            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 5), ('APAC', 7)");

            // Stale read must already agree with the base, and must not disturb the later refresh.
            assertEquals(
                    computeActual(stitchSession(), "SELECT region, total FROM " + view + " ORDER BY region").getMaterializedRows(),
                    computeActual("SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region").getMaterializedRows(),
                    "stale read disagrees with the base");

            refreshExpectingRowLevel(view);
            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    @Test
    public void testAppendTouchingEveryExistingGroup()
    {
        run("touch_all",
                "region varchar, amount bigint",
                "ARRAY['region']",
                "SELECT region, SUM(amount) AS total FROM %s GROUP BY region",
                "VALUES ('NA', 10), ('EU', 20), ('APAC', 30)",
                "VALUES ('NA', 1), ('EU', 2), ('APAC', 3)",
                "SELECT region, total FROM %s ORDER BY region",
                "SELECT region, SUM(amount) FROM %s GROUP BY region ORDER BY region");
    }

    /**
     * The delete-blind case. affected_identifiers is built only from rows still present in the base,
     * so a group whose rows have all been deleted contributes nothing and its stale row survives.
     * This connector permits a whole-partition DELETE on a V3 table, so this is reachable.
     */
    @Test
    public void testWholePartitionDeleteThenRefresh()
    {
        String base = "del_base";
        String view = "del_mv";
        try {
            create(base, "region varchar, amount bigint", "ARRAY['region']");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 10), ('EU', 20), ('APAC', 30)");
            createView(view, base, "ARRAY['region']", "SELECT region, SUM(amount) AS total FROM " + base + " GROUP BY region");
            refreshExpectingFallback(view);

            // Removes the EU group entirely, and appends to another group.
            assertQuerySucceeds("DELETE FROM " + base + " WHERE region = 'EU'");
            assertQuerySucceeds("INSERT INTO " + base + " VALUES ('NA', 5)");

            // affected_identifiers is built only from rows still present in the base, so a group
            // whose rows were all deleted would contribute nothing and its stale row would survive.
            // What keeps this correct today is that the connector reports no partition staleness for
            // a range containing a delete, so the refresh declines both granularities and recomputes
            // in full. This asserts that protection rather than leaving it incidental: if a future
            // change lets row-level through here, this fails instead of silently going stale.
            refreshExpectingFallback(view);

            assertViewMatchesBase(
                    "SELECT region, total FROM " + view + " ORDER BY region",
                    "SELECT region, SUM(amount) FROM " + base + " GROUP BY region ORDER BY region");
        }
        finally {
            drop(view, base);
        }
    }

    // ------------------------------------------------------------- boilerplate

    private void run(
            String name,
            String columns,
            String partitioning,
            String viewSqlTemplate,
            String initialRows,
            String appendedRows,
            String viewQueryTemplate,
            String baseQueryTemplate)
    {
        String base = name + "_base";
        String view = name + "_mv";
        try {
            create(base, columns, partitioning);
            assertQuerySucceeds("INSERT INTO " + base + " " + initialRows);
            createView(view, base, partitioning, String.format(viewSqlTemplate, base));
            // The first refresh is the initial load: the view has never been materialized, so there
            // is no recorded version to compute changes against.
            refreshExpectingFallback(view);

            assertQuerySucceeds("INSERT INTO " + base + " " + appendedRows);
            refreshExpectingRowLevel(view);

            assertViewMatchesBase(
                    String.format(viewQueryTemplate, view),
                    String.format(baseQueryTemplate, base));
        }
        finally {
            drop(view, base);
        }
    }

    private void create(String base, String columns, String partitioning)
    {
        assertQuerySucceeds("CREATE TABLE " + base + " (" + columns + ") "
                + "WITH (\"format-version\" = '3', partitioning = " + partitioning + ")");
    }

    private void createView(String view, String base, String partitioning, String viewSql)
    {
        assertQuerySucceeds("CREATE MATERIALIZED VIEW " + view
                + " WITH (partitioning = " + partitioning + ", refresh_type = 'INCREMENTAL')"
                + " AS " + viewSql);
    }

    private void drop(String view, String base)
    {
        assertQuerySucceeds("DROP MATERIALIZED VIEW IF EXISTS " + view);
        assertQuerySucceeds("DROP TABLE IF EXISTS " + base);
    }
}
