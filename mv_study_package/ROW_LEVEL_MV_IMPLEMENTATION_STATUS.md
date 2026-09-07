# Row-Level Incremental Materialized View Refresh

## Source documents

The authoritative design documents are:

- `/Users/nandakumarb/Downloads/MV — M3 Row-Level Incremental Refresh Handover (1).pdf`
- `/Users/nandakumarb/Downloads/MV — Row-Level Incremental Refresh (1).pdf`

The handover document supersedes the earlier draft when the two differ.

## Current branches

- `mv-row-level-incremental-refresh` — main feature branch.
- `mv-row-level-tier0` — isolated Tier 0 SPI/foundation branch, based on `origin/master`.
- `master` — local fork branch tracking the fork's master baseline.

Remote repository:

`https://github.com/Nandakumar-Balagopal/presto.git`

## Blocking limitation: V3 tables cannot take row-level writes

Row-level refresh needs durable row identity, which needs Iceberg format
version 3. This connector cannot apply a row-level write to a V3 table. The two
gates in `IcebergUtil` are mutually exclusive:

```java
MAX_FORMAT_VERSION_FOR_ROW_LEVEL_OPERATIONS = 2;  // DELETE/UPDATE/MERGE need v <= 2
MIN_FORMAT_VERSION_FOR_ROW_LINEAGE          = 3;  // _row_id needs v >= 3
```

`beginDelete`, `beginUpdate` and `beginMerge` all raise
`"Iceberg table updates for format version 3 are not supported yet"`, and
`TestIcebergV3.testDeleteOnV3TableNotSupported` asserts that as intended
behaviour. Two independent confirmations of the same wall:

- Iceberg 1.9.2's `BaseIncrementalChangelogScan` raises
  `"Delete files are currently not supported in changelog scans"` for any
  snapshot range containing delete manifests.
- Reading V3 deletion vectors raises
  `"Iceberg deletion vectors using PUFFIN format are not supported"`.

So on a V3 table the only reachable mutations are appends and whole-file
deletes. The handover's stated primary workload — a CDC-fed fact table taking a
small fraction of row *updates* per window — cannot be produced on this
codebase, let alone refreshed. Tier 4's "inserts, deletes and updates" suite is
not reachable until the connector gains V3 row-level write support.

Everything below was therefore built and validated for append and
whole-file-delete change sets.

## What works

Verified by running it, with plan-shape assertions rather than answer checks
alone -- a full recompute returns the right answer too, so correctness by itself
never shows which plan ran.

### Reading a stale materialized view: works

Row-level stitching returns correct results over an append-only V3 base, on
partitioned and unpartitioned storage alike. The fresh branch anti-joins the
storage table against `affected_identifiers` and the delta recomputes the
affected groups:

```
FilterProject[IS_NULL(affected)]
└── LeftJoin[NOT(region IS DISTINCT FROM region_96)]
    ├── TableScan[__mv_storage__…]
    └── Project[affected := true] ← Aggregate(DISTINCT)[region_96]
        └── ScanFilterProject[base, _last_updated_sequence_number > 1]
```

### REFRESH MATERIALIZED VIEW: works in one safe configuration

The refresh recomputes only changed groups, gated on two conditions:

- the storage table is partitioned by exactly the view's grouping columns, so
  replacing the partitions of the files written replaces exactly those groups;
- the status reports partition refresh data, because without it the connector
  commits by overwriting the whole storage table.

Outside that configuration row-level declines and the refresh stays
partition-level. Both directions are covered by `TestIcebergRowLevelStitching`.

### Enablement

Nothing is on by default. Reading a stale view needs
`materialized_view_stale_read_behavior=USE_STITCHING` (the default re-runs the
view query), `materialized_view_stitching_strategy` other than `NEVER`, and
`materialized_view_row_level_incremental_strategy` other than `NEVER`. Refresh
additionally needs the view to have `refresh_type = 'INCREMENTAL'`, since the
default refresh type is `FULL` and is recorded when the view is created.

Row-level defaults to `NEVER` because `affected_identifiers` is still built only
from `from_current_base`. A group whose rows were all deleted has nothing left to
identify it, so its stale rows would survive. Appends cannot hit this; a
whole-partition `DELETE` can.

## Correctness defects fixed along the way

Each of these produced wrong answers or a crash rather than a clean fallback:

- Change-set splits reported `firstRowId = -1`, the V1/V2 sentinel, so
  `computeRowIdBlock` short-circuited to an all-null block before reading the
  file. **Every row of every change set had a null `$row_id`**, which alone made
  row-level refresh impossible.
- `buildStalePredicate` translated stale disjuncts with `TupleDomain.transform`,
  which silently drops unresolvable columns. An unbound disjunct widened to
  `all()`, became `TRUE`, and the delta branch recomputed the whole base while
  the fresh branch still contributed its rows — **duplicate output**, violating
  the disjointness invariant. Now declines to stitch.
- `buildColumnToVariableMapping` collected into an `ImmutableMap`, whose
  `build()` throws on a duplicate key. A self-join MV **failed the query**
  instead of falling back.
- `buildExpandedDelta` shared one subtree instance between the delta and current
  variants, placing the same plan node ids at two positions in one plan.
- `estimateChangeSetSize` returned **bytes** where the cost picker consumes a row
  count, which would push the picker away from row-level exactly when it is
  cheapest.
- `system.changes` rejected any call whose `FROM_VERSION` exceeded its
  `TO_VERSION`. Snapshot ids are random 64-bit values, so this rejected valid
  ranges about half the time.
- Neither `DelegatingMetadataManager` nor `StatsRecordingMetadataManager`
  overrode the delete-plus-insert `finishRefreshMaterializedView`, so an atomic
  refresh through either wrapper failed even on a capable connector.

## The partition-level test suite was vacuous

`DifferentialPlanRewriter` keys stale constraints on the `SchemaTableName` the
connector reports. `TestDifferentialPlanRewriter` keyed them on `tiny.orders`
while TPCH reports `sf0.01.orders`, so every lookup missed, `or([])` collapsed to
`FALSE`, and every assertion described a degenerate plan where the delta was
`Filter(FALSE)`. Case B expansion had never once executed. `setUp` now asserts
the binding, so this cannot regress silently.

`buildStitchedPlan` had no coverage at all — nothing anywhere constructed a
`MaterializedViewScanNode`, which is why the fresh branch could be rewritten
from a filter to an anti-join without any test noticing. It is covered now.

## Remaining work

### Reachable today

- `from_changeset_deletes`, the second input to `affected_identifiers`. Until it
  lands, row-level is correct only for change sets without deletions, which is
  why it defaults to `NEVER`. Iceberg's `CHANGELOG` table type may be scannable
  as an ordinary `TableScanNode`, which would be far cheaper than constructing a
  table-function node inside the optimizer.
- `estimateChangeSetSize` is still not read by the stats calculator, so the cost
  picker prices the row-level leaf from generic table-scan statistics and will
  rarely prefer it on `AUTOMATIC`.
- A refresh that commits by replacing arbitrary rows rather than whole
  partitions. That needs the delete-fragment path: `RefreshMaterializedViewCommit`
  carries delete fragments end to end, but its only producer passes an empty
  list, and the Iceberg commit uses `ReplacePartitions`. With it, the
  storage-partitioning precondition above disappears.
- Per-MV `row_level_incremental_refresh` property and its precedence against the
  session property.
- `MATERIALIZED_VIEW_ROW_LEVEL_REJECTED_ON_COST` warning.
- A stale partition-level warning still claims a fall back to full recompute in
  cases where row-level then succeeded.
- `visitExcept` still uses partition replacement rather than the delta-minus
  rule, which needs delta-plus/delta-minus tracking.
- Classical IVM for SUM and COUNT.

### Blocked on V3 row-level write support

- Row-preserving materialized views, which need the `$rowId_origin` column.
- Deletion vectors in change sets.
- The Tier 4 update and row-level-delete cases.

## Scope note on the query path

Query-time row-level stitching only reads the MV storage table and unions it
with a recomputed delta. It performs no write, so it is deliberately not gated
on `supportsMaterializedViewRowLevelRefresh`; that capability governs the atomic
commit the REFRESH path needs. The REFRESH path remains gated off, and
`Metadata.supportsMaterializedViewRowLevelRefresh` defaults to false so a
connector that has not implemented the commit falls back to partition-level.

## Validation

Local validation works. The `.m2` permission error noted in the previous
revision of this document no longer occurs, and it had been masking the fact
that the branch did not compile. Use JDK 17:

```sh
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home
mvn -B -pl presto-main-base -am -DskipTests install
mvn -B -o -pl presto-main-base -Dtest=TestDifferentialPlanRewriter test
mvn -B -o -pl presto-iceberg   -Dtest=TestIcebergV3 test
mvn -B -o -pl presto-spi,presto-main-base,presto-iceberg,presto-main,presto-tests -DskipTests validate
```
