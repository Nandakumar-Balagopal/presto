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

Verified by running it, not by reading it.

- **The branch compiles.** It did not before: `presto-spi` failed outright, so
  nothing downstream had ever been built and none of the earlier work had been
  exercised.
- **The coordinator starts.** `ChangeKindEnumType` was registered twice on one
  unconditional startup path and `addUserDefinedType` rejects a duplicate, so
  startup always threw.
- **`system.changes(table, from, to)`** runs end to end on a V3 Iceberg table
  and returns non-null `$row_id` values. This needed four separate fixes: plan
  fragment serialization for `ConnectorTableVersion` and `ChangesFunctionSplit`,
  a real `SessionPropertyManager` instead of a testing one, the row-lineage
  column looked up under the name the connector exposes, and change-set splits
  carrying a real `first_row_id` instead of the V1/V2 sentinel.
- **`affected_identifiers`** backs the fresh branch of both candidates, with a
  null-safe anti-join.
- **A row-level candidate** is offered to the cost picker alongside the
  partition-level one and full recompute, driven by the connector's
  `changedRowsPredicates`.

`mvn validate` passes for `presto-spi`, `presto-main-base`, `presto-iceberg`,
`presto-main` and `presto-tests`, which is where `presto-checks.xml` runs.

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

- Per-MV `row_level_incremental_refresh` property (`true`/`false`/`auto`). Only
  the `materialized_view_row_level_incremental_strategy` session property
  exists, so the spec's MV-versus-session precedence rule is unimplemented.
- `MATERIALIZED_VIEW_ROW_LEVEL_REJECTED_ON_COST` warning.
- Feed `estimateChangeSetSize` into the stats calculator so the row-level leaf
  is costed from change-set cardinality rather than a default.
- Case A is dead code: `projectClosure` returns empty for any non-empty closure,
  so the first `ProjectNode` destroys the closure and every aggregation takes
  Case B. Fixing it needs pass-through column bindings on `PlanVariants`.
- `visitExcept` still uses partition replacement; the delta-minus rule needs
  delta-plus/delta-minus tracking.
- Classical IVM for SUM/COUNT.

### Blocked on V3 row-level write support

- Row-preserving MVs, which need the `$rowId_origin` storage column.
- The atomic delete-plus-insert refresh commit. The main branch has
  refresh-handle, fragment-payload and planner-finish scaffolding but generates
  no real delete fragments and performs no Iceberg commit.
- Deletion vectors in change sets.
- The Tier 4 correctness suite's update and row-level-delete cases.

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
