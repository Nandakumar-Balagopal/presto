# Append-only row-level materialized view stitching

## 1. What it is

**A stale read of a materialized view over an append-only Iceberg base is answered from the
materialized rows plus the appended ones, instead of by recomputing the view.**

Reads only. `REFRESH MATERIALIZED VIEW` is unchanged and remains partition-level or full — see §7.1
for why, with measurements.

Two view shapes allow the materialized rows to be reused. Everything else declines with a reason and
falls back to the partition-level or full plan.

| shape | what the delta contributes | what storage contributes |
|---|---|---|
| row-preserving — projection, filter, `UNION ALL` | the appended rows | every row, unchanged |
| mergeable aggregate — `SUM`, `COUNT` | an increment per group | the value to add it to |

The delta is identified by a sequence number:

```
watermark = the base snapshot's sequence number at the last refresh
delta     = rows whose $data_sequence_number > watermark
```

Nothing is enabled by default.

## 2. Why append-only, and why that is not a format-version question

A predicate over a sequence number can only find rows that are *still there*. A row deleted in the
range is invisible to it, and a row updated in the range is indistinguishable from a newly appended
one — it is present and it matches. Either case breaks the premise that the materialized rows can be
kept: a group that lost all its rows would retain its stale aggregate, and a modified row's old
contribution would still sit in storage while its new one arrived in the delta.

So the connector reports `additionsOnly` only for a range in which every snapshot is an `APPEND`, and
withholds the changed-rows predicate otherwise. `isAppendOnlyRange` is that check.

This has nothing to do with Iceberg format version 3. Every data file carries the sequence number of
the commit that added it, exposed per row as `$data_sequence_number`, and on an append-only range
that *is* the row's own change point — appended rows land only in new files. Version 3's per-row
`_last_updated_sequence_number` answers a different question only once a file has been rewritten with
some rows changed and some not, which is a `REPLACE` snapshot and therefore already declined.

Version 3 is in any case where row lineage is least useful here: this connector raises *"Iceberg
table updates for format version 3 are not supported yet"* for `DELETE`, `UPDATE` and `MERGE`, so an
append is the only mutation a version 3 table can receive. **The feature has no dependency on row
lineage, and the code says so by using one column for every supported version.**

Format version 1 is excluded: it has no sequence numbers at all, reporting zero for every snapshot
and every file, so the predicate would match nothing and an empty change set is indistinguishable
from nothing having changed.

## 3. Architecture — where each decision lives

Four decisions, separated by the kind of question each answers.

```
                          ┌─ D1 eligibility (correctness)   → rule-based, optimizer
MaterializedViewScanNode ─┼─ D2 shape classification        → rule-based, optimizer
                          ├─ D3 change-set assertion        → connector asserts, engine trusts
                          └─ D4 candidate selection (cost)  → cost framework, not yet priceable
```

**D1 and D2 are rule-based because they are correctness predicates.** Whether the materialized rows
can be reused is not a magnitude a cost model can weigh. Their inputs are all plan-time constants:
the node-type whitelist, aggregate mergeability, `additionsOnly`, every stale base covered by a
row-level predicate, and format version. Both run before any plan is constructed, so a decline costs
nothing.

**D3 belongs to the connector and is the one premise the engine takes on trust.** Nothing above the
connector can distinguish a row that replaced an earlier version of itself from a newly appended one.
A connector reporting `additionsOnly` wrongly causes stale values to be served and, on a refresh,
persisted.

**D4 uses the existing `SelectLowestCostMVRewrite`.** It already compares candidate plans through
`costProvider`/`statsProvider` and already reports a cost rejection. It cannot yet choose — see §7.3.

### Node types are a whitelist

A shape is served only if every node in it is one that was reasoned about:
`TableScanNode`, `FilterNode`, `ProjectNode`, `UnionNode`, `AggregationNode`. Everything else
declines, including node types added to the planner later.

This is deliberate and was earned. The predecessor tested for a join and for aggregations and
defaulted everything else to row-preserving — a decline-by-omission — so shapes it could not serve
were stopped only by the delta builder refusing to visit them. For a set operation, which the delta
builder *does* visit, nothing stopped them (§6).

### What declines, and why

| shape | why |
|---|---|
| `MIN`, `MAX`, `AVG`, `COUNT(DISTINCT)` | not expressible as stored value plus increment; a stored extreme cannot absorb a removal |
| no `GROUP BY` | one row that no grouping key names, so neither branch can place it |
| set operations | `EXCEPT` is anti-monotonic in its right input — see §6 |
| joins, stacked aggregations | row identity is not expressible from the storage table |
| format version 1 | no sequence numbers at all; every row reads zero |
| any range with a delete, update, overwrite or compaction | `additionsOnly` is false, so the materialized rows cannot be trusted |
| a stale base with no row-level predicate | its delta replaces a value rather than adding to it, and its storage filter can delete rows a row-level sibling contributed |
| `LIMIT`, `TopN`, `ORDER BY`, window functions, `UNNEST`, views over views | outside the whitelist |

### One decision, consumed in several places

The fresh branch, the delta builder and the merge all read the same verdict rather than re-deriving
it. Deriving it independently is what produced two of the three defects in §6.

## 4. Flow

```
            stale read of a materialized view
                          │
            ┌─────────────┴─────────────┐
   changed-rows predicate?        no → partition-level / full
            │ yes
   additions only, every stale base covered?   no → decline (warning)
            │ yes
   shape in the whitelist?                     no → decline (warning)
            │ yes
   ┌────────┴────────┐
row-preserving   aggregation
   │                 │
   │          every aggregate mergeable?       no → decline (warning)
   │                 │ yes
   ▼                 ▼
Union(storage,    Aggregate[merge](
      delta)        Union(storage, Aggregate(delta)))
```

The merge re-aggregates the union by the grouping keys. `SUM` adds; `COUNT` is merged by **summing**
the counts, not by counting again. A group the delta never touched meets nothing to merge with and
sums to itself, which is why the fresh branch can keep every row instead of excluding the affected
ones.

`MIN`/`MAX`/`AVG` are not merged. `MIN` and `MAX` merge correctly for appends, but a stored extreme
cannot absorb a removal, and the range being append-only is a property of the change set rather than
of the view — so they are excluded rather than merged on the strength of the range happening to
qualify. `AVG` cannot be merged without the count beside it.

## 5. Results

### 5.1 Mergeable aggregate

Base 200k rows / 101 groups, storage 101 rows:

| delta | row-level | partition-level | full recompute |
|---|---|---|---|
| 1 row | **102** rows / 12 ms | 2,483 / 25 ms | 201,167 / 184 ms |
| 10 | **111** / 13 ms | 8,352 / 52 ms | 202,288 / 192 ms |
| 100 | **201** / 12 ms | 8,452 / 56 ms | 202,388 / 151 ms |
| 1,000 | **1,101** / 13 ms | 9,452 / 52 ms | 203,388 / 146 ms |

24–75× fewer rows than partition-level and ~180–1,970× fewer than a full recompute, with CPU
following: roughly 4× less than partition-level and 11–15× less than recompute. The cost is flat in
delta size because storage holds one row per group.

### 5.2 Row-preserving

`SELECT region, amount WHERE amount > 197000`, storage 150 rows:

| delta | row-level | partition-level | full recompute |
|---|---|---|---|
| 1 row | **151** / 3 ms | 151 / 4 ms | 10,001 / 8 ms |
| 10 | **161** / 4 ms | 2,162 / 9 ms | 10,011 / 13 ms |
| 100 | **261** / 5 ms | 2,272 / 11 ms | 10,111 / 15 ms |
| 1,000 | **1,261** / 6 ms | 3,372 / 13 ms | 15 ms |

Ties partition-level at a single-partition delta and wins once the delta spreads. The full-recompute
baseline reads 10,001 rather than 200k because the filter prunes files by `amount`.

### 5.3 Correctness

292 tests across `presto-spi` (4), `presto-main-base` (77) and `presto-iceberg` (211), from a clean
build. Plus a live matrix of 15 assertions against a real coordinator, each comparing the stitched
read against the same query run directly over the base:

| | case | result |
|---|---|---|
| 1 | version 1 base — stale read and refresh | declines, exact |
| 2 | global aggregate — value and row count | one row, exact |
| 3 | `SUM`+`COUNT` — merges, no expansion | exact |
| 4 | `MIN` — declines | exact |
| 5 | `NULL` grouping key | exact |
| 6 | version 2 base | exact |
| 7 | refresh preserves untouched groups | exact |
| 8 | `HAVING` view whose group drops out | removed correctly |
| 9 | range containing a `DELETE` | declines, exact |

Row-preserving is covered across version 2 and 3 × partitioned and unpartitioned base, including a
group appearing for the first time in the delta and rows the view's filter excludes.

**Randomized invariant coverage.** `testRandomAppendSequencesPreserveTheView` generates append and
refresh sequences — two seeds, twelve steps, both supported shapes, refreshes interleaved at random
— and after every step asserts the stitched read equals the view's own query over the base as it
stands. That single comparison covers all four invariants at once: no missing rows and no duplicate
rows (equality in both directions), snapshot equivalence (the comparison is against the base now, so
a refresh landing mid-sequence must not change the answer), and monotonic freshness (the assertion
holds on both sides of every refresh).

It was verified to have teeth rather than assumed to: making `COUNT` merge with `COUNT` instead of
`SUM` — the classic error in this algebra — fails every seed by step one and names the diverging row.
Kept deliberately short so it does not dominate the suite; adding seeds is how to widen the search.

**Eligibility coverage.** The decision to offer a row-level candidate at all is now tested at the
rule level in `TestMaterializedViewRewrite`, using the standard `RuleTester` harness and
`PlanMatchPattern`, asserting both that a declining view falls back to the partition-level plan and
that the reason is reported. Previously that method had no unit coverage at all.

### 5.4 View-shape matrix on object storage

Nineteen view shapes built on IBM Cloud Object Storage over a partitioned 40,000-row base (plus a
second base for the two-table shapes), each read under all three paths after a 200-row append that
touched every partition. "Ratio" is rows read by the best alternative over rows read by row-level.
Every shape's row-level and partition-level answer was compared against the full recompute.

| view shape | fires | row-level | alternative | ratio | agrees |
|---|---|---|---|---|---|
| `GROUP BY k, SUM` | yes | 220 | 40,200 | **183×** | yes |
| `GROUP BY k, COUNT(*)` | yes | 220 | 40,200 | **183×** | yes |
| `GROUP BY k, SUM + COUNT` | yes | 220 | 40,200 | **183×** | yes |
| `WHERE` + `GROUP BY k, SUM` | yes | 220 | 40,200 | **183×** | yes |
| `GROUP BY k, SUM`, unpartitioned storage | yes | 220 | 40,220 | **183×** | yes |
| `GROUP BY k1, k2, SUM` | yes | 1,200 | 40,200 | 34× | yes |
| selective filter (row-preserving) | yes | 315 | 40,200 | **128×** | yes |
| filter + expression | yes | 2,460 | 40,060 | 16× | yes |
| `UNION ALL`, two bases | yes | 532 | 41,500 | 78× | yes |
| `UNION` distinct, two bases | yes | 532 | 41,500 | 78× | yes |
| 1:1 projection | yes | 40,200 | 40,200 | 1× | yes |
| `SELECT DISTINCT k` | yes | 220 | **20** | **0.09×** | yes |
| `GROUP BY k, AVG` | no | — | 40,200 | — | yes |
| `GROUP BY k, MIN/MAX` | no | — | 40,200 | — | yes |
| `SUM`, no `GROUP BY` | no | — | 40,200 | — | yes |
| `GROUP BY k` + `HAVING` | no | — | 40,200 | — | yes |
| `GROUP BY k, COUNT(DISTINCT)` | no | — | 40,200 | — | yes |
| `EXCEPT`, two bases | no | — | 41,200 | — | yes |
| `INNER JOIN`, two bases | no | — | 45,250 | — | yes |

Twelve fire, seven decline, **nineteen of nineteen agree with the full recompute** — no missing rows,
no duplicates, no stale values, on any shape.

Three results are worth reading closely.

**`SELECT DISTINCT k` fires and is a pessimization.** A `DISTINCT` plans as an aggregation with
grouping keys and no aggregate functions, so `mergeableAggregationRoot`'s loop over aggregates is
vacuous and the view is classified mergeable. The answer is correct — `DISTINCT` is idempotent under
append, so re-grouping storage beside the delta is sound — but here `k` is the partition column, and
Iceberg answers `SELECT DISTINCT <partition column>` from partition metadata alone: the full
recompute read **0 rows** and partition-level read 20, against row-level's 220. Firing is a loss.
It is not fixed by declining on "no aggregates", because on an unpartitioned base the same shape is a
genuine win — the metadata shortcut is what makes it a loss, and that is a cost question, not an
eligibility one. It is left as the clearest illustration of why §7.3 matters.

**Four shapes are rejected before this feature sees them.** `UNION ALL`, `EXCEPT` and `JOIN` over the
*same* base fail at `CREATE MATERIALIZED VIEW` with *"Materialized View definition does not support
multiple instances of same table"*, and a window function fails with *"OrderBy are not supported for
materialized view"*. These are pre-existing Presto materialized-view restrictions. They were retested
with two distinct bases, which is how the set-operation and join rows above were obtained.

**`UNION` distinct behaves like `UNION ALL`.** Both fire and both agree. `UnionNode` is on the
whitelist and the deduplicating aggregation above it merges exactly as `DISTINCT` does.

## 6. Defects this feature had, and what they had in common

Three wrong answers, all found by review rather than by tests, all silent — no warning, no fallback,
no crash. Each is now fixed with a test.

| shape | symptom |
|---|---|
| format version 1 base | stale rows served as fresh; a refresh wrote 0 rows, advanced the watermark, and left the view permanently wrong with no later refresh able to repair it |
| `SELECT SUM(x) FROM t`, no `GROUP BY` | two rows — the old total and the increment side by side — where the view defines one |
| `EXCEPT` | appending to the right side must *remove* a row; the view kept returning one its base no longer held |

All three are the same failure: a shape fell into a path that could not serve it, because the
classification defaulted to permitting rather than declining, and because the decision was made
independently in several places. §3 addresses both.

Two near-misses recorded for the same reason. Skipping the fresh branch's exclusion while the merge
was not applied would have duplicated every affected group — caught by threading one decision instead
of two. And the first attempt at reading storage during a refresh used the write handle, which
Iceberg reports as `…$data`; scanning it inside the same statement returns the empty output snapshot,
and it silently overwrote two groups with the delta alone.

## 7. Caveats

### 7.1 Refresh is row-level only for mergeable views, and only sometimes worth it

Two different row-level refresh plans have to be told apart here.

**Recomputing changed groups from the base is not offered.** It rereads each changed group's other
base rows, and the affected keys are only known once the delta has been read, so nothing prunes that
scan:

| | rows read | CPU | rows written |
|---|---|---|---|
| recompute-from-base | 200,049 | 251–489 ms | 1 |
| partition-level | 4,010 | 28 ms | 1 |

Identical output. Partition-level derives its stale set from manifest metadata, a plan-time constant
that does prune — and that path was gated on partition staleness being available, so the cheaper plan
was reachable in every case it was. The gate admitted it only where something better already existed.

Relaxing that gate is worse, not better, and both failure modes were reproduced: dropping the
partition-staleness requirement makes the connector commit `overwriteByRowFilter(alwaysTrue())`
against a delta that wrote one group, **destroying every group the delta did not write**; and adding
`!hasRowLevelChanges()` to `fullRefreshRequired` makes a view whose group drops out keep it forever,
reachable even with row-level disabled by session.

**Merging the stored aggregates is offered**, for mergeable views only, via
`buildMergedRefreshPlan`. It computes `total = stored_total + delta_sum` and so never scans the base
beyond the delta. Because it emits the *complete* view rather than only the affected groups, it is
safe under either commit mode — replacing the written partitions and overwriting the whole table
produce the same table — which is why it needs neither partition-level staleness nor storage
partitioned by the grouping columns.

Whether it is *worth* it depends entirely on how many groups the delta touches, and the deciding cost
is objects written, not rows. Measured on IBM Cloud Object Storage, 20-group view, partitioned
storage:

| delta touches | path | rows read | CPU | wall |
|---|---|---|---|---|
| all 20 groups | merged | **420** | 1,030 ms | 78.0 s |
| all 20 groups | partition-level | 40,600 | 1,070 ms | 79.8 s |
| 1 group | merged | **70** | 694 ms | 76.2 s |
| 1 group | partition-level | 2,130 | **130 ms** | **23.4 s** |

With a wide delta the merge is free-to-better. With a narrow delta it reads 30× less and still costs
**5.3× the CPU and 3.3× the wall**, because it emits 20 groups into 20 partition objects where
partition-level emits one into one, and each object costs round trips at ~245 ms. Rows read is the
wrong metric for a refresh; a refresh is priced in objects written.

On **unpartitioned** storage it is a straight win, because there is one object either way and
partition-level does not apply at all — it warns *"stale columns are not valid refresh columns"* and
degrades to a full refresh (40,600 rows against the merge's 420).

Hence the default. `materialized_view_row_level_incremental_strategy` defaults to `NEVER`, so this
plan ships off and cannot regress a narrow-delta refresh that nobody asked to change. Turn it on for
views whose storage is unpartitioned, or whose deltas routinely touch most groups.

The open-writer limit applies to the merged plan for the same reason: emitting every group writes
every partition, so a view with more groups than the limit cannot refresh this way. See §7.6 — it is
a property of wide partitioned tables, not of this plan.

### 7.2 The IO reduction is real and does not become a latency reduction

This was previously recorded as untested. It has now been measured against IBM Cloud Object Storage
(`s3.us-west.cloud-object-storage.test.appdomain.cloud`, ~235 ms RTT, ~245 ms per warm object GET),
which is the environment the earlier local and MinIO-on-loopback runs could not represent.

Grouped `SUM`+`COUNT` over a partitioned base, every partition stale, medians over 4–5 runs:

| base rows | path | rows read | CPU | wall (median) |
|---|---|---|---|---|
| 40,200 | row-level | **220** | 108 ms | 4,780 ms |
| 40,200 | partition-level | 40,200 | 116 ms | 4,770 ms |
| 40,200 | full recompute | 40,200 | 115 ms | 4,220 ms |
| 5,000,500 | row-level | **520** | **201 ms** | 5,170 ms |
| 5,000,500 | partition-level | 5,000,500 | 446 ms | 4,770 ms |
| 5,000,500 | full recompute | 5,000,500 | 426 ms | 4,275 ms |

The IO claim holds and strengthens with scale: 183× fewer rows at 40 k, **9,616× fewer at 5 M**. CPU
follows once the base is large enough to matter — a wash at 40 k, **2.2× better at 5 M**, because
row-level's work is a function of group count plus delta size while the alternatives' is a function
of base size.

Wall-clock does not follow, and on this endpoint row-level is consistently a little *worse*
(+8% at 5 M). The reason is that none of these queries is data-bound. A stale read costs ~4.2 s of
fixed work before any data is touched — `getMaterializedViewStatus` alone measured 1.13 s on COS —
and row-level adds to it by resolving a second table's metadata (the storage table) on the critical
path. Scanning 5 M narrow rows is parallel across splits and small in bytes, so the thing being
eliminated was never what the clock was waiting on.

The practical reading: **this is a cost and throughput optimization, not a latency optimization.**
Halving CPU and eliminating multi-million-row scans matters for concurrency, cluster cost, and how
many such queries a cluster can serve at once. A single query's response time will not improve, and
a user measuring only wall-clock on one query will conclude the feature does nothing. Any latency
claim would need a base whose scan genuinely dominates the fixed overhead; that point is beyond 5 M
narrow rows on this endpoint and has still not been located.

### 7.3 Cost-based selection does not work yet

`SelectLowestCostMVRewrite` compares candidate plans and reports a cost rejection, but nothing teaches
the stats calculator that `$data_sequence_number > N` selects a handful of rows out of hundreds of
thousands. The delta leaf prices like a full scan, so the row-level candidate loses on cost and
`AUTOMATIC` cannot choose it.

There is a second, blunter mechanism in the same rule, and it is pre-existing: when any cost
component is unknown — the common case for Iceberg tables with no `ANALYZE` — the rule falls back to
comparing candidates by **output** row count. Every stitching candidate computes the same view and so
produces the same output; the comparison is `candidateRows < selectedRows`, a tie, and the original
plan keeps winning. What differs between these plans is rows *read*, which the fallback never looks
at. So `AUTOMATIC` does not merely mis-price row-level, it cannot distinguish it from a full
recompute at all.

Confirmed on COS: under `AUTOMATIC`, `mv_sum` was planned as a full recompute reading 5,000,500 rows
when a row-level plan reading 520 was available and offered. The
`MATERIALIZED_VIEW_ROW_LEVEL_REJECTED_ON_COST` warning this feature adds is what makes that visible —
without it the rejection is indistinguishable from the view being ineligible. Both the fallback and
the missing statistic are upstream of this feature; it only surfaces them.

The missing input is a row estimate for the change-tracking predicate, which Iceberg can compute
exactly and cheaply from manifest metadata — Σ `record_count` over files with
`data_sequence_number > watermark`, no scan. The right fix is to supply that statistic so the existing
framework works, **not** to build a second cost model beside it. §5.2 shows why it matters: for
row-preserving views the answer genuinely depends on how far the delta spreads.

Until then the honest posture is explicit opt-in.

### 7.4 Enablement is silent when incomplete

Three session properties are required for a stale read:

```
materialized_view_stale_read_behavior=USE_STITCHING
materialized_view_stitching_strategy=ALWAYS
materialized_view_row_level_incremental_strategy=ALWAYS
```

Miss any one and results stay correct — the optimization simply never happens, with nothing to see.
A `RuntimeStats` counter, `materializedViewRowLevelStitchingApplied`, now reports when it did fire;
declines report a reason as a warning. Two properties of the counter matter: it is written during
planning, so it means the plan was chosen rather than that it ran, and it is session-scoped, so a
query reading two views contributes twice without saying which.

### 7.5 Inherited, not introduced

A view with `HAVING` whose group drops out keeps that group forever after an incremental refresh and
then reports itself fresh. Reproduced on both granularities and with row-level disabled — it is
upstream partition-level behaviour, not this feature's. Anyone setting `refresh_type = 'INCREMENTAL'`
can hit it, and it deserves its own issue.

### 7.6 Open-writer limit on partitioned storage

A view whose storage is partitioned by its grouping column cannot have more groups than the
open-writer limit, which defaults to 100 — the refresh fails with *"Exceeded limit of 100 open
writers for partitions"*. Not specific to this feature; it bites a plain full refresh equally. It is
recorded because it invalidated a benchmark run during development and will mislead anyone sizing a
partitioned materialized view.

### 7.7 Untested areas

Rollback, expired watermark snapshots, compaction in range, concurrent reads during a refresh, and
materialized-view schema changes are reasoned about but not covered by tests. The snapshot walk in
`isAppendOnlyRange` is now bounded, but the bound itself is untested.

## 8. Deliberately absent

Updates and deletes; deletion vectors; change sets and `system.builtin.changes`; version 3 row-level
writes; compaction row-lineage preservation; row-preserving views that need to *exclude* a
materialized row, which would require row-origin identity.

Also absent by removal rather than omission: the affected-identifier and anti-join machinery, 613
lines, unreachable once both supported shapes keep every materialized row and row-level refresh is
no longer offered. `buildExpandedDelta` and `requiresExpandedDelta` deliberately remain — expansion
still serves the partition-level delta, which this feature does not own.

## 9. Invariants

Each has been violated once; each is now load-bearing.

1. **The engine never infers append-only-ness.** It is the connector's assertion, unverifiable from
   above.
2. **The shape whitelist is deny-by-default.** A node type added to the planner declines until
   someone reasons about it.
3. **One decision, consumed in many places** — never re-derived.
4. **Writing a partial delta is only safe under a partition-replacing commit.** Out of scope while
   refresh is gated off, but the coupling must stay documented or it will be reintroduced.
5. **Row counts are an IO claim, not a latency claim**, until measured where scans cost something.

## 10. Review outcomes

A code review of this branch against surrounding Presto conventions produced the following. Recorded
here because the decisions not to change things are as load-bearing as the changes.

### Acted on

| finding | change |
|---|---|
| The candidate-offering rule had no unit coverage, while its 746-line test class sat unused | Eligibility tests added to `TestMaterializedViewRewrite` on the standard harness |
| Plan-shape assertions used raw cast chains; 130 comparable rule tests use `PlanMatchPattern` | New assertions use `PlanMatchPattern`; the merge test's cast chain replaced with `PlanNodeSearcher`, which survives a node being inserted above the union |
| The merge decision was a mutable field assigned after construction | Made final and constructor-injected; the rewriter is now built after the gate decides |
| Two failure conventions coexisted undocumented | Class-level note distinguishing the fallback signal from internal-invariant checks |
| No randomized correctness test | Added, and sabotage-verified — see §5.3 |

### Acted on — second review round

A second pass over the whole implementation, looking specifically for regressions in pre-existing
code, redundancy, and optimization headroom.

| finding | severity | change |
|---|---|---|
| `mergeFunctionName` matched aggregates by bare object name, so a catalog serving its own function namespace could expose a `sum` with non-distributive semantics and have it merged as if it were the built-in; the merge function was itself resolved through the session's default namespace rather than the built-in one | correctness | Both halves pinned to `JAVA_BUILTIN_NAMESPACE`, following `RewriteSpatialPartitioningAggregation` |
| Two javadoc blocks sat on `buildMergedRefreshPlan`; Java binds only the last, so the first was dead text — and it was the one carrying a `{@link}` to the deleted `rowLevelReplacementIsSafe`. `buildDeltaPlanForRefresh`, its real owner, had no documentation at all | correctness of docs | Block moved back and the dead link replaced with the actual caller obligation |
| `supportsRowLineage` was left with zero callers after both of its call sites were removed | redundancy | Deleted; `MIN_FORMAT_VERSION_FOR_ROW_LINEAGE` stays, still used elsewhere |
| `ChangedRowsPredicate` is new in this branch, yet its two-argument constructor documented itself as retaining a signature that "predates" `additionsOnly` — there is no predecessor | wrong provenance | Rewritten as what it is: the conservative default for a connector that has not established the stronger property |
| `TableFinishOperator` used a fully-qualified `java.util.Collections.emptyList()` inline in a file that already imports `ImmutableList` | style | `ImmutableList.of()` |
| `buildMergedRefreshPlan` built the same output-identity map twice | redundancy | Hoisted to a local |
| `buildRowLevelPlan`'s javadoc described the candidate as spanning "V3 and V2 bases", language left from the removed version-3 design | stale doc | Reworded to bases that can and cannot report changed rows |

### Examined and found sound

Worth recording, because these were the places a regression would have been most costly:

- **`TableFinishOperator` now routes every table write through a method named
  `finishRefreshMaterializedView`.** That operator commits `INSERT`, `CTAS`, `DELETE` and `UPDATE`,
  not just refreshes, so this looked like the highest-risk change in the branch. It is safe: the sole
  implementation guards on `target instanceof RefreshMaterializedViewHandle` and otherwise delegates
  to the original `finishTable`, so a plain `INSERT` still reaches `finishInsert` unchanged. The
  naming is unfortunate; the behaviour is not.
- **`MetadataManager`'s new five-argument overload** throws `NOT_SUPPORTED` rather than silently
  dropping delete fragments when a connector cannot commit them atomically.
- **Removing the `BOUNDED_REFRESH_IGNORED` warning was not a loss of diagnostics.** It warned that
  `max_snapshots_per_refresh` was ignored on version 2 bases. This branch replaced
  `chooseTargetSnapshot`'s row-lineage guard with `supportsChangeTracking`, so bounded refresh now
  genuinely works on version 2 and the warning would be false. The new guard is also the more correct
  one, and says why: a version 1 table reports sequence number 0 for every row, so the cap would be
  vacuously true and the storage table would end up holding data its own watermark denies.
- **The `AUTOMATIC` selection logic is entirely pre-existing**; this branch adds only the applied
  counter and the cost-rejection warning. See §7.3.

### Considered and declined

**Collapsing the three snapshot traversals per stale base.** The review proposed merging
`isAncestorOf`, `chooseTargetSnapshot` and `isAppendOnlyRange` into one walk, on the theory that they
were a measurable share of the per-query metadata cost. They are not. All three follow parent
pointers through `TableMetadata`, which is already in memory; the measured 41 ms → 132 ms is manifest
I/O inside `detectChangedPartitions` — `hasPartitionEvolution`'s two `allManifests` reads and
`IncrementalAppendScan.planFiles`. Collapsing in-memory pointer chases would not move it, and would
trade three clear predicates for one that answers three questions. The bound added to
`isAppendOnlyRange` stays, because that guards against a pathological chain rather than a cost.

**Converting the `checkState` sites to fallbacks.** Six of them sit outside the fallback boundary, so
one firing fails the query rather than degrading — an asymmetry against the discipline the rest of
the feature keeps. Left as is: they mark invariants the eligibility gate makes unreachable, and a
planner bug that silently degrades is worse than one that surfaces. Documented instead, with the rule
that any shape a user can legitimately write must reach the other mechanism.

**Porting the inherited cast-chain assertions.** `TestDifferentialPlanRewriter` pre-dates this branch
and calls `buildStitchedPlan` as a static helper on hand-built nodes, which is the wrong shape for
`PlanMatchPattern` — that needs a rule application. Converting it means restructuring an inherited
test, which is beyond this feature's scope. New assertions use the better mechanism; the old ones are
left for whoever touches that file next.

**Splitting `DifferentialPlanRewriter`.** At 2,046 lines it is a god class, and the merge logic would
sit happily on its own. It also pre-exists at 1,124 lines, and splitting it would obscure this
feature's diff for a reviewer comparing against master. Worth doing, not worth doing here.

## 11. Remaining work, in the order worth doing it

1. ~~A randomized invariant test.~~ **Done** — see §5.3.
2. **Connector statistics for the change-tracking predicate** (§7.3). The gate on `AUTOMATIC` being
   useful. Spike how connector statistics reach the stats calculator for a hidden metadata column
   before scheduling it — that path has not been traced, and if it needs new SPI the work is
   materially larger than it looks.
3. **Reduce the in-call status cost** (§7.2). The honest target for read latency, and distinct from
   caching, which does not help a read.
4. **Refresh-side merge** (§7.1). Only worth starting when someone needs refresh cost reduced, and
   only with the affected-group restriction designed in from the start.
5. **The decline matrix as tests.** Several entries in the table under §3 are verified only by live
   scripts, so widening the whitelist would not have to confront them.
