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

### 7.1 Refresh is not row-level

A row-level refresh recomputes each changed group by rereading that group's other base rows. The
affected keys are only known once the delta has been read, so nothing prunes that scan:

| | rows read | CPU | rows written |
|---|---|---|---|
| row-level | 200,049 | 251–489 ms | 1 |
| partition-level | 4,010 | 28 ms | 1 |

Identical output. Partition-level derives its stale set from manifest metadata, a plan-time constant
that does prune — and the row-level path was gated on partition staleness being available, so the
cheaper plan was reachable in every case row-level was. The gate admitted row-level only where
something better already existed.

Relaxing that gate is worse, not better, and both failure modes were reproduced: dropping the
partition-staleness requirement makes the connector commit `overwriteByRowFilter(alwaysTrue())`
against a delta that wrote one group, **destroying every group the delta did not write**; and adding
`!hasRowLevelChanges()` to `fullRefreshRequired` makes a view whose group drops out keep it forever,
reachable even with row-level disabled by session.

Reusing the stored aggregates, as the read path does, would change the comparison — it needs no base
scan. Doing that here additionally requires the storage branch restricted to the affected groups:
emitting the complete view writes every partition and exceeds the open-writer limit, which a
101-group view confirmed.

### 7.2 The IO reduction is not a demonstrated latency reduction

Every measurement here is rows read and CPU. Wall-clock does **not** follow: it sits at 85–245 ms for
all three paths, and a full recompute often has the lowest wall despite reading ~1,900× more rows.

Two causes. Scanning local Parquet is nearly free — 50,000 narrow rows costs about 5 ms — so there is
little for the reduction to save. And a stale read pays roughly 92 ms of metadata that
`USE_VIEW_QUERY` does not: a fresh view analyses in 41 ms and the same view one insert later in
132 ms, independent of delta size.

That overhead is **not** repetition across call sites. On a plain stale read there is a single
`getMaterializedViewStatus` call, so there is nothing to deduplicate; the cost is inside one call —
`hasPartitionEvolution`'s two `allManifests` reads, `IncrementalAppendScan.planFiles`, and
constructing both candidate plans. A per-transaction memo would help `REFRESH` and
`information_schema` sweeps, not reads.

Read the row counts as an IO claim. Confirming a latency win needs object storage, wide rows, and
partitions large enough that scans dominate. **It has not been done.**

### 7.3 Cost-based selection does not work yet

`SelectLowestCostMVRewrite` compares candidate plans and reports a cost rejection, but nothing teaches
the stats calculator that `$data_sequence_number > N` selects a handful of rows out of hundreds of
thousands. The delta leaf prices like a full scan, so the row-level candidate loses on cost and
`AUTOMATIC` cannot choose it.

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
