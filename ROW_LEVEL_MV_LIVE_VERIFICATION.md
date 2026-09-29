# Row-level MV refresh and Iceberg V3 — live server verification

Every query below was run against a real Presto coordinator, not a test harness, and
every result is copied verbatim from the CLI.

| | |
|---|---|
| Branch / commit | `mv-row-level-incremental-refresh` @ `069a6c85f3` |
| Server | `com.facebook.presto.server.PrestoServer`, single node (coordinator + worker), port 8091 |
| Catalog | `iceberg`, `hive.metastore=file`, warehouse `file:///Users/nandakumarb/iceberg` |
| Iceberg library | 1.10.1 |
| Client | `presto-cli`, `--output-format TSV_HEADER` |
| Schema | `iceberg.mvdemo` |

Two environment notes, because both cost time:

- Port 8080 was already held by an unrelated `ssh` port-forward on IPv4. Presto bound
  IPv6 and its own internal node calls hit the tunnel, getting `401 ... Authentication
  challenge without WWW-Authenticate header`, so the node never announced and no query
  could run. Moved to 8091 with `node.internal-address=127.0.0.1`.
- The server needs `-Djdk.attach.allowAttachSelf=true` or Guice fails at startup with
  `IOException: Can not attach to current VM`.

---

## 1. V3 table and row lineage

```sql
CREATE TABLE sales (id INTEGER, region VARCHAR, amount DOUBLE)
WITH ("format-version" = '3',
      "write.delete.mode" = 'merge-on-read',
      "write.update.mode" = 'merge-on-read');

INSERT INTO sales VALUES (1,'east',100.0),(2,'east',200.0),(3,'west',300.0),(4,'west',400.0);

SELECT id, region, amount, "_row_id", "_last_updated_sequence_number" AS seq
FROM sales ORDER BY id;
```

```
id	region	amount	_row_id	seq
1	east	100.0	0	1
2	east	200.0	1	1
3	west	300.0	2	1
4	west	400.0	3	1
```

Ids are implicit here — the file states none, and the reader derives `first_row_id + position`.

## 2. V3 DELETE writes a deletion vector

```sql
DELETE FROM sales WHERE id = 3;

SELECT id, region, amount, "_row_id", "_last_updated_sequence_number" AS seq
FROM sales ORDER BY id;
```

```
id	region	amount	_row_id	seq
1	east	100.0	0	1
2	east	200.0	1	1
4	west	400.0	3	1
```

Row 3 is gone and the survivors keep their ids — `_row_id` 3 is *not* renumbered to 2.
Before this branch the commit failed outright with
`Must use DVs for position deletes in V3`.

## 3. V3 UPDATE preserves row lineage

The headline result.

```sql
UPDATE sales SET amount = 250.0 WHERE id = 2;

SELECT id, region, amount, "_row_id", "_last_updated_sequence_number" AS seq
FROM sales ORDER BY id;
```

```
id	region	amount	_row_id	seq
1	east	100.0	0	1
2	east	250.0	1	3
4	west	400.0	3	1
```

Row 2 is a physically new row in a new data file, and it still answers to `_row_id` 1.
Only *its* sequence number advanced (1 → 3); the untouched rows stayed at 1. Without the
carry it would read `_row_id` 4 and a consumer keyed on row identity would lose the row
it was tracking.

## 4. V3 MERGE — works, with a known lineage gap

```sql
CREATE TABLE sales_src (id INTEGER, region VARCHAR, amount DOUBLE);
INSERT INTO sales_src VALUES (1,'east',111.0),(9,'north',900.0);

MERGE INTO sales t USING sales_src s ON t.id = s.id
  WHEN MATCHED THEN UPDATE SET amount = s.amount
  WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount);
```

```
MERGE: 2 rows
```

```sql
SELECT id, region, amount, "_row_id" FROM sales ORDER BY id;
```

```
id	region	amount	_row_id
1	east	111.0	5
2	east	250.0	1
4	west	400.0	3
9	north	900.0	6
```

The data is right: id 1 updated, id 9 inserted. **Row 1's `_row_id` moved from 0 to 5** —
the documented limitation, reproduced live. The engine expands a matched update into a
delete row and an insert row and nulls the row-id on the insert half, so the sink cannot
tell it from an unmatched insert. Pinned by
`TestIcebergV3.testMergeOnV3TableDoesNotPreserveRowLineage`.

## 5. Change sets over deletion-vector ranges

`$snapshots` for the four operations above:

```
snapshot_id	operation
342159798924884823	append
944016770724822618	delete
3219351881665231725	overwrite
5296803829690692445	overwrite
```

### The delete range — recovering a row a deletion vector removed

```sql
SELECT operation, rowdata
FROM "sales@342159798924884823$changelog@944016770724822618"
ORDER BY operation;
```

```
operation	rowdata
DELETE	{id=3, region=west, amount=300.0}
```

This is the case Iceberg's own `BaseIncrementalChangelogScan` refuses outright
(`Delete files are currently not supported in changelog scans`). The removed row's values
are recovered and reported.

### The update range — one update is two events

```sql
SELECT operation, rowdata
FROM "sales@944016770724822618$changelog@3219351881665231725"
ORDER BY operation;
```

```
operation	rowdata
DELETE	{id=2, region=east, amount=200.0}
INSERT	{id=2, region=east, amount=250.0}
```

### The merge range

```sql
SELECT operation, rowdata
FROM "sales@3219351881665231725$changelog@5296803829690692445"
ORDER BY operation, rowdata;
```

```
operation	rowdata
DELETE	{id=1, region=east, amount=100.0}
INSERT	{id=1, region=east, amount=111.0}
INSERT	{id=9, region=north, amount=900.0}
```

Retracting the old grouping and accumulating the new one is exactly what an aggregating
view needs, and it is why the DELETE half cannot be dropped.

## 6. Incremental materialized view refresh

```sql
CREATE TABLE b3 (region varchar, amount bigint)
WITH ("format-version" = '3', partitioning = ARRAY['region'],
      "write.delete.mode" = 'merge-on-read');
INSERT INTO b3 VALUES ('NA', 10), ('EU', 20), ('NA', 30);

CREATE MATERIALIZED VIEW mv3
WITH (partitioning = ARRAY['region'], refresh_type = 'INCREMENTAL',
      row_level_incremental_refresh = true)
AS SELECT region, SUM(amount) AS total FROM b3 GROUP BY region;

REFRESH MATERIALIZED VIEW mv3;
```

```
WARNING: Cannot perform incremental refresh for materialized view iceberg.mvdemo.mv3:
no partition-level staleness available (unpartitioned base, untracked partitions, or
non-append base changes), and the refresh commit replaces the whole storage table
without it. Falling back to full refresh.
REFRESH MATERIALIZED VIEW: 2 rows
```

Correct: a first refresh has no prior materialization, so there is no staleness to
compute and a full refresh is the right answer.

```sql
INSERT INTO b3 VALUES ('NA', 5), ('APAC', 7);
REFRESH MATERIALIZED VIEW mv3;
```

```
REFRESH MATERIALIZED VIEW: 2 rows
```

No warning this time, and **2 rows written for 3 groups** — `NA` (40 → 45) and the new
`APAC`, with `EU`'s partition left alone.

```sql
SELECT region, total FROM __mv_storage__mv3 ORDER BY region;
```

```
region	total
APAC	7
EU	20
NA	45
```

`__mv_storage__mv3` is the physical storage table; reading it directly is how you see
what was actually materialized rather than what the view returns.

## 7. A stale view read returns the truth

On a separate view over an unpartitioned base, after an append *and* a V3 delete:

```sql
SELECT region, total, n FROM __mv_storage__orders_by_region ORDER BY region;  -- storage
```

```
region	total	n
east	30.0	2
west	30.0	1
```

```sql
SELECT region, total, n FROM orders_by_region ORDER BY region;                -- the view
```

```
region	total	n
east	35.0	3
north	70.0	1
```

The storage is stale in three different ways and the query still returns the truth:
`east` corrected, `north` added, and `west` dropped entirely — its only row had been
removed by a deletion vector.

**Read the caveat in §8.3 before treating this as evidence of stitching.** It is not.

---

## 8. What the live run found that the test suite did not

### 8.1 `system.builtin.changes` failed on a real server — since fixed

```sql
SELECT id, region, amount, change_kind
FROM TABLE(system.builtin.changes('iceberg.mvdemo.sales',
                                  342159798924884823, 944016770724822618));
```

```
Query failed: Table metadata is missing
```

The server log shows the real cause, after a 60-second Iceberg retry loop:

```
PrestoException: Unknown connector iceberg
  at SessionPropertyManager.getConnectorSessionPropertyMetadata(SessionPropertyManager.java:261)
  at SessionPropertyManager.decodeCatalogPropertyValue(SessionPropertyManager.java:360)
  at FullConnectorSession.getProperty(FullConnectorSession.java:264)
  at HiveSessionProperties.isCacheEnabled(HiveSessionProperties.java:1066)
  at HiveCachingHdfsConfiguration$CachingJobConf.createFileSystem(...)
```

The session the table function hands to the target connector carries a connector id the
`SessionPropertyManager` does not know, so every filesystem access inside it fails and
the retry loop turns that into a misleading `Table metadata is missing`. The same
function passes under `DistributedQueryRunner` in `TestIcebergV3`, which is why no test
caught it.

The `$changelog` table (§5) returns the same information and works, so this was a defect
in the table-function surface only.

**Fixed after this run.** `Changes.analyze` now builds its session with the injected
`SessionPropertyManager` rather than the testing one. Re-run live over the same table, the
function and `$changelog` agree row for row, and the narrow append→delete range correctly
returns only the deletion-vector removal.

### 8.2 Incremental refresh requires a partitioned base

The warning in §6 is explicit: with an unpartitioned base there is no partition-level
staleness, and the refresh commit replaces the whole storage table, so it falls back to a
full refresh. Enabling `row_level_incremental_refresh` does not lift this. Worth stating
plainly in the RFC, because "row level" reads as though it should free the base from
needing partitions, and it does not.

### 8.3 Stitching needs `materialized_view_stale_read_behavior=USE_STITCHING`

Stitching did not fire in my first several attempts, and the cause was my own
configuration: I set `materialized_view_stitching_strategy` and
`materialized_view_row_level_incremental_strategy` but not
`materialized_view_stale_read_behavior=USE_STITCHING`. Without it a stale view is simply
not used and the query reads the base — which returns correct answers and so hides the
fact that nothing was stitched. **§7 therefore demonstrates correctness only, not
stitching.** §9 is the actual demonstration.

Worth recording because the failure mode is silent: results stay right, the optimisation
just never happens, and no warning is emitted.

### 8.4 Refresh granularity is not observable from these results

The second refresh in §6 wrote 2 rows of 3 groups, which is incremental. It does not show
*which* incremental path ran: the base is partitioned by `region` and the view groups by
`region`, so the changed partitions and the changed groups are the same two, and
partition-level refresh alone would write the same 2 rows. Distinguishing them needs a
case where a storage partition holds more than one group — which
`rowLevelReplacementIsSafe` deliberately forbids, since it requires storage partitions to
*be* the groups.

---

## 9. Row-level stitching, firing

All three session properties this time:

```
--session materialized_view_stale_read_behavior=USE_STITCHING
--session materialized_view_stitching_strategy=ALWAYS
--session materialized_view_row_level_incremental_strategy=ALWAYS
```

`mv3` is fresh at `APAC 7, EU 20, NA 45`. Make it stale with a single appended row:

```sql
INSERT INTO b3 VALUES ('EU', 3);
```

### The result

```sql
SELECT region, total FROM __mv_storage__mv3 ORDER BY region;   -- physical storage
```

```
region	total
APAC	7
EU	20
NA	45
```

```sql
SELECT region, total FROM mv3 ORDER BY region;                 -- the view, stitched
```

```
region	total
APAC	7
EU	23
NA	45
```

```sql
SELECT region, SUM(amount) AS total FROM b3 GROUP BY region ORDER BY region;  -- truth
```

```
region	total
APAC	7
EU	23
NA	45
```

Storage says `EU 20`, the view says `EU 23`, the base says `EU 23`. Only the changed group
was recomputed; `APAC` and `NA` came from storage.

### The plan proves it is the row-level path

`EXPLAIN (TYPE LOGICAL)` with row-level `ALWAYS` versus `NEVER` — the same discriminator
`TestIcebergRowLevelStitching` uses (*"row-level and partition-level produced identical
plans, so row-level did not fire"*):

| marker | row-level `ALWAYS` | partition-level `NEVER` |
|---|---|---|
| plan size | 59 lines | 43 lines |
| `__mv_storage__mv3` scan | 2 | 2 |
| `_last_updated_sequence_number` | **6** | 0 |
| `LeftJoin` (anti-join over storage) | **1** | 0 |
| `IS_NULL(affected)` (unmatched-row filter) | **1** | 0 |
| `IS DISTINCT FROM` (null-safe identifiers) | **2** | 0 |

The row-level plan pushes the connector's changed-rows predicate down to the base scan as
a `_last_updated_sequence_number` bound, anti-joins the affected identifiers against the
storage table, and matches identifiers null-safely. The partition-level plan does none of
that. This is the delta algebra running on a real server.

---

## Summary

| Capability | Live result |
|---|---|
| V3 table, `_row_id` / `_last_updated_sequence_number` reads | works (§1) |
| V3 `DELETE` via deletion vector | works (§2) |
| V3 `UPDATE` preserving row lineage | works (§3) |
| V3 `MERGE` | works; lineage not preserved on a matched update (§4) |
| `$changelog` over a deletion-vector range | works, recovers removed rows (§5) |
| Change set for update / merge as DELETE + INSERT | works (§5) |
| Incremental MV refresh, only affected groups written | works, partitioned base required (§6, §8.2) |
| Stale view returns correct results | works (§7) |
| Row-level stale-read stitching (delta algebra) | works, plan-verified (§9) |
| `system.builtin.changes` table function | fixed after this run; see §8.1 |

The one genuine defect the live run found is §8.1. §8.2 and §8.3 are configuration
requirements, not faults — but §8.3 fails silently, which is worth a doc note upstream.

One further defect was found after this run, by test rather than on the server, so it is
not written up here: compaction discards row lineage, which also costs incrementality
because every row's sequence number advances to the compaction's. See **BUG-1** in
`ROW_LEVEL_MV_ARCHITECTURE.md` and `TestIcebergV3.testCompactionDiscardsRowLineage`.
