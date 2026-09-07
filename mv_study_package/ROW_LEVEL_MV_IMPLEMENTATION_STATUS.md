# Row-Level Incremental Materialized View Refresh

## Source documents

The authoritative design documents are:

- `/Users/nandakumarb/Downloads/MV — M3 Row-Level Incremental Refresh Handover (1).pdf`
- `/Users/nandakumarb/Downloads/MV — Row-Level Incremental Refresh (1).pdf`

The handover document supersedes the earlier draft when the two differ.

## Current branches

- `mv-row-level-incremental-refresh` — main feature branch containing Tier 0 work plus later Tier 1 scaffolding.
- `mv-row-level-tier0` — isolated Tier 0 SPI/foundation branch, based on `origin/master`.
- `master` — local fork branch tracking the fork's master baseline.

Remote repository:

`https://github.com/Nandakumar-Balagopal/presto.git`

## Tier 0 status

The Tier 0 SPI/foundation task is implemented on `mv-row-level-tier0`:

- `ConnectorMetadata.getCurrentTableVersion`.
- `ConnectorMetadata.getChangeSet`.
- `ConnectorMetadata.estimateChangeSetSize`.
- `MaterializedViewStatus.recordedBaseTableHandles`.
- `MaterializedViewStatus.changedRowsPredicates`.
- `ChangedRowsPredicate`.
- `ChangeKindPageSource`.
- `ChangeKindEnumType` and startup registration.
- Focused SPI and type tests.

The second Tier 0 task is still pending:

- Refactor partition-level stitching around a shared `affected_identifiers` relation.
- Use an anti-join for the fresh branch.
- Use a semi-join for the delta branch.
- Preserve partition pruning and existing partition-level correctness.

## Later implementation required

### Tier 1 — Core engine

- Build `affected_identifiers` for current changed rows and deleted change-set rows.
- Add row-level candidate construction to `MaterializedViewRewrite`.
- Add fresh-branch anti-join and delta-branch semi-join plans.
- Extend `DifferentialPlanRewriter` closure tracking and delta propagation.
- Add cost-picker selection among full, partition-level, and row-level candidates.

### Tier 2 — Rewriter extensions

- Add aggregation Case A/Case B expansion.
- Add classical IVM for SUM and COUNT.
- Track delta-plus and delta-minus where required.
- Propagate row identities through supported plan operators.
- Preserve documented fallbacks for unsupported shapes.

### Tier 3 — Connector and surface

- Complete Iceberg V3 change-set provider using changelog/deletion-vector data.
- Surface pre-state values for DELETE and UPDATE_BEFORE records.
- Add session and MV properties for row-level strategy selection.
- Add fallback and cost-rejection warnings.
- Add row-origin storage for row-preserving materialized views.
- Implement atomic delete-plus-insert refresh commit.

### Tier 4 — Validation

- Test inserts, deletes, and updates.
- Test aggregate groups becoming empty.
- Test null grouping keys.
- Test row-preserving `$row_id` refreshes.
- Test V2 fallback to partition-level refresh.
- Test high-change-rate fallback to full refresh.
- Test transaction failure and recovery behavior.
- Run focused module tests and full CI with Temurin JDK 17.

## Current implementation caveat

The main feature branch contains refresh-handle, fragment-payload, and planner-finish scaffolding for atomic refresh. It does not yet generate real delete fragments or perform the final Iceberg atomic delete-plus-insert commit. Row-level capability must remain disabled until those pieces and the Tier 4 correctness suite are complete.

## Validation note

Local Maven validation has been limited by a filesystem permission error writing Maven resolver metadata under `/Users/nandakumarb/.m2`. CI should be used to confirm compilation, checkstyle, and test execution.
