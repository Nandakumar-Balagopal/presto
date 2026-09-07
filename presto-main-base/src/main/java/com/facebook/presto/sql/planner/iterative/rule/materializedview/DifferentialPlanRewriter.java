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
package com.facebook.presto.sql.planner.iterative.rule.materializedview;

import com.facebook.presto.Session;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ChangedRowsPredicate;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MaterializedViewScanNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.RefreshMaterializedViewNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.optimizations.SetOperationNodeUtils;
import com.facebook.presto.sql.planner.optimizations.SymbolMapper;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import static com.facebook.presto.spi.StandardWarningCode.MATERIALIZED_VIEW_STITCHING_FALLBACK;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.relational.Expressions.not;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Builds the delta plan for materialized view differential stitching.
 *
 * <p>Uses standard IVM delta algebra for monotonic operators (Join, Union, Intersect),
 * and falls back to partition-level recompute for Except and Aggregation.
 *
 * <p>Terminology (matching IVM formalism):
 * <ul>
 *   <li>R = unchanged rows (from non-stale partitions)</li>
 *   <li>R' = current state (all rows)</li>
 *   <li>∆R = delta (rows from stale partitions)</li>
 * </ul>
 *
 * <p>Identity for partition-aligned staleness: R' = R ∪ ∆R
 *
 * <p>Delta rules by operator:
 * <ul>
 *   <li>Join: ∆(R ⋈ S) = (∆R ⋈ S') ∪ (R ⋈ ∆S)</li>
 *   <li>Union: ∆(R ∪ S) = ∆R ∪ ∆S</li>
 *   <li>Intersect: ∆(R ∩ S) = (∆R ∩ S') ∪ (R ∩ ∆S)</li>
 *   <li>Except: Uses partition replacement (see below)</li>
 *   <li>Selection/Projection/Aggregation: ∆(op(R)) = op(∆R)</li>
 * </ul>
 *
 * <p><b>EXCEPT handling:</b> EXCEPT is anti-monotonic in the right input, requiring
 * ∆⁺/∆⁻ tracking per the formal rule: ∆⁺(R − S) = (∆⁺R − S') ∪ (R ∩ ∆⁻S).
 * Since we don't track deletions separately, we fall back to partition replacement:
 * when S has stale partitions, we identify affected output partitions and recompute
 * from the current base table state.
 */
public class DifferentialPlanRewriter
{
    private final Metadata metadata;
    private final Session session;
    private final PlanNodeIdAllocator idAllocator;
    private final VariableAllocator variableAllocator;
    private final RowExpressionDomainTranslator translator;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;
    private final Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints;
    private final Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates;
    private final PassthroughColumnEquivalences columnEquivalences;
    private final Lookup lookup;
    private final WarningCollector warningCollector;

    public DifferentialPlanRewriter(
            Metadata metadata,
            Session session,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints,
            PassthroughColumnEquivalences columnEquivalences,
            Lookup lookup,
            WarningCollector warningCollector)
    {
        this(metadata, session, idAllocator, variableAllocator, staleConstraints, ImmutableMap.of(), columnEquivalences, lookup, warningCollector);
    }

    /**
     * @param changedRowsPredicates per-base row-level changed-rows predicates. A base listed here
     *        gets a row-level delta leaf; a base absent from it keeps the partition-level leaf, which
     *        is how one candidate mixes granularities across V3 and V2 bases.
     */
    public DifferentialPlanRewriter(
            Metadata metadata,
            Session session,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Map<SchemaTableName, List<TupleDomain<String>>> staleConstraints,
            Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates,
            PassthroughColumnEquivalences columnEquivalences,
            Lookup lookup,
            WarningCollector warningCollector)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.translator = new RowExpressionDomainTranslator(metadata);
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager());
        this.staleConstraints = ImmutableMap.copyOf(requireNonNull(staleConstraints, "staleConstraints is null"));
        this.changedRowsPredicates = ImmutableMap.copyOf(requireNonNull(changedRowsPredicates, "changedRowsPredicates is null"));
        this.columnEquivalences = requireNonNull(columnEquivalences, "columnEquivalences is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    /**
     * Builds a stitched query plan combining fresh MV data with recomputed delta.
     * This is the main entry point for query-time stitching.
     *
     * @return Optional containing the stitched plan, or empty if stitching is not possible
     */
    public static Optional<PlanNode> buildStitchedPlan(
            Metadata metadata,
            Session session,
            MaterializedViewScanNode node,
            Map<SchemaTableName, MaterializedDataPredicates> constraints,
            MaterializedViewDefinition materializedViewDefinition,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Lookup lookup,
            WarningCollector warningCollector)
    {
        return buildStitchedPlan(metadata, session, node, constraints, ImmutableMap.of(), materializedViewDefinition, variableAllocator, idAllocator, lookup, warningCollector);
    }

    /**
     * Builds a stitched plan whose leaves are row-level for every base the connector reports
     * changed rows for, and partition-level for the rest.
     *
     * <p>Row-level identifiers are the view query's grouping keys, because Case B expansion
     * recomputes whole affected groups and the fresh branch has to exclude exactly the storage rows
     * for those groups. Keying the anti-join at any other grain would leave the two branches
     * overlapping or gapped.
     *
     * @param changedRowsPredicates per-base row-level changed-rows predicates; empty yields the
     *        partition-level candidate
     */
    public static Optional<PlanNode> buildStitchedPlan(
            Metadata metadata,
            Session session,
            MaterializedViewScanNode node,
            Map<SchemaTableName, MaterializedDataPredicates> constraints,
            Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates,
            MaterializedViewDefinition materializedViewDefinition,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Lookup lookup,
            WarningCollector warningCollector)
    {
        SchemaTableName dataTable = new SchemaTableName(materializedViewDefinition.getSchema(), materializedViewDefinition.getTable());
        PassthroughColumnEquivalences columnEquivalences = new PassthroughColumnEquivalences(materializedViewDefinition, dataTable);

        Map<SchemaTableName, List<TupleDomain<String>>> filteredConstraints = filterPredicatesToMappedColumns(constraints, columnEquivalences);
        // If any base table is stale yet has no mapped predicates, stitching is not possible
        if (filteredConstraints.values().stream().anyMatch(List::isEmpty)) {
            warningCollector.add(new PrestoWarning(
                    MATERIALIZED_VIEW_STITCHING_FALLBACK,
                    "Cannot use differential stitching for materialized view " + node.getMaterializedViewName() +
                            ": a stale base table has no predicates mapped to data-table columns. Falling back to full recompute."));
            return Optional.empty();
        }

        DifferentialPlanRewriter builder = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                filteredConstraints,
                changedRowsPredicates,
                columnEquivalences,
                lookup,
                warningCollector);

        PlanNode freshPlan;
        NodeWithMapping deltaResult;
        try {
            // The fresh branch shares the column-binding logic with the delta branch, so it has to
            // sit inside the fallback boundary too: an unstitchable plan must degrade to a full
            // recompute rather than fail the query.
            freshPlan = buildDataTableBranch(metadata, session, node, filteredConstraints, changedRowsPredicates, columnEquivalences, dataTable, idAllocator, variableAllocator, lookup);
            deltaResult = builder.buildDeltaPlan(node.getViewQueryPlan(), node.getViewQueryMappings());
        }
        catch (UnsupportedOperationException e) {
            warningCollector.add(new PrestoWarning(
                    MATERIALIZED_VIEW_STITCHING_FALLBACK,
                    "Cannot use differential stitching for materialized view " + node.getMaterializedViewName() +
                            ": " + e.getMessage() + ". Falling back to full recompute."));
            return Optional.empty();
        }

        return Optional.of(buildUnionNode(
                node,
                freshPlan,
                node.getDataTableMappings(),
                deltaResult.getNode(),
                deltaResult.getMapping(),
                idAllocator));
    }

    /**
     * Filters stale predicates from all tables to only include columns that have equivalence mappings.
     */
    private static Map<SchemaTableName, List<TupleDomain<String>>> filterPredicatesToMappedColumns(
            Map<SchemaTableName, MaterializedDataPredicates> constraints,
            PassthroughColumnEquivalences columnEquivalences)
    {
        return constraints.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> filterPredicatesForTable(
                                entry.getValue().getPredicateDisjuncts(),
                                entry.getKey(),
                                columnEquivalences)));
    }

    private static List<TupleDomain<String>> filterPredicatesForTable(
            List<TupleDomain<String>> stalePredicates,
            SchemaTableName table,
            PassthroughColumnEquivalences columnEquivalences)
    {
        return stalePredicates.stream()
                .filter(predicate -> predicate.getDomains().isPresent())
                .map(predicate -> {
                    Map<String, Domain> filteredDomains = predicate.getDomains().get().entrySet().stream()
                            .filter(entry -> columnEquivalences.hasEquivalence(new TableColumn(table, entry.getKey())))
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
                    return TupleDomain.withColumnDomains(filteredDomains);
                })
                .filter(predicate -> !predicate.isAll())
                .collect(toImmutableList());
    }

    /**
     * Builds the fresh branch: the MV storage rows that do not need recomputing.
     *
     * <p>Two mechanisms, chosen per base table, because they are not interchangeable:
     *
     * <ul>
     *   <li>A partition-level stale predicate maps to the storage table's own columns, so the
     *       branch excludes affected rows with {@code Filter(NOT stale_predicate)}. The connector
     *       can prune partitions from that predicate, and it stays correct when a partition's rows
     *       are all deleted -- the predicate still names the partition even though no base row
     *       survives to identify it.</li>
     *   <li>A row-level changed-rows predicate is expressed over change-tracking columns that have
     *       no equivalent on the storage table, so it cannot become a filter there. The branch
     *       anti-joins against the {@code affected_identifiers} relation instead.</li>
     * </ul>
     *
     * <p>Exactly one mechanism applies per base: applying both would exclude more from the fresh
     * branch than the delta branch recomputes, dropping rows. A row is excluded when any base made
     * it stale, so the mechanisms compose across bases.
     */
    private static PlanNode buildDataTableBranch(
            Metadata metadata,
            Session session,
            MaterializedViewScanNode node,
            Map<SchemaTableName, List<TupleDomain<String>>> constraints,
            Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates,
            PassthroughColumnEquivalences columnEquivalences,
            SchemaTableName dataTable,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Lookup lookup)
    {
        PlanNode freshPlan = node.getDataTablePlan();
        Map<TableColumn, VariableReferenceExpression> storageColumns =
                buildColumnToVariableMapping(metadata, session, freshPlan, lookup);

        ImmutableList.Builder<TupleDomain<String>> storagePredicates = ImmutableList.builder();
        for (Map.Entry<SchemaTableName, List<TupleDomain<String>>> entry : constraints.entrySet()) {
            SchemaTableName baseTable = entry.getKey();
            if (entry.getValue().isEmpty() || isRowLevel(changedRowsPredicates.get(baseTable))) {
                continue;
            }
            storagePredicates.addAll(equivalentDataTablePredicates(entry.getValue(), baseTable, columnEquivalences, dataTable));
        }
        freshPlan = filterOutStaleRows(metadata, freshPlan, storagePredicates.build(), storageColumns, dataTable, idAllocator);

        for (Map.Entry<SchemaTableName, List<TupleDomain<String>>> entry : constraints.entrySet()) {
            SchemaTableName baseTable = entry.getKey();
            ChangedRowsPredicate changedRows = changedRowsPredicates.get(baseTable);
            if (!isRowLevel(changedRows)) {
                continue;
            }
            freshPlan = antiJoinAffectedIdentifiers(
                    metadata,
                    session,
                    freshPlan,
                    storageColumns,
                    dataTable,
                    baseTable,
                    entry.getValue(),
                    changedRows,
                    columnEquivalences,
                    node.getViewQueryPlan(),
                    idAllocator,
                    variableAllocator,
                    lookup);
        }
        return freshPlan;
    }

    private static boolean isRowLevel(ChangedRowsPredicate changedRows)
    {
        return changedRows != null && !changedRows.getDataDisjuncts().isEmpty();
    }

    /**
     * Rewrites a base table's stale disjuncts onto the equivalent storage table columns.
     */
    private static List<TupleDomain<String>> equivalentDataTablePredicates(
            List<TupleDomain<String>> disjuncts,
            SchemaTableName baseTable,
            PassthroughColumnEquivalences columnEquivalences,
            SchemaTableName dataTable)
    {
        ImmutableList.Builder<TupleDomain<String>> result = ImmutableList.builder();
        for (TupleDomain<String> disjunct : disjuncts) {
            TupleDomain<String> dataTablePredicate = columnEquivalences.getEquivalentPredicates(baseTable, disjunct).get(dataTable);
            if (dataTablePredicate == null || dataTablePredicate.isAll()) {
                throw new UnsupportedOperationException(format(
                        "stale predicate for %s has no equivalent over the storage table's columns", baseTable));
            }
            result.add(dataTablePredicate);
        }
        return result.build();
    }

    private static PlanNode filterOutStaleRows(
            Metadata metadata,
            PlanNode freshPlan,
            List<TupleDomain<String>> stalePredicates,
            Map<TableColumn, VariableReferenceExpression> storageColumns,
            SchemaTableName dataTable,
            PlanNodeIdAllocator idAllocator)
    {
        if (stalePredicates.isEmpty()) {
            return freshPlan;
        }

        RowExpressionDomainTranslator translator = new RowExpressionDomainTranslator(metadata);
        ImmutableList.Builder<RowExpression> staleExpressions = ImmutableList.builder();
        for (TupleDomain<String> predicate : stalePredicates) {
            TupleDomain<VariableReferenceExpression> bound =
                    predicate.transform(column -> storageColumns.get(new TableColumn(dataTable, column)));
            // An unbound predicate widens to all(), which would negate to FALSE and empty the fresh
            // branch. Decline instead of silently dropping every fresh row.
            if (bound.isAll()) {
                throw new UnsupportedOperationException(format(
                        "stale predicate is not expressible over the columns %s reads", dataTable));
            }
            staleExpressions.add(translator.toPredicate(bound));
        }

        return new FilterNode(
                freshPlan.getSourceLocation(),
                idAllocator.getNextId(),
                freshPlan,
                not(metadata.getFunctionAndTypeManager(), or(staleExpressions.build())));
    }

    /**
     * The identifier columns for a base table are the base columns its stale disjuncts constrain,
     * paired with the storage column each is equivalent to. Both sides must resolve: without the
     * pairing there is no key to anti-join on.
     */
    private static Map<String, VariableReferenceExpression> resolveIdentifierColumns(
            Map<TableColumn, VariableReferenceExpression> storageColumns,
            SchemaTableName dataTable,
            SchemaTableName baseTable,
            List<TupleDomain<String>> disjuncts,
            PassthroughColumnEquivalences columnEquivalences)
    {
        ImmutableMap.Builder<String, VariableReferenceExpression> identifiers = ImmutableMap.builder();
        disjuncts.stream()
                .flatMap(disjunct -> disjunct.getDomains().orElse(ImmutableMap.of()).keySet().stream())
                .distinct()
                .forEach(baseColumn -> {
                    String storageColumn = columnEquivalences.getStorageColumnName(dataTable, baseTable, baseColumn)
                            .orElseThrow(() -> new UnsupportedOperationException(format(
                                    "stale column %s.%s has no equivalent column on the storage table", baseTable, baseColumn)));
                    VariableReferenceExpression storageVariable = storageColumns.get(new TableColumn(dataTable, storageColumn));
                    if (storageVariable == null) {
                        throw new UnsupportedOperationException(format(
                                "storage column %s.%s is not read by the data table plan", dataTable, storageColumn));
                    }
                    identifiers.put(baseColumn, storageVariable);
                });
        return identifiers.buildKeepingLast();
    }

    private static PlanNode antiJoinAffectedIdentifiers(
            Metadata metadata,
            Session session,
            PlanNode freshPlan,
            Map<TableColumn, VariableReferenceExpression> storageColumns,
            SchemaTableName dataTable,
            SchemaTableName baseTable,
            List<TupleDomain<String>> disjuncts,
            ChangedRowsPredicate changedRows,
            PassthroughColumnEquivalences columnEquivalences,
            PlanNode viewQueryPlan,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Lookup lookup)
    {
        TableScanNode baseScan = findTableScan(metadata, session, viewQueryPlan, baseTable, lookup);
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, baseScan.getTable());

        boolean rowLevel = changedRows != null && !changedRows.getDataDisjuncts().isEmpty();
        Map<String, VariableReferenceExpression> identifierColumns = rowLevel
                ? resolveGroupingIdentifiers(metadata, session, viewQueryPlan, storageColumns, dataTable, baseTable, columnEquivalences, lookup)
                : resolveIdentifierColumns(storageColumns, dataTable, baseTable, disjuncts, columnEquivalences);
        List<TupleDomain<ColumnHandle>> changedRowDisjuncts = rowLevel
                ? changedRows.getDataDisjuncts()
                : toHandleDisjuncts(disjuncts, columnHandles, baseTable);

        AffectedIdentifiers affected = buildAffectedIdentifiers(
                metadata,
                session,
                baseScan,
                changedRowDisjuncts,
                resolveIdentifierHandles(identifierColumns.keySet(), columnHandles, baseTable),
                idAllocator,
                variableAllocator);

        return antiJoin(metadata, freshPlan, identifierColumns, affected, idAllocator);
    }

    /**
     * Row-level identifiers are the view query's grouping keys, mapped back to the base columns they
     * read and on to the storage columns those are equivalent to.
     *
     * <p>Declines for shapes v1 defers: a view query with no grouping keys is row-preserving and
     * needs the $rowId_origin storage column, and a join needs the affected groups back-joined to
     * the other base's changed rows.
     */
    private static Map<String, VariableReferenceExpression> resolveGroupingIdentifiers(
            Metadata metadata,
            Session session,
            PlanNode viewQueryPlan,
            Map<TableColumn, VariableReferenceExpression> storageColumns,
            SchemaTableName dataTable,
            SchemaTableName baseTable,
            PassthroughColumnEquivalences columnEquivalences,
            Lookup lookup)
    {
        if (!searchFrom(viewQueryPlan, lookup).where(JoinNode.class::isInstance).findAll().isEmpty()) {
            throw new UnsupportedOperationException("row-level refresh of a materialized view with a join is not supported yet");
        }

        AggregationNode aggregation = searchFrom(viewQueryPlan, lookup)
                .where(AggregationNode.class::isInstance)
                .findAll()
                .stream()
                .map(AggregationNode.class::cast)
                .filter(node -> !node.getGroupingKeys().isEmpty())
                .findFirst()
                .orElseThrow(() -> new UnsupportedOperationException(
                        "row-level refresh of a row-preserving materialized view requires row-origin storage, which is not implemented yet"));

        Map<VariableReferenceExpression, TableColumn> baseColumnByVariable = new HashMap<>();
        buildColumnToVariableMapping(metadata, session, viewQueryPlan, lookup).forEach((column, variable) -> {
            if (column.getTableName().equals(baseTable)) {
                baseColumnByVariable.putIfAbsent(variable, column);
            }
        });

        ImmutableMap.Builder<String, VariableReferenceExpression> identifiers = ImmutableMap.builder();
        for (VariableReferenceExpression groupingKey : aggregation.getGroupingKeys()) {
            TableColumn baseColumn = baseColumnByVariable.get(groupingKey);
            if (baseColumn == null) {
                throw new UnsupportedOperationException(format(
                        "grouping key %s is not a column of %s, so affected groups cannot be identified", groupingKey.getName(), baseTable));
            }
            String storageColumn = columnEquivalences.getStorageColumnName(dataTable, baseTable, baseColumn.getColumnName())
                    .orElseThrow(() -> new UnsupportedOperationException(format(
                            "grouping column %s has no equivalent column on the storage table", baseColumn)));
            VariableReferenceExpression storageVariable = storageColumns.get(new TableColumn(dataTable, storageColumn));
            if (storageVariable == null) {
                throw new UnsupportedOperationException(format(
                        "storage column %s.%s is not read by the data table plan", dataTable, storageColumn));
            }
            identifiers.put(baseColumn.getColumnName(), storageVariable);
        }
        return identifiers.buildKeepingLast();
    }

    /**
     * Partition-level disjuncts arrive keyed by column name while row-level disjuncts arrive keyed by
     * connector column handle. Everything downstream works on handles, which are unambiguous.
     */
    private static List<TupleDomain<ColumnHandle>> toHandleDisjuncts(
            List<TupleDomain<String>> disjuncts,
            Map<String, ColumnHandle> columnHandles,
            SchemaTableName baseTable)
    {
        ImmutableList.Builder<TupleDomain<ColumnHandle>> result = ImmutableList.builder();
        for (TupleDomain<String> disjunct : disjuncts) {
            for (String column : disjunct.getDomains().orElse(ImmutableMap.of()).keySet()) {
                if (!columnHandles.containsKey(column)) {
                    throw new UnsupportedOperationException(format("base table %s does not expose column %s", baseTable, column));
                }
            }
            result.add(disjunct.transform(columnHandles::get));
        }
        return result.build();
    }

    private static Map<String, ColumnHandle> resolveIdentifierHandles(
            Set<String> identifierColumns,
            Map<String, ColumnHandle> columnHandles,
            SchemaTableName baseTable)
    {
        ImmutableMap.Builder<String, ColumnHandle> handles = ImmutableMap.builder();
        for (String column : identifierColumns) {
            ColumnHandle handle = columnHandles.get(column);
            if (handle == null) {
                throw new UnsupportedOperationException(format("base table %s does not expose column %s", baseTable, column));
            }
            handles.put(column, handle);
        }
        return handles.build();
    }

    /**
     * Emits {@code ANTI JOIN(freshPlan, affected_identifiers)}, keyed on the storage column each
     * identifier is equivalent to.
     */
    private static PlanNode antiJoin(
            Metadata metadata,
            PlanNode freshPlan,
            Map<String, VariableReferenceExpression> identifierColumns,
            AffectedIdentifiers affected,
            PlanNodeIdAllocator idAllocator)
    {
        // Equi-join criteria treat null as unequal, so a null identifier would leave the stale
        // storage row in the fresh branch while the delta branch also recomputes it. Matching on
        // NOT(IS DISTINCT FROM) keeps a null identifier joinable, which matters because a null
        // grouping key is a group of its own.
        FunctionResolution functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
        ImmutableList.Builder<RowExpression> matches = ImmutableList.builder();
        for (Map.Entry<String, VariableReferenceExpression> identifier : identifierColumns.entrySet()) {
            VariableReferenceExpression storageVariable = identifier.getValue();
            VariableReferenceExpression affectedVariable = affected.getIdentifier(identifier.getKey());
            matches.add(new CallExpression(
                    "NOT",
                    functionResolution.notFunction(),
                    BooleanType.BOOLEAN,
                    ImmutableList.of(new CallExpression(
                            OperatorType.IS_DISTINCT_FROM.name(),
                            functionResolution.comparisonFunction(OperatorType.IS_DISTINCT_FROM, storageVariable.getType(), affectedVariable.getType()),
                            BooleanType.BOOLEAN,
                            ImmutableList.of(storageVariable, affectedVariable)))));
        }

        List<VariableReferenceExpression> storageOutputs = freshPlan.getOutputVariables();
        JoinNode antiJoin = new JoinNode(
                freshPlan.getSourceLocation(),
                idAllocator.getNextId(),
                LEFT,
                freshPlan,
                affected.getNode(),
                ImmutableList.of(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(storageOutputs)
                        .add(affected.getMarker())
                        .build(),
                Optional.of(and(matches.build())),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        // The marker is non-null for every row the right side produced, so a null marker is exactly
        // an unmatched -- unaffected -- storage row.
        FilterNode unmatched = new FilterNode(
                freshPlan.getSourceLocation(),
                idAllocator.getNextId(),
                antiJoin,
                new SpecialFormExpression(IS_NULL, BooleanType.BOOLEAN, affected.getMarker()));

        return new ProjectNode(
                freshPlan.getSourceLocation(),
                idAllocator.getNextId(),
                unmatched,
                identityAssignments(storageOutputs),
                ProjectNode.Locality.LOCAL);
    }

    /**
     * {@code affected_identifiers(base, identifier_columns) = DISTINCT(from_current_base)}, where
     * {@code from_current_base} scans the base table, keeps the rows the changed-rows disjuncts
     * select, and projects the identifier columns.
     *
     * <p>The disjuncts are the partition-level stale predicates for a partition-level candidate and
     * the connector's row-level changed-rows predicates for a row-level one; the construction is
     * identical either way, which is the point of the shared helper.
     */
    private static AffectedIdentifiers buildAffectedIdentifiers(
            Metadata metadata,
            Session session,
            TableScanNode baseScan,
            List<TupleDomain<ColumnHandle>> disjuncts,
            Map<String, ColumnHandle> identifierColumns,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator)
    {
        // The predicate can constrain columns the identifier set does not include -- a row-level
        // predicate is expressed entirely over change-tracking columns -- so the scan reads the
        // union and the DISTINCT narrows to the identifiers afterwards.
        Map<ColumnHandle, VariableReferenceExpression> variablesByHandle = new LinkedHashMap<>();
        for (ColumnHandle handle : identifierColumns.values()) {
            allocateScanVariable(metadata, session, baseScan, handle, variablesByHandle, variableAllocator);
        }
        for (TupleDomain<ColumnHandle> disjunct : disjuncts) {
            for (ColumnHandle handle : disjunct.getDomains().orElse(ImmutableMap.of()).keySet()) {
                allocateScanVariable(metadata, session, baseScan, handle, variablesByHandle, variableAllocator);
            }
        }

        TableScanNode scan = new TableScanNode(
                baseScan.getSourceLocation(),
                idAllocator.getNextId(),
                baseScan.getTable(),
                ImmutableList.copyOf(variablesByHandle.values()),
                ImmutableMap.copyOf(variablesByHandle).entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey)),
                baseScan.getTableConstraints(),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());

        RowExpressionDomainTranslator translator = new RowExpressionDomainTranslator(metadata);
        ImmutableList.Builder<RowExpression> changedExpressions = ImmutableList.builder();
        for (TupleDomain<ColumnHandle> disjunct : disjuncts) {
            TupleDomain<VariableReferenceExpression> bound = disjunct.transform(variablesByHandle::get);
            if (bound.isAll()) {
                throw new UnsupportedOperationException(format(
                        "changed-rows predicate for %s is not expressible over the scanned columns",
                        metadata.getTableMetadata(session, baseScan.getTable()).getTable()));
            }
            changedExpressions.add(translator.toPredicate(bound));
        }

        FilterNode changedRows = new FilterNode(
                baseScan.getSourceLocation(),
                idAllocator.getNextId(),
                scan,
                or(changedExpressions.build()));

        Map<String, VariableReferenceExpression> identifierVariables = identifierColumns.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> variablesByHandle.get(entry.getValue())));

        AggregationNode distinctIdentifiers = new AggregationNode(
                baseScan.getSourceLocation(),
                idAllocator.getNextId(),
                changedRows,
                ImmutableMap.of(),
                new AggregationNode.GroupingSetDescriptor(ImmutableList.copyOf(identifierVariables.values()), 1, ImmutableSet.of()),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        VariableReferenceExpression marker = variableAllocator.newVariable("affected", BooleanType.BOOLEAN);
        Assignments.Builder markerAssignments = Assignments.builder();
        identifierVariables.values().forEach(variable -> markerAssignments.put(variable, variable));
        markerAssignments.put(marker, TRUE_CONSTANT);

        ProjectNode withMarker = new ProjectNode(
                baseScan.getSourceLocation(),
                idAllocator.getNextId(),
                distinctIdentifiers,
                markerAssignments.build(),
                ProjectNode.Locality.LOCAL);

        return new AffectedIdentifiers(withMarker, identifierVariables, marker);
    }

    private static void allocateScanVariable(
            Metadata metadata,
            Session session,
            TableScanNode baseScan,
            ColumnHandle handle,
            Map<ColumnHandle, VariableReferenceExpression> variablesByHandle,
            VariableAllocator variableAllocator)
    {
        variablesByHandle.computeIfAbsent(handle, column -> {
            ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, baseScan.getTable(), column);
            return variableAllocator.newVariable(columnMetadata.getName(), columnMetadata.getType());
        });
    }

    private static TableScanNode findTableScan(
            Metadata metadata,
            Session session,
            PlanNode plan,
            SchemaTableName tableName,
            Lookup lookup)
    {
        return searchFrom(plan, lookup)
                .where(TableScanNode.class::isInstance)
                .findAll()
                .stream()
                .map(TableScanNode.class::cast)
                .filter(scan -> metadata.getTableMetadata(session, scan.getTable()).getTable().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new UnsupportedOperationException(format(
                        "stale base table %s is not scanned by the view query", tableName)));
    }

    /**
     * The {@code affected_identifiers} relation, the variable each identifier column is bound to,
     * and the marker column that makes the anti-join's unmatched side detectable.
     */
    private static class AffectedIdentifiers
    {
        private final PlanNode node;
        private final Map<String, VariableReferenceExpression> identifiers;
        private final VariableReferenceExpression marker;

        AffectedIdentifiers(PlanNode node, Map<String, VariableReferenceExpression> identifiers, VariableReferenceExpression marker)
        {
            this.node = requireNonNull(node, "node is null");
            this.identifiers = ImmutableMap.copyOf(requireNonNull(identifiers, "identifiers is null"));
            this.marker = requireNonNull(marker, "marker is null");
        }

        PlanNode getNode()
        {
            return node;
        }

        VariableReferenceExpression getIdentifier(String column)
        {
            VariableReferenceExpression variable = identifiers.get(column);
            checkState(variable != null, "no affected-identifier variable for column %s", column);
            return variable;
        }

        VariableReferenceExpression getMarker()
        {
            return marker;
        }
    }

    private static Map<TableColumn, VariableReferenceExpression> buildColumnToVariableMapping(
            Metadata metadata,
            Session session,
            PlanNode plan,
            Lookup lookup)
    {
        Map<TableColumn, VariableReferenceExpression> columnToVariable = new HashMap<>();
        for (PlanNode node : searchFrom(plan, lookup).where(TableScanNode.class::isInstance).findAll()) {
            TableScanNode tableScan = (TableScanNode) node;
            SchemaTableName tableName = metadata.getTableMetadata(session, tableScan.getTable()).getTable();
            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : tableScan.getAssignments().entrySet()) {
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableScan.getTable(), entry.getValue());
                TableColumn column = new TableColumn(tableName, columnMetadata.getName());
                VariableReferenceExpression previous = columnToVariable.putIfAbsent(column, entry.getKey());
                // Two variables bound to one base column means the subtree scans the table more than
                // once (a self-join, say), so there is no single variable a stale predicate on that
                // column can be rewritten to. Bail out rather than pick one arbitrarily; an
                // ImmutableMap builder would instead throw IllegalArgumentException, which is not
                // caught by the stitching fallback and would fail the query outright.
                if (previous != null && !previous.equals(entry.getKey())) {
                    throw new UnsupportedOperationException(format(
                            "base column %s is bound to more than one variable in the plan", column));
                }
            }
        }
        return ImmutableMap.copyOf(columnToVariable);
    }

    private static PlanNode buildUnionNode(
            MaterializedViewScanNode node,
            PlanNode freshPlan,
            Map<VariableReferenceExpression, VariableReferenceExpression> freshMapping,
            PlanNode deltaPlan,
            Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping,
            PlanNodeIdAllocator idAllocator)
    {
        ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs =
                ImmutableListMultimap.builder();

        for (VariableReferenceExpression outputVar : node.getOutputVariables()) {
            VariableReferenceExpression freshVar = freshMapping.get(outputVar);
            VariableReferenceExpression deltaVar = deltaMapping.get(outputVar);
            checkState(freshVar != null && deltaVar != null,
                    "Missing mapping for output variable %s: freshVar=%s, deltaVar=%s", outputVar, freshVar, deltaVar);
            outputsToInputs.put(outputVar, freshVar);
            outputsToInputs.put(outputVar, deltaVar);
        }

        ListMultimap<VariableReferenceExpression, VariableReferenceExpression> mapping = outputsToInputs.build();
        return new UnionNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                ImmutableList.of(freshPlan, deltaPlan),
                ImmutableList.copyOf(mapping.keySet()),
                SetOperationNodeUtils.fromListMultimap(mapping));
    }

    public static Optional<PlanNode> buildDeltaPlanForRefresh(
            RefreshMaterializedViewNode node,
            Metadata metadata,
            Session session,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Map<SchemaTableName, List<TupleDomain<String>>> constraints,
            PassthroughColumnEquivalences columnEquivalences,
            Lookup lookup,
            WarningCollector warningCollector)
    {
        PlanNode sourcePlan = node.getSource();

        Map<VariableReferenceExpression, VariableReferenceExpression> identityMapping =
                sourcePlan.getOutputVariables().stream()
                        .collect(toImmutableMap(identity(), identity()));

        DifferentialPlanRewriter rewriter = new DifferentialPlanRewriter(
                metadata,
                session,
                idAllocator,
                variableAllocator,
                constraints,
                columnEquivalences,
                lookup,
                warningCollector);

        NodeWithMapping deltaResult;
        try {
            deltaResult = rewriter.buildDeltaPlan(sourcePlan, identityMapping);
        }
        catch (UnsupportedOperationException e) {
            return Optional.empty();
        }

        // Build projection to map delta variables back to original output variables
        Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping = deltaResult.getMapping();
        List<VariableReferenceExpression> originalOutputs = node.getOutputVariables();

        Assignments.Builder assignments = Assignments.builder();
        for (VariableReferenceExpression originalVar : originalOutputs) {
            VariableReferenceExpression deltaVar = deltaMapping.get(originalVar);
            checkState(deltaVar != null,
                    "Delta plan is missing mapping for output variable %s. " +
                    "This indicates an incomplete variable mapping from the differential plan rewriter.", originalVar);
            assignments.put(originalVar, deltaVar);
        }

        ProjectNode projectedDelta = new ProjectNode(
                idAllocator.getNextId(),
                deltaResult.getNode(),
                assignments.build());

        return Optional.of(projectedDelta);
    }

    /**
     * Builds a delta plan for the given view query plan.
     *
     * @param viewQueryPlan The view query plan to transform
     * @param viewQueryMappings Mapping from MV output variables to view query variables
     * @return NodeWithMapping containing the delta plan and composed mapping (MV output var → delta var)
     * @throws UnsupportedOperationException if the plan contains unsupported nodes
     */
    public NodeWithMapping buildDeltaPlan(
            PlanNode viewQueryPlan,
            Map<VariableReferenceExpression, VariableReferenceExpression> viewQueryMappings)
    {
        PlanVariants result = viewQueryPlan.accept(new DeltaBuilder(), null);
        Map<VariableReferenceExpression, VariableReferenceExpression> deltaMapping = result.delta().getMapping();

        // Compose mappings: MV output var → view query var → delta var
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> composedMapping = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : viewQueryMappings.entrySet()) {
            VariableReferenceExpression deltaVar = deltaMapping.get(entry.getValue());
            checkState(deltaVar != null,
                    "Missing delta mapping for view query variable %s", entry.getValue());
            composedMapping.put(entry.getKey(), deltaVar);
        }

        return new NodeWithMapping(result.delta().getNode(), composedMapping.build());
    }

    private class DeltaBuilder
            extends InternalPlanVisitor<PlanVariants, Void>
    {
        @Override
        public PlanVariants visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("Unsupported node type: " + node.getClass().getSimpleName());
        }

        @Override
        public PlanVariants visitTableScan(TableScanNode node, Void context)
        {
            SchemaTableName tableName = metadata.getTableMetadata(session, node.getTable()).getTable();

            ChangedRowsPredicate changedRows = changedRowsPredicates.get(tableName);
            if (changedRows != null && !changedRows.getDataDisjuncts().isEmpty()) {
                return rowLevelTableScan(node, tableName, changedRows);
            }
            return partitionLevelTableScan(node, tableName);
        }

        /**
         * The partition-level leaf: the delta is the base rows in a stale partition, and the stale
         * boundary is expressed over the partition columns, so the closure records them and an
         * aggregation above can take Case A when they cover its grouping keys.
         */
        private PlanVariants partitionLevelTableScan(TableScanNode node, SchemaTableName tableName)
        {
            List<TupleDomain<String>> stalePredicates = staleConstraints.getOrDefault(tableName, ImmutableList.of());

            // Build three table scan variants with fresh variables
            NodeWithMapping deltaResult = buildTableScan(node, node.getOutputVariables());
            NodeWithMapping currentResult = buildTableScan(node, node.getOutputVariables());
            NodeWithMapping unchangedResult = buildTableScan(node, node.getOutputVariables());

            RowExpression stalePredicate = buildStalePredicate(node, stalePredicates, deltaResult.getMapping());
            RowExpression unchangedPredicate = not(metadata.getFunctionAndTypeManager(),
                    buildStalePredicate(node, stalePredicates, unchangedResult.getMapping()));

            // Apply stale/non-stale filters
            PlanNode deltaNode = new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), deltaResult.getNode(), stalePredicate);
            PlanNode unchangedNode = new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), unchangedResult.getNode(), unchangedPredicate);

            return new PlanVariants(
                    new NodeWithMapping(deltaNode, deltaResult.getMapping()),
                    currentResult,
                    new NodeWithMapping(unchangedNode, unchangedResult.getMapping()),
                    Optional.of(stalePredicates.stream()
                            .flatMap(predicate -> predicate.getDomains().orElse(ImmutableMap.of()).keySet().stream())
                            .map(column -> new TableColumn(tableName, column))
                            .collect(toImmutableSet())));
        }

        /**
         * The row-level leaf: the delta is the base rows the connector reports as changed. The
         * predicate is expressed over change-tracking columns the view query does not project, so
         * each variant re-reads the table with those columns added and projects them away again,
         * leaving the schema the parent operators expect.
         *
         * <p>The closure is None because a row-level boundary says nothing about any grouping key.
         * That is what forces an aggregation above into Case B expansion, which is correct for any
         * aggregation function.
         */
        private PlanVariants rowLevelTableScan(TableScanNode node, SchemaTableName tableName, ChangedRowsPredicate changedRows)
        {
            Set<ColumnHandle> predicateColumns = changedRows.getDataDisjuncts().stream()
                    .flatMap(disjunct -> disjunct.getDomains().orElse(ImmutableMap.of()).keySet().stream())
                    .collect(toImmutableSet());

            RowLevelScan delta = buildRowLevelScan(node, predicateColumns);
            RowLevelScan unchanged = buildRowLevelScan(node, predicateColumns);
            NodeWithMapping current = buildTableScan(node, node.getOutputVariables());

            RowExpression changedPredicate = buildChangedRowsPredicate(tableName, changedRows, delta);
            RowExpression unchangedPredicate = not(metadata.getFunctionAndTypeManager(),
                    buildChangedRowsPredicate(tableName, changedRows, unchanged));

            return new PlanVariants(
                    delta.filterAndRestore(node, changedPredicate),
                    current,
                    unchanged.filterAndRestore(node, unchangedPredicate),
                    Optional.empty());
        }

        private RowExpression buildChangedRowsPredicate(SchemaTableName tableName, ChangedRowsPredicate changedRows, RowLevelScan scan)
        {
            ImmutableList.Builder<RowExpression> disjuncts = ImmutableList.builder();
            for (TupleDomain<ColumnHandle> disjunct : changedRows.getDataDisjuncts()) {
                TupleDomain<VariableReferenceExpression> bound = disjunct.transform(scan::variableFor);
                if (bound.isAll()) {
                    throw new UnsupportedOperationException(format(
                            "changed-rows predicate for %s is not expressible over the scanned columns", tableName));
                }
                disjuncts.add(translator.toPredicate(bound));
            }
            return or(disjuncts.build());
        }

        /**
         * Rebuilds a scan with extra columns appended, remembering both the mapping the parent
         * operators use and the variable each extra column landed on.
         */
        private RowLevelScan buildRowLevelScan(TableScanNode original, Set<ColumnHandle> extraColumns)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping = createFreshMapping(original.getOutputVariables());
            TableScanNode remapped = new SymbolMapper(mapping, warningCollector).map(original, idAllocator.getNextId());

            Map<ColumnHandle, VariableReferenceExpression> alreadyRead = new HashMap<>();
            remapped.getAssignments().forEach((variable, handle) -> alreadyRead.putIfAbsent(handle, variable));

            List<VariableReferenceExpression> outputs = new ArrayList<>(remapped.getOutputVariables());
            Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>(remapped.getAssignments());
            ImmutableMap.Builder<ColumnHandle, VariableReferenceExpression> extras = ImmutableMap.builder();
            for (ColumnHandle handle : extraColumns) {
                VariableReferenceExpression present = alreadyRead.get(handle);
                if (present != null) {
                    extras.put(handle, present);
                    continue;
                }
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, original.getTable(), handle);
                VariableReferenceExpression variable = variableAllocator.newVariable(columnMetadata.getName(), columnMetadata.getType());
                outputs.add(variable);
                assignments.put(variable, handle);
                extras.put(handle, variable);
            }

            TableScanNode scan = new TableScanNode(
                    remapped.getSourceLocation(),
                    idAllocator.getNextId(),
                    remapped.getTable(),
                    ImmutableList.copyOf(outputs),
                    ImmutableMap.copyOf(assignments),
                    remapped.getTableConstraints(),
                    remapped.getCurrentConstraint(),
                    remapped.getEnforcedConstraint(),
                    remapped.getCteMaterializationInfo());

            return new RowLevelScan(scan, mapping, extras.build());
        }

        private NodeWithMapping buildTableScan(TableScanNode original, List<VariableReferenceExpression> variables)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping = createFreshMapping(variables);
            return new NodeWithMapping(new SymbolMapper(mapping, warningCollector).map(original, idAllocator.getNextId()), mapping);
        }

        private RowExpression buildStalePredicate(
                TableScanNode node,
                List<TupleDomain<String>> stalePredicates,
                Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping)
        {
            Map<String, VariableReferenceExpression> columnToVariable = node.getAssignments().entrySet().stream()
                    .collect(toImmutableMap(
                            entry -> metadata.getColumnMetadata(session, node.getTable(), entry.getValue()).getName(),
                            entry -> variableMapping.get(entry.getKey())));
            ImmutableList.Builder<RowExpression> predicates = ImmutableList.builder();
            for (TupleDomain<String> disjunct : stalePredicates) {
                TupleDomain<VariableReferenceExpression> bound = disjunct.transform(columnToVariable::get);
                // TupleDomain.transform silently drops columns the mapper cannot resolve, so an
                // unbound disjunct widens to all() and toPredicate turns it into TRUE. That would
                // make the delta branch recompute the whole base while the fresh branch still
                // contributes rows, duplicating output and breaking the disjointness invariant.
                if (bound.isAll()) {
                    throw new UnsupportedOperationException(format(
                            "stale predicate for %s is not expressible over the scanned columns",
                            metadata.getTableMetadata(session, node.getTable()).getTable()));
                }
                predicates.add(translator.toPredicate(bound));
            }
            // An empty disjunct list means this base table is not stale, so an empty delta is correct.
            return or(predicates.build());
        }

        @Override
        public PlanVariants visitFilter(FilterNode node, Void context)
        {
            checkDeterministic(node.getPredicate(), "filter predicate");
            PlanVariants child = node.getSources().get(0).accept(this, context);

            // ∆(σ(R)) = σ(∆R), σ(R') = σ(R'), σ(R) = σ(R)
            return new PlanVariants(
                    buildFilter(node, child.delta()),
                    buildFilter(node, child.current()),
                    buildFilter(node, child.unchanged()),
                    child.deltaClosureColumns());
        }

        private NodeWithMapping buildFilter(FilterNode original, NodeWithMapping source)
        {
            return new NodeWithMapping(
                    new SymbolMapper(source.getMapping(), warningCollector).map(original, source.getNode(), idAllocator.getNextId()),
                    source.getMapping());
        }

        @Override
        public PlanVariants visitProject(ProjectNode node, Void context)
        {
            node.getAssignments().getExpressions().forEach(expr -> checkDeterministic(expr, "projection"));
            PlanVariants child = node.getSources().get(0).accept(this, context);

            // ∆(π(R)) = π(∆R), π(R') = π(R'), π(R) = π(R)
            return new PlanVariants(
                    buildProject(node, child.delta()),
                    buildProject(node, child.current()),
                    buildProject(node, child.unchanged()),
                    projectClosure(child.deltaClosureColumns()));
        }

        private NodeWithMapping buildProject(ProjectNode original, NodeWithMapping source)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(source.getMapping(), original.getOutputVariables());
            return new NodeWithMapping(
                    new SymbolMapper(mapping, warningCollector).map(original, source.getNode(), idAllocator.getNextId()),
                    mapping);
        }

        @Override
        public PlanVariants visitAggregation(AggregationNode node, Void context)
        {
            node.getAggregations().values().forEach(agg -> agg.getCall().getArguments()
                    .forEach(expr -> checkDeterministic(expr, "aggregation")));

            PlanVariants child = node.getSources().get(0).accept(this, context);

            NodeWithMapping delta = child.delta();
            if (requiresExpandedDelta(node, child)) {
                delta = buildExpandedDelta(node, child);
            }

            return new PlanVariants(
                    buildAggregation(node, delta),
                    buildAggregation(node, child.current()),
                    buildAggregation(node, child.unchanged()),
                    Optional.empty());
        }

        private boolean requiresExpandedDelta(AggregationNode node, PlanVariants child)
        {
            if (node.getGroupingKeys().isEmpty()) {
                return false;
            }

            Optional<Set<TableColumn>> closure = child.deltaClosureColumns();
            if (!closure.isPresent()) {
                return true;
            }

            Map<TableColumn, VariableReferenceExpression> sourceColumns =
                    buildColumnToVariableMapping(metadata, session, node.getSource(), lookup);
            Set<TableColumn> groupingColumns = sourceColumns.entrySet().stream()
                    .filter(entry -> node.getGroupingKeys().contains(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(toImmutableSet());
            return !groupingColumns.containsAll(closure.get());
        }

        private NodeWithMapping buildExpandedDelta(AggregationNode aggregation, PlanVariants child)
        {
            // The caller also emits child.current() as this aggregation's current variant, so the
            // expansion must read its own copy. Sharing the instance would place one subtree, with
            // its plan node ids and variables, at two positions in the same plan.
            NodeWithMapping current = cloneNodeWithMapping(child.current());

            List<VariableReferenceExpression> deltaGroupingKeys = aggregation.getGroupingKeys().stream()
                    .map(child.delta().getMapping()::get)
                    .collect(toImmutableList());
            List<VariableReferenceExpression> currentGroupingKeys = aggregation.getGroupingKeys().stream()
                    .map(current.getMapping()::get)
                    .collect(toImmutableList());

            checkState(!deltaGroupingKeys.contains(null) && !currentGroupingKeys.contains(null),
                    "Missing grouping-key mapping for expanded materialized view delta");

            AggregationNode affectedGroups = new AggregationNode(
                    aggregation.getSourceLocation(),
                    idAllocator.getNextId(),
                    child.delta().getNode(),
                    ImmutableMap.of(),
                    new AggregationNode.GroupingSetDescriptor(deltaGroupingKeys, 1, ImmutableSet.of()),
                    ImmutableList.of(),
                    SINGLE,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            FunctionResolution functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
            List<RowExpression> matches = new ArrayList<>();
            for (int index = 0; index < currentGroupingKeys.size(); index++) {
                VariableReferenceExpression currentKey = currentGroupingKeys.get(index);
                VariableReferenceExpression affectedKey = deltaGroupingKeys.get(index);
                matches.add(new CallExpression(
                        "NOT",
                        functionResolution.notFunction(),
                        BooleanType.BOOLEAN,
                        ImmutableList.of(new CallExpression(
                                OperatorType.IS_DISTINCT_FROM.name(),
                                functionResolution.comparisonFunction(OperatorType.IS_DISTINCT_FROM, currentKey.getType(), affectedKey.getType()),
                                BooleanType.BOOLEAN,
                                ImmutableList.of(currentKey, affectedKey)))));
            }

            List<VariableReferenceExpression> joinOutputs = ImmutableList.<VariableReferenceExpression>builder()
                    .addAll(current.getNode().getOutputVariables())
                    .addAll(affectedGroups.getOutputVariables())
                    .build();
            JoinNode matchingRows = new JoinNode(
                    aggregation.getSourceLocation(),
                    idAllocator.getNextId(),
                    INNER,
                    current.getNode(),
                    affectedGroups,
                    ImmutableList.of(),
                    joinOutputs,
                    Optional.of(and(matches)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of());

            Assignments.Builder assignments = Assignments.builder();
            for (VariableReferenceExpression variable : current.getNode().getOutputVariables()) {
                assignments.put(variable, variable);
            }
            return new NodeWithMapping(
                    new ProjectNode(
                            aggregation.getSourceLocation(),
                            idAllocator.getNextId(),
                            matchingRows,
                            assignments.build(),
                            ProjectNode.Locality.LOCAL),
                    current.getMapping());
        }

        private NodeWithMapping buildAggregation(AggregationNode original, NodeWithMapping source)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(source.getMapping(), original.getOutputVariables());
            return new NodeWithMapping(
                    new SymbolMapper(mapping, warningCollector).map(original, source.getNode(), idAllocator.getNextId()),
                    mapping);
        }

        @Override
        public PlanVariants visitJoin(JoinNode node, Void context)
        {
            // Only inner joins are supported - outer joins have different IVM rules
            if (node.getType() != INNER) {
                throw new UnsupportedOperationException("Outer joins not supported: " + node.getType());
            }
            node.getFilter().ifPresent(filter -> checkDeterministic(filter, "join filter"));

            // ∆(R ⋈ S) = (∆R ⋈ S') ∪ (R ⋈ ∆S)
            PlanVariants leftVariants = node.getLeft().accept(this, context);
            PlanVariants rightVariants = node.getRight().accept(this, context);

            // Current join: R' ⋈ S'
            NodeWithMapping currentResult = buildJoin(node, leftVariants.current(), rightVariants.current());

            // Unchanged join: R ⋈ S (for propagation through the plan)
            NodeWithMapping unchangedResult = buildJoin(node, cloneNodeWithMapping(leftVariants.unchanged()), rightVariants.unchanged());

            // First delta term: ∆R ⋈ S' (delta from left, current from right)
            NodeWithMapping deltaLeftResult = buildJoin(node, leftVariants.delta(), cloneNodeWithMapping(rightVariants.current()));

            // Second delta term: R ⋈ ∆S (unchanged from left, delta from right)
            // Uses the original left.unchanged() (other use was cloned above)
            NodeWithMapping deltaRightResult = buildJoin(node, leftVariants.unchanged(), rightVariants.delta());

            // Union the delta terms
            NodeWithMapping deltaResult = createBinaryUnion(node, deltaLeftResult.getNode(), deltaRightResult.getNode());

            return new PlanVariants(deltaResult, currentResult, unchangedResult, intersectClosures(leftVariants, rightVariants));
        }

        private NodeWithMapping buildJoin(JoinNode original, NodeWithMapping left, NodeWithMapping right)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(combineMapping(left, right), original.getOutputVariables());
            return new NodeWithMapping(
                    new SymbolMapper(mapping, warningCollector).map(original, left.getNode(), right.getNode(), idAllocator.getNextId()),
                    mapping);
        }

        @Override
        public PlanVariants visitUnion(UnionNode node, Void context)
        {
            // ∆(R ∪ S) = ∆R ∪ ∆S
            List<PlanVariants> children = visitAllSources(node.getSources(), context);
            return new PlanVariants(
                    buildUnion(node, children.stream().map(PlanVariants::delta).collect(toImmutableList())),
                    buildUnion(node, children.stream().map(PlanVariants::current).collect(toImmutableList())),
                    buildUnion(node, children.stream().map(PlanVariants::unchanged).collect(toImmutableList())),
                    intersectClosures(children));
        }

        private NodeWithMapping buildUnion(UnionNode original, List<NodeWithMapping> sources)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(combineMapping(sources.toArray(new NodeWithMapping[0])), original.getOutputVariables());
            List<PlanNode> sourceNodes = sources.stream().map(NodeWithMapping::getNode).collect(toImmutableList());
            return new NodeWithMapping(
                    new SymbolMapper(mapping, warningCollector).map(original, sourceNodes, idAllocator.getNextId()),
                    mapping);
        }

        @Override
        public PlanVariants visitIntersect(IntersectNode node, Void context)
        {
            // ∆(R ∩ S) = (∆R ∩ S') ∪ (R ∩ ∆S)
            // The delta formula assumes binary INTERSECT; n-ary INTERSECT should be
            // decomposed into a binary tree before reaching this code.
            List<PlanNode> sources = node.getSources();
            checkState(sources.size() == 2,
                    "INTERSECT with more than 2 sources is not supported for differential stitching, found %s sources",
                    sources.size());
            List<PlanVariants> allVariants = visitAllSources(sources, context);

            // Current: R' ∩ S'
            NodeWithMapping currentResult = buildIntersect(node, allVariants.stream().map(PlanVariants::current).collect(toImmutableList()));

            // Unchanged: R ∩ S
            NodeWithMapping unchangedResult = buildIntersect(node, allVariants.stream().map(PlanVariants::unchanged).collect(toImmutableList()));

            // Delta: union of left and right delta terms
            IntersectNode deltaLeft = buildIntersectDeltaLeft(node, sources, allVariants);
            IntersectNode deltaRight = buildIntersectDeltaRight(node, sources, allVariants);
            NodeWithMapping deltaUnionResult = createBinaryUnion(node, deltaLeft, deltaRight);

            return new PlanVariants(deltaUnionResult, currentResult, unchangedResult, intersectClosures(allVariants));
        }

        private IntersectNode buildIntersectDeltaLeft(
                IntersectNode original,
                List<PlanNode> originalSources,
                List<PlanVariants> allVariants)
        {
            List<NodeWithMapping> sources = new ArrayList<>();
            sources.add(allVariants.get(0).delta());

            // Filter remaining sources' current to first source's stale partitions
            for (int i = 1; i < allVariants.size(); i++) {
                NodeWithMapping clonedCurrent = cloneNodeWithMapping(allVariants.get(i).current());
                RowExpression stalePredicate = buildPropagatedStalePredicate(clonedCurrent.getNode(), originalSources.get(0));
                sources.add(new NodeWithMapping(
                        buildFilter(clonedCurrent.getNode(), stalePredicate),
                        clonedCurrent.getMapping()));
            }
            return buildIntersectFromSources(original, sources);
        }

        private IntersectNode buildIntersectDeltaRight(
                IntersectNode original,
                List<PlanNode> originalSources,
                List<PlanVariants> allVariants)
        {
            // Clone and filter R's unchanged to S's stale partitions
            NodeWithMapping firstUnchanged = cloneNodeWithMapping(allVariants.get(0).unchanged());
            ImmutableList.Builder<RowExpression> stalePredicates = ImmutableList.builder();
            for (int i = 1; i < allVariants.size(); i++) {
                stalePredicates.add(buildPropagatedStalePredicate(firstUnchanged.getNode(), originalSources.get(i)));
            }
            NodeWithMapping filteredFirst = new NodeWithMapping(
                    buildFilter(firstUnchanged.getNode(), or(stalePredicates.build())),
                    firstUnchanged.getMapping());

            List<NodeWithMapping> sources = new ArrayList<>();
            sources.add(filteredFirst);
            for (int i = 1; i < allVariants.size(); i++) {
                sources.add(allVariants.get(i).delta());
            }
            return buildIntersectFromSources(original, sources);
        }

        private IntersectNode buildIntersectFromSources(IntersectNode original, List<NodeWithMapping> sources)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(combineMapping(sources.toArray(new NodeWithMapping[0])), original.getOutputVariables());
            List<PlanNode> sourceNodes = sources.stream().map(NodeWithMapping::getNode).collect(toImmutableList());
            return new SymbolMapper(mapping, warningCollector).map(original, sourceNodes, idAllocator.getNextId());
        }

        private List<PlanVariants> visitAllSources(List<PlanNode> sources, Void context)
        {
            return sources.stream()
                    .map(source -> source.accept(this, context))
                    .collect(toImmutableList());
        }

        private PlanNode buildFilter(PlanNode source, RowExpression predicate)
        {
            return new FilterNode(source.getSourceLocation(), idAllocator.getNextId(), source, predicate);
        }

        private NodeWithMapping buildIntersect(IntersectNode original, List<NodeWithMapping> sources)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(combineMapping(sources.toArray(new NodeWithMapping[0])), original.getOutputVariables());
            List<PlanNode> sourceNodes = sources.stream().map(NodeWithMapping::getNode).collect(toImmutableList());
            return new NodeWithMapping(
                    new SymbolMapper(mapping, warningCollector).map(original, sourceNodes, idAllocator.getNextId()),
                    mapping);
        }

        @Override
        public PlanVariants visitExcept(ExceptNode node, Void context)
        {
            // EXCEPT is anti-monotonic in the right input.
            // deltaLeft: (∆A - B') handles stale left side via delta algebra
            // deltaRight: (A[B's stale] - B') handles stale right side via partition replacement
            // The delta formula assumes binary EXCEPT; n-ary EXCEPT should be
            // decomposed into a binary tree before reaching this code.
            List<PlanNode> sources = node.getSources();
            checkState(sources.size() == 2,
                    "EXCEPT with more than 2 sources is not supported for differential stitching, found %s sources",
                    sources.size());
            List<PlanVariants> allVariants = visitAllSources(sources, context);

            // Current: A' - B'
            NodeWithMapping currentResult = buildExcept(node, allVariants.stream().map(PlanVariants::current).collect(toImmutableList()));

            // Unchanged: A - B'
            // The right side must be B' (current) because rows added to B since the last
            // refresh may cancel out rows in A. Using stale B would include rows that no
            // longer survive the EXCEPT.
            ImmutableList.Builder<NodeWithMapping> unchangedSources = ImmutableList.builder();
            unchangedSources.add(allVariants.get(0).unchanged());
            for (int i = 1; i < allVariants.size(); i++) {
                unchangedSources.add(cloneNodeWithMapping(allVariants.get(i).current()));
            }
            NodeWithMapping unchangedResult = buildExcept(node, unchangedSources.build());

            // Delta: union of left and right delta terms
            ExceptNode deltaLeft = buildExceptDeltaLeft(node, allVariants);
            ExceptNode deltaRight = buildExceptDeltaRight(node, sources, allVariants);
            NodeWithMapping deltaUnionResult = createBinaryUnion(node, deltaLeft, deltaRight);

            return new PlanVariants(deltaUnionResult, currentResult, unchangedResult, intersectClosures(allVariants));
        }

        private Optional<Set<TableColumn>> projectClosure(Optional<Set<TableColumn>> closure)
        {
            // A projection can remove or transform a stale-boundary column. Until the
            // pass-through mapping is carried with PlanVariants, treat non-empty closures
            // as unknown so aggregation dispatch selects the conservative expansion path.
            if (!closure.isPresent() || !closure.get().isEmpty()) {
                return Optional.empty();
            }
            return closure;
        }

        private Optional<Set<TableColumn>> intersectClosures(PlanVariants... variants)
        {
            return intersectClosures(Arrays.asList(variants));
        }

        private Optional<Set<TableColumn>> intersectClosures(List<PlanVariants> variants)
        {
            if (variants.stream().anyMatch(variant -> !variant.deltaClosureColumns().isPresent())) {
                return Optional.empty();
            }

            if (variants.isEmpty()) {
                return Optional.of(ImmutableSet.of());
            }

            ImmutableSet.Builder<TableColumn> result = ImmutableSet.builder();
            for (TableColumn column : variants.get(0).deltaClosureColumns().get()) {
                if (variants.stream().allMatch(variant -> variant.deltaClosureColumns().get().contains(column))) {
                    result.add(column);
                }
            }
            return Optional.of(result.build());
        }

        private ExceptNode buildExceptDeltaLeft(ExceptNode original, List<PlanVariants> allVariants)
        {
            List<NodeWithMapping> sources = new ArrayList<>();
            sources.add(allVariants.get(0).delta());
            for (int i = 1; i < allVariants.size(); i++) {
                sources.add(cloneNodeWithMapping(allVariants.get(i).current()));
            }
            return buildExceptFromSources(original, sources);
        }

        private ExceptNode buildExceptDeltaRight(
                ExceptNode original,
                List<PlanNode> originalSources,
                List<PlanVariants> allVariants)
        {
            // Filter A's unchanged rows to only those matching B's stale partitions
            NodeWithMapping firstUnchanged = cloneNodeWithMapping(allVariants.get(0).unchanged());
            RowExpression stalePredicate = buildPropagatedStalePredicate(firstUnchanged.getNode(), originalSources.get(1));
            NodeWithMapping filteredFirst = new NodeWithMapping(
                    buildFilter(firstUnchanged.getNode(), stalePredicate),
                    firstUnchanged.getMapping());

            List<NodeWithMapping> sources = new ArrayList<>();
            sources.add(filteredFirst);
            for (int i = 1; i < allVariants.size(); i++) {
                sources.add(cloneNodeWithMapping(allVariants.get(i).current()));
            }
            return buildExceptFromSources(original, sources);
        }

        private ExceptNode buildExceptFromSources(ExceptNode original, List<NodeWithMapping> sources)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(combineMapping(sources.toArray(new NodeWithMapping[0])), original.getOutputVariables());
            List<PlanNode> sourceNodes = sources.stream().map(NodeWithMapping::getNode).collect(toImmutableList());
            return new SymbolMapper(mapping, warningCollector).map(original, sourceNodes, idAllocator.getNextId());
        }

        private NodeWithMapping buildExcept(ExceptNode original, List<NodeWithMapping> sources)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mapping =
                    extendMapping(combineMapping(sources.toArray(new NodeWithMapping[0])), original.getOutputVariables());
            List<PlanNode> sourceNodes = sources.stream().map(NodeWithMapping::getNode).collect(toImmutableList());
            return new NodeWithMapping(
                    new SymbolMapper(mapping, warningCollector).map(original, sourceNodes, idAllocator.getNextId()),
                    mapping);
        }

        @Override
        public PlanVariants visitSort(SortNode node, Void context)
        {
            // Sort cannot be stitched: UNION of sorted subsets is not globally sorted
            throw new UnsupportedOperationException(
                    "Sort cannot be differentially stitched: UNION of sorted partitions does not preserve global ordering");
        }

        @Override
        public PlanVariants visitLimit(LimitNode node, Void context)
        {
            // Limit cannot be stitched: UNION of limited subsets may have wrong row count
            throw new UnsupportedOperationException(
                    "Limit cannot be differentially stitched: UNION of limited partitions may return incorrect row count");
        }

        @Override
        public PlanVariants visitTopN(TopNNode node, Void context)
        {
            // TopN cannot be stitched: UNION of top-N subsets is not global top-N
            throw new UnsupportedOperationException(
                    "TopN cannot be differentially stitched: UNION of top-N partitions does not preserve global top-N ordering");
        }

        @Override
        public PlanVariants visitGroupReference(GroupReference node, Void context)
        {
            return lookup.resolve(node).accept(this, context);
        }

        private void checkDeterministic(RowExpression expression, String context)
        {
            if (!determinismEvaluator.isDeterministic(expression)) {
                throw new UnsupportedOperationException("Non-deterministic expression in " + context);
            }
        }

        /**
         * Builds a stale predicate for the left side of INTERSECT/EXCEPT by finding stale TableScans
         * in the right subtree and rewriting their stale predicates to left-side columns
         * using column equivalences from MV metadata.
         */
        private RowExpression buildPropagatedStalePredicate(PlanNode leftSubtree, PlanNode rightSubtree)
        {
            // Build column-to-variable mapping for the left subtree
            Map<TableColumn, VariableReferenceExpression> leftColumnMapping =
                    buildColumnToVariableMapping(metadata, session, leftSubtree, lookup);

            List<RowExpression> predicates = searchFrom(rightSubtree, lookup)
                    .where(TableScanNode.class::isInstance)
                    .findAll()
                    .stream()
                    .map(TableScanNode.class::cast)
                    .flatMap(tableScan -> rewriteStalePredicatesToLeftColumns(tableScan, leftColumnMapping).stream())
                    .collect(toImmutableList());

            return or(predicates);
        }

        /**
         * For a given TableScan on the right side, converts its stale predicates to RowExpressions
         * that reference left-side variables using column equivalences from MV metadata.
         */
        private List<RowExpression> rewriteStalePredicatesToLeftColumns(
                TableScanNode tableScan,
                Map<TableColumn, VariableReferenceExpression> leftColumnMapping)
        {
            SchemaTableName tableName = metadata.getTableMetadata(session, tableScan.getTable()).getTable();
            List<TupleDomain<String>> stalePredicates = staleConstraints.getOrDefault(tableName, ImmutableList.of());
            return columnEquivalences.translatePredicatesToVariables(tableName, stalePredicates, leftColumnMapping, translator);
        }

        private Map<VariableReferenceExpression, VariableReferenceExpression> createFreshMapping(
                List<VariableReferenceExpression> variables)
        {
            return variables.stream()
                    .collect(toImmutableMap(
                            Function.identity(),
                            expression -> variableAllocator.newVariable(expression.getName(), expression.getType())));
        }

        private Map<VariableReferenceExpression, VariableReferenceExpression> extendMapping(
                Map<VariableReferenceExpression, VariableReferenceExpression> existing,
                List<VariableReferenceExpression> outputVariables)
        {
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> builder = ImmutableMap.builder();
            builder.putAll(existing);
            outputVariables.stream()
                    .filter(variable -> !existing.containsKey(variable))
                    .forEach(variable -> builder.put(variable, variableAllocator.newVariable(variable.getName(), variable.getType())));
            return builder.buildKeepingLast();
        }

        private Map<VariableReferenceExpression, VariableReferenceExpression> combineMapping(NodeWithMapping... sources)
        {
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> builder = ImmutableMap.builder();
            Arrays.stream(sources).map(NodeWithMapping::getMapping).forEach(builder::putAll);
            return builder.buildKeepingLast();
        }

        private NodeWithMapping createBinaryUnion(PlanNode original, PlanNode left, PlanNode right)
        {
            List<VariableReferenceExpression> leftOutputs = left.getOutputVariables();
            List<VariableReferenceExpression> rightOutputs = right.getOutputVariables();

            List<VariableReferenceExpression> unionOutputs = new ArrayList<>();
            Map<VariableReferenceExpression, List<VariableReferenceExpression>> variableMapping = new HashMap<>();
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> outputMapping = ImmutableMap.builder();

            for (int i = 0; i < leftOutputs.size(); i++) {
                VariableReferenceExpression leftVar = leftOutputs.get(i);
                VariableReferenceExpression rightVar = rightOutputs.get(i);
                VariableReferenceExpression outputVar = variableAllocator.newVariable(leftVar.getName(), leftVar.getType());
                unionOutputs.add(outputVar);
                variableMapping.put(outputVar, ImmutableList.of(leftVar, rightVar));

                if (i < original.getOutputVariables().size()) {
                    outputMapping.put(original.getOutputVariables().get(i), outputVar);
                }
            }

            UnionNode unionNode = new UnionNode(
                    original.getSourceLocation(),
                    idAllocator.getNextId(),
                    Optional.empty(),
                    ImmutableList.of(left, right),
                    unionOutputs,
                    variableMapping);

            return new NodeWithMapping(unionNode, outputMapping.build());
        }

        private NodeWithMapping cloneNodeWithMapping(NodeWithMapping original)
        {
            SubtreeRemappingVisitor visitor = new SubtreeRemappingVisitor();
            PlanNode clonedPlan = original.getNode().accept(visitor, null);
            Map<VariableReferenceExpression, VariableReferenceExpression> variableRenaming = visitor.getMapping();

            // Compose mappings: for each (origVar, currVar) in original mapping,
            // the new mapping is (origVar, renaming[currVar])
            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> newMapping = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : original.getMapping().entrySet()) {
                VariableReferenceExpression renamedVariable = variableRenaming.get(entry.getValue());
                if (renamedVariable != null) {
                    newMapping.put(entry.getKey(), renamedVariable);
                }
            }

            return new NodeWithMapping(clonedPlan, newMapping.build());
        }

        private class SubtreeRemappingVisitor
                extends InternalPlanVisitor<PlanNode, Void>
        {
            private final Map<VariableReferenceExpression, VariableReferenceExpression> mapping = new HashMap<>();

            public Map<VariableReferenceExpression, VariableReferenceExpression> getMapping()
            {
                return ImmutableMap.copyOf(mapping);
            }

            private void ensureVariablesMapped(List<VariableReferenceExpression> variables)
            {
                for (VariableReferenceExpression variable : variables) {
                    mapping.computeIfAbsent(variable, v -> variableAllocator.newVariable(v.getName(), v.getType()));
                }
            }

            private SymbolMapper getMapper()
            {
                return new SymbolMapper(mapping, warningCollector);
            }

            @Override
            public PlanNode visitPlan(PlanNode node, Void context)
            {
                throw new UnsupportedOperationException(
                        "Cannot clone node type: " + node.getClass().getSimpleName() +
                        ". Add visitXxx method to SubtreeRemappingVisitor to support this node type.");
            }

            @Override
            public PlanNode visitTableScan(TableScanNode node, Void context)
            {
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitFilter(FilterNode node, Void context)
            {
                PlanNode newSource = node.getSource().accept(this, context);
                // Filter passes through source variables, no new allocations needed
                return getMapper().map(node, newSource, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitProject(ProjectNode node, Void context)
            {
                PlanNode newSource = node.getSource().accept(this, context);
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, newSource, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitAggregation(AggregationNode node, Void context)
            {
                PlanNode newSource = node.getSource().accept(this, context);
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, newSource, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitJoin(JoinNode node, Void context)
            {
                PlanNode newLeft = node.getLeft().accept(this, context);
                PlanNode newRight = node.getRight().accept(this, context);
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, newLeft, newRight, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitUnion(UnionNode node, Void context)
            {
                List<PlanNode> newSources = node.getSources().stream()
                        .map(source -> source.accept(this, context))
                        .collect(toImmutableList());
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, newSources, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitIntersect(IntersectNode node, Void context)
            {
                List<PlanNode> newSources = node.getSources().stream()
                        .map(source -> source.accept(this, context))
                        .collect(toImmutableList());
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, newSources, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitExcept(ExceptNode node, Void context)
            {
                List<PlanNode> newSources = node.getSources().stream()
                        .map(source -> source.accept(this, context))
                        .collect(toImmutableList());
                ensureVariablesMapped(node.getOutputVariables());
                return getMapper().map(node, newSources, idAllocator.getNextId());
            }

            @Override
            public PlanNode visitGroupReference(GroupReference node, Void context)
            {
                throw new IllegalStateException(
                        "GroupReference should have been resolved by DeltaBuilder before cloning. " +
                        "This indicates the plan was not fully resolved before SubtreeRemappingVisitor was invoked.");
            }
        }
    }

    /**
     * A base scan re-read with extra change-tracking columns appended, so a predicate over columns
     * the view query does not project can still be applied at the leaf.
     */
    private class RowLevelScan
    {
        private final TableScanNode scan;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> mapping;
        private final Map<ColumnHandle, VariableReferenceExpression> extraColumns;

        RowLevelScan(
                TableScanNode scan,
                Map<VariableReferenceExpression, VariableReferenceExpression> mapping,
                Map<ColumnHandle, VariableReferenceExpression> extraColumns)
        {
            this.scan = requireNonNull(scan, "scan is null");
            this.mapping = ImmutableMap.copyOf(requireNonNull(mapping, "mapping is null"));
            this.extraColumns = ImmutableMap.copyOf(requireNonNull(extraColumns, "extraColumns is null"));
        }

        VariableReferenceExpression variableFor(ColumnHandle column)
        {
            return extraColumns.get(column);
        }

        /**
         * Applies the predicate and projects back to the columns the view query scan produced, so
         * the appended change-tracking columns do not leak into the parent operators.
         */
        NodeWithMapping filterAndRestore(TableScanNode original, RowExpression predicate)
        {
            FilterNode filtered = new FilterNode(scan.getSourceLocation(), idAllocator.getNextId(), scan, predicate);
            List<VariableReferenceExpression> restored = original.getOutputVariables().stream()
                    .map(mapping::get)
                    .collect(toImmutableList());
            if (restored.equals(scan.getOutputVariables())) {
                return new NodeWithMapping(filtered, mapping);
            }
            return new NodeWithMapping(
                    new ProjectNode(
                            scan.getSourceLocation(),
                            idAllocator.getNextId(),
                            filtered,
                            identityAssignments(restored),
                            ProjectNode.Locality.LOCAL),
                    mapping);
        }
    }

    /**
     * A plan node paired with its variable mapping (original variable → new variable).
     * Used for results of node building operations.
     */
    public static class NodeWithMapping
    {
        private final PlanNode node;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> mapping;

        NodeWithMapping(PlanNode node, Map<VariableReferenceExpression, VariableReferenceExpression> mapping)
        {
            this.node = requireNonNull(node, "node is null");
            this.mapping = ImmutableMap.copyOf(requireNonNull(mapping, "mapping is null"));
        }

        public PlanNode getNode()
        {
            return node;
        }

        public Map<VariableReferenceExpression, VariableReferenceExpression> getMapping()
        {
            return mapping;
        }
    }

    /**
     * Three plan variants for IVM (matching the algebraic framework):
     * <ul>
     *   <li>delta (∆R): rows from stale partitions — what changed</li>
     *   <li>current (R'): complete current state</li>
     *   <li>unchanged (R): rows from non-stale partitions — R'[non-stale] = R[non-stale]</li>
     * </ul>
     *
     * <p>R' = R ∪ ∆R (for insert-only, partition-aligned staleness)
     */
    private static class PlanVariants
    {
        private final NodeWithMapping delta;
        private final NodeWithMapping current;
        private final NodeWithMapping unchanged;
        private final Optional<Set<TableColumn>> deltaClosureColumns;

        PlanVariants(NodeWithMapping delta, NodeWithMapping current, NodeWithMapping unchanged)
        {
            this(delta, current, unchanged, Optional.empty());
        }

        PlanVariants(NodeWithMapping delta, NodeWithMapping current, NodeWithMapping unchanged, Optional<Set<TableColumn>> deltaClosureColumns)
        {
            this.delta = requireNonNull(delta, "delta is null");
            this.current = requireNonNull(current, "current is null");
            this.unchanged = requireNonNull(unchanged, "unchanged is null");
            this.deltaClosureColumns = requireNonNull(deltaClosureColumns, "deltaClosureColumns is null")
                    .map(ImmutableSet::copyOf);
        }

        NodeWithMapping delta()
        {
            return delta;
        }

        NodeWithMapping current()
        {
            return current;
        }

        NodeWithMapping unchanged()
        {
            return unchanged;
        }

        Optional<Set<TableColumn>> deltaClosureColumns()
        {
            return deltaClosureColumns;
        }
    }
}
