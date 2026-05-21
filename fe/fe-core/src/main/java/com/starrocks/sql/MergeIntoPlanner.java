// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.ScalarOperatorToIcebergExpr;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.EnforceUniqueRowLocatorNode;
import com.starrocks.planner.HashJoinNode;
import com.starrocks.planner.IcebergRowDeltaSink;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.NestLoopJoinNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TIcebergWriteMode;
import com.starrocks.thrift.TResultSinkType;
import org.apache.iceberg.expressions.Expression;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

public class MergeIntoPlanner {

    private static final String OP_CODE_COLUMN_NAME = "op_code";

    /**
     * Append the analyzer-prepared routing Expr to the SELECT list as a sink-private
     * column. After this, the SELECT carries [_file, _pos, data_cols..., op_code] and
     * the rest of the planner (transformer, optimizer, fragment builder) handles op_code
     * exactly like any other output column. Since op_code's ColumnRefOperator is part of
     * the optimizer's required-output set, column pruning cannot remove it; the BE
     * row-delta sink later validates the trailing TINYINT slot via write_mode = ROW_DELTA_MIXED.
     */
    private static void injectOpCodeRoutingColumn(MergeIntoStmt stmt, QueryRelation query) {
        Expr routingExpr = stmt.getRoutingExpr();
        Preconditions.checkState(routingExpr != null,
                "analyzer must produce a routing expression for MERGE INTO");
        Preconditions.checkState(query instanceof SelectRelation,
                "MERGE INTO query relation must be a SelectRelation");
        SelectRelation selectRelation = (SelectRelation) query;
        SelectList selectList = selectRelation.getSelectList();
        // Runs exactly once per statement: analyze + plan are not re-run on the same
        // MergeIntoStmt (DML retries re-parse to a fresh AST, and the analyzer already
        // mutates this AST upstream), so no double-injection guard is needed here.
        selectList.addItem(new SelectListItem(routingExpr, OP_CODE_COLUMN_NAME));
        List<Expr> extendedOutput = Lists.newArrayList(selectRelation.getOutputExpression());
        extendedOutput.add(routingExpr);
        selectRelation.setOutputExpr(extendedOutput);
    }

    public ExecPlan plan(MergeIntoStmt mergeIntoStmt, ConnectContext session) {
        // The BE EnforceUniqueRowLocatorNode is pipeline-engine-only (its non-pipeline entry
        // points return NotSupported), so fail fast with a clear message instead of
        // letting the BE error out at runtime.
        if (!session.getSessionVariable().isEnablePipelineEngine()) {
            throw new SemanticException("MERGE INTO requires the pipeline engine; " +
                    "set enable_pipeline_engine=true");
        }
        QueryRelation query = mergeIntoStmt.getQueryStatement().getQueryRelation();
        // Inject the op_code routing column as a sink-private projection. The analyzer
        // hands us a routing Expr (resolved against the join scope) and a clean SELECT;
        // we add op_code here, just before transformation, so it (a) goes through the
        // standard Expr → ScalarOperator pipeline together with the data columns and
        // (b) ends up in the optimizer's required-output set so column pruning cannot
        // drop it. The user-visible columnOutputNames on the stmt remain data-only;
        // BE consumes the trailing op_code slot via ROW_DELTA_MIXED.
        injectOpCodeRoutingColumn(mergeIntoStmt, query);
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transform(query);

        List<ColumnRefOperator> outputColumns = logicalPlan.getOutputColumn();
        Table targetTable = mergeIntoStmt.getTable();

        if (!(targetTable instanceof IcebergTable)) {
            throw new SemanticException("MERGE INTO is only supported for Iceberg tables");
        }

        IcebergTable icebergTable = (IcebergTable) targetTable;
        // Build the sink-tuple column-name list: analyzer-supplied user columns + the
        // trailing routing column the planner just added.
        List<String> colNames = Lists.newArrayList(mergeIntoStmt.getOutputColumnNames());
        colNames.add(OP_CODE_COLUMN_NAME);

        // The required property only serves Iceberg WRITE CLUSTERING (partitioned
        // targets shuffle by partition columns so each sink instance writes few
        // partitions); it plays no role in duplicate-match correctness. The check is a
        // LOCAL operation directly above the merge join — the join's shuffle
        // distribution (hint set by MergeIntoAnalyzer) already co-locates all matches
        // of one target row. (The 3-arg overload matches partition columns by the
        // saved output names — MERGE wraps data columns in CASE exprs, so
        // ColumnRefOperator names degrade to "case".)
        PhysicalPropertySet requiredProperty =
                IcebergPlannerUtils.createShuffleProperty(icebergTable, outputColumns, colNames);

        return createMergePlan(mergeIntoStmt, session, logicalPlan.getRootBuilder().getRoot(),
                columnRefFactory, outputColumns, colNames, icebergTable, requiredProperty);
    }

    private ExecPlan createMergePlan(MergeIntoStmt mergeIntoStmt, ConnectContext session,
                                     OptExpression logicalRoot, ColumnRefFactory columnRefFactory,
                                     List<ColumnRefOperator> outputColumns, List<String> colNames,
                                     IcebergTable icebergTable, PhysicalPropertySet requiredProperty) {
        boolean prevIsEnableLocalShuffleAgg = session.getSessionVariable().isEnableLocalShuffleAgg();
        try {
            session.getSessionVariable().setEnableLocalShuffleAgg(false);

            // Optimize
            OptimizerContext optimizerContext = OptimizerFactory.initContext(session, columnRefFactory);
            Optimizer optimizer = OptimizerFactory.create(optimizerContext);
            OptExpression optimizedPlan = optimizer.optimize(
                    logicalRoot, requiredProperty, new ColumnRefSet(outputColumns));

            // Build physical plan
            ExecPlan execPlan = PlanFragmentBuilder.createPhysicalPlan(optimizedPlan, session,
                    outputColumns, columnRefFactory, colNames, TResultSinkType.MYSQL_PROTOCAL, false);

            // Setup Iceberg sink and configure pipeline. plan() already rejected
            // non-pipeline sessions, so the sink always runs on the pipeline engine.
            setupIcebergMergeSink(execPlan, mergeIntoStmt, colNames, icebergTable, session);
            IcebergPlannerUtils.configureIcebergSinkPipeline(execPlan, session, true);

            return execPlan;
        } finally {
            session.getSessionVariable().setEnableLocalShuffleAgg(prevIsEnableLocalShuffleAgg);
        }
    }

    private void setupIcebergMergeSink(ExecPlan execPlan, MergeIntoStmt mergeIntoStmt, List<String> colNames,
                                       IcebergTable icebergTable, ConnectContext session) {
        DescriptorTable descriptorTable = execPlan.getDescTbl();
        TupleDescriptor rowDeltaTuple = descriptorTable.createTupleDescriptor();

        List<Expr> outputExprs = execPlan.getOutputExprs();
        Preconditions.checkArgument(colNames.size() == outputExprs.size(),
                "output column size mismatch");
        for (int index = 0; index < colNames.size(); ++index) {
            SlotDescriptor slot = descriptorTable.addSlotDescriptor(rowDeltaTuple);
            slot.setIsMaterialized(true);
            slot.setType(outputExprs.get(index).getType());
            slot.setColumn(new Column(colNames.get(index), outputExprs.get(index).getType()));
            slot.setIsNullable(outputExprs.get(index).isNullable());
        }
        rowDeltaTuple.computeMemLayout();

        descriptorTable.addReferencedTable(icebergTable);
        IcebergRowDeltaSink dataSink = new IcebergRowDeltaSink(
                icebergTable, rowDeltaTuple, session.getSessionVariable(), TIcebergWriteMode.ROW_DELTA_MIXED);
        dataSink.init();

        IcebergMetadata.IcebergSinkExtra icebergSinkExtra = new IcebergMetadata.IcebergSinkExtra();
        icebergSinkExtra.setOperationType(IcebergMetadata.IcebergSinkExtra.OperationType.MERGE);
        // For MERGE, the plan has source LEFT JOIN target — there may be multiple
        // IcebergScanNodes (e.g., self-merge or USING another Iceberg table).
        // Keep the target scan and merge join bound together so sink validation and
        // duplicate checking never fall back to scans/joins from the source subtree.
        MergeTargetContext mergeTargetContext = findMergeTargetContext(execPlan, icebergTable);
        Expression filterExpr = mergeTargetContext == null
                ? null
                : buildTargetIcebergFilterExpr(mergeTargetContext.targetScan);
        if (filterExpr != null) {
            icebergSinkExtra.setConflictDetectionFilter(filterExpr);
        }
        // Freeze the validation starting point at the same target scan used for the
        // conflict filter. A self-merge source may also scan the target table and
        // project _file/_pos, so table id plus usedForDelete is not enough to
        // identify the scan being modified.
        if (mergeTargetContext != null && mergeTargetContext.targetScan != null) {
            icebergSinkExtra.setBaseSnapshotId(mergeTargetContext.targetScan.getBaseSnapshotId().orElse(null));
        }
        dataSink.setSinkExtraInfo(icebergSinkExtra);

        execPlan.getFragments().get(0).setSink(dataSink);

        // Insert EnforceUniqueRowLocatorNode to check that each target row is matched at most once.
        // Key columns are _file and _pos, identified by SLOT ID — the BE resolves the
        // chunk columns through the chunk's slot-id map.
        // Elide the check when source uniqueness over the ON keys already guarantees
        // at-most-one match (see canElideEnforceUnique).
        if (!canElideEnforceUnique(mergeIntoStmt)) {
            insertEnforceUniqueRowLocatorNode(execPlan, mergeTargetContext);
        }
    }

    /**
     * MERGE requires at most one source row to match each target row. If the source
     * relation is known to be unique on key set U, and every column in U is covered
     * by an equality predicate between target and source in the ON clause, then any
     * fixed target row can match at most one source row: two matching source rows
     * would have identical U values, contradicting source uniqueness. In that case
     * the runtime EnforceUniqueRowLocatorNode is redundant and can be safely omitted.
     */
    private boolean canElideEnforceUnique(MergeIntoStmt mergeIntoStmt) {
        Set<String> sourceUniqueKeyColumns = extractSourceUniqueKeyColumns(mergeIntoStmt.getSourceRelation());
        if (sourceUniqueKeyColumns.isEmpty()) {
            return false;
        }

        Set<String> sourceJoinKeyColumns = collectSourceJoinKeyColumns(mergeIntoStmt);
        return sourceJoinKeyColumns.containsAll(sourceUniqueKeyColumns);
    }

    private static Set<String> extractSourceUniqueKeyColumns(Relation sourceRelation) {
        SelectRelation selectRelation = unwrapSelectRelation(sourceRelation);
        if (selectRelation == null) {
            return newCaseInsensitiveSet();
        }

        if (selectRelation.isDistinct()) {
            return collectUnambiguousOutputNames(selectRelation.getColumnOutputNames());
        }

        List<Expr> groupBy = selectRelation.getGroupBy();
        if (groupBy == null || groupBy.isEmpty()
                || (selectRelation.getGroupingSetsList() != null && !selectRelation.getGroupingSetsList().isEmpty())
                || (selectRelation.getGroupingFunctionCallExprs() != null
                && !selectRelation.getGroupingFunctionCallExprs().isEmpty())) {
            return newCaseInsensitiveSet();
        }

        Set<String> uniqueKeyColumns = newCaseInsensitiveSet();
        List<Expr> outputExprs = selectRelation.getOutputExpression();
        List<String> outputNames = selectRelation.getColumnOutputNames();
        for (Expr groupByExpr : groupBy) {
            String outputName = findOutputNameForExpression(groupByExpr, outputExprs, outputNames);
            if (outputName == null || !uniqueKeyColumns.add(outputName)) {
                return newCaseInsensitiveSet();
            }
        }
        return uniqueKeyColumns;
    }

    private static SelectRelation unwrapSelectRelation(Relation relation) {
        if (relation instanceof SubqueryRelation subqueryRelation) {
            QueryRelation queryRelation = subqueryRelation.getQueryStatement().getQueryRelation();
            return queryRelation instanceof SelectRelation selectRelation ? selectRelation : null;
        }
        return relation instanceof SelectRelation selectRelation ? selectRelation : null;
    }

    private static Set<String> collectUnambiguousOutputNames(List<String> outputNames) {
        Set<String> uniqueNames = newCaseInsensitiveSet();
        if (outputNames == null || outputNames.isEmpty()) {
            return uniqueNames;
        }
        for (String outputName : outputNames) {
            if (outputName == null || !uniqueNames.add(outputName)) {
                return newCaseInsensitiveSet();
            }
        }
        return uniqueNames;
    }

    private static String findOutputNameForExpression(Expr expr, List<Expr> outputExprs, List<String> outputNames) {
        if (outputExprs == null || outputNames == null || outputExprs.size() != outputNames.size()) {
            return null;
        }

        if (expr instanceof SlotRef groupBySlot) {
            for (int i = 0; i < outputExprs.size(); i++) {
                if (outputExprs.get(i) instanceof SlotRef outputSlot
                        && sameAnalyzedSlot(groupBySlot, outputSlot)) {
                    return outputNames.get(i);
                }
            }
        }

        String exprSql = ExprToSql.toSql(expr);
        for (int i = 0; i < outputExprs.size(); i++) {
            if (exprSql.equalsIgnoreCase(ExprToSql.toSql(outputExprs.get(i)))) {
                return outputNames.get(i);
            }
        }
        return null;
    }

    private static boolean sameAnalyzedSlot(SlotRef left, SlotRef right) {
        return left.getDesc() != null && right.getDesc() != null
                && left.getSlotId().equals(right.getSlotId());
    }

    private static Set<String> collectSourceJoinKeyColumns(MergeIntoStmt mergeIntoStmt) {
        Set<String> sourceJoinKeyColumns = newCaseInsensitiveSet();
        String sourceName = resolveSourceName(mergeIntoStmt.getSourceRelation(), mergeIntoStmt.getSourceAlias());
        String targetName = mergeIntoStmt.getTargetAlias() == null
                ? mergeIntoStmt.getTable().getName()
                : mergeIntoStmt.getTargetAlias();
        if (sourceName == null || targetName == null) {
            return sourceJoinKeyColumns;
        }

        for (Expr conjunct : AnalyzerUtils.extractConjuncts(mergeIntoStmt.getMergeCondition())) {
            if (!(conjunct instanceof BinaryPredicate binaryPredicate)
                    || binaryPredicate.getOp() != BinaryType.EQ) {
                continue;
            }

            Expr left = binaryPredicate.getChild(0);
            Expr right = binaryPredicate.getChild(1);
            if (left instanceof SlotRef leftSlot && right instanceof SlotRef rightSlot) {
                addSourceJoinKeyColumn(sourceJoinKeyColumns, leftSlot, rightSlot, sourceName, targetName);
                addSourceJoinKeyColumn(sourceJoinKeyColumns, rightSlot, leftSlot, sourceName, targetName);
            }
        }
        return sourceJoinKeyColumns;
    }

    private static void addSourceJoinKeyColumn(Set<String> sourceJoinKeyColumns, SlotRef sourceSlot,
                                               SlotRef targetSlot, String sourceName, String targetName) {
        if (!isSlotFromRelation(sourceSlot, sourceName) || !isSlotFromRelation(targetSlot, targetName)
                || sourceSlot.getColumnName() == null) {
            return;
        }
        sourceJoinKeyColumns.add(sourceSlot.getColumnName());
    }

    private static String resolveSourceName(Relation sourceRelation, String sourceAlias) {
        if (sourceAlias != null) {
            return sourceAlias;
        }
        if (sourceRelation.getResolveTableName() != null) {
            return sourceRelation.getResolveTableName().getTbl();
        }
        if (sourceRelation instanceof TableRelation tableRelation) {
            return tableRelation.getName().getTbl();
        }
        return null;
    }

    private static boolean isSlotFromRelation(SlotRef slotRef, String relationName) {
        return slotRef.getTblName() != null
                && slotRef.getTblName().getTbl() != null
                && slotRef.getTblName().getTbl().equalsIgnoreCase(relationName);
    }

    private static Set<String> newCaseInsensitiveSet() {
        return new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    }

    /**
     * Place the duplicate-match check DIRECTLY ABOVE the merge join, in the join's
     * fragment, before any write-distribution exchange. Correctness of the local
     * per-driver check rests on the join, not on the sink distribution:
     * <ul>
     *   <li>cross-BE: the join is a shuffle join (hint set by MergeIntoAnalyzer,
     *       validated below), so each target row is owned by exactly one instance and
     *       all source rows matching it meet there;</li>
     *   <li>within a BE: the hash join's probe side is locally shuffled by join key
     *       across drivers, so all matches of one target row land on one driver.</li>
     * </ul>
     * Everything above the check (e.g. the partition-column shuffle for partitioned
     * targets) is free to redistribute rows arbitrarily.
     */
    private void insertEnforceUniqueRowLocatorNode(ExecPlan execPlan, MergeTargetContext mergeTargetContext) {
        if (mergeTargetContext == null) {
            // Row-locator outputs are NULL projections because the target side of
            // the MERGE join was eliminated (for example, an empty source or a
            // constant-false ON predicate). With no matched target rows, the
            // duplicate-match check is unnecessary. Do not fall back to joins inside
            // the USING subquery.
            return;
        }
        PlanNode joinNode = mergeTargetContext.mergeJoin;
        validateMergeJoinKeepsTargetUnreplicated(joinNode);

        PlanFragment joinFragment = joinNode.getFragment();
        PlanNode currentRoot = joinFragment.getPlanRoot();
        PlanNodeId nodeId = execPlan.getNextNodeId();
        EnforceUniqueRowLocatorNode enforceNode = new EnforceUniqueRowLocatorNode(
                nodeId, currentRoot, Arrays.asList(
                        mergeTargetContext.rowLocatorSlotIds.fileSlotId.asInt(),
                        mergeTargetContext.rowLocatorSlotIds.posSlotId.asInt()));
        joinFragment.setPlanRoot(enforceNode);
    }

    /**
     * Locate the physical MERGE join from the row-locator slots. The analyzer emits
     * source LEFT JOIN target, so the MERGE target lives in child 1 of that join.
     * Source subqueries can contain their own joins and can even scan the same
     * Iceberg table with _file/_pos projected; those must not be mistaken for the
     * MERGE target side.
     */
    private static MergeTargetContext findMergeTargetContext(ExecPlan execPlan, IcebergTable targetTable) {
        RowLocatorSlotIds rowLocatorSlotIds = tryExtractRowLocatorSlotIds(execPlan);
        if (rowLocatorSlotIds == null) {
            return null;
        }

        DescriptorTable descTbl = execPlan.getDescTbl();
        PlanNode root = execPlan.getFragments().get(0).getPlanRoot();
        Queue<PlanNode> queue = new ArrayDeque<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            PlanNode node = queue.poll();
            if (node instanceof JoinNode joinNode
                    && joinNode.getChildren().size() > 1
                    && joinNode.getJoinOp().isAnyLeftOuterJoin()) {
                // MergeIntoAnalyzer constructs source LEFT JOIN target. If that
                // contract changes, this MERGE-join locator must be updated too.
                // The target side is child 1; bind the check to that side rather
                // than to a global scan list, where source self-scans are
                // indistinguishable.
                PlanNode targetSubtree = joinNode.getChild(1);
                IcebergScanNode targetScan = findTargetScan(targetSubtree, targetTable);
                if (targetScan != null) {
                    return new MergeTargetContext(joinNode, targetScan, rowLocatorSlotIds);
                }
                if (producesRowLocatorSlots(targetSubtree, rowLocatorSlotIds, descTbl)) {
                    // Defensive invariant, not a user error: child 1 emits the target
                    // row-locator slots, so a target scan must exist below the join
                    // (findTargetScan walks through exchanges to reach a shuffled-away scan).
                    // Not finding it means an inconsistent physical plan; fail loud rather
                    // than commit a row delta without a frozen base snapshot.
                    throw new IllegalStateException("MERGE INTO physical plan emits target "
                            + "row-locator slots but the target scan could not be located");
                }
            }
            queue.addAll(node.getChildren());
        }
        return null;
    }

    private static boolean producesRowLocatorSlots(PlanNode root, RowLocatorSlotIds rowLocatorSlotIds,
                                                   DescriptorTable descTbl) {
        var slotIds = root.getSlotIds(descTbl);
        return slotIds.contains(rowLocatorSlotIds.fileSlotId.asInt())
                && slotIds.contains(rowLocatorSlotIds.posSlotId.asInt());
    }

    private static IcebergScanNode findTargetScan(PlanNode root, IcebergTable targetTable) {
        Queue<PlanNode> queue = new ArrayDeque<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            PlanNode node = queue.poll();
            if (node instanceof IcebergScanNode scanNode
                    && isTargetScan(scanNode, targetTable)) {
                return scanNode;
            }
            queue.addAll(node.getChildren());
        }
        return null;
    }

    private static boolean isTargetScan(IcebergScanNode scanNode, IcebergTable targetTable) {
        // The row-locator SlotRefs can be carried by Exchange/Project nodes above the scan,
        // and IcebergScanNode may not be marked usedForDelete at this point. External
        // Iceberg tables can be represented by different FE table objects, so compare
        // their catalog/db/table identity instead of the transient Table id.
        return scanNode.getIcebergTable().equals(targetTable);
    }

    /**
     * Fail-loud guard: the local per-driver duplicate-match check is sound only when
     * no target row can be matched by two source rows that end up on different drivers.
     * Two plan shapes satisfy that:
     * <ul>
     *   <li>a shuffle/bucket/colocate hash join — the join key owns each target row
     *       on exactly one instance AND the probe side is locally shuffled by join
     *       key, so all source rows matching one target row land on one driver (the
     *       analyzer's SHUFFLE hint pins this);</li>
     *   <li>a nestloop join whose probe (source) side is a constant relation yielding
     *       AT MOST ONE ROW (no scans, cardinality &le; 1): one source row cannot
     *       match any target row twice, so no duplicate is possible regardless of how
     *       the broadcast target is replicated across drivers. This shape appears when
     *       constant folding rewrites the ON equality against a single-row literal
     *       source (e.g. {@code USING (SELECT 1 AS id) s ON t.id = s.id} becomes a
     *       pushed-down {@code t.id = 1}).</li>
     * </ul>
     * A nestloop probing a MULTI-row source is rejected: with no source-target
     * equality the broadcast target is replicated and the multi-row probe is split
     * across drivers under pipeline_dop &gt; 1, so two source rows matching the same
     * target row would hit different per-driver seen-sets and the duplicate would be
     * missed silently. Cardinality is only trusted for no-scan (constant) sources,
     * where it is exact; a scanned source is always rejected.
     */
    private static void validateMergeJoinKeepsTargetUnreplicated(PlanNode joinNode) {
        if (joinNode instanceof HashJoinNode) {
            JoinNode.DistributionMode mode = ((HashJoinNode) joinNode).getDistrMode();
            Preconditions.checkState(mode == JoinNode.DistributionMode.PARTITIONED
                            || mode == JoinNode.DistributionMode.SHUFFLE_HASH_BUCKET
                            || mode == JoinNode.DistributionMode.COLOCATE,
                    "MERGE join must not broadcast/replicate the target side (shuffle hint should have "
                            + "prevented this), got distribution mode %s", mode);
            return;
        }
        if (joinNode instanceof NestLoopJoinNode) {
            PlanNode source = joinNode.getChild(0);
            long sourceCardinality = source.getCardinality();
            if (!subtreeHasScanNode(source) && sourceCardinality >= 0 && sourceCardinality <= 1) {
                return;
            }
        }
        throw new SemanticException("MERGE INTO requires at least one equality predicate between the "
                + "target and the source in the ON clause");
    }

    private static boolean subtreeHasScanNode(PlanNode root) {
        Queue<PlanNode> queue = new ArrayDeque<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            PlanNode node = queue.poll();
            if (node instanceof ScanNode) {
                return true;
            }
            queue.addAll(node.getChildren());
        }
        return false;
    }

    /**
     * Extract the target row-locator slots from the MERGE output. MergeIntoAnalyzer
     * emits _file at output-position 0 and _pos at position 1. When the optimizer
     * eliminates the target side, these outputs can become NULL projections that
     * still use slot refs, in which case there is no target row to check.
     */
    private static RowLocatorSlotIds tryExtractRowLocatorSlotIds(ExecPlan execPlan) {
        List<Expr> outputExprs = execPlan.getOutputExprs();
        if (outputExprs == null || outputExprs.size() < 2) {
            return null;
        }
        SlotId fileSlotId = tryExtractSlotId(outputExprs.get(0));
        SlotId posSlotId = tryExtractSlotId(outputExprs.get(1));
        if (fileSlotId == null || posSlotId == null) {
            return null;
        }
        return new RowLocatorSlotIds(fileSlotId, posSlotId);
    }

    private static SlotId tryExtractSlotId(Expr expr) {
        if (expr instanceof SlotRef slotRef) {
            return slotRef.getSlotId();
        }
        List<SlotRef> slotRefs = Lists.newArrayList();
        expr.collect(SlotRef.class, slotRefs);
        return slotRefs.size() == 1 ? slotRefs.get(0).getSlotId() : null;
    }

    /**
     * Build conflict detection filter from the scan proven to be the MERGE target
     * side. Do not search the global scan list here: source scans may share the same
     * table id and usedForDelete flag in metadata self-merges.
     */
    private static Expression buildTargetIcebergFilterExpr(IcebergScanNode targetScan) {
        if (targetScan == null) {
            return null;
        }

        var predicate = targetScan.getIcebergJobPlanningPredicate();
        var nativeSchema = targetScan.getIcebergTable().getNativeTable().schema();
        if (predicate == null || nativeSchema == null) {
            return null;
        }
        var icebergContext = new ScalarOperatorToIcebergExpr.IcebergContext(nativeSchema.asStruct());
        return new ScalarOperatorToIcebergExpr()
                .convert(Collections.singletonList(predicate), icebergContext);
    }

    private static class MergeTargetContext {
        private final JoinNode mergeJoin;
        private final IcebergScanNode targetScan;
        private final RowLocatorSlotIds rowLocatorSlotIds;

        private MergeTargetContext(JoinNode mergeJoin, IcebergScanNode targetScan,
                                   RowLocatorSlotIds rowLocatorSlotIds) {
            this.mergeJoin = mergeJoin;
            this.targetScan = targetScan;
            this.rowLocatorSlotIds = rowLocatorSlotIds;
        }
    }

    private static class RowLocatorSlotIds {
        private final SlotId fileSlotId;
        private final SlotId posSlotId;

        private RowLocatorSlotIds(SlotId fileSlotId, SlotId posSlotId) {
            this.fileSlotId = fileSlotId;
            this.posSlotId = posSlotId;
        }
    }

}
