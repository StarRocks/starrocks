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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.ScalarOperatorToIcebergExpr;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.EnforceUniqueNode;
import com.starrocks.planner.IcebergRowDeltaSink;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.expression.Expr;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
        // This mutates the analyzed AST, so it must run exactly once per statement.
        // Guard against re-planning paths (e.g. retry loops that re-enter
        // StatementPlanner.plan with the same parsed statement) appending a second
        // op_code column.
        List<SelectListItem> items = selectList.getItems();
        Preconditions.checkState(items.isEmpty()
                        || !OP_CODE_COLUMN_NAME.equals(items.get(items.size() - 1).getAlias()),
                "op_code routing column has already been injected");
        selectList.addItem(new SelectListItem(routingExpr, OP_CODE_COLUMN_NAME));
        List<Expr> extendedOutput = Lists.newArrayList(selectRelation.getOutputExpression());
        extendedOutput.add(routingExpr);
        selectRelation.setOutputExpr(extendedOutput);
    }

    public ExecPlan plan(MergeIntoStmt mergeIntoStmt, ConnectContext session) {
        // The BE EnforceUniqueNode is pipeline-engine-only (its non-pipeline entry
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

        // Use the 3-arg overload: MERGE wraps all data columns in CASE expressions, causing
        // ColumnRefOperator names to become "case" instead of the original column name.
        // The 3-arg overload matches partition columns by the saved column output names.
        PhysicalPropertySet requiredProperty = IcebergPlannerUtils.createShuffleProperty(
                icebergTable, outputColumns, colNames);

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
            setupIcebergMergeSink(execPlan, colNames, icebergTable, session);
            IcebergPlannerUtils.configureIcebergSinkPipeline(execPlan, session, true);

            return execPlan;
        } finally {
            session.getSessionVariable().setEnableLocalShuffleAgg(prevIsEnableLocalShuffleAgg);
        }
    }

    private void setupIcebergMergeSink(ExecPlan execPlan, List<String> colNames,
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
        // We must build the conflict filter from the TARGET scan, not the source.
        Expression filterExpr = buildTargetIcebergFilterExpr(execPlan, icebergTable);
        if (filterExpr != null) {
            icebergSinkExtra.setConflictDetectionFilter(filterExpr);
        }
        dataSink.setSinkExtraInfo(icebergSinkExtra);

        execPlan.getFragments().get(0).setSink(dataSink);

        // Insert EnforceUniqueNode to check that each target row is matched at most once.
        // Key columns are _file and _pos — resolved against the plan root's physical
        // output layout below, NOT hardcoded to [0, 1].
        insertEnforceUniqueNode(execPlan);
    }

    private void insertEnforceUniqueNode(ExecPlan execPlan) {
        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        PlanNode currentRoot = sinkFragment.getPlanRoot();

        // Resolve the physical chunk positions of _file and _pos.
        //
        // MergeIntoAnalyzer builds the SELECT list with _file at output-position 0
        // and _pos at position 1, so execPlan.getOutputExprs().get(0)/(1) reference
        // those two slots.
        //
        // The physical chunk flowing into the EnforceUniqueOperator is emitted by
        // the sender ProjectNode, which on the BE side orders its columns by
        // ASCENDING slot ID (project_node.cpp init() iterates Thrift's
        // `map<SlotId, Expr> slot_map` which in C++ is a `std::map` — keys come
        // out sorted). That's true whether the FE root here is the ProjectNode
        // itself (single-BE / non-partitioned) or an ExchangeNode receiver (the
        // multi-BE partition-shuffle case — the Exchange shares the sender
        // Project's tupleId and preserves column order over the wire).
        //
        // We MUST NOT call `currentRoot.getOutputSlotIds(descTbl)` here:
        //   - ProjectNode overrides it to return slot-ID-sorted order (correct).
        //   - ExchangeNode does NOT override it, so it falls back to
        //     PlanNode.getOutputSlotIds, which returns tuple-descriptor
        //     insertion order. PlanFragmentBuilder populates the ProjectNode's
        //     tuple via iterating `Maps.newHashMap()` (node.getColumnRefMap
        //     entries), so insertion order ≠ slot-ID order in general.
        //
        // Taking the plan root's tuple slots and sorting by slot-ID ourselves
        // gives the same order the BE actually sees, for both topologies.
        // When source-side ColumnRefOperators get smaller slot IDs than the
        // target-side _file/_pos (which happens routinely with any non-trivial
        // source — multi-branch UNION ALL, MATCHED+NOT MATCHED pairs,
        // partition-transform shuffles), the _file/_pos chunk indices are NOT
        // 0/1. A hardcoded [0, 1] points at source data columns, and the BE-side
        // EnforceUniqueOperator does a
        //   down_cast<const FixedLengthColumn<int64_t>*>(row_pos_col)->get_data().data()
        // on a column that isn't that type — in RELEASE builds this silently
        // yields a bad pointer and SIGSEGVs, or — when the column happens to be
        // readable as binary — surfaces as a spurious "matched by at most one
        // source row" error with a bogus file path (e.g. a STRING data column's
        // value like 'hot').
        List<Expr> outputExprs = execPlan.getOutputExprs();
        Preconditions.checkArgument(outputExprs.size() >= 2,
                "MERGE output must have at least _file and _pos; got %s", outputExprs.size());
        SlotId fileSlotId = extractSlotId(outputExprs.get(0), "_file");
        SlotId posSlotId = extractSlotId(outputExprs.get(1), "_pos");

        List<SlotId> physicalOrder = collectSlotsSortedById(currentRoot, execPlan.getDescTbl());
        int fileIdx = physicalOrder.indexOf(fileSlotId);
        int posIdx = physicalOrder.indexOf(posSlotId);
        Preconditions.checkArgument(fileIdx >= 0 && posIdx >= 0,
                "could not locate _file(slot=%s) / _pos(slot=%s) in plan root output slots %s",
                fileSlotId, posSlotId, physicalOrder);

        PlanNodeId nodeId = execPlan.getNextNodeId();
        EnforceUniqueNode enforceNode = new EnforceUniqueNode(
                nodeId, currentRoot, Arrays.asList(fileIdx, posIdx));
        sinkFragment.setPlanRoot(enforceNode);
    }

    /**
     * Collect every slot reachable from the given plan node's output tuples and
     * return their SlotIds in ASCENDING slot-ID order.
     *
     * This mirrors the BE ProjectOperator's behavior: `project_node.cpp` iterates
     * `tnode.project_node.slot_map` (a Thrift `map<SlotId, Expr>` → C++
     * `std::map`, which keys are sorted), producing chunk columns in slot-ID
     * ascending order regardless of how the FE tuple was populated. Using this
     * sorted view here keeps FE's index resolution consistent with the BE
     * chunk layout for every topology EnforceUniqueNode can sit under — both
     * Project-at-root and Exchange-at-root (multi-BE partition-shuffle).
     *
     * Only MATERIALIZED slots are included. A ProjectNode's tuple descriptor
     * holds both materialized output slots (populated via
     * `setIsMaterialized(true)` in PlanFragmentBuilder) and non-materialized
     * common sub-operator slots (`setIsMaterialized(false)`). The two travel
     * through Thrift in DIFFERENT fields: `project_node.slot_map` (materialized
     * outputs — what ends up as chunk columns) vs `project_node.common_slot_map`
     * (common sub-expressions — evaluated in place, not emitted). Counting the
     * common-sub slots here would shift the physical indices of `_file` / `_pos`
     * whenever a common-sub slot happens to have a smaller slot-ID — e.g. for
     * the CASE WHEN `_file IS NOT NULL` pattern that MergeIntoAnalyzer emits on
     * every MERGE output column, which the optimizer CSE's out.
     */
    @VisibleForTesting
    static List<SlotId> collectSlotsSortedById(PlanNode node, DescriptorTable descTbl) {
        List<SlotId> slotIds = Lists.newArrayList();
        for (TupleId tid : node.getTupleIds()) {
            TupleDescriptor tupleDesc = descTbl.getTupleDesc(tid);
            if (tupleDesc == null) {
                continue;
            }
            for (SlotDescriptor slot : tupleDesc.getSlots()) {
                if (slot.isMaterialized()) {
                    slotIds.add(slot.getId());
                }
            }
        }
        slotIds.sort(Comparator.comparingInt(SlotId::asInt));
        return slotIds;
    }

    /**
     * Extract the slot ID that the given output-expr references. MergeIntoAnalyzer
     * emits plain SlotRefs for _file and _pos (no casts, no wrapping), so in the
     * common path this is just a cast. Falls back to collecting all SlotRefs for
     * robustness in case future optimizer passes rewrite the output expression
     * into a single-slot scalar.
     */
    private static SlotId extractSlotId(Expr expr, String metaColName) {
        if (expr instanceof SlotRef slotRef) {
            return slotRef.getSlotId();
        }
        List<SlotRef> slotRefs = Lists.newArrayList();
        expr.collect(SlotRef.class, slotRefs);
        Preconditions.checkArgument(slotRefs.size() == 1,
                "MERGE output expression for %s must reference exactly one slot; got %s from %s",
                metaColName, slotRefs.size(), expr.debugString());
        return slotRefs.get(0).getSlotId();
    }

    /**
     * Build conflict detection filter from the TARGET table's IcebergScanNode, not the first
     * Iceberg scan found. In MERGE, the source may also be an Iceberg table, and the generic
     * buildIcebergFilterExpr() picks the first scan it finds — which could be the source.
     *
     * We identify the target scan by {@code isUsedForDelete() == true}, which is set by
     * PlanFragmentBuilder when the output contains _file/_pos metadata columns. This works
     * even for self-merges where both scans reference the same native table, because only
     * the target side outputs delete-path metadata.
     */
    private static Expression buildTargetIcebergFilterExpr(ExecPlan execPlan, IcebergTable targetTable) {
        if (execPlan == null || execPlan.getScanNodes() == null) {
            return null;
        }

        for (PlanNode node : execPlan.getScanNodes()) {
            if (node instanceof IcebergScanNode scanNode && scanNode.isUsedForDelete()) {
                var predicate = scanNode.getIcebergJobPlanningPredicate();
                var nativeSchema = scanNode.getIcebergTable().getNativeTable().schema();
                if (predicate == null || nativeSchema == null) {
                    return null;
                }
                var icebergContext = new ScalarOperatorToIcebergExpr.IcebergContext(nativeSchema.asStruct());
                return new ScalarOperatorToIcebergExpr()
                        .convert(Collections.singletonList(predicate), icebergContext);
            }
        }
        return null;
    }

}
