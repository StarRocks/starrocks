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

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.iceberg.IcebergDeleteSchema;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergMORParams.EqDeleteScope;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.IcebergDeleteSink;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.IcebergEqualityDeleteScanBuilder;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultSinkType;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.IcebergTable.FILE_PATH;
import static com.starrocks.catalog.IcebergTable.ROW_POSITION;

// Plans the equality-delete -> position-delete conversion for one Iceberg table at a snapshot.
// The De Morgan dual of the read path: where IcebergEqualityDeleteRewriteRule hides deleted rows with
// a LEFT ANTI JOIN, convert KEEPS them with a LEFT SEMI JOIN, projects ($file_path, $pos, partition)
// and feeds an IcebergDeleteSink that writes position deletes. One semi-join leg per present
// (equalityIds, scope); a PARTITIONED leg additionally keys on $partition_id so a partitioned delete
// only matches data of its own partition. Multiple legs are UNIONed (distinct), never chained -- a row
// is deleted if matched by ANY leg, and chaining would give the intersection.
public class ConvertEqualityDeletesPlanner {

    // Builds the conversion ExecPlan, or returns null if the snapshot has no equality-delete files.
    public ExecPlan plan(IcebergTable icebergTable, long snapshotId, ConnectContext session) {
        Table nativeTable = icebergTable.getNativeTable();

        Set<DeleteFile> deleteFiles = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDeleteFiles(icebergTable, snapshotId, null, FileContent.EQUALITY_DELETES);
        if (deleteFiles.isEmpty()) {
            return null;
        }

        Set<IcebergDeleteSchema> deleteSchemas = deleteFiles.stream()
                .map(f -> IcebergDeleteSchema.of(f.equalityFieldIds(), f.specId()))
                .collect(Collectors.toSet());
        Map<Integer, PartitionSpec> specs = nativeTable.specs();
        List<IcebergMORParams> legs = IcebergEqualityDeleteScanBuilder.splitIntoLegs(deleteSchemas, specs);

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        TvrVersionRange tvr = TvrTableSnapshot.of(snapshotId);

        // tableFullMORParams ties the split scan nodes to one RemoteFileInfoSource and keys its queues.
        // It mirrors the read rule's [WITHOUT, WITH, legs...] shape: the trigger requires size >= 3 and
        // derives needToCheckEqualityIds from it. Convert does not materialize the WITHOUT branch (no
        // scan node consumes it -- non-deleted rows are irrelevant to conversion); it is present only so
        // the trigger keys the WITH/eq-delete queues correctly.
        List<IcebergMORParams> fullMorParams = new ArrayList<>();
        fullMorParams.add(IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE);
        fullMorParams.add(IcebergMORParams.DATA_FILE_WITH_EQ_DELETE);
        fullMorParams.addAll(legs);
        IcebergTableMORParams tableFullMorParams = new IcebergTableMORParams(icebergTable.getId(), fullMorParams);

        List<OptExpression> legPlans = new ArrayList<>();
        List<List<ColumnRefOperator>> legOutputs = new ArrayList<>();
        for (IcebergMORParams leg : legs) {
            LegPlan legPlan = buildLegPlan(leg, icebergTable, columnRefFactory, tvr, tableFullMorParams);
            legPlans.add(legPlan.root);
            legOutputs.add(legPlan.output);
        }

        OptExpression root;
        List<ColumnRefOperator> outputColumns;
        if (legPlans.size() == 1) {
            root = legPlans.get(0);
            outputColumns = legOutputs.get(0);
        } else {
            // UNION (distinct) the per-leg (file, pos, partition) outputs so a position matched by
            // several legs is written once. The BE sink does not dedup positions itself.
            outputColumns = legOutputs.get(0).stream()
                    .map(ref -> columnRefFactory.create(ref.getName(), ref.getType(), ref.isNullable()))
                    .collect(Collectors.toList());
            LogicalUnionOperator union = LogicalUnionOperator.builder()
                    .isUnionAll(false)
                    .setOutputColumnRefOp(outputColumns)
                    .setChildOutputColumns(legOutputs)
                    .build();
            root = OptExpression.create(union, legPlans);
        }

        List<String> colNames = outputColumns.stream().map(ColumnRefOperator::getName).collect(Collectors.toList());

        ExecPlan execPlan = optimizeAndBuild(root, outputColumns, colNames, columnRefFactory, icebergTable, session);
        enableScanFileRecording(execPlan);
        setupConvertDeleteSink(execPlan, colNames, icebergTable, snapshotId, session);
        return execPlan;
    }

    // The first scan-range setup (inside createPhysicalPlan) runs with recording off. Turn it on and
    // rebuild so the scan nodes record which equality-delete files they apply, which the executor reads
    // back as the removal set. Costs a second planFiles pass; acceptable for an admin-triggered convert.
    private void enableScanFileRecording(ExecPlan execPlan) {
        for (PlanFragment fragment : execPlan.getFragments()) {
            for (ScanNode scan : fragment.collectScanNodes().values()) {
                if (scan instanceof IcebergScanNode icebergScan
                        && scan.getPlanNodeName().equals("IcebergScanNode")) {
                    icebergScan.setRecordScanFiles(true);
                    icebergScan.prepareRetry();
                }
            }
        }
    }

    private record LegPlan(OptExpression root, List<ColumnRefOperator> output) {
    }

    // One leg: data-with-delete scan (LEFT) LEFT SEMI JOIN eq-delete scan (RIGHT, broadcast), then
    // project ($file_path, $pos, partition cols). The data scan exposes the leg's equality-key columns
    // + $data_sequence_number (+ $partition_id for a PARTITIONED leg) so buildOnPredicate can match.
    private LegPlan buildLegPlan(IcebergMORParams leg, IcebergTable icebergTable, ColumnRefFactory columnRefFactory,
                                 TvrVersionRange tvr, IcebergTableMORParams tableFullMorParams) {
        boolean partitionLeg = leg.getEqDeleteScope() == EqDeleteScope.PARTITIONED;
        Table nativeTable = icebergTable.getNativeTable();

        // Project order = ($file_path, $pos, partition cols); these are the conversion output.
        List<Column> outputCols = new ArrayList<>();
        outputCols.add(icebergTable.getColumn(FILE_PATH));
        outputCols.add(icebergTable.getColumn(ROW_POSITION));
        List<Column> partitionColumns = icebergTable.getPartitionColumns().stream()
                .filter(java.util.Objects::nonNull)
                .collect(Collectors.toList());
        outputCols.addAll(partitionColumns);

        // Data scan column maps: equality-key columns (for the join) + the output columns. Dedup by
        // name, since a partition column may also be an equality-key column.
        Map<String, Column> baseColumns = new LinkedHashMap<>();
        for (Integer fieldId : leg.getEqualityIds()) {
            String name = nativeTable.schema().findColumnName(fieldId);
            baseColumns.put(name, icebergTable.getColumn(name));
        }
        for (Column col : outputCols) {
            baseColumns.putIfAbsent(col.getName(), col);
        }

        ImmutableMap.Builder<ColumnRefOperator, Column> colRefToCol = ImmutableMap.builder();
        ImmutableMap.Builder<Column, ColumnRefOperator> colToColRef = ImmutableMap.builder();
        Map<String, ColumnRefOperator> nameToColRef = new LinkedHashMap<>();
        for (Map.Entry<String, Column> entry : baseColumns.entrySet()) {
            Column col = entry.getValue();
            ColumnRefOperator ref = IcebergEqualityDeleteScanBuilder.buildColumnRef(col, columnRefFactory, icebergTable);
            colRefToCol.put(ref, col);
            colToColRef.put(col, ref);
            nameToColRef.put(entry.getKey(), ref);
        }
        // $data_sequence_number (+ $partition_id for a PARTITIONED leg), matching the read path.
        IcebergEqualityDeleteScanBuilder.fillExtendedColumns(
                columnRefFactory, colRefToCol, colToColRef, partitionLeg, icebergTable);

        LogicalIcebergScanOperator dataScan = new LogicalIcebergScanOperator(
                icebergTable, colRefToCol.build(), colToColRef.build(), -1, null, tvr);
        dataScan.setMORParam(IcebergMORParams.DATA_FILE_WITH_EQ_DELETE);
        dataScan.setTableFullMORParams(tableFullMorParams);
        dataScan.setFromEqDeleteRewriteRule(true);

        LogicalIcebergEqualityDeleteScanOperator eqScan = IcebergEqualityDeleteScanBuilder
                .buildEqualityDeleteScanOperator(leg, icebergTable, columnRefFactory, null, tableFullMorParams, tvr);

        ScalarOperator onPredicate = IcebergEqualityDeleteScanBuilder.buildOnPredicate(
                dataScan.getColumnNameToColRefMap(), eqScan.getOutputColumns());

        LogicalJoinOperator semiJoin = LogicalJoinOperator.builder()
                .setJoinType(JoinOperator.LEFT_SEMI_JOIN)
                .setJoinHint(HintNode.HINT_JOIN_BROADCAST)
                .setOnPredicate(onPredicate)
                .setOriginalOnPredicate(onPredicate)
                .build();
        OptExpression joinExpr = OptExpression.create(semiJoin,
                OptExpression.create(dataScan), OptExpression.create(eqScan));

        // Project ($file_path, $pos, partition) out of the surviving (deleted) rows.
        ImmutableMap.Builder<ColumnRefOperator, ScalarOperator> projectMap = ImmutableMap.builder();
        List<ColumnRefOperator> output = new ArrayList<>();
        for (Column col : outputCols) {
            ColumnRefOperator ref = nameToColRef.get(col.getName());
            projectMap.put(ref, ref);
            output.add(ref);
        }
        OptExpression projectExpr = OptExpression.create(new LogicalProjectOperator(projectMap.build()), joinExpr);
        return new LegPlan(projectExpr, output);
    }

    private ExecPlan optimizeAndBuild(OptExpression root, List<ColumnRefOperator> outputColumns, List<String> colNames,
                                      ColumnRefFactory columnRefFactory, IcebergTable icebergTable,
                                      ConnectContext session) {
        boolean prevLocalShuffleAgg = session.getSessionVariable().isEnableLocalShuffleAgg();
        // Non-query execution assigns scan ranges per driver sequence, which local shuffle agg cannot use.
        session.getSessionVariable().setEnableLocalShuffleAgg(false);
        try {
            PhysicalPropertySet requiredProperty = IcebergPlannerUtils.createShuffleProperty(icebergTable, outputColumns);
            Optimizer optimizer = OptimizerFactory.create(OptimizerFactory.initContext(session, columnRefFactory));
            OptExpression optimizedPlan = optimizer.optimize(root, requiredProperty, new ColumnRefSet(outputColumns));
            return PlanFragmentBuilder.createPhysicalPlan(optimizedPlan, session, outputColumns, columnRefFactory,
                    colNames, TResultSinkType.MYSQL_PROTOCAL, false);
        } finally {
            session.getSessionVariable().setEnableLocalShuffleAgg(prevLocalShuffleAgg);
        }
    }

    // Attaches an IcebergDeleteSink whose IcebergSinkExtra carries the convert marker, the base snapshot
    // id for conflict detection, and the equality-delete files to remove. The removal set is read back
    // from the plan's scan nodes (the same files the scan layer actually applied), so it stays in lock
    // step with what was scanned -- not an independent manifest sweep.
    private void setupConvertDeleteSink(ExecPlan execPlan, List<String> colNames, IcebergTable icebergTable,
                                        long snapshotId, ConnectContext session) {
        DescriptorTable descriptorTable = execPlan.getDescTbl();
        TupleDescriptor deleteTuple = descriptorTable.createTupleDescriptor();

        List<Expr> outputExprs = execPlan.getOutputExprs();
        for (int i = 0; i < colNames.size(); ++i) {
            SlotDescriptor slot = descriptorTable.addSlotDescriptor(deleteTuple);
            slot.setIsMaterialized(true);
            slot.setType(outputExprs.get(i).getType());
            slot.setColumn(new Column(colNames.get(i), outputExprs.get(i).getType()));
            slot.setIsNullable(outputExprs.get(i).isNullable());
        }
        deleteTuple.computeMemLayout();

        descriptorTable.addReferencedTable(icebergTable);
        IcebergDeleteSink dataSink = new IcebergDeleteSink(icebergTable, deleteTuple, session.getSessionVariable());
        dataSink.init();

        IcebergMetadata.IcebergSinkExtra extra = new IcebergMetadata.IcebergSinkExtra();
        extra.setConvertEqualityDeletes(true);
        extra.setBaseSnapshotId(snapshotId);
        // The equality-delete files to remove are read back from the scan nodes after execution (in
        // StmtExecutor's Iceberg delete-sink branch), once the scan layer has assigned its ranges and
        // recorded which delete files it actually applied -- they aren't known yet at plan time.
        dataSink.setSinkExtraInfo(extra);

        execPlan.getFragments().get(0).setSink(dataSink);
        IcebergPlannerUtils.configureIcebergSinkPipeline(execPlan, session, DataSink.canTableSinkUsePipeline(icebergTable));
    }
}
