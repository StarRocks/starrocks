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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.IcebergRowDeltaSink;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.SchemaTableSink;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ScalarOperatorRewriteRule;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TResultSinkType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class UpdatePlanner {

    public ExecPlan plan(UpdateStmt updateStmt, ConnectContext session) {
        QueryRelation query = updateStmt.getQueryStatement().getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transform(query);

        List<ColumnRefOperator> outputColumns = logicalPlan.getOutputColumn();
        Table targetTable = updateStmt.getTable();

        // Determine required physical property and prepare the optimizer root
        OptExprBuilder optExprBuilder = logicalPlan.getRootBuilder();
        PhysicalPropertySet requiredProperty;

        if (targetTable instanceof IcebergTable icebergTable) {
            requiredProperty = IcebergPlannerUtils.createShuffleProperty(icebergTable, outputColumns);
        } else {
            // OLAP/System: cast output column types to target schema types
            optExprBuilder = castOutputColumnsTypeToTargetColumns(columnRefFactory, targetTable,
                    colNames, outputColumns, optExprBuilder);
            requiredProperty = new PhysicalPropertySet();
        }

        return createUpdatePlan(updateStmt, session, optExprBuilder.getRoot(), columnRefFactory,
                outputColumns, colNames, targetTable, requiredProperty);
    }

    /**
     * Shared planning core: pipeline guard → optimize → build physical plan → setup sink.
     */
    private ExecPlan createUpdatePlan(UpdateStmt updateStmt, ConnectContext session,
                                      OptExpression logicalRoot, ColumnRefFactory columnRefFactory,
                                      List<ColumnRefOperator> outputColumns, List<String> colNames,
                                      Table targetTable, PhysicalPropertySet requiredProperty) {
        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline = isEnablePipeline && DataSink.canTableSinkUsePipeline(targetTable);
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;
        boolean prevIsEnableLocalShuffleAgg = session.getSessionVariable().isEnableLocalShuffleAgg();
        try {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(false);
            }
            session.getSessionVariable().setEnableLocalShuffleAgg(false);

            // Optimize
            OptimizerContext optimizerContext = OptimizerFactory.initContext(session, columnRefFactory);
            if (targetTable instanceof OlapTable) {
                optimizerContext.setUpdateTableId(targetTable.getId());
            }
            Optimizer optimizer = OptimizerFactory.create(optimizerContext);
            OptExpression optimizedPlan = optimizer.optimize(
                    logicalRoot, requiredProperty, new ColumnRefSet(outputColumns));

            // Build physical plan
            ExecPlan execPlan = PlanFragmentBuilder.createPhysicalPlan(optimizedPlan, session,
                    outputColumns, columnRefFactory, colNames, TResultSinkType.MYSQL_PROTOCAL, false);

            // Setup sink and configure pipeline based on table type
            if (targetTable instanceof IcebergTable icebergTable) {
                setupIcebergRowDeltaSink(execPlan, colNames, icebergTable, session);
                IcebergPlannerUtils.configureIcebergSinkPipeline(execPlan, session, canUsePipeline);
            } else if (targetTable instanceof OlapTable) {
                setupOlapTableSink(execPlan, updateStmt, session, targetTable, canUsePipeline);
            } else if (targetTable instanceof SystemTable) {
                DataSink dataSink = new SchemaTableSink((SystemTable) targetTable,
                        ConnectContext.get().getCurrentComputeResource());
                execPlan.getFragments().get(0).setSink(dataSink);
                configureSinkPipelineDop(execPlan, session, canUsePipeline, false);
            } else {
                throw new SemanticException("Unsupported table type: " + targetTable.getClass().getName());
            }
            return execPlan;
        } finally {
            session.getSessionVariable().setEnableLocalShuffleAgg(prevIsEnableLocalShuffleAgg);
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(true);
            }
        }
    }

    private void setupOlapTableSink(ExecPlan execPlan, UpdateStmt updateStmt,
                                     ConnectContext session, Table targetTable,
                                     boolean canUsePipeline) {
        DescriptorTable descriptorTable = execPlan.getDescTbl();
        TupleDescriptor olapTuple = descriptorTable.createTupleDescriptor();
        long tableId = targetTable.getId();

        List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
        for (Column column : targetTable.getFullSchema()) {
            if (updateStmt.usePartialUpdate() && !column.isGeneratedColumn() &&
                    !updateStmt.isAssignmentColumn(column.getName()) && !column.isKey()) {
                continue;
            }
            SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
            slotDescriptor.setIsMaterialized(true);
            slotDescriptor.setType(column.getType());
            slotDescriptor.setColumn(column);
            slotDescriptor.setIsNullable(column.isAllowNull());
            if (column.getType().isVarchar() &&
                    IDictManager.getInstance().hasGlobalDict(tableId, column.getColumnId())) {
                Optional<ColumnDict> dict = IDictManager.getInstance().getGlobalDict(tableId, column.getColumnId());
                dict.ifPresent(
                        columnDict -> globalDicts.add(new Pair<>(slotDescriptor.getId().asInt(), columnDict)));
            }
        }
        olapTuple.computeMemLayout();

        OlapTable olapTable = (OlapTable) targetTable;
        List<Long> partitionIds = Lists.newArrayList();
        for (Partition partition : olapTable.getPartitions()) {
            partitionIds.add(partition.getId());
        }
        DataSink dataSink = new OlapTableSink(olapTable, olapTuple, partitionIds, olapTable.writeQuorum(),
                olapTable.enableReplicatedStorage(), false,
                olapTable.supportedAutomaticPartition(), session.getCurrentComputeResource());
        if (updateStmt.usePartialUpdate()) {
            ((OlapTableSink) dataSink).setPartialUpdateMode(TPartialUpdateMode.COLUMN_UPDATE_MODE);
        }
        if (session.getTxnId() != 0) {
            ((OlapTableSink) dataSink).setIsMultiStatementsTxn(true);
        }

        execPlan.getFragments().get(0).setSink(dataSink);
        execPlan.getFragments().get(0).setLoadGlobalDicts(globalDicts);

        // if sink is OlapTableSink Assigned to Be execute this sql [cn execute OlapTableSink will crash]
        session.getSessionVariable().setPreferComputeNode(false);
        session.getSessionVariable().setUseComputeNodes(0);
        OlapTableSink olapTableSink = (OlapTableSink) dataSink;
        TableRef tableRef = updateStmt.getTableRef();
        TableName catalogDbTable = TableName.fromTableRef(tableRef);
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(session, catalogDbTable.getCatalog(),
                catalogDbTable.getDb());
        try {
            olapTableSink.init(session.getExecutionId(), updateStmt.getTxnId(), db.getId(), session.getExecTimeout());
            olapTableSink.complete();
        } catch (StarRocksException e) {
            throw new SemanticException(e.getMessage());
        }

        configureSinkPipelineDop(execPlan, session, canUsePipeline, true);
    }

    private void setupIcebergRowDeltaSink(ExecPlan execPlan, List<String> colNames,
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
                icebergTable, rowDeltaTuple, session.getSessionVariable());
        dataSink.init();

        IcebergMetadata.IcebergSinkExtra icebergSinkExtra = new IcebergMetadata.IcebergSinkExtra();
        org.apache.iceberg.expressions.Expression filterExpr = IcebergPlannerUtils.buildIcebergFilterExpr(execPlan);
        if (filterExpr != null) {
            icebergSinkExtra.setConflictDetectionFilter(filterExpr);
        }
        dataSink.setSinkExtraInfo(icebergSinkExtra);

        execPlan.getFragments().get(0).setSink(dataSink);
    }

    /**
     * Configures pipeline DOP for OLAP table sink.
     */
    private void configureSinkPipelineDop(ExecPlan execPlan, ConnectContext session,
                                           boolean canUsePipeline, boolean isOlapSink) {
        if (!canUsePipeline) {
            execPlan.getFragments().get(0).setPipelineDop(1);
            return;
        }
        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        SessionVariable sv = session.getSessionVariable();
        if (sv.getEnableAdaptiveSinkDop()) {
            long warehouseId = session.getCurrentComputeResource().getWarehouseId();
            sinkFragment.setPipelineDop(sv.getSinkDegreeOfParallelism(warehouseId));
        } else {
            sinkFragment.setPipelineDop(sv.getParallelExecInstanceNum());
        }
        if (isOlapSink) {
            sinkFragment.setHasOlapTableSink();
        }
        sinkFragment.setForceSetTableSinkDop();
        sinkFragment.setForceAssignScanRangesPerDriverSeq();
        sinkFragment.disableRuntimeAdaptiveDop();
    }

    private static OptExprBuilder castOutputColumnsTypeToTargetColumns(ColumnRefFactory columnRefFactory,
                                                                       Table targetTable,
                                                                       List<String> colNames,
                                                                       List<ColumnRefOperator> outputColumns,
                                                                       OptExprBuilder root) {
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        List<ScalarOperatorRewriteRule> rewriteRules = Arrays.asList(new FoldConstantsRule());
        Preconditions.checkState(colNames.size() == outputColumns.size(), "Column name's size %s should be equal " +
                "to output column refs' size %s", colNames.size(), outputColumns.size());

        for (int columnIdx = 0; columnIdx < outputColumns.size(); ++columnIdx) {
            ColumnRefOperator outputColumn = outputColumns.get(columnIdx);
            String colName = colNames.get(columnIdx);
            Column column = targetTable.getColumn(colName);
            Preconditions.checkState(column != null, "Column %s not found in table %s", colName,
                    targetTable.getName());
            if (!column.getType().matchesType(outputColumn.getType())) {
                if (!TypeManager.canCastTo(outputColumn.getType(), column.getType())) {
                    throw new SemanticException(String.format("Output column type %s is not compatible table column type: %s",
                            outputColumn.getType(), column.getType()));
                }
                ColumnRefOperator k = columnRefFactory.create(column.getName(), column.getType(), column.isAllowNull());
                ScalarOperator castOperator = new CastOperator(column.getType(), outputColumn, true);
                columnRefMap.put(k, rewriter.rewrite(castOperator, rewriteRules));
                outputColumns.set(columnIdx, k);
            } else {
                columnRefMap.put(outputColumn, outputColumn);
            }
        }
        return root.withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }
}
