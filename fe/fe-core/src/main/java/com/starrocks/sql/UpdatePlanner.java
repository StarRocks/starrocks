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
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.SchemaTableSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.UpdateStmt;
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

        //1. Cast output columns type to target type
        OptExprBuilder optExprBuilder = logicalPlan.getRootBuilder();
        optExprBuilder = castOutputColumnsTypeToTargetColumns(columnRefFactory, targetTable,
                colNames, outputColumns, optExprBuilder);

        // TODO: remove forceDisablePipeline when all the operators support pipeline engine.
        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline = isEnablePipeline && DataSink.canTableSinkUsePipeline(updateStmt.getTable());
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;
        boolean prevIsEnableLocalShuffleAgg = session.getSessionVariable().isEnableLocalShuffleAgg();
        try {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(false);
            }
            // Non-query must use the strategy assign scan ranges per driver sequence, which local shuffle agg cannot use.
            session.getSessionVariable().setEnableLocalShuffleAgg(false);

            long tableId = targetTable.getId();
            OptimizerContext optimizerContext = OptimizerFactory.initContext(session, columnRefFactory);
            optimizerContext.setUpdateTableId(tableId);

            Optimizer optimizer = OptimizerFactory.create(optimizerContext);
            OptExpression optimizedPlan = optimizer.optimize(
                    optExprBuilder.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(outputColumns));
            ExecPlan execPlan = PlanFragmentBuilder.createPhysicalPlan(optimizedPlan, session,
                    outputColumns, columnRefFactory, colNames, TResultSinkType.MYSQL_PROTOCAL, false);
            DescriptorTable descriptorTable = execPlan.getDescTbl();
            TupleDescriptor olapTuple = descriptorTable.createTupleDescriptor();

            List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
            for (Column column : targetTable.getFullSchema()) {
                if (updateStmt.usePartialUpdate() && !column.isGeneratedColumn() &&
                        !updateStmt.isAssignmentColumn(column.getName()) && !column.isKey()) {
                    // When using partial update, skip columns which aren't key column and not be assign, except for
                    // generated column
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

            if (targetTable instanceof OlapTable) {
                List<Long> partitionIds = Lists.newArrayList();
                for (Partition partition : targetTable.getPartitions()) {
                    partitionIds.add(partition.getId());
                }
                OlapTable olapTable = (OlapTable) targetTable;
                DataSink dataSink = new OlapTableSink(olapTable, olapTuple, partitionIds, olapTable.writeQuorum(),
                        olapTable.enableReplicatedStorage(), false,
                        olapTable.supportedAutomaticPartition(), session.getCurrentWarehouseId());
                if (updateStmt.usePartialUpdate()) {
                    // using column mode partial update in UPDATE stmt
                    ((OlapTableSink) dataSink).setPartialUpdateMode(TPartialUpdateMode.COLUMN_UPDATE_MODE);
                }
                execPlan.getFragments().get(0).setSink(dataSink);
                execPlan.getFragments().get(0).setLoadGlobalDicts(globalDicts);

                // if sink is OlapTableSink Assigned to Be execute this sql [cn execute OlapTableSink will crash]
                session.getSessionVariable().setPreferComputeNode(false);
                session.getSessionVariable().setUseComputeNodes(0);
                OlapTableSink olapTableSink = (OlapTableSink) dataSink;
                TableName catalogDbTable = updateStmt.getTableName();
                Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(session, catalogDbTable.getCatalog(),
                        catalogDbTable.getDb());
                try {
                    olapTableSink.init(session.getExecutionId(), updateStmt.getTxnId(), db.getId(), session.getExecTimeout());
                    olapTableSink.complete();
                } catch (StarRocksException e) {
                    throw new SemanticException(e.getMessage());
                }
            } else if (targetTable instanceof SystemTable) {
                DataSink dataSink = new SchemaTableSink((SystemTable) targetTable, ConnectContext.get().getCurrentWarehouseId());
                execPlan.getFragments().get(0).setSink(dataSink);
            } else {
                throw new SemanticException("Unsupported table type: " + targetTable.getClass().getName());
            }
            if (canUsePipeline) {
                PlanFragment sinkFragment = execPlan.getFragments().get(0);
                if (ConnectContext.get().getSessionVariable().getEnableAdaptiveSinkDop()) {
                    sinkFragment.setPipelineDop(ConnectContext.get().getSessionVariable().getSinkDegreeOfParallelism());
                } else {
                    sinkFragment
                            .setPipelineDop(ConnectContext.get().getSessionVariable().getParallelExecInstanceNum());
                }
                if (targetTable instanceof OlapTable) {
                    sinkFragment.setHasOlapTableSink();
                }
                sinkFragment.setForceSetTableSinkDop();
                sinkFragment.setForceAssignScanRangesPerDriverSeq();
                sinkFragment.disableRuntimeAdaptiveDop();
            } else {
                execPlan.getFragments().get(0).setPipelineDop(1);
            }
            return execPlan;
        } finally {
            session.getSessionVariable().setEnableLocalShuffleAgg(prevIsEnableLocalShuffleAgg);
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(true);
            }
        }
    }

    /**
     * Cast output columns type to target type.
     * @param columnRefFactory :  column ref factory of update stmt.
     * @param targetTable: target table of update stmt.
     * @param colNames: column names of update stmt.
     * @param outputColumns: output columns of update stmt.
     * @param root: root logical plan of update stmt.
     * @return: new root logical plan with cast operator.
     */
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
            // It's safe to use getColumn directly, because the column name's case-insensitive is the same with table's schema.
            Column column = targetTable.getColumn(colName);
            Preconditions.checkState(column != null, "Column %s not found in table %s", colName,
                    targetTable.getName());
            if (!column.getType().matchesType(outputColumn.getType())) {
                // This should be always true but add a check here to avoid updating the wrong column type.
                if (!Type.canCastTo(outputColumn.getType(), column.getType())) {
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
