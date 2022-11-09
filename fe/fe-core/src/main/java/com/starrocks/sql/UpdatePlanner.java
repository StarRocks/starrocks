// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql;

import com.google.common.collect.Lists;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.Pair;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultSinkType;

import java.util.List;
import java.util.Optional;

public class UpdatePlanner {
    public ExecPlan plan(UpdateStmt updateStmt, ConnectContext session) {
        QueryRelation query = updateStmt.getQueryStatement().getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transformWithSelectLimit(query);

        // TODO: remove forceDisablePipeline when all the operators support pipeline engine.
        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline = isEnablePipeline && DataSink.canTableSinkUsePipeline(updateStmt.getTable()) &&
                logicalPlan.canUsePipeline();
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;
        try {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(false);
            }

            Optimizer optimizer = new Optimizer();
            OptExpression optimizedPlan = optimizer.optimize(
                    session,
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);
            ExecPlan execPlan = new PlanFragmentBuilder().createPhysicalPlan(optimizedPlan, session,
                    logicalPlan.getOutputColumn(), columnRefFactory, colNames, TResultSinkType.MYSQL_PROTOCAL, false);
            DescriptorTable descriptorTable = execPlan.getDescTbl();
            TupleDescriptor olapTuple = descriptorTable.createTupleDescriptor();

            List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
            OlapTable table = (OlapTable) updateStmt.getTable();
            long tableId = table.getId();
            for (Column column : table.getFullSchema()) {
                SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
                slotDescriptor.setIsMaterialized(true);
                slotDescriptor.setType(column.getType());
                slotDescriptor.setColumn(column);
                slotDescriptor.setIsNullable(column.isAllowNull());
                if (column.getType().isVarchar() &&
                        IDictManager.getInstance().hasGlobalDict(tableId, column.getName())) {
                    Optional<ColumnDict> dict = IDictManager.getInstance().getGlobalDict(tableId, column.getName());
                    dict.ifPresent(
                            columnDict -> globalDicts.add(new Pair<>(slotDescriptor.getId().asInt(), columnDict)));
                }
            }
            olapTuple.computeMemLayout();

            List<Long> partitionIds = Lists.newArrayList();
            for (Partition partition : table.getPartitions()) {
                partitionIds.add(partition.getId());
            }
            DataSink dataSink = new OlapTableSink(table, olapTuple, partitionIds, table.writeQuorum(),
                    table.enableReplicatedStorage());
            execPlan.getFragments().get(0).setSink(dataSink);
            execPlan.getFragments().get(0).setLoadGlobalDicts(globalDicts);
            if (isEnablePipeline && canUsePipeline) {
                PlanFragment sinkFragment = execPlan.getFragments().get(0);
                if (ConnectContext.get().getSessionVariable().getPipelineSinkDop() <= 0) {
                    sinkFragment.setPipelineDop(ConnectContext.get().getSessionVariable().getParallelExecInstanceNum());
                } else {
                    sinkFragment.setPipelineDop(ConnectContext.get().getSessionVariable().getPipelineSinkDop());
                }
                sinkFragment.setHasOlapTableSink();
                sinkFragment.setForceSetTableSinkDop();
                sinkFragment.setForceAssignScanRangesPerDriverSeq();
            } else {
                execPlan.getFragments().get(0).setPipelineDop(1);
            }
            return execPlan;
        } finally {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(true);
            }
        }
    }
}
