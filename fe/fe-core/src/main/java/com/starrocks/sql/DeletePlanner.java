// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql;

import com.google.common.collect.Lists;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Type;
import com.starrocks.load.Load;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;

import java.util.List;

public class DeletePlanner {
    public ExecPlan plan(DeleteStmt deleteStatement, ConnectContext session) {
        if (!deleteStatement.supportNewPlanner()) {
            // executor will use DeleteHandler to handle delete statement
            // so just return empty plan here
            return null;
        }
        QueryRelation query = deleteStatement.getQueryStatement().getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transformWithSelectLimit(query);

        // TODO: remove forceDisablePipeline when all the operators support pipeline engine.
        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline =
                isEnablePipeline && DataSink.canTableSinkUsePipeline(deleteStatement.getTable()) &&
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
            ExecPlan execPlan = new PlanFragmentBuilder().createPhysicalPlanWithoutOutputFragment(
                    optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory, colNames);
            DescriptorTable descriptorTable = execPlan.getDescTbl();
            TupleDescriptor olapTuple = descriptorTable.createTupleDescriptor();

            OlapTable table = (OlapTable) deleteStatement.getTable();
            for (Column column : table.getBaseSchema()) {
                if (column.isKey()) {
                    SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
                    slotDescriptor.setIsMaterialized(true);
                    slotDescriptor.setType(column.getType());
                    slotDescriptor.setColumn(column);
                    slotDescriptor.setIsNullable(column.isAllowNull());
                } else {
                    continue;
                }
            }
            SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
            slotDescriptor.setIsMaterialized(true);
            slotDescriptor.setType(Type.TINYINT);
            slotDescriptor.setColumn(new Column(Load.LOAD_OP_COLUMN, Type.TINYINT));
            slotDescriptor.setIsNullable(false);
            olapTuple.computeMemLayout();

            List<Long> partitionIds = Lists.newArrayList();
            for (Partition partition : table.getPartitions()) {
                partitionIds.add(partition.getId());
            }
            DataSink dataSink = new OlapTableSink(table, olapTuple, partitionIds);
            execPlan.getFragments().get(0).setSink(dataSink);
            return execPlan;
        } finally {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(true);
            }
        }
    }
}
