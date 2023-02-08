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

import com.google.common.collect.Lists;
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
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.DeleteStmt;
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
import com.starrocks.thrift.TResultSinkType;

import java.util.List;

public class DeletePlanner {
    public ExecPlan plan(DeleteStmt deleteStatement, ConnectContext session) {
        if (deleteStatement.shouldHandledByDeleteHandler()) {
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
        boolean canUsePipeline = isEnablePipeline && DataSink.canTableSinkUsePipeline(deleteStatement.getTable());
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
            ExecPlan execPlan = PlanFragmentBuilder.createPhysicalPlan(optimizedPlan, session,
                    logicalPlan.getOutputColumn(), columnRefFactory,
                    colNames, TResultSinkType.MYSQL_PROTOCAL, false);
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
            DataSink dataSink = new OlapTableSink(table, olapTuple, partitionIds, table.writeQuorum(),
                    table.enableReplicatedStorage());
            execPlan.getFragments().get(0).setSink(dataSink);
            if (canUsePipeline) {
                PlanFragment sinkFragment = execPlan.getFragments().get(0);
                if (ConnectContext.get().getSessionVariable().getEnableAdaptiveSinkDop()) {
                    sinkFragment.setPipelineDop(ConnectContext.get().getSessionVariable().getDegreeOfParallelism());
                } else {
                    sinkFragment.setPipelineDop(ConnectContext.get().getSessionVariable().getParallelExecInstanceNum());
                }
                sinkFragment.setHasOlapTableSink();
                sinkFragment.setForceSetTableSinkDop();
                sinkFragment.setForceAssignScanRangesPerDriverSeq();
                sinkFragment.disableRuntimeAdaptiveDop();
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
