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
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.IcebergTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.MergeStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MergePlanner {


    public ExecPlan plan(MergeStmt mergeStatement, ConnectContext session) {
        QueryRelation query = mergeStatement.getQueryStatement().getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transform(query);
        ArrayList<Integer> outputSlots = new ArrayList<>();
        List<ColumnRefOperator> outputColumn = logicalPlan.getOutputColumn();
        for (int i = 0; i < outputColumn.size()-3; i++) {
            outputSlots.add(outputColumn.get(i).getId());
        }
        if(outputColumn.size() >= 3) {
            outputSlots.add(outputColumn.get(outputColumn.size() - 1).getId());//is_distinct_slot_id
            outputSlots.add(outputColumn.get(outputColumn.size() - 2).getId());//op_slot_id
        }

        // TODO: remove forceDisablePipeline when all the operators support pipeline engine.
        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline = isEnablePipeline && DataSink.canTableSinkUsePipeline(mergeStatement.getTable());
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;
        boolean prevIsEnableLocalShuffleAgg = session.getSessionVariable().isEnableLocalShuffleAgg();
        try {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(false);
            }
            // Non-query must use the strategy assign scan ranges per driver sequence, which local shuffle agg cannot use.
            session.getSessionVariable().setEnableLocalShuffleAgg(false);

            Table table = mergeStatement.getTable();
            long tableId = table.getId();
            Optimizer optimizer = new Optimizer();
            optimizer.setUpdateTableId(tableId);
            OptExpression optimizedPlan = optimizer.optimize(
                    session,
                    logicalPlan.getRoot(),
                    new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);
            ExecPlan execPlan = PlanFragmentBuilder.createPhysicalPlan(optimizedPlan, session,
                    logicalPlan.getOutputColumn(), columnRefFactory, colNames,
                    com.starrocks.thrift.TResultSinkType.MYSQL_PROTOCAL, false);
            DescriptorTable descriptorTable = execPlan.getDescTbl();
            TupleDescriptor olapTuple = descriptorTable.createTupleDescriptor();

            List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
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
            if (table instanceof IcebergTable) {
//                descriptorTable.getTupleDescs().stream().forEach(t -> t.setTable(table));
                DataSink dataSink = new IcebergTableSink((IcebergTable) table, olapTuple, true,3, outputSlots);
                execPlan.getFragments().get(0).setSink(dataSink);
            } else {
                throw new SemanticException("Unsupported table type: " + table.getClass().getName());
            }
            if (canUsePipeline) {
                PlanFragment sinkFragment = execPlan.getFragments().get(0);
                if (ConnectContext.get().getSessionVariable().getEnableAdaptiveSinkDop()) {
                    sinkFragment.setPipelineDop(ConnectContext.get().getSessionVariable().getSinkDegreeOfParallelism());
                } else {
                    sinkFragment
                            .setPipelineDop(ConnectContext.get().getSessionVariable().getParallelExecInstanceNum());
                }
                if (table instanceof OlapTable) {
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
}
