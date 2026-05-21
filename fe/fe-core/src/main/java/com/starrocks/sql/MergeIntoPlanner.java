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
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.IcebergDeleteSink;
import com.starrocks.planner.IcebergTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.MergeIntoStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultSinkType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Plans MERGE INTO statements for Iceberg tables.
 * Creates two execution plans:
 * 1. Delete plan: writes positional delete files via IcebergDeleteSink
 * 2. Insert plan: writes new data files via IcebergTableSink
 */
public class MergeIntoPlanner {
    private static final Logger LOG = LogManager.getLogger(MergeIntoPlanner.class);

    public void plan(MergeIntoStmt stmt, ConnectContext session) {
        IcebergTable table = (IcebergTable) stmt.getTargetTable();

        // Plan delete phase
        if (stmt.getDeleteQueryStatement() != null) {
            ExecPlan deletePlan = planDeletePhase(stmt, table, session);
            stmt.setDeletePlan(deletePlan);
        }

        // Plan insert phase
        if (stmt.getInsertQueryStatement() != null) {
            ExecPlan insertPlan = planInsertPhase(stmt, table, session);
            stmt.setInsertPlan(insertPlan);
        }
    }

    /**
     * Plan the delete phase: SELECT _file, _pos, partition_cols FROM target JOIN source
     * Output goes to IcebergDeleteSink for positional deletes.
     */
    private ExecPlan planDeletePhase(MergeIntoStmt stmt, IcebergTable table, ConnectContext session) {
        QueryRelation query = stmt.getDeleteQueryStatement().getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transform(query);

        // Create shuffle property based on partition columns
        List<ColumnRefOperator> outputColumns = logicalPlan.getOutputColumn();
        PhysicalPropertySet requiredProperty = createShuffleProperty(table, outputColumns);

        return buildPlanWithSink(logicalPlan, columnRefFactory, session, requiredProperty, colNames,
                table, true /* isDeletePhase */);
    }

    /**
     * Plan the insert phase: SELECT updated/new values
     * Output goes to IcebergTableSink for new data files.
     */
    private ExecPlan planInsertPhase(MergeIntoStmt stmt, IcebergTable table, ConnectContext session) {
        QueryRelation query = stmt.getInsertQueryStatement().getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transform(query);

        PhysicalPropertySet requiredProperty = new PhysicalPropertySet();

        return buildPlanWithSink(logicalPlan, columnRefFactory, session, requiredProperty, colNames,
                table, false /* isDeletePhase */);
    }

    private ExecPlan buildPlanWithSink(LogicalPlan logicalPlan, ColumnRefFactory columnRefFactory,
                                       ConnectContext session, PhysicalPropertySet requiredProperty,
                                       List<String> colNames, IcebergTable table,
                                       boolean isDeletePhase) {
        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline = isEnablePipeline && DataSink.canTableSinkUsePipeline(table);
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;
        boolean prevIsEnableLocalShuffleAgg = session.getSessionVariable().isEnableLocalShuffleAgg();

        try {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(false);
            }
            session.getSessionVariable().setEnableLocalShuffleAgg(false);

            Optimizer optimizer = OptimizerFactory.create(OptimizerFactory.initContext(session, columnRefFactory));
            OptExpression optimizedPlan = optimizer.optimize(
                    logicalPlan.getRoot(),
                    requiredProperty,
                    new ColumnRefSet(logicalPlan.getOutputColumn()));
            ExecPlan execPlan = PlanFragmentBuilder.createPhysicalPlan(optimizedPlan, session,
                    logicalPlan.getOutputColumn(), columnRefFactory,
                    colNames, TResultSinkType.MYSQL_PROTOCAL, false);

            if (isDeletePhase) {
                setupIcebergDeleteSink(execPlan, colNames, table, session);
            } else {
                setupIcebergInsertSink(execPlan, colNames, table, session);
            }
            configureIcebergPipeline(execPlan, session, canUsePipeline, isDeletePhase);

            return execPlan;
        } finally {
            session.getSessionVariable().setEnableLocalShuffleAgg(prevIsEnableLocalShuffleAgg);
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(true);
            }
        }
    }

    private void setupIcebergDeleteSink(ExecPlan execPlan, List<String> colNames,
                                        IcebergTable table, ConnectContext session) {
        DescriptorTable descriptorTable = execPlan.getDescTbl();
        TupleDescriptor deleteTuple = descriptorTable.createTupleDescriptor();

        List<Expr> outputExprs = execPlan.getOutputExprs();
        Preconditions.checkArgument(colNames.size() == outputExprs.size(),
                "output column size mismatch");
        for (int i = 0; i < colNames.size(); i++) {
            SlotDescriptor slot = descriptorTable.addSlotDescriptor(deleteTuple);
            slot.setIsMaterialized(true);
            slot.setType(outputExprs.get(i).getType());
            slot.setColumn(new Column(colNames.get(i), outputExprs.get(i).getType()));
            slot.setIsNullable(outputExprs.get(i).isNullable());
        }
        deleteTuple.computeMemLayout();

        descriptorTable.addReferencedTable(table);
        IcebergDeleteSink dataSink = new IcebergDeleteSink(
                table, deleteTuple, session.getSessionVariable());
        dataSink.init();

        // Create IcebergSinkExtra for conflict detection
        IcebergMetadata.IcebergSinkExtra sinkExtra = new IcebergMetadata.IcebergSinkExtra();
        dataSink.setSinkExtraInfo(sinkExtra);

        execPlan.getFragments().get(0).setSink(dataSink);
    }

    private void setupIcebergInsertSink(ExecPlan execPlan, List<String> colNames,
                                        IcebergTable table, ConnectContext session) {
        DescriptorTable descriptorTable = execPlan.getDescTbl();
        TupleDescriptor tupleDesc = descriptorTable.createTupleDescriptor();

        List<Expr> outputExprs = execPlan.getOutputExprs();
        Preconditions.checkArgument(colNames.size() == outputExprs.size(),
                "output column size mismatch");
        for (int i = 0; i < colNames.size(); i++) {
            SlotDescriptor slot = descriptorTable.addSlotDescriptor(tupleDesc);
            slot.setIsMaterialized(true);
            slot.setType(outputExprs.get(i).getType());
            slot.setColumn(new Column(colNames.get(i), outputExprs.get(i).getType()));
            slot.setIsNullable(outputExprs.get(i).isNullable());
        }
        tupleDesc.computeMemLayout();

        descriptorTable.addReferencedTable(table);
        IcebergTableSink dataSink = new IcebergTableSink(
                table, tupleDesc, false /* isStaticPartitionSink */,
                session.getSessionVariable(), null /* targetBranch */);

        execPlan.getFragments().get(0).setSink(dataSink);
    }

    private void configureIcebergPipeline(ExecPlan execPlan, ConnectContext session,
                                          boolean canUsePipeline, boolean isDeletePhase) {
        if (!canUsePipeline) {
            execPlan.getFragments().get(0).setPipelineDop(1);
            return;
        }

        SessionVariable sv = session.getSessionVariable();
        if (sv.isEnableConnectorSinkSpill()) {
            sv.setEnableSpill(true);
            if (sv.getConnectorSinkSpillMemLimitThreshold() < sv.getSpillMemLimitThreshold()) {
                sv.setSpillMemLimitThreshold(sv.getConnectorSinkSpillMemLimitThreshold());
            }
        }

        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        if (sv.getEnableAdaptiveSinkDop()) {
            long warehouseId = session.getCurrentComputeResource().getWarehouseId();
            sinkFragment.setPipelineDop(sv.getSinkDegreeOfParallelism(warehouseId));
        } else {
            sinkFragment.setPipelineDop(sv.getParallelExecInstanceNum());
        }

        sinkFragment.setHasIcebergTableSink();
        sinkFragment.disableRuntimeAdaptiveDop();
        sinkFragment.setForceSetTableSinkDop();
    }

    private PhysicalPropertySet createShuffleProperty(IcebergTable table,
                                                       List<ColumnRefOperator> outputColumns) {
        if (!table.isPartitioned()) {
            return new PhysicalPropertySet();
        }

        List<String> partitionColNames = table.getPartitionColumnNames();
        List<Integer> partitionColumnIds = Lists.newArrayList();
        for (String partCol : partitionColNames) {
            for (ColumnRefOperator outputCol : outputColumns) {
                if (outputCol.getName().equalsIgnoreCase(partCol)) {
                    partitionColumnIds.add(outputCol.getId());
                    break;
                }
            }
        }

        if (partitionColumnIds.isEmpty()) {
            return new PhysicalPropertySet();
        }

        HashDistributionDesc distributionDesc = new HashDistributionDesc(
                partitionColumnIds, HashDistributionDesc.SourceType.SHUFFLE_AGG);
        DistributionProperty distributionProperty = DistributionProperty.createProperty(
                DistributionSpec.createHashDistributionSpec(distributionDesc));
        return new PhysicalPropertySet(distributionProperty);
    }
}
