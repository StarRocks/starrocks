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
import com.starrocks.catalog.TableName;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.ScalarOperatorToIcebergExpr;
import com.starrocks.load.Load;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.IcebergDeleteSink;
import com.starrocks.planner.IcebergMetadataDeleteNode;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.TableRef;
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
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.IntegerType;
import org.apache.iceberg.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public class DeletePlanner {
    private static final Logger LOG = LogManager.getLogger(DeletePlanner.class);

    public ExecPlan plan(DeleteStmt deleteStatement, ConnectContext session) {
        if (deleteStatement.shouldHandledByDeleteHandler()) {
            // executor will use DeleteHandler to handle delete statement
            // so just return empty plan here
            return null;
        }
        return planDelete(deleteStatement, session);
    }

    /**
     * Main method to plan delete operations for different table types
     */
    private ExecPlan planDelete(DeleteStmt deleteStatement, ConnectContext session) {
        com.starrocks.catalog.Table table = deleteStatement.getTable();
        // Transform logical plan
        QueryRelation query = deleteStatement.getQueryStatement().getQueryRelation();
        List<String> colNames = query.getColumnOutputNames();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, session).transform(query);

        // Determine physical properties based on table type
        PhysicalPropertySet requiredProperty;

        // Check if we can use metadata-level delete optimization for Iceberg
        if (table instanceof IcebergTable icebergTable) {
            Optional<ConnectorMetadata> connectorMetadata = GlobalStateMgr.getCurrentState()
                    .getMetadataMgr().getOptionalMetadata(table.getCatalogName());
            // Extract predicate from logical plan
            ScalarOperator predicate = extractPredicateFromOptExpression(logicalPlan.getRoot());
            if (predicate != null && connectorMetadata.isPresent()) {
                ConnectorMetadata metadata = connectorMetadata.get();
                if (metadata.canDeleteUsingMetadata(icebergTable, predicate)) {
                    // Return a plan with IcebergMetadataDeleteNode
                    return createMetadataDeletePlan(icebergTable, predicate);
                }
            }

            // For Iceberg, create shuffled property based on partitioning
            List<ColumnRefOperator> outputColumns = logicalPlan.getOutputColumn();
            requiredProperty = createShuffleProperty(icebergTable, outputColumns);
        } else {
            // For other tables, use default empty property
            requiredProperty = new PhysicalPropertySet();
        }

        // Optimize logical plan, create physical plan, setup sink and configure pipeline
        return createDeletePlan(
                deleteStatement,
                logicalPlan,
                columnRefFactory,
                session,
                requiredProperty,
                colNames,
                table
        );
    }

    /**
     * Extract predicate from OptExpression tree
     */
    private ScalarOperator extractPredicateFromOptExpression(OptExpression expr) {
        if (expr == null || expr.getOp() == null) {
            return null;
        }
        if (expr.getOp() instanceof LogicalApplyOperator) {
            return null;
        }

        // Try to get predicate from the operator
        if (expr.getOp() instanceof LogicalFilterOperator filterOp) {
            return filterOp.getPredicate();
        }

        // Recursively check children for filter operators
        for (OptExpression child : expr.getInputs()) {
            ScalarOperator pred = extractPredicateFromOptExpression(child);
            if (pred != null) {
                return pred;
            }
        }

        return null;
    }

    /**
     * Creates complete delete plan including optimization, sink setup and pipeline configuration
     */
    private ExecPlan createDeletePlan(
            DeleteStmt deleteStatement,
            LogicalPlan logicalPlan,
            ColumnRefFactory columnRefFactory,
            ConnectContext session,
            PhysicalPropertySet requiredProperty,
            List<String> colNames,
            com.starrocks.catalog.Table table) {

        // TODO: remove forceDisablePipeline when all the operators support pipeline engine.
        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline = isEnablePipeline && DataSink.canTableSinkUsePipeline(table);
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;
        boolean prevIsEnableLocalShuffleAgg = session.getSessionVariable().isEnableLocalShuffleAgg();
        try {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(false);
            }
            // Non-query must use the strategy assign scan ranges per driver sequence, which local shuffle agg cannot use.
            session.getSessionVariable().setEnableLocalShuffleAgg(false);

            Optimizer optimizer = OptimizerFactory.create(OptimizerFactory.initContext(session, columnRefFactory));
            OptExpression optimizedPlan = optimizer.optimize(
                    logicalPlan.getRoot(),
                    requiredProperty,
                    new ColumnRefSet(logicalPlan.getOutputColumn()));
            ExecPlan execPlan = PlanFragmentBuilder.createPhysicalPlan(optimizedPlan, session,
                    logicalPlan.getOutputColumn(), columnRefFactory,
                    colNames, TResultSinkType.MYSQL_PROTOCAL, false);

            // Create sink based on table type
            if (table instanceof IcebergTable) {
                setupIcebergDeleteSink(execPlan, colNames, (IcebergTable) table, session);
                configureIcebergTableSinkPipeline(execPlan, session, canUsePipeline);
            } else if (table instanceof OlapTable) {
                setupOlapTableSink(execPlan, deleteStatement, session);
                configureOlapTableSinkPipeline(execPlan, session, canUsePipeline);
            } else {
                throw new SemanticException("Unsupported table type for delete: " + table.getType());
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
     * Creates a metadata delete plan for Iceberg tables.
     * This plan contains a single IcebergMetadataDeleteNode for EXPLAIN output.
     */
    private ExecPlan createMetadataDeletePlan(IcebergTable table, ScalarOperator predicate) {
        ExecPlan plan = new ExecPlan();

        // Create the metadata delete node
        IcebergMetadataDeleteNode node = new IcebergMetadataDeleteNode(
                new PlanNodeId(0), table, predicate);

        // Create a fragment with the node
        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId(0),
                node,
                DataPartition.UNPARTITIONED
        );

        plan.getFragments().add(fragment);
        return plan;
    }

    /**
     * Sets up OLAP table sink for delete operations
     */
    private void setupOlapTableSink(ExecPlan execPlan, DeleteStmt deleteStatement, ConnectContext session) {
        DescriptorTable descriptorTable = execPlan.getDescTbl();
        TupleDescriptor olapTuple = descriptorTable.createTupleDescriptor();

        OlapTable table = (OlapTable) deleteStatement.getTable();
        for (Column column : table.getBaseSchema()) {
            if (column.isKey() || column.isNameWithPrefix(FeConstants.GENERATED_PARTITION_COLUMN_PREFIX)) {
                SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
                slotDescriptor.setIsMaterialized(true);
                slotDescriptor.setType(column.getType());
                slotDescriptor.setColumn(column);
                slotDescriptor.setIsNullable(column.isAllowNull());
            }
        }
        SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
        slotDescriptor.setIsMaterialized(true);
        slotDescriptor.setType(IntegerType.TINYINT);
        slotDescriptor.setColumn(new Column(Load.LOAD_OP_COLUMN, IntegerType.TINYINT));
        slotDescriptor.setIsNullable(false);
        olapTuple.computeMemLayout();

        List<Long> partitionIds = Lists.newArrayList();
        for (Partition partition : table.getPartitions()) {
            partitionIds.add(partition.getId());
        }
        DataSink dataSink = new OlapTableSink(table, olapTuple, partitionIds, table.writeQuorum(),
                table.enableReplicatedStorage(), false, false,
                session.getCurrentComputeResource());
        execPlan.getFragments().get(0).setSink(dataSink);
        if (session.getTxnId() != 0) {
            ((OlapTableSink) dataSink).setIsMultiStatementsTxn(true);
        }

        // if sink is OlapTableSink Assigned to Be execute this sql [cn execute OlapTableSink will crash]
        session.getSessionVariable().setPreferComputeNode(false);
        session.getSessionVariable().setUseComputeNodes(0);
        OlapTableSink olapTableSink = (OlapTableSink) dataSink;
        TableRef tableRef = deleteStatement.getTableRef();
        TableName catalogDbTable = TableName.fromTableRef(tableRef);
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getDb(session, catalogDbTable.getCatalog(), catalogDbTable.getDb());
        try {
            olapTableSink.init(session.getExecutionId(), deleteStatement.getTxnId(), db.getId(), session.getExecTimeout());
            olapTableSink.complete();
        } catch (StarRocksException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    /**
     * Sets up Iceberg delete sink for delete operations
     */
    private void setupIcebergDeleteSink(ExecPlan execPlan, List<String> colNames,
                                        IcebergTable icebergTable, ConnectContext session) {
        DescriptorTable descriptorTable = execPlan.getDescTbl();
        TupleDescriptor deleteTuple = descriptorTable.createTupleDescriptor();

        List<Expr> outputExprs = execPlan.getOutputExprs();
        Preconditions.checkArgument(colNames.size() == outputExprs.size(),
                "output column size mismatch");
        for (int index = 0; index < colNames.size(); ++index) {
            SlotDescriptor slot = descriptorTable.addSlotDescriptor(deleteTuple);
            slot.setIsMaterialized(true);
            slot.setType(outputExprs.get(index).getType());
            slot.setColumn(new Column(colNames.get(index), outputExprs.get(index).getType()));
            slot.setIsNullable(outputExprs.get(index).isNullable());
        }
        deleteTuple.computeMemLayout();

        // Initialize IcebergDeleteSink
        descriptorTable.addReferencedTable(icebergTable);
        IcebergDeleteSink dataSink = new IcebergDeleteSink(
                icebergTable,
                deleteTuple,
                session.getSessionVariable()
        );
        dataSink.init();
        // Create IcebergSinkExtra for DELETE operations
        IcebergMetadata.IcebergSinkExtra icebergSinkExtra = new IcebergMetadata.IcebergSinkExtra();
        // Build Iceberg filter expression from scan node for conflict detection
        org.apache.iceberg.expressions.Expression filterExpr = buildIcebergFilterExpr(execPlan);
        if (filterExpr != null) {
            icebergSinkExtra.setConflictDetectionFilter(filterExpr);
        }
        // Set the sink extra info to be used during commit
        dataSink.setSinkExtraInfo(icebergSinkExtra);

        execPlan.getFragments().get(0).setSink(dataSink);
    }

    /**
     * Configures pipeline for Olap Table sink fragment
     */
    private void configureOlapTableSinkPipeline(ExecPlan execPlan, ConnectContext session,
                                                boolean canUsePipeline) {
        if (!canUsePipeline) {
            execPlan.getFragments().get(0).setPipelineDop(1);
            return;
        }

        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        configureCommonSinkPipeline(sinkFragment, session, PlanFragment::setHasOlapTableSink);
        sinkFragment.setForceAssignScanRangesPerDriverSeq();
    }

    /**
     * Configures pipeline for Iceberg Table sink fragment
     */
    private void configureIcebergTableSinkPipeline(ExecPlan execPlan, ConnectContext session, boolean canUsePipeline) {
        if (!canUsePipeline) {
            execPlan.getFragments().get(0).setPipelineDop(1);
            return;
        }

        // enable spill for connector sink
        SessionVariable sv = session.getSessionVariable();
        if (sv.isEnableConnectorSinkSpill()) {
            sv.setEnableSpill(true);
            if (sv.getConnectorSinkSpillMemLimitThreshold() < sv.getSpillMemLimitThreshold()) {
                sv.setSpillMemLimitThreshold(sv.getConnectorSinkSpillMemLimitThreshold());
            }
        }

        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        configureCommonSinkPipeline(sinkFragment, session, PlanFragment::setHasIcebergTableSink);
    }

    /**
     * Common pipeline configuration logic for both OLAP and Iceberg sinks.
     * Extracts the shared configuration patterns to reduce duplication.
     *
     * @param sinkFragment    The sink fragment to configure
     * @param session         The connect context
     * @param setSinkTypeFlag Consumer to set the sink type flag on the fragment
     */
    private void configureCommonSinkPipeline(
            PlanFragment sinkFragment,
            ConnectContext session,
            Consumer<PlanFragment> setSinkTypeFlag) {
        SessionVariable sv = session.getSessionVariable();

        // Set pipeline dop based on adaptive sink configuration
        if (sv.getEnableAdaptiveSinkDop()) {
            long warehouseId = session.getCurrentComputeResource().getWarehouseId();
            sinkFragment.setPipelineDop(sv.getSinkDegreeOfParallelism(warehouseId));
        } else {
            sinkFragment.setPipelineDop(sv.getParallelExecInstanceNum());
        }
        setSinkTypeFlag.accept(sinkFragment);

        // Common pipeline settings for sink operations
        sinkFragment.disableRuntimeAdaptiveDop();
        sinkFragment.setForceSetTableSinkDop();
    }


    /**
     *
     * @param icebergTable  The Iceberg table
     * @param outputColumns Output columns from the logical plan (includes virtual columns + partition columns)
     * @return PhysicalPropertySet with shuffle requirement or empty property
     */
    private PhysicalPropertySet createShuffleProperty(IcebergTable icebergTable,
                                                      List<ColumnRefOperator> outputColumns) {
        // Check if table is partitioned
        if (!icebergTable.isPartitioned()) {
            // No shuffle for non-partitioned tables
            return new PhysicalPropertySet();
        }

        List<String> partitionColNames = icebergTable.getPartitionColumnNames();
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
            // Partition column not in output, cannot shuffle
            return new PhysicalPropertySet();
        }

        // Create HASH distribution spec
        HashDistributionDesc distributionDesc = new HashDistributionDesc(
                partitionColumnIds,
                HashDistributionDesc.SourceType.SHUFFLE_AGG
        );

        DistributionProperty distributionProperty = DistributionProperty.createProperty(
                DistributionSpec.createHashDistributionSpec(distributionDesc));

        return new PhysicalPropertySet(distributionProperty);
    }

    /**
     * Build Iceberg filter expression from scan node for conflict detection
     */
    private org.apache.iceberg.expressions.Expression buildIcebergFilterExpr(
            ExecPlan execPlan) {
        if (execPlan == null || execPlan.getScanNodes() == null) {
            return null;
        }

        // Find IcebergScanNode and get its predicate
        ScalarOperator predicate = null;
        Schema nativeSchema = null;

        for (PlanNode node : execPlan.getScanNodes()) {
            if (node instanceof IcebergScanNode scanNode) {
                predicate = scanNode.getIcebergJobPlanningPredicate();
                nativeSchema = scanNode.getIcebergTable().getNativeTable().schema();
                break;
            }
        }

        if (predicate == null || nativeSchema == null) {
            return null;
        }

        // Convert ScalarOperator to Iceberg Expression
        ScalarOperatorToIcebergExpr.IcebergContext icebergContext =
                new ScalarOperatorToIcebergExpr.IcebergContext(nativeSchema.asStruct());
        return new ScalarOperatorToIcebergExpr().convert(Collections.singletonList(predicate), icebergContext);
    }
}
