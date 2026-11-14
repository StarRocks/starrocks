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
import com.starrocks.alter.SchemaChangeHandler;
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
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.common.MetaUtils;
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
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TIcebergWriteMode;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.thrift.TResultSinkType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class UpdatePlanner {

    public ExecPlan plan(UpdateStmt updateStmt, ConnectContext session) {
        QueryRelation query = updateStmt.getQueryStatement().getQueryRelation();
        List<String> colNames = new ArrayList<>(query.getColumnOutputNames());
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
            if (targetTable instanceof OlapTable olapTable) {
                optExprBuilder = alignOutputToOlapSink(updateStmt, session, columnRefFactory, olapTable,
                        colNames, outputColumns, optExprBuilder);
            }
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
                setupIcebergRowDeltaSink(execPlan, colNames, icebergTable, updateStmt.getTargetBranch(), session);
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
        OlapTable olapTable = (OlapTable) targetTable;

        List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
        List<Column> sinkColumns = olapSinkColumns(updateStmt, olapTable);
        // alignOutputToOlapSink built the output from this same list, one expression per column in this order.
        Preconditions.checkState(sinkColumns.size() == execPlan.getOutputExprs().size(),
                "update of %s writes %s columns but produces %s values", olapTable.getName(), sinkColumns.size(),
                execPlan.getOutputExprs().size());
        for (Column column : sinkColumns) {
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

        List<Long> partitionIds = Lists.newArrayList();
        for (Partition partition : olapTable.getPartitions()) {
            partitionIds.add(partition.getId());
        }
        DataSink dataSink = new OlapTableSink(olapTable, olapTuple, partitionIds, olapTable.writeQuorum(),
                olapTable.enableReplicatedStorage(), false,
                olapTable.supportedAutomaticPartition(), session.getCurrentComputeResource());
        if (updateStmt.usePartialUpdate()) {
            // Row mode for a range-distributed primary-key table whose ORDER BY differs from its primary
            // key: its column-mode DCG would be dropped by the UNSHARE rewrite a split drags behind it,
            // so OlapTableSink refuses that mode. Refusing here instead would leave the statement with no
            // way to run at all -- UpdateAnalyzer only reaches setUsePartialUpdate() with no WHERE
            // clause, and dropping partial update is what makes a WHERE mandatory. Row mode rewrites
            // whole rows, survives the split, and needs no WHERE.
            TPartialUpdateMode mode = MetaUtils.hasSeparateSortKey(olapTable, olapTable.getBaseIndexMetaId())
                    ? TPartialUpdateMode.ROW_MODE
                    : TPartialUpdateMode.COLUMN_UPDATE_MODE;
            ((OlapTableSink) dataSink).setPartialUpdateMode(mode);
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
            olapTableSink.setEstimatedWriteBytes(DmlWriteEstimates.fromExecPlan(execPlan));
            olapTableSink.complete();
        } catch (StarRocksException e) {
            throw new SemanticException(e.getMessage());
        }

        configureSinkPipelineDop(execPlan, session, canUsePipeline, true);
    }

    private void setupIcebergRowDeltaSink(ExecPlan execPlan, List<String> colNames,
                                          IcebergTable icebergTable, String targetBranch, ConnectContext session) {
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
                icebergTable, rowDeltaTuple, session.getSessionVariable(), TIcebergWriteMode.ROW_DELTA_UPDATE);
        dataSink.init();
        dataSink.setTargetBranch(targetBranch);

        IcebergMetadata.IcebergSinkExtra icebergSinkExtra = new IcebergMetadata.IcebergSinkExtra();
        // The BE partitions the output by the spec of the table descriptor built now.
        icebergSinkExtra.setWriteSpecId(icebergTable.getReadSpec().specId());
        org.apache.iceberg.expressions.Expression filterExpr =
                IcebergPlannerUtils.buildIcebergFilterExpr(execPlan, icebergTable);
        if (filterExpr != null) {
            icebergSinkExtra.setConflictDetectionFilter(filterExpr);
        }
        icebergSinkExtra.setBaseSnapshotId(IcebergPlannerUtils.extractBaseSnapshotId(execPlan, icebergTable));
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

    /**
     * The columns an UPDATE of an OLAP table writes, in sink tuple order. UpdateAnalyzer builds the output
     * expressions from baseSchema. Only a real schema change needs fullSchema so concurrent writes can target its
     * shadow indexes. Online OPTIMIZE can leave duplicate generated-column entries in fullSchema, including entries
     * without the shadow-name prefix, so filtering by Column.isShadowColumn() is not sufficient here.
     */
    private static List<Column> olapSinkColumns(UpdateStmt updateStmt, OlapTable olapTable) {
        List<Column> schema = olapTable.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE
                ? olapTable.getFullSchema()
                : olapTable.getBaseSchema();
        List<Column> columns = new ArrayList<>();
        for (Column column : schema) {
            if (updateStmt.usePartialUpdate() && !column.isGeneratedColumn() &&
                    !updateStmt.isAssignmentColumn(column.getName()) && !column.isKey()) {
                continue;
            }
            columns.add(column);
        }
        return columns;
    }

    /**
     * Give the output one value per sink column, in sink order. During a schema change the sink writes the
     * shadow index too, so its tuple carries columns the base schema does not have and UpdateAnalyzer therefore
     * produced nothing for: a column being added (generated or not) and the shadow of a column whose type is
     * changing. Left out, the BE rejected the fragment ("number of exprs is not same with slots") and every
     * row-mode UPDATE failed until the schema change finished. They are filled the way InsertPlanner fills them:
     * a generated column from its expression over the updated row, a shadow column from its origin, a new column
     * from its default.
     */
    private static OptExprBuilder alignOutputToOlapSink(UpdateStmt updateStmt, ConnectContext session,
                                                        ColumnRefFactory columnRefFactory, OlapTable olapTable,
                                                        List<String> colNames, List<ColumnRefOperator> outputColumns,
                                                        OptExprBuilder root) {
        List<Column> sinkColumns = olapSinkColumns(updateStmt, olapTable);
        if (sinkColumns.size() == outputColumns.size()) {
            return root;
        }
        Map<String, ColumnRefOperator> outputByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < colNames.size(); i++) {
            outputByName.put(colNames.get(i), outputColumns.get(i));
        }

        Map<ColumnRefOperator, ScalarOperator> projection = new HashMap<>();
        List<ColumnRefOperator> alignedColumns = new ArrayList<>();
        List<String> alignedNames = new ArrayList<>();
        for (Column column : sinkColumns) {
            ColumnRefOperator output = outputByName.get(column.getName());
            if (output == null) {
                ScalarOperator value;
                String originName = Column.removeNamePrefix(column.getName());
                if (column.isGeneratedColumn()) {
                    value = generatedColumnValue(updateStmt, session, columnRefFactory, olapTable, column,
                            outputByName);
                } else if (column.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PREFIX)
                        && outputByName.containsKey(originName)) {
                    value = outputByName.get(originName);
                } else {
                    // What is left must be a column the schema change adds, which no row holds a value for yet.
                    // Anything else -- a column the base schema has under this name, or one defined by an
                    // expression -- would get a default where the row has a value, so refuse rather than guess.
                    if (column.getDefineExpr() != null || olapTable.getBaseSchema().stream()
                            .anyMatch(c -> c.nameEquals(originName, false))) {
                        throw new SemanticException("UPDATE of %s cannot write column %s during a schema change",
                                olapTable.getName(), column.getName());
                    }
                    value = InsertPlanner.schemaChangeColumnDefault(column);
                }
                if (!value.getType().matchesType(column.getType())) {
                    value = new CastOperator(column.getType(), value, true);
                }
                output = columnRefFactory.create(column.getName(), column.getType(), column.isAllowNull());
                projection.put(output, value);
            } else {
                projection.put(output, output);
            }
            alignedColumns.add(output);
            alignedNames.add(column.getName());
        }
        outputColumns.clear();
        outputColumns.addAll(alignedColumns);
        colNames.clear();
        colNames.addAll(alignedNames);
        return root.withNewRoot(new LogicalProjectOperator(projection));
    }

    /** A generated column's expression over the row as updated: its column references read the output. */
    private static ScalarOperator generatedColumnValue(UpdateStmt updateStmt, ConnectContext session,
                                                       ColumnRefFactory columnRefFactory, OlapTable olapTable,
                                                       Column column, Map<String, ColumnRefOperator> outputByName) {
        TableName tableName = TableName.fromTableRef(updateStmt.getTableRef());
        Expr expr = column.getGeneratedColumnExpr(olapTable.getIdToColumn());
        ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(),
                new Scope(RelationId.anonymous(), new RelationFields(olapTable.getBaseSchema().stream()
                        .map(c -> new Field(c.getName(), c.getType(), tableName, null))
                        .collect(Collectors.toList()))), session);
        List<SlotRef> slots = new ArrayList<>();
        expr.collect(SlotRef.class, slots);
        ExpressionMapping mapping =
                new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()), Lists.newArrayList());
        for (SlotRef slot : slots) {
            ColumnRefOperator ref = outputByName.get(slot.getColumnName());
            if (ref == null) {
                // A column-mode partial update writes only the columns it assigns; UpdateAnalyzer rejects the
                // same for a generated column the base schema already has.
                throw new SemanticException("All ref Column must be sepecfied in partial update mode");
            }
            mapping.put(slot, ref);
        }
        return SqlToScalarOperatorTranslator.translate(expr, mapping, columnRefFactory);
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
