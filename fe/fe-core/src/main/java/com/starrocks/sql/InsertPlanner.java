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
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.IcebergTableSink;
import com.starrocks.planner.MysqlTableSink;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DefaultValueExpr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.GatherDistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.SortProperty;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
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
import com.starrocks.thrift.TResultSinkType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.catalog.DefaultExpr.SUPPORTED_DEFAULT_FNS;
import static com.starrocks.sql.optimizer.rule.mv.MVUtils.MATERIALIZED_VIEW_NAME_PREFIX;

public class InsertPlanner {
    // Only for unit test
    public static boolean enableSingleReplicationShuffle = false;
    private boolean shuffleServiceEnable = false;
    private boolean forceReplicatedStorage = false;
    private Map<String, Database> dbs;
    private boolean useOptimisticLock;

    private static final Logger LOG = LogManager.getLogger(InsertPlanner.class);

    public InsertPlanner() {
        this.useOptimisticLock = false;
    }

    public InsertPlanner(Map<String, Database> dbs, boolean optimisticLock) {
        this.dbs = dbs;
        this.useOptimisticLock = optimisticLock;
    }

    public ExecPlan plan(InsertStmt insertStmt, ConnectContext session) {
        QueryRelation queryRelation = insertStmt.getQueryStatement().getQueryRelation();
        List<ColumnRefOperator> outputColumns = new ArrayList<>();
        Table targetTable = insertStmt.getTargetTable();

        //1. Process the literal value of the insert values type and cast it into the type of the target table
        if (queryRelation instanceof ValuesRelation) {
            castLiteralToTargetColumnsType(insertStmt);
        }

        //2. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan;
        try (PlannerProfile.ScopedTimer ignore = PlannerProfile.getScopedTimer("Transform")) {
            logicalPlan = new RelationTransformer(columnRefFactory, session).transform(queryRelation);
        }

        //3. Fill in the default value and NULL
        OptExprBuilder optExprBuilder = fillDefaultValue(logicalPlan, columnRefFactory, insertStmt, outputColumns);

        //4. Fill key partition columns constant value for data lake format table (hive/iceberg/hudi/delta_lake)
        if (insertStmt.isSpecifyKeyPartition()) {
            optExprBuilder = fillKeyPartitionsColumn(columnRefFactory, insertStmt, outputColumns, optExprBuilder);
        }

        //5. Fill in the generated columns
        optExprBuilder = fillGeneratedColumns(columnRefFactory, insertStmt, outputColumns, optExprBuilder, session);

        //6. Fill in the shadow column
        optExprBuilder = fillShadowColumns(columnRefFactory, insertStmt, outputColumns, optExprBuilder, session);

        //7. Cast output columns type to target type
        optExprBuilder =
                castOutputColumnsTypeToTargetColumns(columnRefFactory, insertStmt, outputColumns, optExprBuilder);

        //8. Optimize logical plan and build physical plan
        logicalPlan = new LogicalPlan(optExprBuilder, outputColumns, logicalPlan.getCorrelation());

        // TODO: remove forceDisablePipeline when all the operators support pipeline engine.
        SessionVariable currentVariable = session.getSessionVariable();
        boolean isEnablePipeline = session.getSessionVariable().isEnablePipelineEngine();
        boolean canUsePipeline = isEnablePipeline && DataSink.canTableSinkUsePipeline(targetTable);
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;
        boolean prevIsEnableLocalShuffleAgg = session.getSessionVariable().isEnableLocalShuffleAgg();
        boolean previousMVRewrite = currentVariable.isEnableMaterializedViewRewrite();
        boolean enableMVRewrite = currentVariable.isEnableMaterializedViewRewriteForInsert() &&
                currentVariable.isEnableMaterializedViewRewrite();
        try (PlannerProfile.ScopedTimer ignore = PlannerProfile.getScopedTimer("InsertPlanner")) {
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(false);
            }
            // Non-query must use the strategy assign scan ranges per driver sequence, which local shuffle agg cannot use.
            session.getSessionVariable().setEnableLocalShuffleAgg(false);
            session.getSessionVariable().setEnableMaterializedViewRewrite(enableMVRewrite);

            ExecPlan execPlan =
                    useOptimisticLock ?
                            buildExecPlanWithRetry(insertStmt, session, outputColumns, logicalPlan, columnRefFactory,
                                    queryRelation, targetTable) :
                            buildExecPlan(insertStmt, session, outputColumns, logicalPlan, columnRefFactory,
                                    queryRelation,
                                    targetTable);

            DescriptorTable descriptorTable = execPlan.getDescTbl();
            TupleDescriptor tupleDesc = descriptorTable.createTupleDescriptor();

            List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
            long tableId = targetTable.getId();
            for (Column column : targetTable.getFullSchema()) {
                SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(tupleDesc);
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
            tupleDesc.computeMemLayout();

            DataSink dataSink;
            if (targetTable instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) targetTable;
                boolean enableAutomaticPartition;
                List<Long> targetPartitionIds = insertStmt.getTargetPartitionIds();
                if (insertStmt.isSystem() && insertStmt.isPartitionNotSpecifiedInOverwrite()) {
                    Preconditions.checkState(!CollectionUtils.isEmpty(targetPartitionIds));
                    enableAutomaticPartition = olapTable.supportedAutomaticPartition();
                } else if (insertStmt.isSpecifyPartitionNames()) {
                    Preconditions.checkState(!CollectionUtils.isEmpty(targetPartitionIds));
                    enableAutomaticPartition = false;
                } else if (insertStmt.isStaticKeyPartitionInsert()) {
                    enableAutomaticPartition = false;
                } else {
                    Preconditions.checkState(!CollectionUtils.isEmpty(targetPartitionIds));
                    enableAutomaticPartition = olapTable.supportedAutomaticPartition();
                }
                boolean nullExprInAutoIncrement = false;
                // In INSERT INTO SELECT, if AUTO_INCREMENT column
                // is specified, the column values must not be NULL
                if (!(queryRelation instanceof ValuesRelation) &&
                        !(targetTable instanceof MaterializedView)) {
                    boolean specifyAutoIncrementColumn = false;
                    if (insertStmt.getTargetColumnNames() != null) {
                        for (String colName : insertStmt.getTargetColumnNames()) {
                            for (Column col : olapTable.getBaseSchema()) {
                                if (col.isAutoIncrement() && col.getName().equals(colName)) {
                                    specifyAutoIncrementColumn = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (insertStmt.getTargetColumnNames() == null || specifyAutoIncrementColumn) {
                        nullExprInAutoIncrement = true;
                    }

                }
                dataSink = new OlapTableSink(olapTable, tupleDesc, targetPartitionIds,
                        canUsePipeline, olapTable.writeQuorum(),
                        forceReplicatedStorage ? true : olapTable.enableReplicatedStorage(),
                        nullExprInAutoIncrement, enableAutomaticPartition);
            } else if (insertStmt.getTargetTable() instanceof MysqlTable) {
                dataSink = new MysqlTableSink((MysqlTable) targetTable);
            } else if (targetTable instanceof IcebergTable) {
                descriptorTable.addReferencedTable(targetTable);
                dataSink = new IcebergTableSink((IcebergTable) targetTable, tupleDesc,
                        isKeyPartitionStaticInsert(insertStmt, queryRelation));
            } else {
                throw new SemanticException("Unknown table type " + insertStmt.getTargetTable().getType());
            }

            PlanFragment sinkFragment = execPlan.getFragments().get(0);
            if (canUsePipeline && (targetTable instanceof OlapTable || targetTable instanceof IcebergTable)) {
                if (shuffleServiceEnable) {
                    // For shuffle insert into, we only support tablet sink dop = 1
                    // because for tablet sink dop > 1, local passthourgh exchange will influence the order of sending,
                    // which may lead to inconsisten replica for primary key.
                    // If you want to set tablet sink dop > 1, please enable single tablet loading and disable shuffle service
                    sinkFragment.setPipelineDop(1);
                } else {
                    if (ConnectContext.get().getSessionVariable().getEnableAdaptiveSinkDop()) {
                        sinkFragment.setPipelineDop(
                                ConnectContext.get().getSessionVariable().getSinkDegreeOfParallelism());
                    } else {
                        sinkFragment
                                .setPipelineDop(ConnectContext.get().getSessionVariable().getParallelExecInstanceNum());
                    }
                }

                if (targetTable instanceof OlapTable) {
                    sinkFragment.setHasOlapTableSink();
                    sinkFragment.setForceAssignScanRangesPerDriverSeq();
                } else {
                    sinkFragment.setHasIcebergTableSink();
                }

                sinkFragment.disableRuntimeAdaptiveDop();
                sinkFragment.setForceSetTableSinkDop();
            }
            sinkFragment.setSink(dataSink);
            sinkFragment.setLoadGlobalDicts(globalDicts);
            return execPlan;
        } finally {
            session.getSessionVariable().setEnableLocalShuffleAgg(prevIsEnableLocalShuffleAgg);
            if (forceDisablePipeline) {
                session.getSessionVariable().setEnablePipelineEngine(true);
            }
            session.getSessionVariable().setEnableMaterializedViewRewrite(previousMVRewrite);
        }
    }

    /**
     * The workhorse of InsertPlanner, which may takes a lot of time, so we would release the lock during planning
     */
    private ExecPlan buildExecPlanWithRetry(InsertStmt insertStmt, ConnectContext session,
                                            List<ColumnRefOperator> outputColumns,
                                            LogicalPlan logicalPlan, ColumnRefFactory columnRefFactory,
                                            QueryRelation queryRelation, Table targetTable) {
        boolean isSchemaValid = true;
        Set<OlapTable> olapTables = StatementPlanner.collectOriginalOlapTables(insertStmt, dbs);
        Stopwatch watch = Stopwatch.createStarted();

        for (int i = 0; i < Config.max_query_retry_time; i++) {
            long planStartTime = OptimisticVersion.generate();
            if (!isSchemaValid) {
                olapTables = StatementPlanner.reAnalyzeStmt(insertStmt, dbs, session);
            }

            // Release the lock during planning, and reacquire the lock before validating
            StatementPlanner.unLock(dbs);
            ExecPlan plan;
            try {
                plan = buildExecPlan(insertStmt, session, outputColumns, logicalPlan, columnRefFactory, queryRelation,
                        targetTable);
            } finally {
                StatementPlanner.lock(dbs);
            }
            isSchemaValid =
                    olapTables.stream().allMatch(t -> OptimisticVersion.validateTableUpdate(t, planStartTime));
            if (isSchemaValid) {
                return plan;
            }
        }
        throw new StarRocksPlannerException(String.format("failed to generate plan for the statement after %dms",
                watch.elapsed(TimeUnit.MILLISECONDS)), ErrorType.INTERNAL_ERROR);
    }

    private ExecPlan buildExecPlan(InsertStmt insertStmt, ConnectContext session, List<ColumnRefOperator> outputColumns,
                                   LogicalPlan logicalPlan, ColumnRefFactory columnRefFactory,
                                   QueryRelation queryRelation, Table targetTable) {
        Optimizer optimizer = new Optimizer();
        PhysicalPropertySet requiredPropertySet = createPhysicalPropertySet(insertStmt, outputColumns);

        LOG.debug("property {}", requiredPropertySet);
        OptExpression optimizedPlan;

        try (PlannerProfile.ScopedTimer ignore2 = PlannerProfile.getScopedTimer("Optimizer")) {
            optimizedPlan = optimizer.optimize(
                    session,
                    logicalPlan.getRoot(),
                    requiredPropertySet,
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);
        }

        //8. Build fragment exec plan
        boolean hasOutputFragment = ((queryRelation instanceof SelectRelation && queryRelation.hasLimit())
                || targetTable instanceof MysqlTable);
        ExecPlan execPlan;
        try (PlannerProfile.ScopedTimer ignore3 = PlannerProfile.getScopedTimer("PlanBuilder")) {
            execPlan = PlanFragmentBuilder.createPhysicalPlan(
                    optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory,
                    queryRelation.getColumnOutputNames(), TResultSinkType.MYSQL_PROTOCAL, hasOutputFragment);
        }

        return execPlan;
    }

    private void castLiteralToTargetColumnsType(InsertStmt insertStatement) {
        Preconditions.checkState(insertStatement.getQueryStatement().getQueryRelation() instanceof ValuesRelation,
                "must values");
        List<Column> fullSchema = insertStatement.getTargetTable().getFullSchema();
        ValuesRelation values = (ValuesRelation) insertStatement.getQueryStatement().getQueryRelation();
        RelationFields fields = insertStatement.getQueryStatement().getQueryRelation().getRelationFields();

        for (int columnIdx = 0; columnIdx < insertStatement.getTargetTable().getBaseSchema().size(); ++columnIdx) {
            if (needToSkip(insertStatement, columnIdx)) {
                continue;
            }

            Column targetColumn = fullSchema.get(columnIdx);
            if (targetColumn.isGeneratedColumn()) {
                continue;
            }
            boolean isAutoIncrement = targetColumn.isAutoIncrement();
            if (insertStatement.getTargetColumnNames() == null) {
                for (List<Expr> row : values.getRows()) {
                    if (isAutoIncrement && row.get(columnIdx).getType() == Type.NULL) {
                        throw new SemanticException(" `NULL` value is not supported for an AUTO_INCREMENT column: " +
                                targetColumn.getName() + " You can use `default` for an" +
                                " AUTO INCREMENT column");
                    }
                    if (row.get(columnIdx) instanceof DefaultValueExpr) {
                        if (isAutoIncrement) {
                            row.set(columnIdx, new NullLiteral());
                        } else {
                            row.set(columnIdx, new StringLiteral(targetColumn.calculatedDefaultValue()));
                        }
                    }
                    row.set(columnIdx, TypeManager.addCastExpr(row.get(columnIdx), targetColumn.getType()));
                }
                fields.getFieldByIndex(columnIdx).setType(targetColumn.getType());
            } else {
                int idx = insertStatement.getTargetColumnNames().indexOf(targetColumn.getName().toLowerCase());
                if (idx != -1) {
                    for (List<Expr> row : values.getRows()) {
                        if (isAutoIncrement && row.get(idx).getType() == Type.NULL) {
                            throw new SemanticException(
                                    " `NULL` value is not supported for an AUTO_INCREMENT column: " +
                                            targetColumn.getName() + " You can use `default` for an" +
                                            " AUTO INCREMENT column");
                        }
                        if (row.get(idx) instanceof DefaultValueExpr) {
                            if (isAutoIncrement) {
                                row.set(idx, new NullLiteral());
                            } else {
                                row.set(idx, new StringLiteral(targetColumn.calculatedDefaultValue()));
                            }
                        }
                        row.set(idx, TypeManager.addCastExpr(row.get(idx), targetColumn.getType()));
                    }
                    fields.getFieldByIndex(idx).setType(targetColumn.getType());
                }
            }
        }
    }

    private OptExprBuilder fillDefaultValue(LogicalPlan logicalPlan, ColumnRefFactory columnRefFactory,
                                            InsertStmt insertStatement, List<ColumnRefOperator> outputColumns) {
        List<Column> baseSchema = insertStatement.getTargetTable().getBaseSchema();
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();

        for (int columnIdx = 0; columnIdx < baseSchema.size(); ++columnIdx) {
            if (needToSkip(insertStatement, columnIdx)) {
                continue;
            }

            Column targetColumn = baseSchema.get(columnIdx);
            if (targetColumn.isGeneratedColumn()) {
                continue;
            }
            if (insertStatement.getTargetColumnNames() == null) {
                outputColumns.add(logicalPlan.getOutputColumn().get(columnIdx));
                columnRefMap.put(logicalPlan.getOutputColumn().get(columnIdx),
                        logicalPlan.getOutputColumn().get(columnIdx));
            } else {
                int idx = insertStatement.getTargetColumnNames().indexOf(targetColumn.getName().toLowerCase());
                if (idx == -1) {
                    ScalarOperator scalarOperator;
                    Column.DefaultValueType defaultValueType = targetColumn.getDefaultValueType();
                    if (defaultValueType == Column.DefaultValueType.NULL || targetColumn.isAutoIncrement()) {
                        scalarOperator = ConstantOperator.createNull(targetColumn.getType());
                    } else if (defaultValueType == Column.DefaultValueType.CONST) {
                        scalarOperator = ConstantOperator.createVarchar(targetColumn.calculatedDefaultValue());
                    } else if (defaultValueType == Column.DefaultValueType.VARY) {
                        if (SUPPORTED_DEFAULT_FNS.contains(targetColumn.getDefaultExpr().getExpr())) {
                            scalarOperator = SqlToScalarOperatorTranslator.
                                    translate(targetColumn.getDefaultExpr().obtainExpr());
                        } else {
                            throw new SemanticException(
                                    "Column:" + targetColumn.getName() + " has unsupported default value:"
                                            + targetColumn.getDefaultExpr().getExpr());
                        }
                    } else {
                        throw new SemanticException("Unknown default value type:%s", defaultValueType.toString());
                    }
                    ColumnRefOperator col = columnRefFactory
                            .create(scalarOperator, scalarOperator.getType(), scalarOperator.isNullable());

                    outputColumns.add(col);
                    columnRefMap.put(col, scalarOperator);
                } else {
                    outputColumns.add(logicalPlan.getOutputColumn().get(idx));
                    columnRefMap.put(logicalPlan.getOutputColumn().get(idx), logicalPlan.getOutputColumn().get(idx));
                }
            }
        }
        return logicalPlan.getRootBuilder().withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }

    private OptExprBuilder fillGeneratedColumns(ColumnRefFactory columnRefFactory, InsertStmt insertStatement,
                                                List<ColumnRefOperator> outputColumns, OptExprBuilder root,
                                                ConnectContext session) {
        List<Column> fullSchema = insertStatement.getTargetTable().getFullSchema();
        Set<Column> baseSchema = Sets.newHashSet(insertStatement.getTargetTable().getBaseSchema());
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();

        for (int columnIdx = 0; columnIdx < fullSchema.size(); ++columnIdx) {
            Column targetColumn = fullSchema.get(columnIdx);

            if (targetColumn.isGeneratedColumn()) {
                // If fe restart and Insert INTO is executed, the re-analyze is needed.
                Expr expr = targetColumn.generatedColumnExpr();
                ExpressionAnalyzer.analyzeExpression(expr,
                        new AnalyzeState(), new Scope(RelationId.anonymous(), new RelationFields(
                                insertStatement.getTargetTable().getBaseSchema().stream()
                                        .map(col -> new Field(col.getName(),
                                                col.getType(), insertStatement.getTableName(), null))
                                        .collect(Collectors.toList()))), session);

                List<SlotRef> slots = new ArrayList<>();
                expr.collect(SlotRef.class, slots);

                ExpressionMapping expressionMapping =
                        new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()),
                                Lists.newArrayList());

                for (SlotRef slot : slots) {
                    String originName = slot.getColumnName();

                    Optional<Column> optOriginColumn = fullSchema.stream()
                            .filter(c -> c.nameEquals(originName, false)).findFirst();
                    Column originColumn = optOriginColumn.get();
                    ColumnRefOperator originColRefOp = outputColumns.get(fullSchema.indexOf(originColumn));

                    expressionMapping.put(slot, originColRefOp);
                }

                ScalarOperator scalarOperator =
                        SqlToScalarOperatorTranslator.translate(expr, expressionMapping, columnRefFactory);

                ColumnRefOperator columnRefOperator =
                        columnRefFactory.create(scalarOperator, scalarOperator.getType(), scalarOperator.isNullable());
                outputColumns.add(columnRefOperator);
                columnRefMap.put(columnRefOperator, scalarOperator);
            } else if (baseSchema.contains(fullSchema.get(columnIdx))) {
                ColumnRefOperator columnRefOperator = outputColumns.get(columnIdx);
                columnRefMap.put(columnRefOperator, columnRefOperator);
            }
        }

        return root.withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }

    private OptExprBuilder fillShadowColumns(ColumnRefFactory columnRefFactory, InsertStmt insertStatement,
                                             List<ColumnRefOperator> outputColumns, OptExprBuilder root,
                                             ConnectContext session) {
        Set<Column> baseSchema = Sets.newHashSet(insertStatement.getTargetTable().getBaseSchema());
        List<Column> fullSchema = insertStatement.getTargetTable().getFullSchema();
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();

        for (int columnIdx = 0; columnIdx < fullSchema.size(); ++columnIdx) {
            Column targetColumn = fullSchema.get(columnIdx);

            if (targetColumn.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX) ||
                    targetColumn.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX_V1)) {
                if (targetColumn.isGeneratedColumn()) {
                    continue;
                }

                String originName = Column.removeNamePrefix(targetColumn.getName());
                Optional<Column> optOriginColumn = fullSchema.stream()
                        .filter(c -> c.nameEquals(originName, false)).findFirst();
                Preconditions.checkState(optOriginColumn.isPresent());
                Column originColumn = optOriginColumn.get();
                ColumnRefOperator originColRefOp = outputColumns.get(fullSchema.indexOf(originColumn));

                ColumnRefOperator columnRefOperator = columnRefFactory.create(
                        targetColumn.getName(), targetColumn.getType(), targetColumn.isAllowNull());

                outputColumns.add(columnRefOperator);
                columnRefMap.put(columnRefOperator, new CastOperator(targetColumn.getType(), originColRefOp, true));
                continue;
            }

            // Target column which starts with "mv" should not be treated as materialized view column when this column exists in base schema,
            // this could be created by user.
            if (targetColumn.isNameWithPrefix(MATERIALIZED_VIEW_NAME_PREFIX) &&
                    !baseSchema.contains(targetColumn)) {
                if (targetColumn.getDefineExpr() == null) {
                    Table targetTable = insertStatement.getTargetTable();
                    // Only olap table can have the synchronized materialized view.
                    OlapTable targetOlapTable = (OlapTable) targetTable;
                    MaterializedIndexMeta targetIndexMeta = null;
                    for (MaterializedIndexMeta indexMeta : targetOlapTable.getIndexIdToMeta().values()) {
                        if (indexMeta.getIndexId() == targetOlapTable.getBaseIndexId()) {
                            continue;
                        }
                        for (Column column : indexMeta.getSchema()) {
                            if (column.getName().equals(targetColumn.getName())) {
                                targetIndexMeta = indexMeta;
                                break;
                            }
                        }
                    }
                    String targetIndexMetaName = targetIndexMeta == null ? "" :
                            targetOlapTable.getIndexNameById(targetIndexMeta.getIndexId());
                    throw new SemanticException(
                            "The define expr of shadow column " + targetColumn.getName() + " is null, " +
                                    "please check the associated materialized view " + targetIndexMetaName
                                    + " of target table:" + insertStatement.getTargetTable().getName());
                }

                ExpressionAnalyzer.analyzeExpression(targetColumn.getDefineExpr(), new AnalyzeState(),
                        new Scope(RelationId.anonymous(),
                                new RelationFields(insertStatement.getTargetTable().getBaseSchema().stream()
                                        .map(col -> new Field(col.getName(), col.getType(),
                                                insertStatement.getTableName(), null))
                                        .collect(Collectors.toList()))), session);

                ExpressionMapping expressionMapping =
                        new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()),
                                Lists.newArrayList());

                List<SlotRef> slots = targetColumn.getRefColumns();
                for (SlotRef slot : slots) {
                    String originName = slot.getColumnName();
                    Optional<Column> optOriginColumn = fullSchema.stream()
                            .filter(c -> c.nameEquals(originName, false)).findFirst();
                    Preconditions.checkState(optOriginColumn.isPresent());
                    Column originColumn = optOriginColumn.get();
                    ColumnRefOperator originColRefOp = outputColumns.get(fullSchema.indexOf(originColumn));
                    expressionMapping.put(slot, originColRefOp);
                }

                ScalarOperator scalarOperator =
                        SqlToScalarOperatorTranslator.translate(targetColumn.getDefineExpr(), expressionMapping,
                                columnRefFactory);

                ColumnRefOperator columnRefOperator =
                        columnRefFactory.create(scalarOperator, scalarOperator.getType(), scalarOperator.isNullable());
                outputColumns.add(columnRefOperator);
                columnRefMap.put(columnRefOperator, scalarOperator);
                continue;
            }

            // columnIdx >= outputColumns.size() mean this is a new add schema change column
            if (columnIdx >= outputColumns.size()) {
                ColumnRefOperator columnRefOperator = columnRefFactory.create(
                        targetColumn.getName(), targetColumn.getType(), targetColumn.isAllowNull());
                outputColumns.add(columnRefOperator);

                Column.DefaultValueType defaultValueType = targetColumn.getDefaultValueType();
                if (defaultValueType == Column.DefaultValueType.NULL) {
                    columnRefMap.put(columnRefOperator, ConstantOperator.createNull(targetColumn.getType()));
                } else if (defaultValueType == Column.DefaultValueType.CONST) {
                    columnRefMap.put(columnRefOperator, ConstantOperator.createVarchar(
                            targetColumn.calculatedDefaultValue()));
                } else if (defaultValueType == Column.DefaultValueType.VARY) {
                    throw new SemanticException("Column:" + targetColumn.getName() + " has unsupported default value:"
                            + targetColumn.getDefaultExpr().getExpr());
                }
            } else {
                columnRefMap.put(outputColumns.get(columnIdx), outputColumns.get(columnIdx));
            }
        }
        return root.withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }

    private OptExprBuilder castOutputColumnsTypeToTargetColumns(ColumnRefFactory columnRefFactory,
                                                                InsertStmt insertStatement,
                                                                List<ColumnRefOperator> outputColumns,
                                                                OptExprBuilder root) {
        List<Column> fullSchema = insertStatement.getTargetTable().getFullSchema();
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        List<ScalarOperatorRewriteRule> rewriteRules = Arrays.asList(new FoldConstantsRule());
        for (int columnIdx = 0; columnIdx < fullSchema.size(); ++columnIdx) {
            if (!fullSchema.get(columnIdx).getType().matchesType(outputColumns.get(columnIdx).getType())) {
                Column c = fullSchema.get(columnIdx);
                ColumnRefOperator k = columnRefFactory.create(c.getName(), c.getType(), c.isAllowNull());
                ScalarOperator castOperator = new CastOperator(fullSchema.get(columnIdx).getType(),
                        outputColumns.get(columnIdx), true);
                columnRefMap.put(k, rewriter.rewrite(castOperator, rewriteRules));
                outputColumns.set(columnIdx, k);
            } else {
                columnRefMap.put(outputColumns.get(columnIdx), outputColumns.get(columnIdx));
            }
        }
        return root.withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }

    /**
     * OlapTableSink may be executed in multiply fragment instances of different machines
     * For non-duplicate key types, we must guarantee that the orders of the same key are
     * exactly the same. In order to achieve this goal, we can perform shuffle before TableSink
     * so that the same key will be sent to the same fragment instance
     */
    private PhysicalPropertySet createPhysicalPropertySet(InsertStmt insertStmt,
                                                          List<ColumnRefOperator> outputColumns) {
        QueryRelation queryRelation = insertStmt.getQueryStatement().getQueryRelation();
        if ((queryRelation instanceof SelectRelation && queryRelation.hasLimit())) {
            DistributionProperty distributionProperty =
                    new DistributionProperty(new GatherDistributionSpec(queryRelation.getLimit().getLimit()));
            return new PhysicalPropertySet(distributionProperty);
        }

        if (insertStmt.getTargetTable() instanceof IcebergTable) {
            IcebergTable icebergTable = (IcebergTable) insertStmt.getTargetTable();
            SortOrder sortOrder = icebergTable.getNativeTable().sortOrder();

            if (sortOrder.isUnsorted()) {
                return new PhysicalPropertySet();
            } else {
                List<SortField> sortFields = sortOrder.fields();
                List<Ordering> orderings = new ArrayList<>();
                List<Integer> sortKeyIndexes = icebergTable.getSortKeyIndexes();
                for (int index : sortKeyIndexes) {
                    ColumnRefOperator columnRef = outputColumns.get(index);
                    SortField sortField = sortFields.get(sortKeyIndexes.indexOf(index));
                    boolean isAsc = sortField.direction() == SortDirection.ASC;
                    boolean isNullFirst = sortField.nullOrder() == NullOrder.NULLS_FIRST;
                    Ordering ordering = new Ordering(columnRef, isAsc, isNullFirst);
                    orderings.add(ordering);
                }
                SortProperty sortProperty = new SortProperty(new OrderSpec(orderings));
                return new PhysicalPropertySet(sortProperty);
            }
        }

        if (!(insertStmt.getTargetTable() instanceof OlapTable)) {
            return new PhysicalPropertySet();
        }

        OlapTable table = (OlapTable) insertStmt.getTargetTable();

        if (KeysType.DUP_KEYS.equals(table.getKeysType())) {
            return new PhysicalPropertySet();
        }

        // No extra distribution property is needed if replication num is 1
        if (!enableSingleReplicationShuffle && table.getDefaultReplicationNum() <= 1) {
            return new PhysicalPropertySet();
        }

        if (table.enableReplicatedStorage()) {
            return new PhysicalPropertySet();
        }

        List<Column> columns = table.getFullSchema();
        Preconditions.checkState(columns.size() == outputColumns.size(),
                "outputColumn's size must equal with table's column size");

        List<Column> keyColumns = table.getKeyColumnsByIndexId(table.getBaseIndexId());
        List<Integer> keyColumnIds = Lists.newArrayList();
        keyColumns.forEach(column -> {
            int index = columns.indexOf(column);
            Preconditions.checkState(index >= 0);
            keyColumnIds.add(outputColumns.get(index).getId());
        });

        HashDistributionDesc desc =
                new HashDistributionDesc(keyColumnIds, HashDistributionDesc.SourceType.SHUFFLE_AGG);
        DistributionSpec spec = DistributionSpec.createHashDistributionSpec(desc);
        DistributionProperty property = new DistributionProperty(spec);

        if (Config.eliminate_shuffle_load_by_replicated_storage) {
            forceReplicatedStorage = true;
            return new PhysicalPropertySet();
        }

        shuffleServiceEnable = true;

        return new PhysicalPropertySet(property);
    }

    private OptExprBuilder fillKeyPartitionsColumn(ColumnRefFactory columnRefFactory,
                                                   InsertStmt insertStatement, List<ColumnRefOperator> outputColumns,
                                                   OptExprBuilder root) {
        Table targetTable = insertStatement.getTargetTable();
        LogicalProjectOperator projectOperator = (LogicalProjectOperator) root.getRoot().getOp();
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = projectOperator.getColumnRefMap();
        List<String> partitionColNames = insertStatement.getTargetPartitionNames().getPartitionColNames();
        List<Expr> partitionColValues = insertStatement.getTargetPartitionNames().getPartitionColValues();
        List<String> tablePartitionColumnNames = targetTable.getPartitionColumnNames();

        for (Column column : targetTable.getFullSchema()) {
            String columnName = column.getName();
            if (tablePartitionColumnNames.contains(columnName)) {
                int index = partitionColNames.indexOf(columnName);
                LiteralExpr expr = (LiteralExpr) partitionColValues.get(index);
                ScalarOperator scalarOperator =
                        ConstantOperator.createObject(expr.getRealObjectValue(), column.getType());
                ColumnRefOperator col = columnRefFactory
                        .create(scalarOperator, scalarOperator.getType(), scalarOperator.isNullable());
                outputColumns.add(col);
                columnRefMap.put(col, scalarOperator);
            }
        }
        return root.withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }

    private boolean needToSkip(InsertStmt stmt, int columnIdx) {
        Table targetTable = stmt.getTargetTable();
        return stmt.isSpecifyKeyPartition() &&
                ((IcebergTable) targetTable).partitionColumnIndexes().contains(columnIdx);
    }

    private boolean isKeyPartitionStaticInsert(InsertStmt insertStmt, QueryRelation queryRelation) {
        if (!(queryRelation instanceof SelectRelation)) {
            return false;
        }

        if (!(insertStmt.getTargetTable() instanceof IcebergTable)) {
            return false;
        }

        IcebergTable icebergTable = (IcebergTable) insertStmt.getTargetTable();
        if (icebergTable.isUnPartitioned()) {
            return false;
        }

        if (insertStmt.isSpecifyKeyPartition()) {
            return true;
        }

        SelectRelation selectRelation = (SelectRelation) queryRelation;
        List<SelectListItem> listItems = selectRelation.getSelectList().getItems();

        for (SelectListItem item : listItems) {
            if (item.isStar()) {
                return false;
            }
        }

        List<String> targetColumnNames;
        if (insertStmt.getTargetColumnNames() == null) {
            targetColumnNames = icebergTable.getColumns().stream()
                    .map(Column::getName).collect(Collectors.toList());
        } else {
            targetColumnNames = Lists.newArrayList(insertStmt.getTargetColumnNames());
        }

        for (int i = 0; i < targetColumnNames.size(); i++) {
            String columnName = targetColumnNames.get(i);
            if (icebergTable.getPartitionColumnNames().contains(columnName)) {
                Expr expr = listItems.get(i).getExpr();
                if (expr instanceof NullLiteral) {
                    throw new SemanticException("partition value can't be null");
                }
                if (!expr.isConstant()) {
                    return false;
                }
            }
        }
        return true;
    }
}
