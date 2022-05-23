// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.DefaultValueExpr;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.MysqlTableSink;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class InsertPlanner {
    // Only for unit test
    public static boolean enableSingleReplicationShuffle = false;

    public ExecPlan plan(InsertStmt insertStmt, ConnectContext session) {
        QueryRelation queryRelation = insertStmt.getQueryStatement().getQueryRelation();
        List<ColumnRefOperator> outputColumns = new ArrayList<>();

        //1. Process the literal value of the insert values type and cast it into the type of the target table
        if (queryRelation instanceof ValuesRelation) {
            castLiteralToTargetColumnsType(insertStmt);
        }

        //2. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan =
                new RelationTransformer(columnRefFactory, session).transform(
                        insertStmt.getQueryStatement().getQueryRelation());

        //3. Fill in the default value and NULL
        OptExprBuilder optExprBuilder = fillDefaultValue(logicalPlan, columnRefFactory, insertStmt, outputColumns);

        //4. Fill in the shadow column
        optExprBuilder = fillShadowColumns(columnRefFactory, insertStmt, outputColumns, optExprBuilder, session);

        //5. Cast output columns type to target type
        optExprBuilder =
                castOutputColumnsTypeToTargetColumns(columnRefFactory, insertStmt, outputColumns, optExprBuilder);

        //6. Optimize logical plan and build physical plan
        logicalPlan = new LogicalPlan(optExprBuilder, outputColumns, logicalPlan.getCorrelation());

        Optimizer optimizer = new Optimizer();
        PhysicalPropertySet requiredPropertySet = createPhysicalPropertySet(insertStmt, outputColumns);
        OptExpression optimizedPlan = optimizer.optimize(
                session,
                logicalPlan.getRoot(),
                requiredPropertySet,
                new ColumnRefSet(logicalPlan.getOutputColumn()),
                columnRefFactory);

        //7. Build fragment exec plan
        ExecPlan execPlan;
        if ((queryRelation instanceof SelectRelation &&
                queryRelation.hasLimit())
                || insertStmt.getTargetTable() instanceof MysqlTable) {
            execPlan = new PlanFragmentBuilder().createPhysicalPlan(
                    optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory,
                    queryRelation.getColumnOutputNames());
        } else {
            execPlan = new PlanFragmentBuilder().createPhysicalPlanWithoutOutputFragment(
                    optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory,
                    queryRelation.getColumnOutputNames());
        }

        DescriptorTable descriptorTable = execPlan.getDescTbl();
        TupleDescriptor olapTuple = descriptorTable.createTupleDescriptor();

        List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
        long tableId = insertStmt.getTargetTable().getId();
        for (Column column : insertStmt.getTargetTable().getFullSchema()) {
            SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
            slotDescriptor.setIsMaterialized(true);
            slotDescriptor.setType(column.getType());
            slotDescriptor.setColumn(column);
            slotDescriptor.setIsNullable(column.isAllowNull());
            if (column.getType().isVarchar() && IDictManager.getInstance().hasGlobalDict(tableId, column.getName())) {
                Optional<ColumnDict> dict = IDictManager.getInstance().getGlobalDict(tableId, column.getName());
                if (dict != null && dict.isPresent()) {
                    globalDicts.add(new Pair<>(slotDescriptor.getId().asInt(), dict.get()));
                }
            }
        }
        olapTuple.computeMemLayout();

        DataSink dataSink;
        if (insertStmt.getTargetTable() instanceof OlapTable) {
            dataSink = new OlapTableSink((OlapTable) insertStmt.getTargetTable(), olapTuple,
                    insertStmt.getTargetPartitionIds());
        } else if (insertStmt.getTargetTable() instanceof MysqlTable) {
            dataSink = new MysqlTableSink((MysqlTable) insertStmt.getTargetTable());
        } else {
            throw new SemanticException("Unknown table type " + insertStmt.getTargetTable().getType());
        }
        execPlan.getFragments().get(0).setSink(dataSink);
        execPlan.getFragments().get(0).setLoadGlobalDicts(globalDicts);
        return execPlan;
    }

    private void castLiteralToTargetColumnsType(InsertStmt insertStatement) {
        Preconditions.checkState(insertStatement.getQueryStatement().getQueryRelation() instanceof ValuesRelation,
                "must values");
        List<Column> fullSchema = insertStatement.getTargetTable().getFullSchema();
        ValuesRelation values = (ValuesRelation) insertStatement.getQueryStatement().getQueryRelation();
        RelationFields fields = insertStatement.getQueryStatement().getQueryRelation().getRelationFields();
        for (int columnIdx = 0; columnIdx < insertStatement.getTargetTable().getBaseSchema().size(); ++columnIdx) {
            Column targetColumn = fullSchema.get(columnIdx);
            if (insertStatement.getTargetColumnNames() == null) {
                for (List<Expr> row : values.getRows()) {
                    if (row.get(columnIdx) instanceof DefaultValueExpr) {
                        row.set(columnIdx, new StringLiteral(targetColumn.calculatedDefaultValue()));
                    }
                    row.set(columnIdx, TypeManager.addCastExpr(row.get(columnIdx), targetColumn.getType()));
                }
                fields.getFieldByIndex(columnIdx).setType(targetColumn.getType());
            } else {
                int idx = insertStatement.getTargetColumnNames().indexOf(targetColumn.getName().toLowerCase());
                if (idx != -1) {
                    for (List<Expr> row : values.getRows()) {
                        if (row.get(idx) instanceof DefaultValueExpr) {
                            row.set(idx, new StringLiteral(targetColumn.calculatedDefaultValue()));
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
            Column targetColumn = baseSchema.get(columnIdx);
            if (insertStatement.getTargetColumnNames() == null) {
                outputColumns.add(logicalPlan.getOutputColumn().get(columnIdx));
                columnRefMap.put(logicalPlan.getOutputColumn().get(columnIdx),
                        logicalPlan.getOutputColumn().get(columnIdx));
            } else {
                int idx = insertStatement.getTargetColumnNames().indexOf(targetColumn.getName().toLowerCase());
                if (idx == -1) {
                    ScalarOperator scalarOperator;
                    Column.DefaultValueType defaultValueType = targetColumn.getDefaultValueType();
                    if (defaultValueType == Column.DefaultValueType.NULL) {
                        scalarOperator = ConstantOperator.createNull(targetColumn.getType());
                    } else if (defaultValueType == Column.DefaultValueType.CONST) {
                        scalarOperator = ConstantOperator.createVarchar(targetColumn.calculatedDefaultValue());
                    } else if (defaultValueType == Column.DefaultValueType.VARY) {
                        throw new SemanticException(
                                "Column:" + targetColumn.getName() + " has unsupported default value:"
                                        + targetColumn.getDefaultExpr().getExpr());

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

    private OptExprBuilder fillShadowColumns(ColumnRefFactory columnRefFactory, InsertStmt insertStatement,
                                             List<ColumnRefOperator> outputColumns, OptExprBuilder root,
                                             ConnectContext session) {
        List<Column> fullSchema = insertStatement.getTargetTable().getFullSchema();
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();

        for (int columnIdx = 0; columnIdx < fullSchema.size(); ++columnIdx) {
            Column targetColumn = fullSchema.get(columnIdx);

            if (targetColumn.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX) ||
                    targetColumn.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX_V1)) {
                String originName = Column.removeNamePrefix(targetColumn.getName());
                Column originColumn = fullSchema.stream()
                        .filter(c -> c.nameEquals(originName, false)).findFirst().get();
                ColumnRefOperator originColRefOp = outputColumns.get(fullSchema.indexOf(originColumn));

                ColumnRefOperator columnRefOperator = columnRefFactory.create(
                        targetColumn.getName(), targetColumn.getType(), targetColumn.isAllowNull());

                outputColumns.add(columnRefOperator);
                columnRefMap.put(columnRefOperator, new CastOperator(targetColumn.getType(), originColRefOp, true));
                continue;
            }

            if (targetColumn.isNameWithPrefix(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX)) {
                String originName = targetColumn.getRefColumn().getColumnName();
                Column originColumn = fullSchema.stream()
                        .filter(c -> c.nameEquals(originName, false)).findFirst().get();
                ColumnRefOperator originColRefOp = outputColumns.get(fullSchema.indexOf(originColumn));

                ExpressionAnalyzer.analyzeExpression(targetColumn.getDefineExpr(), new AnalyzeState(),
                        new Scope(RelationId.anonymous(),
                                new RelationFields(insertStatement.getTargetTable().getBaseSchema().stream()
                                        .map(col -> new Field(col.getName(), col.getType(),
                                                new TableName(null, insertStatement.getTargetTable().getName()), null))
                                        .collect(Collectors.toList()))), session);

                ExpressionMapping expressionMapping =
                        new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()),
                                Lists.newArrayList());
                expressionMapping.put(targetColumn.getRefColumn(), originColRefOp);
                ScalarOperator scalarOperator =
                        SqlToScalarOperatorTranslator.translate(targetColumn.getDefineExpr(), expressionMapping);

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

        for (int columnIdx = 0; columnIdx < fullSchema.size(); ++columnIdx) {
            if (!fullSchema.get(columnIdx).getType().matchesType(outputColumns.get(columnIdx).getType())) {
                Column c = fullSchema.get(columnIdx);
                ColumnRefOperator k = columnRefFactory.create(c.getName(), c.getType(), c.isAllowNull());
                columnRefMap.put(k,
                        new CastOperator(fullSchema.get(columnIdx).getType(), outputColumns.get(columnIdx), true));
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
        return new PhysicalPropertySet(property);
    }
}
