// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.DefaultValueExpr;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
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
import com.starrocks.sql.analyzer.relation.InsertRelation;
import com.starrocks.sql.analyzer.relation.Relation;
import com.starrocks.sql.analyzer.relation.SelectRelation;
import com.starrocks.sql.analyzer.relation.ValuesRelation;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
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
import java.util.stream.Collectors;

public class InsertPlanner {
    public ExecPlan plan(Relation relation, ConnectContext session) {
        InsertRelation insertRelation = (InsertRelation) relation;
        List<ColumnRefOperator> outputColumns = new ArrayList<>();

        //1. Process the literal value of the insert values type and cast it into the type of the target table
        if (insertRelation.getQueryRelation() instanceof ValuesRelation) {
            castLiteralToTargetColumnsType(insertRelation);
        }

        //2. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan =
                new RelationTransformer(columnRefFactory, session).transform(insertRelation.getQueryRelation());

        //3. Fill in the default value and NULL
        OptExprBuilder optExprBuilder = fillDefaultValue(logicalPlan, columnRefFactory, insertRelation, outputColumns);

        //4. Fill in the shadow column
        optExprBuilder = fillShadowColumns(columnRefFactory, insertRelation, outputColumns, optExprBuilder, session);

        //5. Cast output columns type to target type
        optExprBuilder =
                castOutputColumnsTypeToTargetColumns(columnRefFactory, insertRelation, outputColumns, optExprBuilder);

        //6. Optimize logical plan and build physical plan
        logicalPlan = new LogicalPlan(optExprBuilder, outputColumns, logicalPlan.getCorrelation());

        Optimizer optimizer = new Optimizer();
        OptExpression optimizedPlan = optimizer.optimize(
                session,
                logicalPlan.getRoot(),
                new PhysicalPropertySet(),
                new ColumnRefSet(logicalPlan.getOutputColumn()),
                columnRefFactory);

        //7. Build fragment exec plan
        ExecPlan execPlan;
        if ((insertRelation.getQueryRelation() instanceof SelectRelation &&
                ((SelectRelation) insertRelation.getQueryRelation()).hasLimit())
                || insertRelation.getTargetTable() instanceof MysqlTable) {
            execPlan = new PlanFragmentBuilder().createPhysicalPlan(
                    optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory,
                    insertRelation.getQueryRelation().getColumnOutputNames());
        } else {
            execPlan = new PlanFragmentBuilder().createPhysicalPlanWithoutOutputFragment(
                    optimizedPlan, session, logicalPlan.getOutputColumn(), columnRefFactory,
                    insertRelation.getQueryRelation().getColumnOutputNames());
        }

        DescriptorTable descriptorTable = execPlan.getDescTbl();
        TupleDescriptor olapTuple = descriptorTable.createTupleDescriptor();

        List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();
        long tableId = insertRelation.getTargetTable().getId();
        for (Column column : insertRelation.getTargetTable().getFullSchema()) {
            SlotDescriptor slotDescriptor = descriptorTable.addSlotDescriptor(olapTuple);
            slotDescriptor.setIsMaterialized(true);
            slotDescriptor.setType(column.getType());
            slotDescriptor.setColumn(column);
            slotDescriptor.setIsNullable(column.isAllowNull());
            if (column.getType().isVarchar() && IDictManager.getInstance().hasGlobalDict(tableId, column.getName())) {
                ColumnDict dict = IDictManager.getInstance().getGlobalDict(tableId, column.getName());
                globalDicts.add(new Pair<>(slotDescriptor.getId().asInt(), dict));
            }
        }
        olapTuple.computeMemLayout();

        DataSink dataSink;
        if (insertRelation.getTargetTable() instanceof OlapTable) {
            dataSink = new OlapTableSink((OlapTable) insertRelation.getTargetTable(), olapTuple,
                    insertRelation.getTargetPartitionIds());
        } else if (insertRelation.getTargetTable() instanceof MysqlTable) {
            dataSink = new MysqlTableSink((MysqlTable) insertRelation.getTargetTable());
        } else {
            throw new SemanticException("Unknown table type " + insertRelation.getTargetTable().getType());
        }
        execPlan.getFragments().get(0).setSink(dataSink);
        execPlan.getFragments().get(0).setLoadGlobalDicts(globalDicts);
        return execPlan;
    }

    void castLiteralToTargetColumnsType(InsertRelation insertRelation) {
        Preconditions.checkState(insertRelation.getQueryRelation() instanceof ValuesRelation, "must values");
        List<Column> fullSchema = insertRelation.getTargetTable().getFullSchema();
        ValuesRelation values = (ValuesRelation) insertRelation.getQueryRelation();
        RelationFields fields = insertRelation.getQueryRelation().getRelationFields();
        for (int columnIdx = 0; columnIdx < insertRelation.getTargetTable().getBaseSchema().size(); ++columnIdx) {
            Column targetColumn = fullSchema.get(columnIdx);
            if (insertRelation.getTargetColumnNames() == null) {
                for (List<Expr> row : values.getRows()) {
                    if (row.get(columnIdx) instanceof DefaultValueExpr) {
                        row.set(columnIdx, new StringLiteral(targetColumn.calculatedDefaultValue()));
                    }
                    row.set(columnIdx, TypeManager.addCastExpr(row.get(columnIdx), targetColumn.getType()));
                }
                fields.getFieldByIndex(columnIdx).setType(targetColumn.getType());
            } else {
                int idx = insertRelation.getTargetColumnNames().indexOf(targetColumn.getName().toLowerCase());
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

    OptExprBuilder fillDefaultValue(LogicalPlan logicalPlan, ColumnRefFactory columnRefFactory,
                                    InsertRelation insertRelation, List<ColumnRefOperator> outputColumns) {
        List<Column> baseSchema = insertRelation.getTargetTable().getBaseSchema();
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();

        for (int columnIdx = 0; columnIdx < baseSchema.size(); ++columnIdx) {
            Column targetColumn = baseSchema.get(columnIdx);
            if (insertRelation.getTargetColumnNames() == null) {
                outputColumns.add(logicalPlan.getOutputColumn().get(columnIdx));
                columnRefMap.put(logicalPlan.getOutputColumn().get(columnIdx),
                        logicalPlan.getOutputColumn().get(columnIdx));
            } else {
                int idx = insertRelation.getTargetColumnNames().indexOf(targetColumn.getName().toLowerCase());
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

    OptExprBuilder fillShadowColumns(ColumnRefFactory columnRefFactory, InsertRelation insertRelation,
                                     List<ColumnRefOperator> outputColumns, OptExprBuilder root,
                                     ConnectContext session) {
        List<Column> fullSchema = insertRelation.getTargetTable().getFullSchema();
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
                                new RelationFields(insertRelation.getTargetTable().getBaseSchema().stream()
                                        .map(col -> new Field(col.getName(), col.getType(),
                                                new TableName(null, insertRelation.getTargetTable().getName()), null))
                                        .collect(Collectors.toList()))),
                        session.getCatalog(), session);

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

    OptExprBuilder castOutputColumnsTypeToTargetColumns(ColumnRefFactory columnRefFactory,
                                                        InsertRelation insertRelation,
                                                        List<ColumnRefOperator> outputColumns, OptExprBuilder root) {
        List<Column> fullSchema = insertRelation.getTargetTable().getFullSchema();
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
}
