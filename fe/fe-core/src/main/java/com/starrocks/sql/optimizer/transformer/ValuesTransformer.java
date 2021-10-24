// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.transformer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.Subquery;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.relation.ValuesRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ValuesTransformer {
    private final ColumnRefFactory columnRefFactory;

    private final BitSet subqueriesIndex = new BitSet();
    private final List<ColumnRefOperator> outputColumns = Lists.newArrayList();

    ValuesTransformer(ColumnRefFactory columnRefFactory) {
        this.columnRefFactory = columnRefFactory;
    }

    public LogicalPlan plan(ValuesRelation node) {
        OptExprBuilder builder = planValues(node);
        builder = subquery(builder, node);
        return new LogicalPlan(builder, outputColumns, Collections.emptyList());
    }

    public OptExprBuilder planValues(ValuesRelation node) {
        List<ColumnRefOperator> valuesOutputColumns = Lists.newArrayList();

        // flag subquery
        for (int i = 0; i < node.getOutputExpr().size(); i++) {
            Expr expr = node.getOutputExpr().get(i);

            List<Subquery> tmp = Lists.newArrayList();
            expr.collect(Subquery.class, tmp);

            subqueriesIndex.set(i, !tmp.isEmpty());
            ColumnRefOperator output = columnRefFactory
                    .create(expr, node.getRelationFields().getFieldByIndex(i).getType(), expr.isNullable());
            if (!output.isNullable()) {
                // The output column should be nullable, if the expression of any row is nullable.
                for (List<Expr> row : node.getRows()) {
                    if (row.get(i).isNullable()) {
                        output.setNullable(true);
                        break;
                    }
                }
            }
            outputColumns.add(output);
            if (tmp.isEmpty()) {
                valuesOutputColumns.add(output);
            }
        }

        // check subquery
        if (subqueriesIndex.cardinality() > 0 && node.getRows().size() != 1) {
            throw new SemanticException("Not support multi-values with subquery");
        }

        if (valuesOutputColumns.isEmpty() && subqueriesIndex.cardinality() > 0) {
            valuesOutputColumns.add(columnRefFactory.create("1", Type.INT, false));
        }

        List<List<ScalarOperator>> values = new ArrayList<>();
        for (List<Expr> row : node.getRows()) {
            List<ScalarOperator> valuesRow = new ArrayList<>();
            for (int fieldIdx = 0; fieldIdx < row.size(); ++fieldIdx) {
                if (subqueriesIndex.get(fieldIdx)) {
                    continue;
                }
                Expr field = row.get(fieldIdx);
                if (!field.getType().matchesType(node.getRelationFields().getFieldByIndex(fieldIdx).getType())) {
                    try {
                        field = field.castTo(node.getRelationFields().getFieldByIndex(fieldIdx).getType());
                    } catch (AnalysisException e) {
                        throw new StarRocksPlannerException(e.getMessage(), ErrorType.INTERNAL_ERROR);
                    }
                }

                ScalarOperator constant = SqlToScalarOperatorTranslator.translate(field,
                        new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields())));
                constant.setType(field.getType());
                valuesRow.add(constant);
            }

            if (valuesRow.isEmpty() && subqueriesIndex.cardinality() > 0) {
                valuesRow.add(ConstantOperator.createInt(1));
            }
            values.add(valuesRow);
        }

        return new OptExprBuilder(new LogicalValuesOperator(valuesOutputColumns, values), Collections.emptyList(),
                new ExpressionMapping(new Scope(RelationId.of(node), node.getRelationFields()), valuesOutputColumns));
    }

    public OptExprBuilder subquery(OptExprBuilder subOpt, ValuesRelation node) {
        if (subqueriesIndex.cardinality() == 0) {
            return subOpt;
        }

        ExpressionMapping outputTranslations = new ExpressionMapping(subOpt.getScope(), subOpt.getFieldMappings());
        Map<ColumnRefOperator, ScalarOperator> projections = Maps.newHashMap();
        SubqueryTransformer subqueryTransformer = new SubqueryTransformer();

        for (int i = 0; i < node.getOutputExpr().size(); i++) {
            if (!subqueriesIndex.get(i)) {
                projections.put(outputColumns.get(i), outputColumns.get(i));
                continue;
            }

            Expr output = node.getOutputExpr().get(i);
            subOpt = subqueryTransformer.handleScalarSubqueries(columnRefFactory, subOpt, output);
            ColumnRefOperator columnRef = SqlToScalarOperatorTranslator
                    .findOrCreateColumnRefForExpr(node.getOutputExpr().get(i), subOpt.getExpressionMapping(),
                            projections, columnRefFactory);
            outputTranslations.put(output, columnRef);
            outputColumns.set(i, columnRef);
        }

        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projections);
        return new OptExprBuilder(projectOperator, Lists.newArrayList(subOpt), outputTranslations);
    }
}
