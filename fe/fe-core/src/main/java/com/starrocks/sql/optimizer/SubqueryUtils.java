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


package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.structure.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.ReplaceSubqueryRewriteRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ScalarOperatorRewriteRule;
import com.starrocks.sql.optimizer.transformer.CTETransformerContext;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SubqueryUtils {

    public static final String EXIST_NON_EQ_PREDICATE =
            "Not support Non-EQ correlated predicate in correlated subquery";

    public static final String NOT_FOUND_CORRELATED_PREDICATE =
            "Not support without correlated predicate in correlated subquery";

    public static final String CONST_QUANTIFIED_COMPARISON = "Not support const value quantified comparison with " +
            "a correlated subquery";

    public static Pair<ScalarOperator, OptExprBuilder> rewriteScalarOperator(
            ScalarOperator scalarOperator,
            OptExprBuilder builder,
            Map<ScalarOperator, SubqueryOperator> subqueryPlaceholders) {
        if (scalarOperator == null) {
            return Pair.create(null, builder);
        }

        List<ScalarOperatorRewriteRule> rules = Lists.newArrayList();
        ReplaceSubqueryRewriteRule subqueryRewriteRule = new ReplaceSubqueryRewriteRule(subqueryPlaceholders, builder);
        rules.add(subqueryRewriteRule);
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        scalarOperator = rewriter.rewrite(scalarOperator, rules);
        builder = subqueryRewriteRule.getBuilder();

        // remove semi-quantified or semi-existential subquery
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(scalarOperator);
        Iterator<ScalarOperator> it = conjuncts.iterator();
        while (it.hasNext()) {
            ScalarOperator conjunct = it.next();
            if (subqueryPlaceholders.containsKey(conjunct)) {
                SubqueryOperator subqueryOperator = subqueryPlaceholders.get(conjunct);
                LogicalApplyOperator applyOperator = subqueryOperator.getApplyOperator();
                if ((applyOperator.isQuantified() || applyOperator.isExistential()) && applyOperator.isUseSemiAnti()) {
                    it.remove();
                }
            }
        }

        return Pair.create(Utils.compoundAnd(conjuncts), builder);
    }

    public static LogicalPlan getLogicalPlan(ConnectContext session, CTETransformerContext cteContext,
                                             ColumnRefFactory columnRefFactory, QueryRelation relation,
                                             ExpressionMapping outer) {
        if (!(relation instanceof SelectRelation) && !(relation instanceof SubqueryRelation)) {
            throw new SemanticException("Currently only subquery of the Select type are supported");
        }

        // For in subQuery, the order by is meaningless
        if (!relation.hasLimit()) {
            relation.getOrderBy().clear();
        }

        return new RelationTransformer(columnRefFactory, session, outer, cteContext).transform(relation);
    }

    private static Function getAggregateFunction(String functionName, Type[] argTypes) {
        Function func = Expr.getBuiltinFunction(functionName, argTypes,
                Function.CompareMode.IS_IDENTICAL);
        if (argTypes.length > 0 && argTypes[0].isDecimalV3()) {
            func = DecimalV3FunctionAnalyzer.rectifyAggregationFunction((AggregateFunction) func,
                    argTypes[0],
                    argTypes[0]);
        }
        return func;
    }

    /**
     * ApplyNode doesn't need to check the number of subquery's return rows
     * when the correlation predicate meets these requirements:
     * 1. All predicate is Binary.EQ
     */
    public static boolean checkAllIsBinaryEQ(List<ScalarOperator> correlationPredicate) {
        for (ScalarOperator predicate : correlationPredicate) {
            if (!OperatorType.BINARY.equals(predicate.getOpType())) {
                return false;
            }

            BinaryPredicateOperator bpo = ((BinaryPredicateOperator) predicate);
            if (!BinaryType.EQ.equals(bpo.getBinaryType())) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkUniqueCorrelation(ScalarOperator correlationPredicate, ColumnRefSet outerRefs) {
        if (correlationPredicate == null) {
            return true;
        }
        
        if (!OperatorType.BINARY.equals(correlationPredicate.getOpType())) {
            return false;
        }

        BinaryPredicateOperator bpo = ((BinaryPredicateOperator) correlationPredicate);
        if (!BinaryType.EQ.equals(bpo.getBinaryType())) {
            return false;
        }

        if (outerRefs.containsAny(bpo.getChild(0).getUsedColumns()) &&
                outerRefs.containsAny(bpo.getChild(1).getUsedColumns())) {
            return false;
        }
        return true;
    }

    public static CallOperator createCountRowsOperator() {
        Function count = getAggregateFunction(FunctionSet.COUNT, new Type[] {Type.BIGINT});
        return new CallOperator(FunctionSet.COUNT, Type.BIGINT, Lists.newArrayList(ConstantOperator.createBigint(1)),
                count, false);
    }

    public static CallOperator createCountRowsOperator(ScalarOperator column) {
        Function count = getAggregateFunction(FunctionSet.COUNT, new Type[] {Type.BIGINT});
        return new CallOperator(FunctionSet.COUNT, Type.BIGINT, Lists.newArrayList(column), count, false);
    }

    public static CallOperator createAnyValueOperator(ScalarOperator column) {
        Function anyValueFn = getAggregateFunction(FunctionSet.ANY_VALUE, new Type[] {column.getType()});
        return new CallOperator(FunctionSet.ANY_VALUE, column.getType(), Lists.newArrayList(column), anyValueFn);
    }

    public static boolean isUnCorrelationScalarSubquery(LogicalApplyOperator apply) {
        if (!apply.isScalar()) {
            return false;
        }

        if (!apply.getCorrelationColumnRefs().isEmpty()) {
            return false;
        }

        // only un-correlation scalar subquery
        return apply.getUnCorrelationSubqueryPredicateColumns() != null &&
                !apply.getUnCorrelationSubqueryPredicateColumns().isEmpty();
    }

    // check the ApplyNode's children contains correlation subquery
    public static boolean containsCorrelationSubquery(OptExpression expression) {
        if (expression.getOp().isLogical() && OperatorType.LOGICAL_APPLY.equals(expression.getOp().getOpType())) {
            LogicalApplyOperator apply = (LogicalApplyOperator) expression.getOp();

            if (apply.getCorrelationColumnRefs().isEmpty()) {
                return false;
            }

            // only check right child
            return checkPredicateContainColumnRef(apply.getCorrelationColumnRefs(), expression.getInputs().get(1));
        }
        return false;
    }

    // GroupExpression
    private static boolean checkPredicateContainColumnRef(List<ColumnRefOperator> cro, OptExpression expression) {
        LogicalOperator logicalOperator = (LogicalOperator) expression.getOp();

        if (Utils.containAnyColumnRefs(cro, logicalOperator.getPredicate())) {
            return true;
        }

        for (OptExpression child : expression.getInputs()) {
            if (checkPredicateContainColumnRef(cro, child)) {
                return true;
            }
        }

        return false;
    }

    public static boolean existNonColumnRef(Collection<ScalarOperator> scalarOperators) {
        return scalarOperators.stream().anyMatch(e -> !e.isColumnRef());
    }

    public static Map<ColumnRefOperator, ScalarOperator> generateChildOutColumns(
            OptExpression input, Map<ColumnRefOperator, ScalarOperator> columns, OptimizerContext context) {
        Map<ColumnRefOperator, ScalarOperator> outPutColumns = Maps.newHashMap();
        context.getColumnRefFactory().getColumnRefs(input.getOutputColumns()).stream()
                .forEach(c -> outPutColumns.put(c, c));
        outPutColumns.putAll(columns);
        return outPutColumns;
    }
}
