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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import jersey.repackaged.com.google.common.collect.Lists;

import java.util.Map;
import java.util.Optional;

public class EquationRewriter {

    Multimap<ScalarOperator, Pair<ColumnRefOperator, ScalarOperator>> equationMap;
    Map<ColumnRefOperator, ColumnRefOperator> columnMapping;

    public EquationRewriter() {
        this.equationMap = ArrayListMultimap.create();
    }

    public void setOutputMapping(Map<ColumnRefOperator, ColumnRefOperator> columnMapping) {
        this.columnMapping = columnMapping;
    }

    protected ScalarOperator replaceExprWithTarget(ScalarOperator expr) {

        BaseScalarOperatorShuttle shuttle = new BaseScalarOperatorShuttle() {
            @Override
            public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
                return null;
            }

            @Override
            public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
                ScalarOperator tmp = replace(predicate);
                return tmp != null ? tmp : super.visitBinaryPredicate(predicate, context);
            }

            @Override
            public ScalarOperator visitCall(CallOperator predicate, Void context) {
                ScalarOperator tmp = replace(predicate);
                return tmp != null ? tmp : super.visitCall(predicate, context);
            }

            @Override
            public ScalarOperator visitCastOperator(CastOperator cast, Void context) {
                ScalarOperator tmp = replace(cast);
                return tmp != null ? tmp : super.visitCastOperator(cast, context);
            }

            @Override
            public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
                ScalarOperator tmp = replace(variable);
                return tmp != null ? tmp : super.visitVariableReference(variable, context);
            }

            ScalarOperator replace(ScalarOperator scalarOperator) {

                if (equationMap.containsKey(scalarOperator)) {

                    Optional<Pair<ColumnRefOperator, ScalarOperator>> mappedColumnAndExprRef =
                            equationMap.get(scalarOperator).stream().findFirst();

                    if (!mappedColumnAndExprRef.isPresent()) {
                        return null;
                    }
                    ColumnRefOperator basedColumn = mappedColumnAndExprRef.get().first;
                    ScalarOperator extendedExpr = mappedColumnAndExprRef.get().second;

                    if (columnMapping == null) {
                        return extendedExpr == null ? basedColumn.clone() : extendedExpr.clone();
                    }

                    ColumnRefOperator replaced = columnMapping.get(basedColumn);
                    if (replaced == null) {
                        return null;
                    }

                    if (extendedExpr == null) {
                        return replaced.clone();
                    }
                    ScalarOperator newExpr = extendedExpr.clone();
                    return replaceColInExpr(newExpr, basedColumn,
                            replaced.clone()) ? newExpr : null;

                }

                return null;
            }

            private boolean replaceColInExpr(ScalarOperator expr, ColumnRefOperator oldCol, ScalarOperator newCol) {
                for (int i = 0; i < expr.getChildren().size(); i++) {
                    if (oldCol.equals(expr.getChild(i))) {
                        expr.setChild(i, newCol);
                        return true;
                    }
                }
                return false;
            }

        };
        return expr.accept(shuttle, null);
    }

    public boolean containsKey(ScalarOperator scalarOperator) {
        return equationMap.containsKey(scalarOperator);
    }

    public void addMapping(ScalarOperator expr, ColumnRefOperator col) {
        equationMap.put(expr, Pair.create(col, null));
        Pair<ScalarOperator, ScalarOperator> extendedEntry = new EquationTransformer(expr, col).getMapping();
        if (extendedEntry.second != col) {
            equationMap.put(extendedEntry.first, Pair.create(col, extendedEntry.second));
        }
    }

    private static class EquationTransformer extends ScalarOperatorVisitor<Void, Void> {

        private static final Map<String, String> COMMUTATIVE_MAP = ImmutableMap.<String, String>builder()
                .put(FunctionSet.ADD, FunctionSet.SUBTRACT)
                .put(FunctionSet.SUBTRACT, FunctionSet.ADD)
                .build();

        private ScalarOperator equalExpr;
        private ScalarOperator needReducedExpr;

        public EquationTransformer(ScalarOperator needReducedExpr, ColumnRefOperator equalExpr) {
            super();
            this.equalExpr = equalExpr;
            this.needReducedExpr = needReducedExpr;
        }

        public Pair<ScalarOperator, ScalarOperator> getMapping() {
            needReducedExpr.accept(this, null);
            return Pair.create(needReducedExpr, equalExpr);
        }

        @Override
        public Void visit(ScalarOperator scalarOperator, Void context) {
            return null;
        }

        @Override
        public Void visitCall(CallOperator call, Void context) {
            if (!COMMUTATIVE_MAP.containsKey(call.getFnName())) {
                return null;
            }

            if (!call.getChild(1).isConstant() && !call.getChild(0).isConstant()) {
                return null;
            }
            ScalarOperator constantOperator = call.getChild(0).isConstant() ? call.getChild(0) : call.getChild(1);
            ScalarOperator varOperator = call.getChild(0).isConstant() ? call.getChild(1) : call.getChild(0);

            String fnName = COMMUTATIVE_MAP.get(call.getFnName());
            equalExpr = new CallOperator(fnName, call.getType(),
                    Lists.newArrayList(equalExpr, constantOperator), findArithmeticFunction(call, fnName));
            needReducedExpr = varOperator;

            varOperator.accept(this, context);

            return null;
        }

        @Override
        public Void visitCastOperator(CastOperator operator, Void context) {

            if (Type.isImplicitlyCastable(operator.getType(), equalExpr.getType(), true)) {
                needReducedExpr = needReducedExpr.getChild(0);
                needReducedExpr.accept(this, context);
            }
            return null;
        }

        private Function findArithmeticFunction(CallOperator call, String fnName) {
            return Expr.getBuiltinFunction(fnName, call.getFunction().getArgs(), Function.CompareMode.IS_IDENTICAL);
        }

    }

}
