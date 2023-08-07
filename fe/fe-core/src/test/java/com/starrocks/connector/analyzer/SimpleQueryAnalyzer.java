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


package com.starrocks.connector.analyzer;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.TypeManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

// This analyzer is used to test the compatibility of query SQL, do not need to verify the
// catalog/database/table exist.
public class SimpleQueryAnalyzer {
    public void analyze(StatementBase node) {
        new Visitor().process(node);
    }

    private static class Visitor extends AstVisitor<Void, Void> {
        public Void process(ParseNode node) {
            return node.accept(this, null);
        }


        @Override
        public Void visitQueryStatement(QueryStatement node, Void context) {
            return visitQueryRelation(node.getQueryRelation(), null);
        }

        @Override
        public Void visitQueryRelation(QueryRelation node, Void context) {
            analyzeCTE(node, context);
            return process(node);
        }

        private void analyzeCTE(QueryRelation stmt, Void context) {
            if (!stmt.hasWithClause()) {
                return;
            }

            for (CTERelation withQuery : stmt.getCteRelations()) {
                process(withQuery.getCteQueryStatement());
            }
            return;
        }

        @Override
        public Void visitSelect(SelectRelation selectRelation, Void context) {
            AnalyzeState analyzeState = new AnalyzeState();
            Relation resolvedRelation = selectRelation.getRelation();
            if (resolvedRelation instanceof TableFunctionRelation) {
                throw unsupportedException("Table function must be used with lateral join");
            }
            selectRelation.setRelation(resolvedRelation);
            process(resolvedRelation);

            SimpleSelectAnalyzer selectAnalyzer = new SimpleSelectAnalyzer();
            selectAnalyzer.analyze(
                    analyzeState,
                    selectRelation.getSelectList(),
                    selectRelation.getRelation(),
                    selectRelation.getGroupByClause(),
                    selectRelation.getHavingClause(),
                    selectRelation.getWhereClause(),
                    selectRelation.getOrderBy(),
                    selectRelation.getLimit());

            selectRelation.fillResolvedAST(analyzeState);
            return null;
        }


        @Override
        public Void visitJoin(JoinRelation join, Void context) {
            process(join.getLeft());
            if (join.getRight() instanceof TableFunctionRelation || join.isLateral()) {
                if (!(join.getRight() instanceof TableFunctionRelation)) {
                    throw new SemanticException("Only support lateral join with UDTF");
                }

                if (!join.getJoinOp().isInnerJoin() && !join.getJoinOp().isCrossJoin()) {
                    throw new SemanticException("Not support lateral join except inner or cross");
                }
            }
            process(join.getRight());

            Expr joinEqual = join.getOnPredicate();
            if (join.getUsingColNames() != null) {
                Expr resolvedUsing = analyzeJoinUsing(join.getUsingColNames(), join.getLeft(), join.getRight());
                if (joinEqual == null) {
                    joinEqual = resolvedUsing;
                } else {
                    joinEqual = new CompoundPredicate(CompoundPredicate.Operator.AND, joinEqual, resolvedUsing);
                }
                join.setOnPredicate(joinEqual);
            }

            if (joinEqual != null) {

                analyzeExpression(joinEqual, new AnalyzeState());

                AnalyzerUtils.verifyNoAggregateFunctions(joinEqual, "JOIN");
                AnalyzerUtils.verifyNoWindowFunctions(joinEqual, "JOIN");
                AnalyzerUtils.verifyNoGroupingFunctions(joinEqual, "JOIN");

                if (!joinEqual.getType().matchesType(Type.BOOLEAN) && !joinEqual.getType().matchesType(Type.NULL)) {
                    throw new SemanticException("WHERE clause must evaluate to a boolean: actual type %s",
                            joinEqual.getType());
                }
                QueryAnalyzer.checkJoinEqual(joinEqual);
            } else {
                if (join.getJoinOp().isOuterJoin() || join.getJoinOp().isSemiAntiJoin()) {
                    throw new SemanticException(join.getJoinOp() + " requires an ON or USING clause.");
                }
            }

            return null;
        }

        private Expr analyzeJoinUsing(List<String> usingColNames, Relation left, Relation right) {
            Expr joinEqual = null;
            for (String colName : usingColNames) {
                TableName leftTableName = left.getResolveTableName();
                TableName rightTableName = right.getResolveTableName();

                // create predicate "<left>.colName = <right>.colName"
                BinaryPredicate resolvedUsing = new BinaryPredicate(BinaryType.EQ,
                        new SlotRef(leftTableName, colName), new SlotRef(rightTableName, colName));

                if (joinEqual == null) {
                    joinEqual = resolvedUsing;
                } else {
                    joinEqual = new CompoundPredicate(CompoundPredicate.Operator.AND, joinEqual, resolvedUsing);
                }
            }
            return joinEqual;
        }


        @Override
        public Void visitSubquery(SubqueryRelation subquery, Void context) {
            if (subquery.getResolveTableName() != null && subquery.getResolveTableName().getTbl() == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DERIVED_MUST_HAVE_ALIAS);
            }

            process(subquery.getQueryStatement());
            if (subquery.hasOrderByClause()) {
                List<Expr> outputExpressions = subquery.getOutputExpression();
                for (OrderByElement orderByElement : subquery.getOrderBy()) {
                    Expr expression = orderByElement.getExpr();
                    AnalyzerUtils.verifyNoGroupingFunctions(expression, "ORDER BY");

                    if (expression instanceof IntLiteral) {
                        long ordinal = ((IntLiteral) expression).getLongValue();
                        if (ordinal < 1 || ordinal > outputExpressions.size()) {
                            throw new SemanticException("ORDER BY position %s is not in select list", ordinal);
                        }
                        expression = new FieldReference((int) ordinal - 1, null);
                    }

                    analyzeExpression(expression, new AnalyzeState());

                    if (!expression.getType().canOrderBy()) {
                        throw new SemanticException(Type.NOT_SUPPORT_ORDER_ERROR_MSG);
                    }
                }
            }
            return null;
        }

        @Override
        public Void visitUnion(UnionRelation node, Void context) {
            return analyzeSetOperation(node, context);
        }

        @Override
        public Void visitExcept(ExceptRelation node, Void context) {
            if (node.getQualifier().equals(SetQualifier.ALL)) {
                throw new SemanticException("EXCEPT does not support ALL qualifier");
            }
            return analyzeSetOperation(node, context);
        }

        @Override
        public Void visitIntersect(IntersectRelation node, Void context) {
            if (node.getQualifier().equals(SetQualifier.ALL)) {
                throw new SemanticException("INTERSECT does not support ALL qualifier");
            }
            return analyzeSetOperation(node, context);
        }

        private Void analyzeSetOperation(SetOperationRelation node, Void context) {
            List<QueryRelation> setOpRelations = node.getRelations();

            process(setOpRelations.get(0));

            for (int i = 1; i < setOpRelations.size(); ++i) {
                process(setOpRelations.get(i));
            }

            if (node.hasOrderByClause()) {
                List<Expr> outputExpressions = node.getOutputExpression();
                for (OrderByElement orderByElement : node.getOrderBy()) {
                    Expr expression = orderByElement.getExpr();
                    AnalyzerUtils.verifyNoGroupingFunctions(expression, "ORDER BY");

                    if (expression instanceof IntLiteral) {
                        long ordinal = ((IntLiteral) expression).getLongValue();
                        if (ordinal < 1 || ordinal > outputExpressions.size()) {
                            throw new SemanticException("ORDER BY position %s is not in select list", ordinal);
                        }
                        expression = new FieldReference((int) ordinal - 1, null);
                    }

                    analyzeExpression(expression, new AnalyzeState());

                    if (!expression.getType().canOrderBy()) {
                        throw new SemanticException(Type.NOT_SUPPORT_ORDER_ERROR_MSG);
                    }
                }
            }
            return null;
        }

        @Override
        public Void visitValues(ValuesRelation node, Void context) {
            AnalyzeState analyzeState = new AnalyzeState();
            List<Expr> firstRow = node.getRow(0);
            firstRow.forEach(e -> analyzeExpression(e, analyzeState));
            List<List<Expr>> rows = node.getRows();
            Type[] outputTypes = firstRow.stream().map(Expr::getType).toArray(Type[]::new);
            for (List<Expr> row : rows) {
                if (row.size() != firstRow.size()) {
                    throw new SemanticException("Values have unequal number of columns");
                }
                for (int fieldIdx = 0; fieldIdx < row.size(); ++fieldIdx) {
                    analyzeExpression(row.get(fieldIdx), analyzeState);
                    Type commonType =
                            TypeManager.getCommonSuperType(outputTypes[fieldIdx], row.get(fieldIdx).getType());
                    if (!commonType.isValid()) {
                        throw new SemanticException(String.format("Incompatible return types '%s' and '%s'",
                                outputTypes[fieldIdx], row.get(fieldIdx).getType()));
                    }
                    outputTypes[fieldIdx] = commonType;
                }
            }

            return null;
        }

        @Override
        public Void visitTableFunction(TableFunctionRelation node, Void scope) {
            AnalyzeState analyzeState = new AnalyzeState();
            List<Expr> args = node.getFunctionParams().exprs();
            Type[] argTypes = new Type[args.size()];
            for (int i = 0; i < args.size(); ++i) {
                analyzeExpression(args.get(i), analyzeState);
                argTypes[i] = args.get(i).getType();

                AnalyzerUtils.verifyNoAggregateFunctions(args.get(i), "Table Function");
                AnalyzerUtils.verifyNoWindowFunctions(args.get(i), "Table Function");
                AnalyzerUtils.verifyNoGroupingFunctions(args.get(i), "Table Function");
            }

            Function fn = Expr.getBuiltinFunction(node.getFunctionName().getFunction(), argTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

            if (fn == null) {
                throw new SemanticException("Unknown table function '%s(%s)'", node.getFunctionName().getFunction(),
                        Arrays.stream(argTypes).map(Object::toString).collect(Collectors.joining(",")));
            }

            if (!(fn instanceof TableFunction)) {
                throw new SemanticException("'%s(%s)' is not table function", node.getFunctionName().getFunction(),
                        Arrays.stream(argTypes).map(Object::toString).collect(Collectors.joining(",")));
            }

            TableFunction tableFunction = (TableFunction) fn;
            node.setTableFunction(tableFunction);
            node.setChildExpressions(node.getFunctionParams().exprs());

            if (node.getColumnOutputNames() == null) {
                if (tableFunction.getFunctionName().getFunction().equals("unnest")) {
                    // If the unnest variadic function does not explicitly specify column name,
                    // all column names are `unnest`. This refers to the return column name of postgresql.
                    List<String> columnNames = new ArrayList<>();
                    for (int i = 0; i < tableFunction.getTableFnReturnTypes().size(); ++i) {
                        columnNames.add("unnest");
                    }
                    node.setColumnOutputNames(columnNames);
                } else {
                    node.setColumnOutputNames(new ArrayList<>(tableFunction.getDefaultColumnNames()));
                }
            } else {
                if (node.getColumnOutputNames().size() != tableFunction.getTableFnReturnTypes().size()) {
                    throw new SemanticException("table %s has %s columns available but %s columns specified",
                            node.getAlias().getTbl(),
                            tableFunction.getTableFnReturnTypes().size(),
                            node.getColumnOutputNames().size());
                }
            }

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (int i = 0; i < tableFunction.getTableFnReturnTypes().size(); ++i) {
                String colName = node.getColumnOutputNames().get(i);

                Field field = new Field(colName,
                        tableFunction.getTableFnReturnTypes().get(i),
                        node.getResolveTableName(),
                        new SlotRef(node.getResolveTableName(), colName, colName));
                fields.add(field);
            }

            Scope outputScope = new Scope(RelationId.of(node), new RelationFields(fields.build()));
            node.setScope(outputScope);
            return null;
        }

        @Override
        public Void visitNormalizedTableFunction(NormalizedTableFunctionRelation node, Void scope) {
            visitJoin(node, scope);
            // Only the scope of the table function is visible outside.
            node.setScope(node.getRight().getScope());
            return null;
        }

        private void analyzeExpression(Expr expr, AnalyzeState state) {
            SimpleExpressionAnalyzer.analyzeExpression(expr, state);
        }
    }
}
