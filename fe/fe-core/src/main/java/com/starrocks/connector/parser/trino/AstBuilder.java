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

package com.starrocks.connector.parser.trino;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CollectionElementExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.GroupByClause;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.analysis.TypeDef;
import com.starrocks.analysis.VarBinaryLiteral;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.LambdaArgument;
import com.starrocks.sql.ast.LambdaFunctionExpr;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.UnitIdentifier;
import com.starrocks.sql.ast.ValueList;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.parser.ParsingException;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Cube;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.JoinUsing;
import io.trino.sql.tree.JsonArray;
import io.trino.sql.tree.JsonArrayElement;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Rollup;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SetOperation;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TimestampLiteral;
import io.trino.sql.tree.Trim;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowSpecification;
import io.trino.sql.tree.WithQuery;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.analysis.AnalyticWindow.BoundaryType.CURRENT_ROW;
import static com.starrocks.analysis.AnalyticWindow.BoundaryType.FOLLOWING;
import static com.starrocks.analysis.AnalyticWindow.BoundaryType.PRECEDING;
import static com.starrocks.analysis.AnalyticWindow.BoundaryType.UNBOUNDED_FOLLOWING;
import static com.starrocks.analysis.AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING;
import static java.util.stream.Collectors.toList;

public class AstBuilder extends AstVisitor<ParseNode, ParseTreeContext> {
    private final long sqlMode;
    public AstBuilder(long sqlMode) {
        this.sqlMode = sqlMode;
    }

    private static final ImmutableMap<Join.Type, JoinOperator> JOIN_TYPE_MAP =
            ImmutableMap.<Join.Type, JoinOperator>builder().
                    put(Join.Type.LEFT, JoinOperator.LEFT_OUTER_JOIN)
                    .put(Join.Type.RIGHT, JoinOperator.RIGHT_OUTER_JOIN)
                    .put(Join.Type.FULL, JoinOperator.FULL_OUTER_JOIN)
                    .put(Join.Type.INNER, JoinOperator.INNER_JOIN)
                    .put(Join.Type.CROSS, JoinOperator.CROSS_JOIN)
                    .put(Join.Type.IMPLICIT, JoinOperator.INNER_JOIN)
                    .build();

    private static final ImmutableMap<ComparisonExpression.Operator, BinaryPredicate.Operator> COMPARISON_OPERATOR_MAP =
            ImmutableMap.<ComparisonExpression.Operator, BinaryPredicate.Operator>builder().
                    put(ComparisonExpression.Operator.EQUAL, BinaryPredicate.Operator.EQ)
                    .put(ComparisonExpression.Operator.LESS_THAN, BinaryPredicate.Operator.LT)
                    .put(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, BinaryPredicate.Operator.LE)
                    .put(ComparisonExpression.Operator.GREATER_THAN, BinaryPredicate.Operator.GT)
                    .put(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, BinaryPredicate.Operator.GE)
                    .put(ComparisonExpression.Operator.NOT_EQUAL, BinaryPredicate.Operator.NE)
                    .build();

    private static final ImmutableMap<ArithmeticBinaryExpression.Operator, ArithmeticExpr.Operator> BINARY_OPERATOR_MAP =
            ImmutableMap.of(ArithmeticBinaryExpression.Operator.ADD, ArithmeticExpr.Operator.ADD,
                    ArithmeticBinaryExpression.Operator.SUBTRACT, ArithmeticExpr.Operator.SUBTRACT,
                    ArithmeticBinaryExpression.Operator.MULTIPLY, ArithmeticExpr.Operator.MULTIPLY,
                    ArithmeticBinaryExpression.Operator.DIVIDE, ArithmeticExpr.Operator.DIVIDE,
                    ArithmeticBinaryExpression.Operator.MODULUS, ArithmeticExpr.Operator.MOD);

    private static ImmutableSet<String> DISTINCT_FUNCTION = ImmutableSet.of("count", "avg", "sum", "min", "max");

    private ParseNode visit(Node node, ParseTreeContext context) {
        return this.process(node, context);
    }

    private <T> List<T> visit(Node node, ParseTreeContext context, Class<T> clazz) {
        return node.getChildren().stream().map(child -> this.process(child, context)).
                map(clazz::cast).collect(Collectors.toList());
    }

    private <T> List<T> visit(List<? extends Node> nodes, ParseTreeContext context, Class<T> clazz) {
        return nodes.stream().map(child -> this.process(child, context)).
                map(clazz::cast).collect(Collectors.toList());
    }

    private ParseNode processOptional(Optional<? extends Node> node, ParseTreeContext context) {
        return node.map(value -> process(value, context)).orElse(null);
    }

    @Override
    protected ParseNode visitNode(Node node, ParseTreeContext context) {
        if (node instanceof JsonArrayElement) {
            return visit(((JsonArrayElement) node).getValue(), context);
        }
        return null;
    }

    @Override
    protected ParseNode visitExplain(Explain node, ParseTreeContext context) {
        QueryStatement queryStatement = (QueryStatement) visit(node.getStatement(), context);

        Optional<ExplainType> explainType = node.getOptions().stream().filter(option -> option instanceof ExplainType).
                map(type -> (ExplainType) type).findFirst();
        if (explainType.isPresent()) {
            ExplainType.Type type = explainType.get().getType();
            if (type == ExplainType.Type.LOGICAL)  {
                queryStatement.setIsExplain(true, StatementBase.ExplainLevel.LOGICAL);
            } else if (type == ExplainType.Type.DISTRIBUTED) {
                queryStatement.setIsExplain(true, StatementBase.ExplainLevel.VERBOSE);
            } else if (type == ExplainType.Type.IO) {
                queryStatement.setIsExplain(true, StatementBase.ExplainLevel.COST);
            } else {
                queryStatement.setIsExplain(true, StatementBase.ExplainLevel.NORMAL);
            }
        } else {
            queryStatement.setIsExplain(true, StatementBase.ExplainLevel.NORMAL);
        }
        return queryStatement;
    }

    @Override
    protected ParseNode visitQuery(Query node, ParseTreeContext context) {
        QueryRelation queryRelation = (QueryRelation) visit(node.getQueryBody(), context);
        // When trino have a simple query specification followed by order by, offset, limit or fetch,
        // it fold the order by, limit, offset or fetch clauses into the query specification,
        // no need to do anything else here.
        if (!(node.getQueryBody() instanceof QuerySpecification)) {
            if (node.getOrderBy().isPresent()) {
                queryRelation.setOrderBy(visit(node.getOrderBy().get(), context, OrderByElement.class));
            } else {
                queryRelation.setOrderBy(new ArrayList<>());
            }
            queryRelation.setLimit((LimitElement) processOptional(node.getLimit(), context));
        }

        // fetch
        // offset
        // with
        List<CTERelation> withQuery = new ArrayList<>();
        if (node.getWith().isPresent()) {
            withQuery = visit(node.getWith().get().getQueries(), context, CTERelation.class);
        }
        withQuery.forEach(queryRelation::addCTERelation);

        return new QueryStatement(queryRelation);
    }

    @Override
    protected ParseNode visitWithQuery(WithQuery node, ParseTreeContext context) {
        QueryStatement queryStatement = (QueryStatement) visit(node.getQuery(), context);

        return new CTERelation(
                RelationId.of(queryStatement.getQueryRelation()).hashCode(),
                node.getName().getValue(),
                getColumnNames(node.getColumnNames()),
                queryStatement);
    }

    public List<String> getColumnNames(Optional<List<Identifier>> columnNames) {
        if (!columnNames.isPresent() || columnNames.get().isEmpty()) {
            return null;
        }

        // StarRocks tables are not case-sensitive, so targetColumnNames are converted
        // to lowercase characters to facilitate subsequent matching.
        return columnNames.get().stream().map(Identifier::getValue).map(String::toLowerCase).collect(toList());
    }

    @Override
    protected ParseNode visitQuerySpecification(QuerySpecification node, ParseTreeContext context) {
        List<SelectListItem> selectListItems = node.getSelect().getSelectItems().stream().map(
                selectItem -> visit(selectItem, context)).map(selectItem -> (SelectListItem) selectItem).
                collect(toList());
        boolean isDistinct = node.getSelect().isDistinct();
        SelectList selectList = new SelectList(selectListItems, isDistinct);
        Relation from = (Relation) processOptional(node.getFrom(), context);

        // from == null means a statement without from or from dual, add a single row of null values here
        if (from == null) {
            ArrayList<Expr> row = new ArrayList<>();
            List<String> columnNames = new ArrayList<>();
            row.add(NullLiteral.create(Type.NULL));
            columnNames.add("");
            List<ArrayList<Expr>> rows = new ArrayList<>();
            rows.add(row);
            ValuesRelation valuesRelation = new ValuesRelation(rows, columnNames);
            valuesRelation.setNullValues(true);
            from = valuesRelation;
        }

        SelectRelation resultSelectRelation = new SelectRelation(
                selectList,
                from,
                (Expr) processOptional(node.getWhere(), context),
                (GroupByClause) processOptional(node.getGroupBy(), context),
                (Expr) processOptional(node.getHaving(), context));

        if (node.getOrderBy().isPresent()) {
            resultSelectRelation.setOrderBy(visit(node.getOrderBy().get(), context, OrderByElement.class));
        } else {
            resultSelectRelation.setOrderBy(new ArrayList<>());
        }
        resultSelectRelation.setLimit((LimitElement) processOptional(node.getLimit(), context));
        return resultSelectRelation;
    }

    @Override
    protected ParseNode visitAliasedRelation(AliasedRelation node, ParseTreeContext context) {
        Relation relation = (Relation) visit(node.getRelation(), context);
        relation.setAlias(new TableName(null, node.getAlias().getValue()));
        List<String> columnNames = getColumnNames(Optional.ofNullable(node.getColumnNames()));
        if (columnNames != null && !columnNames.isEmpty()) {
            relation.setColumnOutputNames(columnNames);

            if (relation instanceof SubqueryRelation) {
                // set value relation alias name here, otherwise sr optimizer will lose the alias name
                SubqueryRelation subqueryRelation = (SubqueryRelation) relation;
                if (subqueryRelation.getQueryStatement().getQueryRelation() instanceof ValuesRelation) {
                    ValuesRelation valuesRelation = (ValuesRelation) subqueryRelation.getQueryStatement().getQueryRelation();
                    valuesRelation.setColumnOutputNames(columnNames);
                }
            }
        }
        return relation;
    }

    @Override
    protected ParseNode visitValues(Values node, ParseTreeContext context) {
        if (node.getRows().isEmpty()) {
            return null;
        } else {
            List<Expr> rows = visit(node.getRows(), context, Expr.class);
            List<ValueList> valueLists = new ArrayList<>();

            if (node.getRows().get(0) instanceof Row) {
                // (values (1,2),(3,4),(5,6)), has three rows, each row is row function call
                for (Expr rowFnCall : rows) {
                    valueLists.add(new ValueList(rowFnCall.getChildren()));
                }
            } else {
                // (values 1,2,3,4,5,6), has six rows, each row has one int value
                for (Expr value : rows) {
                    valueLists.add(new ValueList(Lists.newArrayList(value)));
                }
            }
            List<ArrayList<Expr>> records = valueLists.stream().map(ValueList::getRow).collect(toList());

            List<String> colNames = Lists.newArrayList();
            for (int i = 0; i < records.get(0).size(); ++i) {
                colNames.add("column_" + i);
            }

            return new ValuesRelation(records, colNames);
        }
    }

    @Override
    protected ParseNode visitUnnest(Unnest node, ParseTreeContext context) {
        List<Expr> arguments = visit(node.getExpressions(), context, Expr.class);
        List<Expr> expressions = new ArrayList<>();

        for (Expr arg : arguments) {
            if (arg instanceof ArrayExpr) {
                if (!arg.getChildren().isEmpty()) {
                    // need to covert array[row(1,2), row(3,4))] to array[1,3], array[2,4], so SR could unnest it
                    Expr firstArrayElement = arg.getChildren().get(0);
                    if (firstArrayElement instanceof FunctionCallExpr && ((FunctionCallExpr) firstArrayElement).getFnName().
                            getFunction().equalsIgnoreCase("row")) {
                        List<List<Expr>> items = new ArrayList<>();
                        for (Expr row : arg.getChildren()) {
                            int rowIndex = 0;
                            for (Expr literal : row.getChildren()) {
                                if (items.size() <= rowIndex) {
                                    items.add(new ArrayList<>());
                                    items.get(rowIndex).add(literal);
                                } else {
                                    items.get(rowIndex).add(literal);
                                }
                                ++rowIndex;
                            }
                        }

                        for (List<Expr> item : items) {
                            Expr arrayExpr = new ArrayExpr(null, item);
                            expressions.add(arrayExpr);
                        }
                        continue;
                    }
                }
            }
            expressions.add(arg);
        }

        TableFunctionRelation tableFunctionRelation = new TableFunctionRelation("unnest",
                new FunctionParams(false, expressions));
        return tableFunctionRelation;
    }

    @Override
    protected ParseNode visitTable(Table node, ParseTreeContext context) {
        TableName tableName = qualifiedNameToTableName(convertQualifiedName(node.getName()));
        return new TableRelation(tableName);
    }

    @Override
    protected ParseNode visitGroupBy(GroupBy node, ParseTreeContext context) {
        if (node.getGroupingElements().isEmpty()) {
            return null;
        }

        if (node.getGroupingElements().stream().map(g -> g.getClass().getName()).distinct().count() > 1) {
            throw new ParsingException("StarRocks do not support Combining multiple grouping expressions now");
        }

        GroupingElement groupingElement = node.getGroupingElements().get(0);
        if (groupingElement instanceof SimpleGroupBy) {
            List<Expr> groupByExpressions = visit(node.getGroupingElements(), context, Expr.class);
            return new GroupByClause(new ArrayList<>(groupByExpressions), GroupByClause.GroupingType.GROUP_BY);
        } else {
            // RollUp/Cube/GroupingSet
            return visit(node.getGroupingElements().get(0), context);
        }
    }

    @Override
    protected ParseNode visitSimpleGroupBy(SimpleGroupBy node, ParseTreeContext context) {
        Preconditions.checkArgument(node.getExpressions().size() == 1,
                "SimpleGroupBy should have only one element in the expression.");
        return visit(node.getExpressions().get(0), context);
    }

    @Override
    protected ParseNode visitRollup(Rollup node, ParseTreeContext context) {
        return new GroupByClause((ArrayList<Expr>) visit(node.getExpressions(), context, Expr.class),
                GroupByClause.GroupingType.ROLLUP);
    }

    @Override
    protected ParseNode visitCube(Cube node, ParseTreeContext context) {
        return new GroupByClause((ArrayList<Expr>) visit(node.getExpressions(), context, Expr.class),
                GroupByClause.GroupingType.CUBE);
    }

    @Override
    protected ParseNode visitGroupingSets(GroupingSets node, ParseTreeContext context) {
        List<ArrayList<Expr>> groupingSets = new ArrayList<>();
        for (List<Expression> groupingSet : node.getSets()) {
            List<Expr> exprList = visit(groupingSet, context, Expr.class);
            groupingSets.add(new ArrayList<>(exprList));
        }

        return new GroupByClause(groupingSets, GroupByClause.GroupingType.GROUPING_SETS);
    }

    @Override
    protected ParseNode visitGroupingOperation(GroupingOperation node, ParseTreeContext context) {
        List<Expr> arguments = visit(node.getGroupingColumns(), context, Expr.class);
        return new GroupingFunctionCallExpr("grouping", arguments);
    }

    @Override
    protected ParseNode visitSortItem(SortItem node, ParseTreeContext context) {
        boolean isAsc = node.getOrdering() == SortItem.Ordering.ASCENDING;
        return new OrderByElement((Expr) visit(node.getSortKey(), context), isAsc,
                getNullOrderingType(isAsc, node.getNullOrdering()));
    }

    @Override
    protected ParseNode visitLimit(Limit node, ParseTreeContext context) {
        long limit = ((LiteralExpr) visit(node.getRowCount(), context)).getLongValue();
        return new LimitElement(0, limit);
    }

    private boolean getNullOrderingType(boolean isAsc, SortItem.NullOrdering nullOrdering) {
        if (nullOrdering == SortItem.NullOrdering.UNDEFINED) {
            return (!SqlModeHelper.check(sqlMode, SqlModeHelper.MODE_SORT_NULLS_LAST)) == isAsc;
        }

        return nullOrdering == SortItem.NullOrdering.FIRST;
    }

    private QualifiedName convertQualifiedName(io.trino.sql.tree.QualifiedName qualifiedName) {
        return QualifiedName.of(qualifiedName.getParts());
    }

    private TableName qualifiedNameToTableName(QualifiedName qualifiedName) {
        // Hierarchy: catalog.database.table
        List<String> parts = qualifiedName.getParts();
        if (parts.size() == 3) {
            return new TableName(parts.get(0), parts.get(1), parts.get(2));
        } else if (parts.size() == 2) {
            return new TableName(null, qualifiedName.getParts().get(0), qualifiedName.getParts().get(1));
        } else if (parts.size() == 1) {
            return new TableName(null, null, qualifiedName.getParts().get(0));
        } else {
            throw new ParsingException("error table name ");
        }
    }

    @Override
    protected ParseNode visitJoin(Join node, ParseTreeContext context) {
        Relation left = (Relation) visit(node.getLeft(), context);
        Relation right = (Relation) visit(node.getRight(), context);
        JoinOperator joinType = JOIN_TYPE_MAP.get(node.getType());
        if (joinType == null) {
            throw new SemanticException("Join Type %s is illegal", node.getType());
        }

        Expr predicate = null;
        List<String> usingColNames = null;
        if (node.getCriteria().isPresent()) {
            JoinCriteria joinCriteria = node.getCriteria().get();
            if (joinCriteria instanceof JoinOn) {
                predicate = (Expr) visit(((JoinOn) joinCriteria).getExpression(), context);
            } else if (joinCriteria instanceof JoinUsing) {
                usingColNames = ((JoinUsing) joinCriteria).getColumns().stream().map(Identifier::getValue).
                        collect(toList());
            }
        }
        JoinRelation joinRelation = new JoinRelation(joinType, left, right, predicate, false);
        joinRelation.setUsingColNames(usingColNames);

        return joinRelation;
    }

    @Override
    protected ParseNode visitTableSubquery(TableSubquery node, ParseTreeContext context) {
        QueryStatement queryStatement = (QueryStatement) visit(node.getQuery(), context);
        return new SubqueryRelation(queryStatement);
    }

    protected ParseNode visitSetOperation(SetOperation node, ParseTreeContext context) {
        QueryRelation left = (QueryRelation) visit(node.getRelations().get(0), context);
        QueryRelation right = (QueryRelation) visit(node.getRelations().get(1), context);
        SetQualifier setQualifier = node.isDistinct() ? SetQualifier.DISTINCT : SetQualifier.ALL;

        if (node instanceof Union) {
            if (left instanceof UnionRelation && ((UnionRelation) left).getQualifier().equals(setQualifier)) {
                ((UnionRelation) left).addRelation(right);
                return left;
            } else {
                return new UnionRelation(Lists.newArrayList(left, right), setQualifier);
            }
        } else if (node instanceof Intersect) {
            if (left instanceof IntersectRelation && ((IntersectRelation) left).getQualifier().equals(setQualifier)) {
                ((IntersectRelation) left).addRelation(right);
                return left;
            } else {
                return new IntersectRelation(Lists.newArrayList(left, right), setQualifier);
            }
        } else if (node instanceof Except) {
            if (left instanceof ExceptRelation && ((ExceptRelation) left).getQualifier().equals(setQualifier)) {
                ((ExceptRelation) left).addRelation(right);
                return left;
            } else {
                return new ExceptRelation(Lists.newArrayList(left, right), setQualifier);
            }
        } else {
            throw new IllegalArgumentException("Unsupported set operation: " + node);
        }
    }

    @Override
    protected ParseNode visitUnion(Union node, ParseTreeContext context) {
        return visitSetOperation(node, context);
    }

    @Override
    protected ParseNode visitIntersect(Intersect node, ParseTreeContext context) {
        return visitSetOperation(node, context);
    }

    @Override
    protected ParseNode visitExcept(Except node, ParseTreeContext context) {
        return visitSetOperation(node, context);
    }

    @Override
    protected ParseNode visitSingleColumn(SingleColumn node, ParseTreeContext context) {
        String alias = null;
        if (node.getAlias().isPresent()) {
            alias = node.getAlias().get().getValue();
        }

        Expr expression = (Expr) visit(node.getExpression(), context);
        return new SelectListItem(expression, alias);
    }


    @Override
    protected ParseNode visitAllColumns(AllColumns node, ParseTreeContext context) {
        if (node.getTarget().isPresent()) {
            SlotRef target = (SlotRef) visit(node.getTarget().get(), context);
            return new SelectListItem(qualifiedNameToTableName(target.getQualifiedName()));
        }
        return new SelectListItem(null);
    }

    @Override
    protected ParseNode visitRow(Row node, ParseTreeContext context) {
        List<Expr> items = visit(node.getItems(), context, Expr.class);
        return new FunctionCallExpr("row", items);
    }

    @Override
    protected ParseNode visitArrayConstructor(ArrayConstructor node, ParseTreeContext context) {
        List<Expr> exprs;
        if (node.getValues() != null) {
            exprs = visit(node.getValues(), context, Expr.class);
        } else {
            exprs = Collections.emptyList();
        }
        return new ArrayExpr(null, exprs);
    }

    @Override
    protected ParseNode visitSubscriptExpression(SubscriptExpression node, ParseTreeContext context) {
        Expr value = (Expr) visit(node.getBase(), context);
        Expr index = (Expr) visit(node.getIndex(), context);
        return new CollectionElementExpr(value, index);
    }

    @Override
    protected ParseNode visitFunctionCall(FunctionCall node, ParseTreeContext context) {
        List<Expr> arguments = visit(node.getArguments(), context, Expr.class);

        Expr callExpr;
        Expr convertedFunctionCall = Trino2SRFunctionCallTransformer.convert(node.getName().toString(), arguments);
        if (convertedFunctionCall != null) {
            callExpr = convertedFunctionCall;
        } else if (DISTINCT_FUNCTION.contains(node.getName().toString())) {
            callExpr = visitDistinctFunctionCall(node, context);
        }  else {
            callExpr = new FunctionCallExpr(node.getName().toString(), arguments);
        }
        if (node.getWindow().isPresent()) {
            return visitWindow((FunctionCallExpr) callExpr, node.getWindow().get(), context);
        }
        return callExpr;

    }

    private static AnalyticWindow.Type getFrameType(WindowFrame.Type type) {
        switch (type) {
            case RANGE:
                return AnalyticWindow.Type.RANGE;
            case ROWS:
                return AnalyticWindow.Type.ROWS;
        }

        throw new IllegalArgumentException("Unsupported frame type: " + type);
    }

    @Override
    protected ParseNode visitFrameBound(FrameBound node, ParseTreeContext context) {
        switch (node.getType()) {
            case UNBOUNDED_PRECEDING:
                return new AnalyticWindow.Boundary(UNBOUNDED_PRECEDING, null);
            case UNBOUNDED_FOLLOWING:
                return new AnalyticWindow.Boundary(UNBOUNDED_FOLLOWING, null);
            case CURRENT_ROW:
                return new AnalyticWindow.Boundary(CURRENT_ROW, null);
            case PRECEDING:
                return new AnalyticWindow.Boundary(PRECEDING, (Expr) visit(node.getValue().get(), context));
            case FOLLOWING:
                return new AnalyticWindow.Boundary(FOLLOWING, (Expr) visit(node.getValue().get(), context));
        }

        throw new IllegalArgumentException("Unsupported frame bound type: " + node.getType());
    }

    @Override
    protected ParseNode visitWindowFrame(WindowFrame node, ParseTreeContext context) {
        if (node.getEnd().isPresent()) {
            return new AnalyticWindow(
                    getFrameType(node.getType()),
                    (AnalyticWindow.Boundary) visit(node.getStart(), context),
                    (AnalyticWindow.Boundary) visit(node.getEnd().get(), context));
        } else {
            return new AnalyticWindow(
                    getFrameType(node.getType()),
                    (AnalyticWindow.Boundary) visit(node.getStart(), context));
        }
    }

    protected AnalyticExpr visitWindowSpecification(FunctionCallExpr functionCallExpr, WindowSpecification node,
                                                 ParseTreeContext context) {
        functionCallExpr.setIsAnalyticFnCall(true);
        List<OrderByElement> orderByElements = new ArrayList<>();
        if (node.getOrderBy().isPresent()) {
            orderByElements = visit(node.getOrderBy().get(), context, OrderByElement.class);
        }
        List<Expr> partitionExprs = visit(node.getPartitionBy(), context, Expr.class);

        return new AnalyticExpr(functionCallExpr, partitionExprs, orderByElements,
                (AnalyticWindow) processOptional(node.getFrame(), context), null);
    }

    private ParseNode visitWindow(FunctionCallExpr functionCallExpr, Window windowSpec, ParseTreeContext context) {
        if (windowSpec instanceof WindowSpecification) {
            return visitWindowSpecification(functionCallExpr, (WindowSpecification) windowSpec, context);
        }
        return null;
    }

    private FunctionCallExpr visitDistinctFunctionCall(FunctionCall node, ParseTreeContext context) {
        String fnName = node.getName().toString();
        return new FunctionCallExpr(fnName, (!node.getArguments().isEmpty()) ?
                        new FunctionParams(node.isDistinct(), visit(node.getArguments(), context, Expr.class)) :
                        FunctionParams.createStarParam());
    }

    @Override
    protected ParseNode visitSubqueryExpression(SubqueryExpression node, ParseTreeContext context) {
        return new Subquery((QueryStatement) visit(node.getQuery(), context));
    }

    @Override
    protected ParseNode visitIdentifier(Identifier node, ParseTreeContext context) {
        List<String> parts = new ArrayList<>();
        parts.add(node.getValue());
        QualifiedName qualifiedName = QualifiedName.of(parts);
        return new SlotRef(qualifiedName);
    }

    @Override
    protected ParseNode visitDereferenceExpression(DereferenceExpression node, ParseTreeContext context) {
        Expr base = (Expr) visit(node.getBase(), context);

        String fieldName = "";
        if (node.getField().isPresent()) {
            fieldName = node.getField().get().getValue();
        }
        if (base instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) base;
            List<String> parts = new ArrayList<>(slotRef.getQualifiedName().getParts());
            parts.add(fieldName);
            return new SlotRef(QualifiedName.of(parts));
        } else if (base instanceof SubfieldExpr) {
            // Merge multi-level subfield access
            SubfieldExpr subfieldExpr = (SubfieldExpr) base;
            ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
            for (String tmpFieldName : subfieldExpr.getFieldNames()) {
                builder.add(tmpFieldName);
            }
            builder.add(fieldName);
            return new SubfieldExpr(subfieldExpr.getChild(0), builder.build());
        }  else {
            return new SubfieldExpr(base, ImmutableList.of(fieldName));
        }
    }

    @Override
    protected ParseNode visitTryExpression(TryExpression node, ParseTreeContext context) {
        return visit(node.getInnerExpression(), context);
    }

    @Override
    protected ParseNode visitJsonQuery(JsonQuery jsonQuery, ParseTreeContext context) {
        Expr inputExpr = (Expr) visit(jsonQuery.getJsonPathInvocation().getInputExpression(), context);
        String jsonPath = jsonQuery.getJsonPathInvocation().getJsonPath().getValue();
        com.starrocks.analysis.StringLiteral jsonPathLiteral =
                new com.starrocks.analysis.StringLiteral(jsonPath.substring(jsonPath.indexOf('$')));
        return new FunctionCallExpr("json_query", ImmutableList.of(inputExpr, jsonPathLiteral));
    }

    @Override
    protected ParseNode visitJsonArray(JsonArray jsonArray, ParseTreeContext context) {
        List<Expr> children = visit(jsonArray.getElements(), context, Expr.class);
        return new FunctionCallExpr("json_array", children);
    }

    private static final BigInteger LONG_MAX = new BigInteger("9223372036854775807");

    private static final BigInteger LARGEINT_MAX_ABS =
            new BigInteger("170141183460469231731687303715884105728"); // 2^127

    @Override
    protected ParseNode visitLongLiteral(LongLiteral node, ParseTreeContext context) {
        try {
            BigInteger intLiteral = new BigInteger(String.valueOf(node.getValue()));
            // Note: val is positive, because we do not recognize minus character in 'IntegerLiteral'
            // -2^63 will be recognized as large int(__int128)
            if (intLiteral.compareTo(LONG_MAX) <= 0) {
                return new IntLiteral(intLiteral.longValue());
            } else if (intLiteral.compareTo(LARGEINT_MAX_ABS) <= 0) {
                return new LargeIntLiteral(intLiteral.toString());
            } else {
                throw new ParsingException("Numeric overflow " + intLiteral);
            }
        } catch (NumberFormatException | AnalysisException e) {
            throw new ParsingException("Invalid numeric literal: " + node.toString());
        }
    }

    @Override
    protected ParseNode visitDoubleLiteral(DoubleLiteral node, ParseTreeContext context) {
        try {
            if (SqlModeHelper.check(sqlMode, SqlModeHelper.MODE_DOUBLE_LITERAL)) {
                return new FloatLiteral(node.getValue());
            } else if (Double.isInfinite(node.getValue())) {
                throw new SemanticException("Numeric overflow " + node.getValue());
            } else {
                BigDecimal decimal = BigDecimal.valueOf(node.getValue());
                int precision = DecimalLiteral.getRealPrecision(decimal);
                int scale = DecimalLiteral.getRealScale(decimal);
                int integerPartWidth = precision - scale;
                if (integerPartWidth > 38) {
                    return new FloatLiteral(node.getValue());
                }
                return new DecimalLiteral(decimal);
            }

        } catch (AnalysisException | NumberFormatException e) {
            throw new ParsingException(e.getMessage());
        }
    }

    @Override
    protected ParseNode visitDecimalLiteral(io.trino.sql.tree.DecimalLiteral node, ParseTreeContext context) {
        try {
            if (SqlModeHelper.check(sqlMode, SqlModeHelper.MODE_DOUBLE_LITERAL)) {
                return new FloatLiteral(node.getValue());
            } else {
                return new DecimalLiteral(node.getValue());
            }
        } catch (AnalysisException e) {
            throw new ParsingException(e.getMessage());
        }
    }

    @Override
    protected ParseNode visitGenericLiteral(GenericLiteral node, ParseTreeContext context) {
        if (node.getType().equalsIgnoreCase("date")) {
            try {
                return new DateLiteral(node.getValue(), Type.DATE);
            } catch (AnalysisException e) {
                throw new ParsingException(e.getMessage());
            }
        } else if (node.getType().equalsIgnoreCase("json")) {
            return new com.starrocks.analysis.StringLiteral(node.getValue());
        } else if (node.getType().equalsIgnoreCase("real")) {
            try {
                return new FloatLiteral(node.getValue());
            } catch (AnalysisException e) {
                throw new RuntimeException(e);
            }
        }
        throw new ParsingException("Parse Error : unknown type " + node.getType());
    }

    @Override
    protected ParseNode visitBinaryLiteral(BinaryLiteral node, ParseTreeContext context) {
        return new VarBinaryLiteral(node.getValue());
    }

    @Override
    protected ParseNode visitStringLiteral(StringLiteral node, ParseTreeContext context) {
        return new com.starrocks.analysis.StringLiteral(node.getValue());
    }

    @Override
    protected ParseNode visitNullLiteral(io.trino.sql.tree.NullLiteral node, ParseTreeContext context) {
        return new NullLiteral();
    }

    @Override
    protected ParseNode visitBooleanLiteral(BooleanLiteral node, ParseTreeContext context) {
        return new BoolLiteral(node.getValue());
    }

    @Override
    protected ParseNode visitIntervalLiteral(IntervalLiteral node, ParseTreeContext context) {
        return new com.starrocks.sql.ast.IntervalLiteral(new com.starrocks.analysis.StringLiteral(node.getValue()),
                new UnitIdentifier(node.getStartField().toString()));
    }

    @Override
    protected ParseNode visitTimestampLiteral(TimestampLiteral node, ParseTreeContext context) {
        try {
            String value = node.getValue();
            if (value.length() <= 10) {
                value += " 00:00:00";
            }
            return new DateLiteral(value, Type.DATETIME);
        } catch (AnalysisException e) {
            throw new ParsingException("Invalid date literal: " + node.getValue());
        }
    }

    @Override
    protected ParseNode visitCoalesceExpression(CoalesceExpression node, ParseTreeContext context) {
        List<Expr> children = visit(node, context, Expr.class);
        FunctionName fnName = FunctionName.createFnName("coalesce");

        return new FunctionCallExpr(fnName, new FunctionParams(false, children));
    }

    @Override
    protected ParseNode visitExtract(Extract node, ParseTreeContext context) {
        String fieldString = node.getField().toString();
        return new FunctionCallExpr(fieldString,
                new FunctionParams(Lists.newArrayList((Expr) visit(node.getExpression(), context))));
    }

    @Override
    protected ParseNode visitArithmeticUnary(ArithmeticUnaryExpression node, ParseTreeContext context) {
        Expr child = (Expr) visit(node.getValue(), context);
        if (node.getSign() == ArithmeticUnaryExpression.Sign.MINUS) {
            return new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, new IntLiteral(-1), child);
        } else  {
            return child;
        }
    }

    @Override
    protected ParseNode visitArithmeticBinary(ArithmeticBinaryExpression node, ParseTreeContext context) {
        Expr left = (Expr) visit(node.getLeft(), context);
        Expr right = (Expr) visit(node.getRight(), context);

        if (left instanceof com.starrocks.sql.ast.IntervalLiteral) {
            return new TimestampArithmeticExpr(BINARY_OPERATOR_MAP.get(node.getOperator()), right,
                    ((com.starrocks.sql.ast.IntervalLiteral) left).getValue(),
                    ((com.starrocks.sql.ast.IntervalLiteral) left).getUnitIdentifier().getDescription(),
                    true);
        }

        if (right instanceof com.starrocks.sql.ast.IntervalLiteral) {
            return new TimestampArithmeticExpr(BINARY_OPERATOR_MAP.get(node.getOperator()), left,
                    ((com.starrocks.sql.ast.IntervalLiteral) right).getValue(),
                    ((com.starrocks.sql.ast.IntervalLiteral) right).getUnitIdentifier().getDescription(),
                    false);
        }

        return new ArithmeticExpr(BINARY_OPERATOR_MAP.get(node.getOperator()), left, right);
    }

    @Override
    protected ParseNode visitComparisonExpression(ComparisonExpression node, ParseTreeContext context) {
        BinaryPredicate.Operator binaryOp = COMPARISON_OPERATOR_MAP.get(node.getOperator());
        if (binaryOp == null) {
            throw new SemanticException("Do not support the comparison type %s", node.getOperator());
        }
        return new BinaryPredicate(binaryOp, (Expr) visit(node.getLeft(), context), (Expr) visit(node.getRight(),
                context));
    }

    @Override
    protected ParseNode visitNotExpression(NotExpression node, ParseTreeContext context) {
        List<Expr> children = visit(node, context, Expr.class);
        Preconditions.checkState(children.size() == 1);
        return new CompoundPredicate(CompoundPredicate.Operator.NOT, children.get(0), null);
    }

    @Override
    protected ParseNode visitBetweenPredicate(BetweenPredicate node, ParseTreeContext context) {
        return new com.starrocks.analysis.BetweenPredicate(
                (Expr) visit(node.getValue(), context),
                (Expr) visit(node.getMin(), context),
                (Expr) visit(node.getMax(), context),
                false);
    }

    @Override
    protected ParseNode visitLikePredicate(LikePredicate node, ParseTreeContext context) {
        return new com.starrocks.analysis.LikePredicate(
                com.starrocks.analysis.LikePredicate.Operator.LIKE,
                (Expr) visit(node.getValue(), context),
                (Expr) visit(node.getPattern(), context));
    }

    @Override
    protected ParseNode visitInPredicate(InPredicate node, ParseTreeContext context) {
        if (node.getValueList() instanceof InListExpression) {
            return new com.starrocks.analysis.InPredicate(
                    (Expr) visit(node.getValue(), context),
                    visit(node.getValueList(), context, Expr.class), false);
        } else {
            // SubqueryExpression
            return new com.starrocks.analysis.InPredicate(
                    (Expr) visit(node.getValue(), context),
                    (Expr) visit(node.getValueList(), context), false);
        }
    }

    @Override
    protected ParseNode visitIsNullPredicate(IsNullPredicate node, ParseTreeContext context) {
        Expr value = (Expr) visit(node.getValue(), context);
        return new com.starrocks.analysis.IsNullPredicate(value, false);
    }

    @Override
    protected ParseNode visitIsNotNullPredicate(IsNotNullPredicate node, ParseTreeContext context) {
        Expr value = (Expr) visit(node.getValue(), context);
        return new com.starrocks.analysis.IsNullPredicate(value, true);
    }

    @Override
    protected ParseNode visitExists(ExistsPredicate node, ParseTreeContext context) {
        return new com.starrocks.analysis.ExistsPredicate((Subquery) visit(node.getSubquery(), context), false);
    }

    @Override
    protected ParseNode visitLogicalExpression(LogicalExpression node, ParseTreeContext context) {
        CompoundPredicate compoundPredicate = null;
        List<Expr> children = visit(node, context, Expr.class);
        Preconditions.checkState(children.size() >= 2);

        Iterator<Expr> iterator = children.iterator();
        Expr left = iterator.next();
        Expr right = iterator.next();
        if (node.getOperator().equals(LogicalExpression.Operator.AND)) {
            compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.AND, left, right);
            while (iterator.hasNext()) {
                compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.AND, compoundPredicate,
                        iterator.next());
            }
        } else {
            compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.OR, left, right);
            while (iterator.hasNext()) {
                compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.OR, compoundPredicate,
                        iterator.next());
            }
        }

        return compoundPredicate;
    }

    @Override
    protected ParseNode visitCurrentTime(CurrentTime node, ParseTreeContext context) {
        return new FunctionCallExpr(node.getFunction().getName(), new ArrayList<>());
    }

    @Override
    protected ParseNode visitSearchedCaseExpression(SearchedCaseExpression node, ParseTreeContext context) {
        return new CaseExpr(null, visit(node.getWhenClauses(), context, CaseWhenClause.class),
                (Expr) processOptional(node.getDefaultValue(), context));
    }

    @Override
    protected ParseNode visitSimpleCaseExpression(SimpleCaseExpression node, ParseTreeContext context) {
        return new CaseExpr((Expr) visit(node.getOperand(), context),
                visit(node.getWhenClauses(), context, CaseWhenClause.class),
                (Expr) processOptional(node.getDefaultValue(), context));
    }

    @Override
    protected ParseNode visitNullIfExpression(NullIfExpression node, ParseTreeContext context) {
        List<Expr> arguments = visit(ImmutableList.of(node.getFirst(), node.getSecond()), context, Expr.class);
        return new FunctionCallExpr("nullif", arguments);
    }

    @Override
    protected ParseNode visitIfExpression(IfExpression node, ParseTreeContext context) {
        List<Node> children = Lists.newArrayList();
        children.add(node.getCondition());
        children.add(node.getTrueValue());
        if (node.getFalseValue().isPresent()) {
            children.add(node.getFalseValue().get());
        } else {
            children.add(new io.trino.sql.tree.NullLiteral());
        }
        List<Expr> arguments = visit(children, context, Expr.class);
        return new FunctionCallExpr("if", arguments);
    }

    @Override
    protected ParseNode visitLambdaExpression(LambdaExpression node, ParseTreeContext context) {
        List<String> names = Lists.newArrayList();
        for (LambdaArgumentDeclaration argumentDeclaration : node.getArguments()) {
            names.add(argumentDeclaration.getName().getValue());
        }

        List<Expr> arguments = Lists.newArrayList();
        Expr expr = null;
        if (node.getBody() != null) {
            expr = (Expr) visit(node.getBody(), context);
        }
        // put lambda body to the first argument
        arguments.add(expr);
        for (String name : names) {
            arguments.add(new LambdaArgument(name));
        }
        return new LambdaFunctionExpr(arguments);
    }

    @Override
    protected ParseNode visitTrim(Trim node, ParseTreeContext context) {
        Expr trimSource = (Expr) visit(node.getTrimSource(), context);
        Expr trimCharacter = (Expr) processOptional(node.getTrimCharacter(), context);
        List<Expr> arguments = Lists.newArrayList();
        arguments.add(trimSource);
        if (trimCharacter != null) {
            arguments.add(trimCharacter);
        }
        return new FunctionCallExpr(node.getSpecification().getFunctionName(), arguments);
    }

    @Override
    protected ParseNode visitWhenClause(WhenClause node, ParseTreeContext context) {
        return new CaseWhenClause((Expr) visit(node.getOperand(), context), (Expr) visit(node.getResult(), context));
    }

    @Override
    protected ParseNode visitCast(Cast node, ParseTreeContext context) {
        return new CastExpr(new TypeDef(getType(node.getType())), (Expr) visit(node.getExpression(), context));
    }

    public Type getType(DataType dataType) {
        if (dataType instanceof GenericDataType) {
            GenericDataType genericDataType = (GenericDataType) dataType;
            return getGenericDataType(genericDataType);
        } else if (dataType instanceof DateTimeDataType) {
            DateTimeDataType dateTimeType = (DateTimeDataType) dataType;
            if (dateTimeType.getType() == DateTimeDataType.Type.TIME) {
                return ScalarType.createType(PrimitiveType.TIME);
            } else {
                return ScalarType.createType(PrimitiveType.DATETIME);
            }
        } else {
            throw new SemanticException("StarRocks do not support the type %s", dataType);
        }
    }

    private Type getGenericDataType(GenericDataType dataType) {
        int length = -1;
        int precision = -1;
        int scale = -1;
        String typeName = dataType.getName().getValue().toLowerCase();
        if (!dataType.getArguments().isEmpty() && dataType.getArguments().get(0) instanceof NumericParameter) {
            length = Integer.parseInt(((NumericParameter) dataType.getArguments().get(0)).getValue());
            precision = length;
            if (dataType.getArguments().size() > 1 && dataType.getArguments().get(1) instanceof NumericParameter) {
                scale = Integer.parseInt(((NumericParameter) dataType.getArguments().get(1)).getValue());
            }
        }
        if (typeName.equals("varchar")) {
            ScalarType type = ScalarType.createVarcharType(length);
            if (length != -1) {
                type.setAssignedStrLenInColDefinition();
            }
            return type;
        } else if (typeName.equals("char")) {
            ScalarType type = ScalarType.createCharType(length);
            if (length != -1) {
                type.setAssignedStrLenInColDefinition();
            }
            return type;
        } else if (typeName.equals("decimal")) {
            if (precision != -1) {
                if (scale != -1) {
                    return ScalarType.createUnifiedDecimalType(precision, scale);
                }
                return ScalarType.createUnifiedDecimalType(precision, 0);
            }
            return ScalarType.createUnifiedDecimalType(38, 0);
        } else if (typeName.contains("decimal")) {
            throw new SemanticException("Unknown type: %s", typeName);
        } else if (typeName.equals("real")) {
            return ScalarType.createType(PrimitiveType.FLOAT);
        } else {
            // this contains datetime/date/numeric type
            return ScalarType.createType(typeName);
        }
    }
}
