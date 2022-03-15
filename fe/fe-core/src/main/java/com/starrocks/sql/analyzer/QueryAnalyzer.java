// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.base.SetQualifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class QueryAnalyzer {
    private final ConnectContext session;

    public QueryAnalyzer(ConnectContext session) {
        this.session = session;
    }

    public void analyze(StatementBase node) {
        new Visitor().process(node, new Scope(RelationId.anonymous(), new RelationFields()));
    }

    public void analyze(StatementBase node, Scope parent) {
        new Visitor().process(node, parent);
    }

    private class Visitor extends AstVisitor<Scope, Scope> {
        public Visitor() {
        }

        public Scope process(ParseNode node, Scope scope) {
            return node.accept(this, scope);
        }

        @Override
        public Scope visitQueryStatement(QueryStatement node, Scope parent) {
            if (node.hasOutFileClause()) {
                try {
                    node.getOutFileClause().analyze();
                } catch (AnalysisException e) {
                    throw new SemanticException(e.getMessage());
                }
            }
            return visitQueryRelation(node.getQueryRelation(), parent);
        }

        @Override
        public Scope visitQueryRelation(QueryRelation node, Scope parent) {
            Scope scope = analyzeCTE(node, parent);
            return process(node, scope);
        }

        private Scope analyzeCTE(QueryRelation stmt, Scope scope) {
            Scope cteScope = new Scope(RelationId.anonymous(), new RelationFields());
            cteScope.setParent(scope);

            if (!stmt.hasWithClause()) {
                return cteScope;
            }

            for (CTERelation withQuery : stmt.getCteRelations()) {
                QueryRelation query = withQuery.getCteQuery();
                process(new QueryStatement(withQuery.getCteQuery()), cteScope);

                /*
                 *  Because the analysis of CTE is sensitive to order
                 *  the latter CTE can call the previous resolved CTE,
                 *  and the previous CTE can rewrite the existing table name.
                 *  So here will save an increasing AnalyzeState to add cte scope
                 */

                /*
                 * use cte column name as output scope of subquery relation fields
                 */
                ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
                ImmutableList.Builder<String> columnOutputNames = ImmutableList.builder();
                for (int fieldIdx = 0; fieldIdx < query.getRelationFields().getAllFields().size();
                        ++fieldIdx) {
                    Field originField = query.getRelationFields().getFieldByIndex(fieldIdx);

                    String database = originField.getRelationAlias() == null ? session.getDatabase() :
                            originField.getRelationAlias().getDb();
                    TableName tableName = new TableName(database, withQuery.getName());
                    outputFields.add(new Field(
                            withQuery.getColLabels() == null ? originField.getName() :
                                    withQuery.getColLabels().get(fieldIdx),
                            originField.getType(),
                            tableName,
                            originField.getOriginExpression()));
                    columnOutputNames.add(withQuery.getColLabels() == null ? originField.getName() :
                            withQuery.getColLabels().get(fieldIdx));
                }
                withQuery.setColumnOutputNames(columnOutputNames.build());
                withQuery.setScope(new Scope(RelationId.of(withQuery), new RelationFields(outputFields.build())));
                cteScope.addCteQueries(withQuery.getName(), withQuery);
            }

            return cteScope;
        }

        @Override
        public Scope visitSelect(SelectRelation selectRelation, Scope scope) {
            AnalyzeState analyzeState = new AnalyzeState();
            Relation resolvedRelation = resolveTableRef(selectRelation.getRelation(), scope);
            if (resolvedRelation instanceof TableFunctionRelation) {
                throw unsupportedException("Table function must be used with lateral join");
            }
            selectRelation.setRelation(resolvedRelation);
            Scope sourceScope = process(resolvedRelation, scope);
            sourceScope.setParent(scope);

            SelectAnalyzer selectAnalyzer = new SelectAnalyzer(session);
            selectAnalyzer.analyze(
                    analyzeState,
                    selectRelation.getSelectList(),
                    selectRelation.getRelation(),
                    sourceScope,
                    selectRelation.getGroupByClause(),
                    selectRelation.getHavingClause(),
                    selectRelation.getWhereClause(),
                    selectRelation.getOrderBy(),
                    selectRelation.getLimit());

            selectRelation.fillResolvedAST(analyzeState);
            return analyzeState.getOutputScope();
        }

        private Relation resolveTableRef(Relation relation, Scope scope) {
            if (relation instanceof JoinRelation) {
                JoinRelation join = (JoinRelation) relation;
                join.setLeft(resolveTableRef(join.getLeft(), scope));
                join.setRight(resolveTableRef(join.getRight(), scope));
                return join;
            } else if (relation instanceof TableRelation) {
                TableRelation tableRelation = (TableRelation) relation;
                TableName tableName = tableRelation.getName();
                if (tableName != null && Strings.isNullOrEmpty(tableName.getDb())) {
                    Optional<CTERelation> withQuery = scope.getCteQueries(tableName.getTbl());
                    if (withQuery.isPresent()) {
                        CTERelation cteRelation = withQuery.get();
                        RelationFields withRelationFields = withQuery.get().getRelationFields();
                        ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

                        for (int fieldIdx = 0; fieldIdx < withRelationFields.getAllFields().size(); ++fieldIdx) {
                            Field originField = withRelationFields.getAllFields().get(fieldIdx);
                            outputFields.add(new Field(
                                    originField.getName(), originField.getType(), tableRelation.getAlias(),
                                    originField.getOriginExpression()));
                        }

                        // The CTERelation stored in the Scope is not used directly here, but a new Relation is copied.
                        // It is because we hope to obtain a new RelationId to distinguish multiple cte reuses.
                        // Because the reused cte should not be considered the same relation.
                        // eg: with w as (select * from t0) select v1,sum(v2) from w group by v1 " +
                        //                "having v1 in (select v3 from w where v2 = 2)
                        // cte used in outer query and subquery can't use same relation-id and field
                        CTERelation newCteRelation = new CTERelation(cteRelation.getCteId(), tableName.getTbl(),
                                cteRelation.getColumnOutputNames(),
                                cteRelation.getCteQuery());
                        newCteRelation.setAlias(tableRelation.getAlias());
                        newCteRelation.setResolvedInFromClause(true);
                        newCteRelation.setScope(
                                new Scope(RelationId.of(newCteRelation), new RelationFields(outputFields.build())));
                        return newCteRelation;
                    }
                }

                Table table = resolveTable(tableRelation.getName());
                if (table instanceof View) {
                    View view = (View) table;
                    QueryStatement queryStatement = view.getQueryStatement();
                    ViewRelation viewRelation =
                            new ViewRelation(tableName, view, queryStatement.getQueryRelation());
                    viewRelation.setAlias(tableName);
                    return viewRelation;
                } else {
                    if (table.isSupported()) {
                        tableRelation.setTable(table);
                        return tableRelation;
                    } else {
                        throw unsupportedException("unsupported scan table type: " + table.getType());
                    }
                }
            } else {
                return relation;
            }
        }

        @Override
        public Scope visitTable(TableRelation node, Scope outerScope) {
            TableName tableName = node.getAlias();
            Table table = node.getTable();

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            ImmutableMap.Builder<Field, Column> columns = ImmutableMap.builder();

            for (Column column : table.getFullSchema()) {
                Field field;
                if (table.getBaseSchema().contains(column)) {
                    field = new Field(column.getName(), column.getType(), tableName, null, true);
                } else {
                    field = new Field(column.getName(), column.getType(), tableName, null, false);
                }
                columns.put(field, column);
                fields.add(field);
            }

            node.setColumns(columns.build());
            session.getDumpInfo().addTable(node.getName().getDb().split(":")[1], table);

            Scope scope = new Scope(RelationId.of(node), new RelationFields(fields.build()));
            node.setScope(scope);
            return scope;
        }

        @Override
        public Scope visitCTE(CTERelation cteRelation, Scope context) {
            QueryRelation query = cteRelation.getCteQuery();
            TableName tableName = cteRelation.getAlias();

            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
            for (int fieldIdx = 0; fieldIdx < query.getRelationFields().getAllFields().size(); ++fieldIdx) {
                Field originField = query.getRelationFields().getFieldByIndex(fieldIdx);
                outputFields.add(new Field(
                        cteRelation.getColLabels() == null ? originField.getName() :
                                cteRelation.getColLabels().get(fieldIdx),
                        originField.getType(),
                        tableName, originField.getOriginExpression()));
            }
            Scope scope = new Scope(RelationId.of(cteRelation), new RelationFields(outputFields.build()));
            cteRelation.setScope(scope);
            return scope;
        }

        @Override
        public Scope visitJoin(JoinRelation join, Scope parentScope) {
            Scope leftScope = process(join.getLeft(), parentScope);
            Scope rightScope;
            if (join.getRight() instanceof TableFunctionRelation || join.isLateral()) {
                if (!(join.getRight() instanceof TableFunctionRelation)) {
                    throw new SemanticException("Only support lateral join with UDTF");
                }

                if (!join.getType().isInnerJoin() && !join.getType().isCrossJoin()) {
                    throw new SemanticException("Not support lateral join except inner or cross");
                }
                rightScope = process(join.getRight(), leftScope);
            } else {
                rightScope = process(join.getRight(), parentScope);
            }

            Expr joinEqual = join.getOnPredicate();
            if (join.getUsingColNames() != null) {
                Expr resolvedUsing = analyzeJoinUsing(join.getUsingColNames(), leftScope, rightScope);
                if (joinEqual == null) {
                    joinEqual = resolvedUsing;
                } else {
                    joinEqual = new CompoundPredicate(CompoundPredicate.Operator.AND, joinEqual, resolvedUsing);
                }
                join.setOnPredicate(joinEqual);
            }

            if (!join.getJoinHint().isEmpty()) {
                analyzeJoinHints(join);
            }

            if (joinEqual != null) {
                if (joinEqual.contains(Subquery.class)) {
                    throw new SemanticException("Not support use subquery in ON clause");
                }

                /*
                 * sourceRelation.getRelationFields() is used to represent the column information of output.
                 * To ensure the OnPredicate in semi/anti is correct, the relation needs to be re-assembled here
                 * with left child and right child relationFields
                 */
                analyzeExpression(joinEqual, new AnalyzeState(), new Scope(RelationId.of(join),
                        leftScope.getRelationFields().joinWith(rightScope.getRelationFields())));

                AnalyzerUtils.verifyNoAggregateFunctions(joinEqual, "JOIN");
                AnalyzerUtils.verifyNoWindowFunctions(joinEqual, "JOIN");
                AnalyzerUtils.verifyNoGroupingFunctions(joinEqual, "JOIN");

                if (!joinEqual.getType().matchesType(Type.BOOLEAN) && !joinEqual.getType().matchesType(Type.NULL)) {
                    throw new SemanticException("WHERE clause must evaluate to a boolean: actual type %s",
                            joinEqual.getType());
                }
            } else {
                if (join.getType().isOuterJoin() || join.getType().isSemiAntiJoin()) {
                    throw new SemanticException(join.getType() + " requires an ON or USING clause.");
                }
            }

            /*
             * New Scope needs to be constructed for select in semi/anti join
             */
            Scope scope;
            if (join.getType().isLeftSemiAntiJoin()) {
                scope = new Scope(RelationId.of(join), leftScope.getRelationFields());
            } else if (join.getType().isRightSemiAntiJoin()) {
                scope = new Scope(RelationId.of(join), rightScope.getRelationFields());
            } else {
                scope = new Scope(RelationId.of(join),
                        leftScope.getRelationFields().joinWith(rightScope.getRelationFields()));
            }
            join.setScope(scope);
            return scope;
        }

        private Expr analyzeJoinUsing(List<String> usingColNames, Scope left, Scope right) {
            Expr joinEqual = null;
            for (String colName : usingColNames) {
                TableName leftTableName =
                        left.resolveField(new SlotRef(null, colName)).getField().getRelationAlias();
                TableName rightTableName =
                        right.resolveField(new SlotRef(null, colName)).getField().getRelationAlias();

                // create predicate "<left>.colName = <right>.colName"
                BinaryPredicate resolvedUsing = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(leftTableName, colName), new SlotRef(rightTableName, colName));

                if (joinEqual == null) {
                    joinEqual = resolvedUsing;
                } else {
                    joinEqual = new CompoundPredicate(CompoundPredicate.Operator.AND, joinEqual, resolvedUsing);
                }
            }
            return joinEqual;
        }

        private void analyzeJoinHints(JoinRelation join) {
            if (join.getJoinHint().equalsIgnoreCase("BROADCAST")) {
                if (join.getType() == JoinOperator.RIGHT_OUTER_JOIN
                        || join.getType() == JoinOperator.FULL_OUTER_JOIN
                        || join.getType() == JoinOperator.RIGHT_SEMI_JOIN
                        || join.getType() == JoinOperator.RIGHT_ANTI_JOIN) {
                    throw new SemanticException(join.getType().toString() + " does not support BROADCAST.");
                }
            } else if (join.getJoinHint().equalsIgnoreCase("SHUFFLE")) {
                if (join.getType() == JoinOperator.CROSS_JOIN ||
                        (join.getType() == JoinOperator.INNER_JOIN && join.getOnPredicate() == null)) {
                    throw new SemanticException("CROSS JOIN does not support SHUFFLE.");
                }
            } else if ("BUCKET".equalsIgnoreCase(join.getJoinHint()) ||
                    "COLOCATE".equalsIgnoreCase(join.getJoinHint())) {
                if (join.getType() == JoinOperator.CROSS_JOIN) {
                    throw new SemanticException("CROSS JOIN does not support " + join.getJoinHint() + ".");
                }
            } else {
                throw new SemanticException("JOIN hint not recognized: " + join.getJoinHint());
            }
        }

        @Override
        public Scope visitSubquery(SubqueryRelation subquery, Scope context) {
            if (subquery.getAlias() == null) {
                throw new SemanticException("Every derived table must have its own alias");
            }

            Scope queryOutputScope = process(new QueryStatement(subquery.getQuery()), context);

            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
            for (Field field : queryOutputScope.getRelationFields().getAllFields()) {
                outputFields.add(new Field(field.getName(), field.getType(),
                        subquery.getAlias(), field.getOriginExpression()));
            }
            Scope scope = new Scope(RelationId.of(subquery), new RelationFields(outputFields.build()));
            subquery.setScope(scope);
            return scope;
        }

        @Override
        public Scope visitView(ViewRelation node, Scope scope) {
            process(new QueryStatement(node.getQuery()), scope);

            View view = node.getView();
            TableName tableName = node.getName();
            List<Field> fields = Lists.newArrayList();
            for (Column column : view.getBaseSchema()) {
                Field field = new Field(column.getName(), column.getType(), tableName, null, true);
                fields.add(field);
            }

            Scope viewScope = new Scope(RelationId.of(node), new RelationFields(fields));
            node.setScope(viewScope);
            return viewScope;
        }

        @Override
        public Scope visitUnion(UnionRelation node, Scope context) {
            return analyzeSetOperation(node, context);
        }

        @Override
        public Scope visitExcept(ExceptRelation node, Scope context) {
            if (node.getQualifier().equals(SetQualifier.ALL)) {
                throw new SemanticException("EXCEPT does not support ALL qualifier");
            }
            return analyzeSetOperation(node, context);
        }

        @Override
        public Scope visitIntersect(IntersectRelation node, Scope context) {
            if (node.getQualifier().equals(SetQualifier.ALL)) {
                throw new SemanticException("INTERSECT does not support ALL qualifier");
            }
            return analyzeSetOperation(node, context);
        }

        private Scope analyzeSetOperation(SetOperationRelation node, Scope context) {
            List<QueryRelation> setOpRelations = node.getRelations();

            Scope leftChildScope = process(setOpRelations.get(0), context);
            Type[] outputTypes = leftChildScope.getRelationFields().getAllFields()
                    .stream().map(Field::getType).toArray(Type[]::new);
            int outputSize = leftChildScope.getRelationFields().size();

            for (int i = 1; i < setOpRelations.size(); ++i) {
                Scope relation = process(setOpRelations.get(i), context);
                if (relation.getRelationFields().size() != outputSize) {
                    throw new SemanticException("Operands have unequal number of columns");
                }
                for (int fieldIdx = 0; fieldIdx < relation.getRelationFields().size(); ++fieldIdx) {
                    Type fieldType = relation.getRelationFields().getAllFields().get(fieldIdx).getType();
                    if (fieldType.isOnlyMetricType() &&
                            !((node instanceof UnionRelation) &&
                                    (node.getQualifier().equals(SetQualifier.ALL)))) {
                        throw new SemanticException("%s not support set operation", fieldType);
                    }

                    Type commonType = TypeManager.getCommonSuperType(outputTypes[fieldIdx],
                            relation.getRelationFields().getFieldByIndex(fieldIdx).getType());
                    if (!commonType.isValid()) {
                        throw new SemanticException(String.format("Incompatible return types '%s' and '%s'",
                                outputTypes[fieldIdx],
                                relation.getRelationFields().getFieldByIndex(fieldIdx).getType()));
                    }
                    outputTypes[fieldIdx] = commonType;
                }
            }

            ArrayList<Field> fields = new ArrayList<>();
            for (int fieldIdx = 0; fieldIdx < outputSize; ++fieldIdx) {
                Field oldField = leftChildScope.getRelationFields().getFieldByIndex(fieldIdx);
                fields.add(new Field(oldField.getName(), outputTypes[fieldIdx], oldField.getRelationAlias(),
                        oldField.getOriginExpression()));
            }

            Scope setOpOutputScope = new Scope(RelationId.of(node), new RelationFields(fields));

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
                        expression = outputExpressions.get((int) ordinal - 1);
                    }

                    analyzeExpression(expression, new AnalyzeState(), setOpOutputScope);

                    if (!expression.getType().canOrderBy()) {
                        throw new SemanticException(Type.OnlyMetricTypeErrorMsg);
                    }

                    orderByElement.setExpr(expression);
                }
            }

            node.setScope(setOpOutputScope);
            node.setColumnOutputNames(setOpRelations.get(0).getColumnOutputNames());
            return setOpOutputScope;
        }

        @Override
        public Scope visitValues(ValuesRelation node, Scope scope) {
            AnalyzeState analyzeState = new AnalyzeState();

            List<Expr> firstRow = node.getRow(0);
            firstRow.forEach(e -> analyzeExpression(e, analyzeState, scope));
            List<List<Expr>> rows = node.getRows();
            Type[] outputTypes = firstRow.stream().map(Expr::getType).toArray(Type[]::new);
            for (List<Expr> row : rows) {
                if (row.size() != firstRow.size()) {
                    throw new SemanticException("Values have unequal number of columns");
                }
                for (int fieldIdx = 0; fieldIdx < row.size(); ++fieldIdx) {
                    analyzeExpression(row.get(fieldIdx), analyzeState, scope);
                    Type commonType =
                            TypeManager.getCommonSuperType(outputTypes[fieldIdx], row.get(fieldIdx).getType());
                    if (!commonType.isValid()) {
                        throw new SemanticException(String.format("Incompatible return types '%s' and '%s'",
                                outputTypes[fieldIdx], row.get(fieldIdx).getType()));
                    }
                    outputTypes[fieldIdx] = commonType;
                }
            }
            List<Field> fields = new ArrayList<>();
            for (int fieldIdx = 0; fieldIdx < outputTypes.length; ++fieldIdx) {
                fields.add(new Field(node.getColumnOutputNames().get(fieldIdx), outputTypes[fieldIdx], null,
                        rows.get(0).get(fieldIdx)));
            }

            Scope valuesScope = new Scope(RelationId.of(node), new RelationFields(fields));
            node.setScope(valuesScope);
            return valuesScope;
        }

        @Override
        public Scope visitTableFunction(TableFunctionRelation node, Scope scope) {
            AnalyzeState analyzeState = new AnalyzeState();
            List<Expr> args = node.getFunctionParams().exprs();
            Type[] argTypes = new Type[args.size()];
            for (int i = 0; i < args.size(); ++i) {
                analyzeExpression(args.get(i), analyzeState, scope);
                argTypes[i] = args.get(i).getType();

                AnalyzerUtils.verifyNoAggregateFunctions(args.get(i), "UNNEST");
                AnalyzerUtils.verifyNoWindowFunctions(args.get(i), "UNNEST");
                AnalyzerUtils.verifyNoGroupingFunctions(args.get(i), "UNNEST");
            }
            TableFunction fn = (TableFunction) Expr.getBuiltinFunction(node.getFunctionName().getFunction(), argTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

            if (fn == null) {
                throw new SemanticException("Unknown table function '%s(%s)'", node.getFunctionName().getFunction(),
                        Arrays.stream(argTypes).map(Object::toString).collect(Collectors.joining(",")));
            }

            node.setTableFunction(fn);
            node.setChildExpressions(node.getFunctionParams().exprs());

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (int i = 0; i < fn.getTableFnReturnTypes().size(); ++i) {
                Field field = new Field(fn.getDefaultColumnNames().get(i),
                        fn.getTableFnReturnTypes().get(i), node.getAlias(), null);
                fields.add(field);
            }

            Scope outputScope = new Scope(RelationId.of(node), new RelationFields(fields.build()));
            node.setScope(outputScope);
            return outputScope;
        }
    }

    private Table resolveTable(TableName tableName) {
        try {
            MetaUtils.normalizationTableName(session, tableName);
            String dbName = tableName.getDb();
            String tbName = tableName.getTbl();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }

            Database database = session.getCatalog().getDb(dbName);
            if (database == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
            }
            Table table = database.getTable(tbName);
            if (table == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, tbName);
            }

            if (table.getType() == Table.TableType.OLAP &&
                    (((OlapTable) table).getState() == OlapTable.OlapTableState.RESTORE
                            || ((OlapTable) table).getState() == OlapTable.OlapTableState.RESTORE_WITH_LOAD)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_STATE, "RESTORING");
            }
            return table;
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    private void analyzeExpression(Expr expr, AnalyzeState analyzeState, Scope scope) {
        ExpressionAnalyzer.analyzeExpression(expr, analyzeState, scope, session);
    }
}
