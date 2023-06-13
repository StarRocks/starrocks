// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
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
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SetQualifier;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.dump.HiveMetaStoreTableDumpInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class QueryAnalyzer {
    private final ConnectContext session;
    private final MetadataMgr metadataMgr;

    public QueryAnalyzer(ConnectContext session) {
        this.session = session;
        this.metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
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
                QueryRelation query = withQuery.getCteQueryStatement().getQueryRelation();
                process(withQuery.getCteQueryStatement(), cteScope);
                String cteName = withQuery.getName();
                if (cteScope.containsCTE(cteName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NONUNIQ_TABLE, cteName);
                }

                if (withQuery.getColumnOutputNames() == null) {
                    withQuery.setColumnOutputNames(new ArrayList<>(query.getColumnOutputNames()));
                } else {
                    if (withQuery.getColumnOutputNames().size() != query.getColumnOutputNames().size()) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_VIEW_WRONG_LIST);
                    }
                }

                /*
                 * use cte column name as output scope of subquery relation fields
                 */
                ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
                for (int fieldIdx = 0; fieldIdx < query.getRelationFields().getAllFields().size(); ++fieldIdx) {
                    Field originField = query.getRelationFields().getFieldByIndex(fieldIdx);

                    String database = originField.getRelationAlias() == null ? session.getDatabase() :
                            originField.getRelationAlias().getDb();
                    TableName tableName = new TableName(database, cteName);
                    outputFields.add(
                            new Field(withQuery.getColumnOutputNames().get(fieldIdx), originField.getType(), tableName,
                                    originField.getOriginExpression()));
                }

                /*
                 *  Because the analysis of CTE is sensitive to order
                 *  the later CTE can call the previous resolved CTE,
                 *  and the previous CTE can rewrite the existing table name.
                 *  So here will save an increasing AnalyzeState to add cte scope
                 */
                withQuery.setScope(new Scope(RelationId.of(withQuery), new RelationFields(outputFields.build())));
                cteScope.addCteQueries(cteName, withQuery);
            }

            return cteScope;
        }

        @Override
        public Scope visitSelect(SelectRelation selectRelation, Scope scope) {
            AnalyzeState analyzeState = new AnalyzeState();
            //Record aliases at this level to prevent alias conflicts
            Set<TableName> aliasSet = new HashSet<>();
            Relation resolvedRelation = resolveTableRef(selectRelation.getRelation(), scope, aliasSet);
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

        private Relation resolveTableRef(Relation relation, Scope scope, Set<TableName> aliasSet) {
            if (relation instanceof JoinRelation) {
                JoinRelation join = (JoinRelation) relation;
                join.setLeft(resolveTableRef(join.getLeft(), scope, aliasSet));
                Relation rightRelation = resolveTableRef(join.getRight(), scope, aliasSet);
                join.setRight(rightRelation);
                if (rightRelation instanceof TableFunctionRelation) {
                    join.setLateral(true);
                }
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
                                    originField.getName(), originField.getType(), tableRelation.getResolveTableName(),
                                    originField.getOriginExpression()));
                        }

                        // The CTERelation stored in the Scope is not used directly here, but a new Relation is copied.
                        // It is because we hope to obtain a new RelationId to distinguish multiple cte reuses.
                        // Because the reused cte should not be considered the same relation.
                        // eg: with w as (select * from t0) select v1,sum(v2) from w group by v1 " +
                        //                "having v1 in (select v3 from w where v2 = 2)
                        // cte used in outer query and sub-query can't use same relation-id and field
                        CTERelation newCteRelation = new CTERelation(cteRelation.getCteMouldId(), tableName.getTbl(),
                                cteRelation.getColumnOutputNames(),
                                cteRelation.getCteQueryStatement());
                        newCteRelation.setAlias(tableRelation.getAlias());
                        newCteRelation.setResolvedInFromClause(true);
                        newCteRelation.setScope(
                                new Scope(RelationId.of(newCteRelation), new RelationFields(outputFields.build())));
                        return newCteRelation;
                    }
                }

                TableName resolveTableName = relation.getResolveTableName();
                MetaUtils.normalizationTableName(session, resolveTableName);
                if (aliasSet.contains(resolveTableName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NONUNIQ_TABLE,
                            relation.getResolveTableName().getTbl());
                } else {
                    aliasSet.add(new TableName(resolveTableName.getCatalog(),
                            resolveTableName.getDb(),
                            resolveTableName.getTbl()));
                }

                Table table = resolveTable(tableRelation.getName());
                if (table instanceof View) {
                    View view = (View) table;
                    QueryStatement queryStatement = view.getQueryStatement();
                    ViewRelation viewRelation = new ViewRelation(tableName, view, queryStatement);
                    viewRelation.setAlias(tableRelation.getAlias());
                    return viewRelation;
                } else {
                    if (tableRelation.getTemporalClause() != null) {
                        if (table.getType() != Table.TableType.MYSQL) {
                            throw unsupportedException("Unsupported table type for temporal clauses: " + table.getType() +
                                    "; only external MYSQL tables support temporal clauses");
                        }
                    }

                    if (table.isSupported()) {
                        tableRelation.setTable(table);
                        return tableRelation;
                    } else {
                        throw unsupportedException("Unsupported scan table type: " + table.getType());
                    }
                }
            } else {
                if (relation.getResolveTableName() != null) {
                    if (aliasSet.contains(relation.getResolveTableName())) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_NONUNIQ_TABLE,
                                relation.getResolveTableName().getTbl());
                    } else {
                        aliasSet.add(relation.getResolveTableName());
                    }
                }
                return relation;
            }
        }

        @Override
        public Scope visitTable(TableRelation node, Scope outerScope) {
            TableName tableName = node.getResolveTableName();
            Table table = node.getTable();

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            ImmutableMap.Builder<Field, Column> columns = ImmutableMap.builder();

            for (Column column : table.getFullSchema()) {
                Field field;
                if (table.getBaseSchema().contains(column)) {
                    field = new Field(column.getName(), column.getType(), tableName,
                            new SlotRef(tableName, column.getName(), column.getName()), true, column.isAllowNull());
                } else {
                    field = new Field(column.getName(), column.getType(), tableName,
                            new SlotRef(tableName, column.getName(), column.getName()), false, column.isAllowNull());
                }
                columns.put(field, column);
                fields.add(field);
            }

            node.setColumns(columns.build());
            String dbName = node.getName().getDb();

            session.getDumpInfo().addTable(dbName, table);
            if (table.isHiveTable()) {
                HiveTable hiveTable = (HiveTable) table;
                Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().
                        getResource(hiveTable.getResourceName());
                if (resource != null) {
                    session.getDumpInfo().addResource(resource);
                }
                session.getDumpInfo().addHMSTable(hiveTable.getResourceName(), hiveTable.getDbName(),
                        hiveTable.getTableName());
                HiveMetaStoreTableDumpInfo hiveMetaStoreTableDumpInfo = session.getDumpInfo().getHMSTable(
                        hiveTable.getResourceName(), hiveTable.getDbName(), hiveTable.getTableName());
                hiveMetaStoreTableDumpInfo.setPartColumnNames(hiveTable.getPartitionColumnNames());
                hiveMetaStoreTableDumpInfo.setDataColumnNames(hiveTable.getDataColumnNames());
            }

            Scope scope = new Scope(RelationId.of(node), new RelationFields(fields.build()));
            node.setScope(scope);
            return scope;
        }

        @Override
        public Scope visitCTE(CTERelation cteRelation, Scope context) {
            QueryRelation query = cteRelation.getCteQueryStatement().getQueryRelation();

            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
            for (int fieldIdx = 0; fieldIdx < query.getRelationFields().getAllFields().size(); ++fieldIdx) {
                Field originField = query.getRelationFields().getFieldByIndex(fieldIdx);
                outputFields.add(new Field(cteRelation.getColumnOutputNames() == null ?
                        originField.getName() : cteRelation.getColumnOutputNames().get(fieldIdx),
                        originField.getType(),
                        cteRelation.getResolveTableName(),
                        originField.getOriginExpression()));
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

                if (!join.getJoinOp().isInnerJoin() && !join.getJoinOp().isCrossJoin()) {
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
                // check the join on predicate, example:
                // we have col_json, we can't join on table_a.col_json = table_b.col_json,
                // but we can join on cast(table_a.col_json->"a" as int) = cast(table_b.col_json->"a" as int)
                // similarly, we can join on table_a.col_map['a'] = table_b.col_map['a'],
                // and table_a.col_struct.a = table_b.col_struct.a
                checkJoinEqual(joinEqual);
            } else {
                if (join.getJoinOp().isOuterJoin() || join.getJoinOp().isSemiAntiJoin()) {
                    throw new SemanticException(join.getJoinOp() + " requires an ON or USING clause.");
                }
            }

            /*
             * New Scope needs to be constructed for select in semi/anti join
             */
            Scope scope;
            if (join.getJoinOp().isLeftSemiAntiJoin()) {
                scope = new Scope(RelationId.of(join), leftScope.getRelationFields());
            } else if (join.getJoinOp().isRightSemiAntiJoin()) {
                scope = new Scope(RelationId.of(join), rightScope.getRelationFields());
            } else if (join.getJoinOp().isLeftOuterJoin()) {
                List<Field> rightFields = getFieldsWithNullable(rightScope);
                scope = new Scope(RelationId.of(join),
                        leftScope.getRelationFields().joinWith(new RelationFields(rightFields)));
            } else if (join.getJoinOp().isRightOuterJoin()) {
                List<Field> leftFields = getFieldsWithNullable(leftScope);
                scope = new Scope(RelationId.of(join),
                        new RelationFields(leftFields).joinWith(rightScope.getRelationFields()));
            } else if (join.getJoinOp().isFullOuterJoin()) {
                List<Field> rightFields = getFieldsWithNullable(rightScope);
                List<Field> leftFields = getFieldsWithNullable(leftScope);
                scope = new Scope(RelationId.of(join),
                        new RelationFields(leftFields).joinWith(new RelationFields(rightFields)));
            } else {
                scope = new Scope(RelationId.of(join),
                        leftScope.getRelationFields().joinWith(rightScope.getRelationFields()));
            }
            join.setScope(scope);
            return scope;
        }

        private List<Field> getFieldsWithNullable(Scope scope) {
            List<Field> newFields = new ArrayList<>();
            for (Field field : scope.getRelationFields().getAllFields()) {
                Field newField = new Field(field);
                newField.setNullable(true);
                newFields.add(newField);
            }
            return newFields;
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
            if (JoinOperator.HINT_BROADCAST.equals(join.getJoinHint())) {
                if (join.getJoinOp() == JoinOperator.RIGHT_OUTER_JOIN
                        || join.getJoinOp() == JoinOperator.FULL_OUTER_JOIN
                        || join.getJoinOp() == JoinOperator.RIGHT_SEMI_JOIN
                        || join.getJoinOp() == JoinOperator.RIGHT_ANTI_JOIN) {
                    throw new SemanticException(join.getJoinOp().toString() + " does not support BROADCAST.");
                }
            } else if (JoinOperator.HINT_SHUFFLE.equals(join.getJoinHint())) {
                if (join.getJoinOp() == JoinOperator.CROSS_JOIN ||
                        (join.getJoinOp() == JoinOperator.INNER_JOIN && join.getOnPredicate() == null)) {
                    throw new SemanticException("CROSS JOIN does not support SHUFFLE.");
                }
            } else if (JoinOperator.HINT_BUCKET.equals(join.getJoinHint()) ||
                    JoinOperator.HINT_COLOCATE.equals(join.getJoinHint())) {
                if (join.getJoinOp() == JoinOperator.CROSS_JOIN) {
                    throw new SemanticException("CROSS JOIN does not support " + join.getJoinHint() + ".");
                }
            } else if (!JoinOperator.HINT_UNREORDER.equals(join.getJoinHint())) {
                throw new SemanticException("JOIN hint not recognized: " + join.getJoinHint());
            }
        }

        @Override
        public Scope visitSubquery(SubqueryRelation subquery, Scope context) {
            if (subquery.getResolveTableName() != null && subquery.getResolveTableName().getTbl() == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_DERIVED_MUST_HAVE_ALIAS);
            }

            Scope queryOutputScope = process(subquery.getQueryStatement(), context);

            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
            for (Field field : queryOutputScope.getRelationFields().getAllFields()) {
                outputFields.add(new Field(field.getName(), field.getType(), subquery.getResolveTableName(),
                        field.getOriginExpression()));
            }
            Scope scope = new Scope(RelationId.of(subquery), new RelationFields(outputFields.build()));

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

                    analyzeExpression(expression, new AnalyzeState(), scope);

                    if (!expression.getType().canOrderBy()) {
                        throw new SemanticException(Type.ONLY_METRIC_TYPE_ERROR_MSG);
                    }

                    orderByElement.setExpr(expression);
                }
            }

            subquery.setScope(scope);
            return scope;
        }

        @Override
        public Scope visitView(ViewRelation node, Scope scope) {
            Scope queryOutputScope = process(node.getQueryStatement(), scope);

            View view = node.getView();
            List<Field> fields = Lists.newArrayList();
            for (int i = 0; i < view.getBaseSchema().size(); ++i) {
                Column column = view.getBaseSchema().get(i);
                Field originField = queryOutputScope.getRelationFields().getFieldByIndex(i);
                // A view can specify its column names optionally, if column names are absent,
                // the output names of the queryRelation is used as the names of the view schema,
                // so column names in view's schema are always correct. Using originField.getName
                // here will gives wrong names when user-specified view column names are different
                // from output names of the queryRelation.
                //
                // view created in previous use originField.getOriginExpression().type as column
                // types in its schema, it is incorrect, so use originField.type instead.
                Field field = new Field(column.getName(), originField.getType(), node.getResolveTableName(),
                        originField.getOriginExpression());
                fields.add(field);
            }

            String dbName = node.getName().getDb();
            session.getDumpInfo().addView(dbName, view);
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
            List<Boolean> nullables = leftChildScope.getRelationFields().getAllFields()
                    .stream().map(field -> field.isNullable()).collect(Collectors.toList());
            int outputSize = leftChildScope.getRelationFields().size();

            for (int i = 1; i < setOpRelations.size(); ++i) {
                Scope relation = process(setOpRelations.get(i), context);
                if (relation.getRelationFields().size() != outputSize) {
                    throw new SemanticException("Operands have unequal number of columns");
                }
                for (int fieldIdx = 0; fieldIdx < relation.getRelationFields().size(); ++fieldIdx) {
                    Field field = relation.getRelationFields().getAllFields().get(fieldIdx);
                    Type fieldType = field.getType();
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
                    nullables.set(fieldIdx, nullables.get(fieldIdx) | field.isNullable());
                }
            }

            ArrayList<Field> fields = new ArrayList<>();
            for (int fieldIdx = 0; fieldIdx < outputSize; ++fieldIdx) {
                Field oldField = leftChildScope.getRelationFields().getFieldByIndex(fieldIdx);
                fields.add(new Field(oldField.getName(), outputTypes[fieldIdx], oldField.getRelationAlias(),
                        oldField.getOriginExpression(), true, nullables.get(fieldIdx)));
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
                        expression = new FieldReference((int) ordinal - 1, null);
                    }

                    analyzeExpression(expression, new AnalyzeState(), setOpOutputScope);

                    if (!expression.getType().canOrderBy()) {
                        throw new SemanticException(Type.ONLY_METRIC_TYPE_ERROR_MSG);
                    }

                    orderByElement.setExpr(expression);
                }
            }

            node.setScope(setOpOutputScope);
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
                fields.add(new Field(node.getColumnOutputNames().get(fieldIdx), outputTypes[fieldIdx],
                        node.getResolveTableName(),
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

                AnalyzerUtils.verifyNoAggregateFunctions(args.get(i), "Table Function");
                AnalyzerUtils.verifyNoWindowFunctions(args.get(i), "Table Function");
                AnalyzerUtils.verifyNoGroupingFunctions(args.get(i), "Table Function");
            }

            Function fn = Expr.getBuiltinFunction(node.getFunctionName().getFunction(), argTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

            if (fn == null) {
                fn = AnalyzerUtils.getUdfFunction(session, node.getFunctionName(), argTypes);
            }

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

            if (node.getColumnNames() == null) {
                if (tableFunction.getFunctionName().getFunction().equals("unnest")) {
                    // If the unnest variadic function does not explicitly specify column name,
                    // all column names are `unnest`. This refers to the return column name of postgresql.
                    List<String> columnNames = new ArrayList<>();
                    for (int i = 0; i < tableFunction.getTableFnReturnTypes().size(); ++i) {
                        columnNames.add("unnest");
                    }
                    node.setColumnNames(columnNames);
                } else {
                    node.setColumnNames(new ArrayList<>(tableFunction.getDefaultColumnNames()));
                }
            } else {
                if (node.getColumnNames().size() != tableFunction.getTableFnReturnTypes().size()) {
                    throw new SemanticException("table %s has %s columns available but %s columns specified",
                            node.getAlias().getTbl(), node.getColumnNames().size(),
                            tableFunction.getTableFnReturnTypes().size());
                }
            }

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (int i = 0; i < tableFunction.getTableFnReturnTypes().size(); ++i) {
                String colName = node.getColumnNames().get(i);

                Field field = new Field(colName,
                        tableFunction.getTableFnReturnTypes().get(i),
                        node.getResolveTableName(),
                        new SlotRef(node.getResolveTableName(), colName, colName));
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
            String catalogName = tableName.getCatalog();
            String dbName = tableName.getDb();
            String tbName = tableName.getTbl();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }

            if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
            }

            Database database = metadataMgr.getDb(catalogName, dbName);
            MetaUtils.checkDbNullAndReport(database, dbName);

            Table table = metadataMgr.getTable(catalogName, dbName, tbName);
            if (table == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR, dbName + "." + tbName);
            }

            if (table.isNativeTable() &&
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

    public static void checkJoinEqual(Expr expr)  {
        if (expr instanceof BinaryPredicate) {
            for (Expr child : expr.getChildren()) {
                if (!child.getType().canJoinOn()) {
                    throw new SemanticException(Type.ONLY_METRIC_TYPE_ERROR_MSG);
                }
            }
        } else {
            for (Expr child : expr.getChildren()) {
                checkJoinEqual(child);
            }
        }
    }
}
