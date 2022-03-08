// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionTableRef;
import com.starrocks.analysis.InlineViewRef;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.QueryStmt;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SelectStmt;
import com.starrocks.analysis.SetOperationStmt;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.Catalog;
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
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.base.SetQualifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.sql.analyzer.AnalyzerUtils.verifyNoAggregateFunctions;
import static com.starrocks.sql.analyzer.AnalyzerUtils.verifyNoGroupingFunctions;
import static com.starrocks.sql.analyzer.AnalyzerUtils.verifyNoWindowFunctions;
import static com.starrocks.sql.common.ErrorType.INTERNAL_ERROR;
import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class QueryAnalyzer {
    private static final Logger LOG = LogManager.getLogger(QueryAnalyzer.class);

    private final Catalog catalog;
    private final ConnectContext session;

    public QueryAnalyzer(Catalog catalog, ConnectContext session) {
        this.catalog = catalog;
        this.session = session;
    }

    public QueryRelation transformQueryStmt(QueryStmt stmt, Scope parent) {
        try {
            stmt.analyzeOutfile();
        } catch (AnalysisException e) {
            throw new StarRocksPlannerException("Error query statement: " + e.getMessage(), INTERNAL_ERROR);
        }

        Scope scope = analyzeCTE(stmt, parent);
        QueryRelation queryRelation;
        if (stmt instanceof SelectStmt) {
            SelectStmt selectStmt = (SelectStmt) stmt;

            if (selectStmt.getValueList() != null) {
                AnalyzeState analyzeState = new AnalyzeState();

                List<Expr> firstRow = selectStmt.getValueList().getFirstRow();
                firstRow.forEach(e -> analyzeExpression(e, analyzeState, scope));

                List<ArrayList<Expr>> rows = selectStmt.getValueList().getRows();
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
                    fields.add(new Field("column_" + fieldIdx, outputTypes[fieldIdx], null, rows.get(0).get(fieldIdx)));
                }

                queryRelation = new ValuesRelation(rows,
                        fields.stream().map(Field::getName).collect(Collectors.toList()));
                queryRelation.setScope(new Scope(RelationId.of(queryRelation), new RelationFields(fields)));
            } else if (selectStmt.getTableRefs().size() == 0) {
                AnalyzeState analyzeState = new AnalyzeState();

                ArrayList<Expr> row = new ArrayList<>();
                for (SelectListItem selectListItem : selectStmt.getSelectList().getItems()) {
                    analyzeExpression(selectListItem.getExpr(), analyzeState, scope);
                    row.add(selectListItem.getExpr());
                }
                Scope outputScope = computeAndAssignOutputScope(selectStmt, analyzeState, scope);

                List<ArrayList<Expr>> rows = new ArrayList<>();
                rows.add(row);
                queryRelation = new ValuesRelation(rows,
                        outputScope.getRelationFields().getAllFields()
                                .stream().map(Field::getName).collect(Collectors.toList()));
                queryRelation.setScope(outputScope);
            } else {
                queryRelation = transformSelectStmt((SelectStmt) stmt, scope);
            }
        } else if (stmt instanceof SetOperationStmt) {
            queryRelation = transformSetOperationStmt((SetOperationStmt) stmt, scope);
        } else {
            throw new StarRocksPlannerException("Error query statement", INTERNAL_ERROR);
        }

        for (Map.Entry<String, CTERelation> entry : scope.getAllCteQueries().entrySet()) {
            queryRelation.addCTERelation(entry.getValue());
        }
        return queryRelation;
    }

    public SelectRelation transformSelectStmt(SelectStmt stmt, Scope parent) {
        AnalyzeState analyzeState = new AnalyzeState();
        Scope sourceScope = analyzeFrom(stmt, analyzeState, parent);
        sourceScope.setParent(parent);

        SelectAnalyzer selectAnalyzer = new SelectAnalyzer(catalog, session);
        selectAnalyzer.analyze(
                analyzeState,
                stmt.getSelectList(),
                analyzeState.getRelation(),
                sourceScope,
                stmt.getGroupByClause(),
                stmt.getHavingClause(),
                stmt.getWhereClause(),
                stmt.getOrderByElements(),
                stmt.getLimitClause());
        return analyzeState.build();
    }

    public SetOperationRelation transformSetOperationStmt(SetOperationStmt stmt, Scope parent) {
        if (stmt.getOperands().size() < 2) {
            throw new StarRocksPlannerException("Set operation must have multi operand", INTERNAL_ERROR);
        }

        List<QueryRelation> setOpRelations = stmt.getOperands().stream()
                .map(setOperand -> transformQueryStmt(setOperand.getQueryStmt(), parent))
                .collect(Collectors.toList());

        QueryRelation setOpRelation = setOpRelations.get(0);
        for (int i = 1; i < setOpRelations.size(); ++i) {
            Type[] outputTypes =
                    setOpRelation.getRelationFields().getAllFields().stream().map(Field::getType).toArray(Type[]::new);
            int outputSize = setOpRelation.getRelationFields().size();

            SetOperationStmt.SetOperand setOperand = stmt.getOperands().get(i);
            QueryRelation relation = setOpRelations.get(i);
            if (relation.getRelationFields().size() != outputSize) {
                throw new SemanticException("Operands have unequal number of columns");
            }
            for (int fieldIdx = 0; fieldIdx < relation.getRelationFields().size(); ++fieldIdx) {
                Type fieldType = relation.getRelationFields().getAllFields().get(fieldIdx).getType();
                if (fieldType.isOnlyMetricType() &&
                        !((setOperand.getOperation().equals(SetOperationStmt.Operation.UNION)) &&
                                (setOperand.getQualifier().equals(SetOperationStmt.Qualifier.ALL)))) {
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

            ArrayList<Field> fields = new ArrayList<>();
            for (int fieldIdx = 0; fieldIdx < outputSize; ++fieldIdx) {
                Field oldField = setOpRelation.getRelationFields().getFieldByIndex(fieldIdx);
                fields.add(new Field(oldField.getName(), outputTypes[fieldIdx],
                        oldField.getRelationAlias(), oldField.getOriginExpression()));
            }

            if (setOperand.getOperation().equals(SetOperationStmt.Operation.UNION)) {
                if (setOpRelation instanceof UnionRelation && ((UnionRelation) setOpRelation).getQualifier()
                        .equals(SetQualifier.convert(setOperand.getQualifier()))) {
                    ((UnionRelation) setOpRelation).addRelation(relation);
                } else {
                    setOpRelation = new UnionRelation(Arrays.asList(setOpRelation, relation),
                            SetQualifier.convert(setOperand.getQualifier()));
                }
            } else if (setOperand.getOperation().equals(SetOperationStmt.Operation.EXCEPT)) {
                if (setOperand.getQualifier().equals(SetOperationStmt.Qualifier.ALL)) {
                    throw new SemanticException("EXCEPT does not support ALL qualifier");
                }

                if (setOpRelation instanceof ExceptRelation) {
                    ((ExceptRelation) setOpRelation).addRelation(relation);
                } else {
                    setOpRelation = new ExceptRelation(Arrays.asList(setOpRelation, relation),
                            SetQualifier.convert(setOperand.getQualifier()));
                }
            } else if (setOperand.getOperation().equals(SetOperationStmt.Operation.INTERSECT)) {
                if (setOperand.getQualifier().equals(SetOperationStmt.Qualifier.ALL)) {
                    throw new SemanticException("INTERSECT does not support ALL qualifier");
                }

                if (setOpRelation instanceof IntersectRelation) {
                    ((IntersectRelation) setOpRelation).addRelation(relation);
                } else {
                    setOpRelation = new IntersectRelation(Arrays.asList(setOpRelation, relation),
                            SetQualifier.convert(setOperand.getQualifier()));
                }
            } else {
                throw new StarRocksPlannerException(
                        "Unsupported set operation " + stmt.getOperands().get(i).getOperation(),
                        INTERNAL_ERROR);
            }

            setOpRelation.setScope(new Scope(RelationId.of(setOpRelation), new RelationFields(fields)));
        }
        return (SetOperationRelation) setOpRelation;
    }

    private Scope analyzeCTE(QueryStmt stmt, Scope scope) {
        Scope cteScope = new Scope(RelationId.anonymous(), new RelationFields());
        cteScope.setParent(scope);

        if (!stmt.hasWithClause()) {
            return cteScope;
        }

        for (View withQuery : stmt.getWithClause().getViews()) {
            QueryRelation query = transformQueryStmt(withQuery.getQueryStmtWithParse(), cteScope);

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
            for (int fieldIdx = 0; fieldIdx < query.getRelationFields().getAllFields().size(); ++fieldIdx) {
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

            CTERelation cteRelation = new CTERelation(RelationId.of(query).hashCode(),
                    withQuery.getName(),
                    columnOutputNames.build(),
                    query);
            cteRelation.setScope(new Scope(RelationId.of(cteRelation), new RelationFields(outputFields.build())));
            cteScope.addCteQueries(withQuery.getName(), cteRelation);
        }
        return cteScope;
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

    private Scope analyzeFrom(SelectStmt node, AnalyzeState analyzeState, Scope scope) {
        Relation sourceRelation = null;
        TableRef lastTableRef = null;

        if (node.getTableRefs().size() == 1 && node.getTableRefs().get(0) instanceof FunctionTableRef) {
            throw unsupportedException("Table function must be used with lateral join");
        }

        for (TableRef tableRef : node.getTableRefs()) {
            Scope resolveTableScope;

            if (sourceRelation != null && (tableRef.isLateral() || tableRef instanceof FunctionTableRef)) {
                if (!(tableRef instanceof FunctionTableRef)) {
                    throw new SemanticException("Only support lateral join with UDTF");
                }

                if (!(tableRef.getJoinOp().isCrossJoin() || tableRef.getJoinOp().isInnerJoin())) {
                    throw new SemanticException("Not support lateral join except inner or cross");
                }

                resolveTableScope = new Scope(RelationId.of(sourceRelation), sourceRelation.getRelationFields());
                resolveTableScope.setParent(scope);
            } else {
                resolveTableScope = scope;
            }

            Relation relation = resolveTableRef(tableRef, analyzeState, resolveTableScope);
            if (sourceRelation != null) {
                if (lastTableRef.getAliasAsName().equals(tableRef.getAliasAsName())) {
                    throw new SemanticException("Not unique table/alias %s", tableRef.getAliasAsName());
                }

                Expr joinEqual = tableRef.getOnClause();
                if (tableRef.getUsingColNames() != null) {
                    Expr resolvedUsing = analyzeJoinUsing(tableRef.getUsingColNames(),
                            sourceRelation.getScope(), relation.getScope());
                    if (joinEqual == null) {
                        joinEqual = resolvedUsing;
                    } else {
                        joinEqual = new CompoundPredicate(CompoundPredicate.Operator.AND, joinEqual, resolvedUsing);
                    }
                }

                JoinRelation joinRelation = new JoinRelation(tableRef.getJoinOp(), sourceRelation, relation, joinEqual,
                        tableRef.isLateral());
                /*
                 * New Scope needs to be constructed for select in semi/anti join
                 */
                Scope joinScope;
                if (tableRef.getJoinOp().isLeftSemiAntiJoin()) {
                    joinScope = sourceRelation.getScope();
                } else if (tableRef.getJoinOp().isRightSemiAntiJoin()) {
                    joinScope = relation.getScope();
                } else {
                    joinScope = new Scope(RelationId.of(joinRelation),
                            sourceRelation.getRelationFields().joinWith(relation.getRelationFields()));
                }
                joinRelation.setScope(joinScope);
                sourceRelation = joinRelation;

                if (tableRef.getJoinHints() != null) {
                    ((JoinRelation) sourceRelation).setJoinHint(tableRef.getJoinHints().get(0));
                    analyzeJoinHints((JoinRelation) sourceRelation);
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
                    JoinRelation join = (JoinRelation) sourceRelation;
                    analyzeExpression(joinEqual, analyzeState, new Scope(RelationId.of(sourceRelation),
                            join.getLeft().getRelationFields().joinWith(join.getRight().getRelationFields())));

                    verifyNoAggregateFunctions(joinEqual, "JOIN");
                    verifyNoWindowFunctions(joinEqual, "JOIN");
                    verifyNoGroupingFunctions(joinEqual, "JOIN");

                    if (!joinEqual.getType().matchesType(Type.BOOLEAN) && !joinEqual.getType().matchesType(Type.NULL)) {
                        throw new SemanticException("WHERE clause must evaluate to a boolean: actual type %s",
                                joinEqual.getType());
                    }
                } else {
                    if (tableRef.getJoinOp().isOuterJoin() || tableRef.getJoinOp().isSemiAntiJoin()) {
                        throw new SemanticException(tableRef.getJoinOp() + " requires an ON or USING clause.");
                    }
                }
            } else {
                sourceRelation = relation;
            }
            lastTableRef = tableRef;
        }

        analyzeState.setRelation(sourceRelation);
        return sourceRelation.getScope();
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
        } else if ("BUCKET".equalsIgnoreCase(join.getJoinHint()) || "COLOCATE".equalsIgnoreCase(join.getJoinHint())) {
            if (join.getType() == JoinOperator.CROSS_JOIN) {
                throw new SemanticException("CROSS JOIN does not support " + join.getJoinHint() + ".");
            }
        } else {
            throw new SemanticException("JOIN hint not recognized: " + join.getJoinHint());
        }
    }

    private TableFunction getUDTF(String fnName, Type[] argTypes) {
        String dbName = session.getDatabase();

        if (!catalog.getAuth().checkDbPriv(session, dbName, PrivPredicate.SELECT)) {
            throw new StarRocksPlannerException("Access denied. need the SELECT " + dbName + " privilege(s)",
                    ErrorType.USER_ERROR);
        }

        Database db = catalog.getDb(dbName);
        if (db == null) {
            return null;
        }

        Function search = new Function(new FunctionName(dbName, fnName), argTypes, Type.INVALID, false);
        Function fn = db.getFunction(search, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

        if (fn instanceof TableFunction) {
            return (TableFunction) fn;
        }

        return null;
    }

    public Relation resolveTableRef(TableRef tableRef, AnalyzeState analyzeState, Scope scope) {
        if (tableRef.getAliasAsName() == null) {
            throw new SemanticException("Every derived table must have its own alias");
        }

        TableName tableName = tableRef.getAliasAsName();

        if (tableRef.getName() != null && Strings.isNullOrEmpty(tableName.getDb())) {
            Optional<CTERelation> withQuery = scope.getCteQueries(tableRef.getName().getTbl());
            if (withQuery.isPresent()) {
                CTERelation cteRelation = withQuery.get();
                RelationFields withRelationFields = withQuery.get().getRelationFields();
                ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

                for (int fieldIdx = 0; fieldIdx < withRelationFields.getAllFields().size(); ++fieldIdx) {
                    Field originField = withRelationFields.getAllFields().get(fieldIdx);
                    outputFields.add(new Field(
                            originField.getName(), originField.getType(), tableName,
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
                newCteRelation.setScope(
                        new Scope(RelationId.of(newCteRelation), new RelationFields(outputFields.build())));
                return newCteRelation;
            }
        }

        /*
         * Resolve subquery
         */
        if (tableRef instanceof InlineViewRef) {
            InlineViewRef viewRef = (InlineViewRef) tableRef;

            QueryRelation query = transformQueryStmt(viewRef.getViewStmt(), scope);
            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
            for (Field field : query.getRelationFields().getAllFields()) {
                outputFields.add(new Field(field.getName(), field.getType(),
                        tableName, field.getOriginExpression()));
            }

            SubqueryRelation subqueryRelation = new SubqueryRelation(tableRef.getAlias(), query);
            subqueryRelation.setScope(
                    new Scope(RelationId.of(subqueryRelation), new RelationFields(outputFields.build())));
            return subqueryRelation;
        }

        if (tableRef instanceof FunctionTableRef) {
            FunctionTableRef functionTableRef = (FunctionTableRef) tableRef;

            List<Expr> child = functionTableRef.getParams().exprs();
            Type[] argTypes = new Type[child.size()];
            for (int i = 0; i < child.size(); ++i) {
                analyzeExpression(child.get(i), analyzeState, scope);
                argTypes[i] = child.get(i).getType();

                verifyNoAggregateFunctions(child.get(i), "UNNEST");
                verifyNoWindowFunctions(child.get(i), "UNNEST");
                verifyNoGroupingFunctions(child.get(i), "UNNEST");
            }

            TableFunction fn =
                    (TableFunction) Expr.getBuiltinFunction(functionTableRef.getFnName(), argTypes,
                            Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

            if (fn == null) {
                fn = getUDTF(functionTableRef.getFnName(), argTypes);
            }

            if (fn == null) {
                throw new SemanticException("Unknown table function '%s(%s)'", functionTableRef.getFnName(),
                        Arrays.stream(argTypes).map(Object::toString).collect(Collectors.joining(",")));
            }

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (int i = 0; i < fn.getTableFnReturnTypes().size(); ++i) {
                Field field = new Field(fn.getDefaultColumnNames().get(i),
                        fn.getTableFnReturnTypes().get(i), tableRef.getAliasAsName(), null);
                fields.add(field);
            }

            TableFunctionRelation tableFunctionRelation = new TableFunctionRelation(fn, child);
            tableFunctionRelation.setScope(
                    new Scope(RelationId.of(tableFunctionRelation), new RelationFields(fields.build())));
            return tableFunctionRelation;
        }

        //Olap table
        Table table = resolveTable(tableRef);
        if (table instanceof View) {
            View view = (View) table;
            QueryRelation query = transformQueryStmt(view.getQueryStmtWithParse(), scope);

            ImmutableList.Builder<Field> outputFields = ImmutableList.builder();
            for (Field field : query.getRelationFields().getAllFields()) {
                outputFields.add(new Field(field.getName(), field.getType(),
                        tableName, field.getOriginExpression()));
            }

            SubqueryRelation subqueryRelation = new SubqueryRelation(tableRef.getAlias(), query);
            subqueryRelation.setScope(
                    new Scope(RelationId.of(subqueryRelation), new RelationFields(outputFields.build())));
            return subqueryRelation;
        } else {
            if (table.isSupported()) {
                ImmutableList.Builder<Field> fields = ImmutableList.builder();
                ImmutableMap.Builder<Field, Column> columns = ImmutableMap.builder();
                for (Column column : table.getFullSchema()) {
                    Field field = table.getBaseSchema().contains(column) ?
                            new Field(column.getName(), column.getType(), tableName, null, true) :
                            new Field(column.getName(), column.getType(), tableName, null, false);
                    columns.put(field, column);
                    fields.add(field);
                }
                TableRelation tableRelation = new TableRelation(tableName, table, columns.build(),
                        tableRef.getPartitionNames(), tableRef.getTabletIds(), tableRef.isMetaQuery());
                tableRelation.setScope(new Scope(RelationId.of(tableRelation), new RelationFields(fields.build())));

                session.getDumpInfo().addTable(tableRef.getName().getDb().split(":")[1], tableRelation.getTable());
                return tableRelation;
            } else {
                throw unsupportedException("unsupported scan table type: " + table.getType());
            }
        }
    }

    Table resolveTable(TableRef tableRef) {
        try {
            MetaUtils.normalizationTableName(session, tableRef.getName());
            String dbName = tableRef.getName().getDb();
            String tbName = tableRef.getName().getTbl();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }

            Database database = catalog.getDb(dbName);
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

    private Scope computeAndAssignOutputScope(SelectStmt stmt, AnalyzeState analyzeState, Scope scope) {
        ImmutableList.Builder<Field> outputFields = ImmutableList.builder();

        for (SelectListItem item : stmt.getSelectList().getItems()) {
            if (item.isStar()) {
                if (item.getTblName() == null) {
                    outputFields.addAll(scope.getRelationFields().getAllFields()
                            .stream().filter(Field::isVisible)
                            .map(f -> new Field(f.getName(), f.getType(), f.getRelationAlias(),
                                    f.getOriginExpression(), f.isVisible())).collect(Collectors.toList()));
                } else {
                    outputFields.addAll(scope.getRelationFields().resolveFieldsWithPrefix(item.getTblName())
                            .stream().filter(Field::isVisible)
                            .map(f -> new Field(f.getName(), f.getType(), f.getRelationAlias(),
                                    f.getOriginExpression(), f.isVisible())).collect(Collectors.toList()));
                }
            } else {
                String name;
                TableName relationAlias = null;
                if (item.getAlias() != null) {
                    name = item.getAlias();
                } else if (item.getExpr() instanceof SlotRef) {
                    name = ((SlotRef) item.getExpr()).getColumnName();
                    relationAlias = ((SlotRef) item.getExpr()).getTblNameWithoutAnalyzed();
                } else {
                    name = item.getExpr().toColumnLabel();
                }

                outputFields.add(new Field(name, item.getExpr().getType(), relationAlias, item.getExpr()));
            }
        }
        Scope outputScope = new Scope(RelationId.anonymous(), new RelationFields(outputFields.build()));

        analyzeState.setOutputScope(outputScope);
        return outputScope;
    }

    private Scope computeAndAssignOrderScope(AnalyzeState analyzeState, Scope sourceScope, Scope outputScope) {
        // The Scope used by order by allows parsing of the same column,
        // such as 'select v1 as v, v1 as v from t0 order by v'
        // but normal parsing does not allow it. So add a de-duplication operation here.
        List<Field> allFields = new ArrayList<>();
        for (Field field : outputScope.getRelationFields().getAllFields()) {
            if (field.getName() != null && field.getOriginExpression() != null &&
                    allFields.stream().anyMatch(f ->
                            f.getOriginExpression() != null &&
                                    f.getName() != null &&
                                    field.getName().equals(f.getName()) &&
                                    field.getOriginExpression().equals(f.getOriginExpression()))) {
                continue;
            }
            allFields.add(field);
        }

        Scope orderScope = new Scope(outputScope.getRelationId(), new RelationFields(allFields));
        /*
         * ORDER BY or HAVING should "see" both output and FROM fields
         * Because output scope and source scope may contain the same columns,
         * so they cannot be in the same level of scope to avoid ambiguous semantics
         */
        orderScope.setParent(sourceScope);
        analyzeState.setOrderScope(orderScope);
        return orderScope;
    }

    private void analyzeExpression(Expr expr, AnalyzeState analyzeState, Scope scope) {
        ExpressionAnalyzer.analyzeExpression(expr, analyzeState, scope, catalog, session);
    }
}
