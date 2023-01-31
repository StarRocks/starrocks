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

package com.starrocks.sql.analyzer;

import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.DateUtils;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.ParsingException;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AnalyzerUtils {

    public static final Set<String> SUPPORTED_PARTITION_FORMAT = ImmutableSet.of("hour", "day", "month", "year");

    public static String getOrDefaultDatabase(String dbName, ConnectContext context) {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = context.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        return dbName;
    }

    public static void verifyNoAggregateFunctions(Expr expression, String clause) {
        List<FunctionCallExpr> functions = Lists.newArrayList();
        expression.collectAll((Predicate<Expr>) arg -> arg instanceof FunctionCallExpr &&
                arg.getFn() instanceof AggregateFunction, functions);
        if (!functions.isEmpty()) {
            throw new SemanticException("%s clause cannot contain aggregations", clause);
        }
    }

    public static void verifyNoWindowFunctions(Expr expression, String clause) {
        List<AnalyticExpr> functions = Lists.newArrayList();
        expression.collectAll((Predicate<Expr>) arg -> arg instanceof AnalyticExpr, functions);
        if (!functions.isEmpty()) {
            throw new SemanticException("%s clause cannot contain window function", clause);
        }
    }

    public static void verifyNoGroupingFunctions(Expr expression, String clause) {
        List<GroupingFunctionCallExpr> calls = Lists.newArrayList();
        expression.collectAll((Predicate<Expr>) arg -> arg instanceof GroupingFunctionCallExpr, calls);
        if (!calls.isEmpty()) {
            throw new SemanticException("%s clause cannot contain grouping", clause);
        }
    }

    public static void verifyNoSubQuery(Expr expression, String clause) {
        List<Subquery> calls = Lists.newArrayList();
        expression.collectAll((Predicate<Expr>) arg -> arg instanceof Subquery, calls);
        if (!calls.isEmpty()) {
            throw new SemanticException("%s clause cannot contain subquery", clause);
        }
    }

    public static boolean isAggregate(List<FunctionCallExpr> aggregates, List<Expr> groupByExpressions) {
        return !aggregates.isEmpty() || !groupByExpressions.isEmpty();
    }

    private static Function getDBUdfFunction(ConnectContext session, FunctionName fnName,
                                             Type[] argTypes) {
        String dbName = fnName.getDb();
        if (StringUtils.isEmpty(dbName)) {
            dbName = session.getDatabase();
        }

        Database db = session.getGlobalStateMgr().getDb(dbName);
        if (db == null) {
            return null;
        }

        Function search = new Function(fnName, argTypes, Type.INVALID, false);
        Function fn = db.getFunction(search, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

        if (fn == null) {
            return null;
        }

        if (GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
            // check SELECT action on any object(table/view/mv) in db
            if (!PrivilegeManager.checkActionInDb(session, dbName, "SELECT")) {
                throw new StarRocksPlannerException(String.format("Access denied. " +
                                "Found UDF: %s and need the SELECT action on any object(table/view/mv) in db %s",
                        fnName, dbName), ErrorType.USER_ERROR);
            }
        } else {
            if (!session.getGlobalStateMgr().getAuth().checkDbPriv(session, dbName, PrivPredicate.SELECT)) {
                throw new StarRocksPlannerException(String.format("Access denied. " +
                        "Found UDF: %s and need the SELECT priv for %s", fnName, dbName),
                        ErrorType.USER_ERROR);
            }

        }
        return fn;
    }

    private static Function getGlobalUdfFunction(ConnectContext session, FunctionName fnName, Type[] argTypes) {
        Function search = new Function(fnName, argTypes, Type.INVALID, false);
        Function fn = session.getGlobalStateMgr().getGlobalFunctionMgr()
                .getFunction(search, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

        if (fn == null) {
            return null;
        }

        String name = fn.signatureString();
        if (GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
            if (!PrivilegeManager.checkGlobalFunctionAction(session, name,
                    PrivilegeType.GlobalFunctionAction.USAGE)) {
                throw new StarRocksPlannerException(String.format("Access denied. " +
                                "Found UDF: %s and need the USAGE priv for GLOBAL FUNCTION",
                        name), ErrorType.USER_ERROR);
            }
        } else {
            if (!session.getGlobalStateMgr().getAuth().checkGlobalPriv(session, PrivPredicate.USAGE)) {
                throw new StarRocksPlannerException(String.format("Access denied. " +
                        "Found UDF: %s and need the USAGE priv for GLOBAL", name),
                        ErrorType.USER_ERROR);
            }
        }
        return fn;
    }

    public static Function getUdfFunction(ConnectContext session, FunctionName fnName, Type[] argTypes) {
        Function fn = getDBUdfFunction(session, fnName, argTypes);
        if (fn == null) {
            fn = getGlobalUdfFunction(session, fnName, argTypes);
        }
        if (fn != null) {
            if (!Config.enable_udf) {
                throw new StarRocksPlannerException("CBO Optimizer don't support UDF function: " + fnName,
                        ErrorType.USER_ERROR);
            }
        }
        return fn;
    }

    //Get all the db used, the query needs to add locks to them
    public static Map<String, Database> collectAllDatabase(ConnectContext session, StatementBase statementBase) {
        Map<String, Database> dbs = Maps.newHashMap();
        new AnalyzerUtils.DBCollector(dbs, session).visit(statementBase);
        return dbs;
    }

    private static class DBCollector extends AstVisitor<Void, Void> {
        private final Map<String, Database> dbs;
        private final ConnectContext session;

        public DBCollector(Map<String, Database> dbs, ConnectContext session) {
            this.dbs = dbs;
            this.session = session;
        }

        @Override
        public Void visitInsertStatement(InsertStmt node, Void context) {
            getDB(node.getTableName());
            return visit(node.getQueryStatement());
        }

        @Override
        public Void visitUpdateStatement(UpdateStmt node, Void context) {
            getDB(node.getTableName());
            //If support DML operations through query results in the future,
            //need to add the corresponding `visit(node.getQueryStatement())`
            return null;
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt node, Void context) {
            getDB(node.getTableName());
            //If support DML operations through query results in the future,
            //need to add the corresponding `visit(node.getQueryStatement())`
            return null;
        }

        @Override
        public Void visitQueryStatement(QueryStatement node, Void context) {
            return visit(node.getQueryRelation());
        }

        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            if (node.hasWithClause()) {
                node.getCteRelations().forEach(this::visit);
            }

            return visit(node.getRelation());
        }

        @Override
        public Void visitSubquery(SubqueryRelation node, Void context) {
            return visit(node.getQueryStatement());
        }

        public Void visitView(ViewRelation node, Void context) {
            getDB(node.getName());
            return visit(node.getQueryStatement(), context);
        }

        @Override
        public Void visitSetOp(SetOperationRelation node, Void context) {
            if (node.hasWithClause()) {
                node.getRelations().forEach(this::visit);
            }
            node.getRelations().forEach(this::visit);
            return null;
        }

        @Override
        public Void visitJoin(JoinRelation node, Void context) {
            visit(node.getLeft());
            visit(node.getRight());
            return null;
        }

        @Override
        public Void visitCTE(CTERelation node, Void context) {
            return visit(node.getCteQueryStatement());
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            getDB(node.getName());
            return null;
        }

        private void getDB(TableName tableName) {
            String catalog = Strings.isNullOrEmpty(tableName.getCatalog()) ? session.getCurrentCatalog() :
                    tableName.getCatalog();
            String dbName = Strings.isNullOrEmpty(tableName.getDb()) ? session.getDatabase() : tableName.getDb();

            if (Strings.isNullOrEmpty(catalog)) {
                return;
            }

            if (Strings.isNullOrEmpty(dbName)) {
                return;
            }

            if (!CatalogMgr.isInternalCatalog(catalog)) {
                return;
            }

            Database db = session.getGlobalStateMgr().getDb(dbName);
            if (db == null) {
                return;
            }
            dbs.put(db.getFullName(), db);
        }
    }

    //Get all the table used
    public static Map<TableName, Table> collectAllTable(StatementBase statementBase) {
        Map<TableName, Table> tables = Maps.newHashMap();
        new AnalyzerUtils.TableCollector(tables).visit(statementBase);
        return tables;
    }

    private static class TableCollector extends AstVisitor<Void, Void> {
        protected final Map<TableName, Table> tables;

        public TableCollector(Map<TableName, Table> dbs) {
            this.tables = dbs;
        }

        @Override
        public Void visitInsertStatement(InsertStmt node, Void context) {
            Table table = node.getTargetTable();
            tables.put(node.getTableName(), table);
            return visit(node.getQueryStatement());
        }

        @Override
        public Void visitQueryStatement(QueryStatement node, Void context) {
            return visit(node.getQueryRelation());
        }

        @Override
        public Void visitSubquery(SubqueryRelation node, Void context) {
            return visit(node.getQueryStatement());
        }

        public Void visitView(ViewRelation node, Void context) {
            return visit(node.getQueryStatement(), context);
        }

        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            if (node.hasWithClause()) {
                node.getCteRelations().forEach(this::visit);
            }

            return visit(node.getRelation());
        }

        @Override
        public Void visitSetOp(SetOperationRelation node, Void context) {
            if (node.hasWithClause()) {
                node.getRelations().forEach(this::visit);
            }
            node.getRelations().forEach(this::visit);
            return null;
        }

        @Override
        public Void visitJoin(JoinRelation node, Void context) {
            visit(node.getLeft());
            visit(node.getRight());
            return null;
        }

        @Override
        public Void visitCTE(CTERelation node, Void context) {
            return visit(node.getCteQueryStatement());
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            Table table = node.getTable();
            tables.put(node.getName(), table);
            return null;
        }
    }

    public static Map<TableName, Table> collectAllTableWithAlias(StatementBase statementBase) {
        Map<TableName, Table> tables = Maps.newHashMap();
        new AnalyzerUtils.TableCollectorWithAlias(tables).visit(statementBase);
        return tables;
    }

    public static Map<TableName, Table> collectAllTableAndViewWithAlias(StatementBase statementBase) {
        Map<TableName, Table> tables = Maps.newHashMap();
        new AnalyzerUtils.TableAndViewCollectorWithAlias(tables).visit(statementBase);
        return tables;
    }

    public static Map<TableName, Table> collectAllTableAndViewWithAlias(SelectRelation statementBase) {
        Map<TableName, Table> tables = Maps.newHashMap();
        new AnalyzerUtils.TableAndViewCollectorWithAlias(tables).visit(statementBase);
        return tables;
    }

    public static Map<String, TableRelation> collectAllTableRelation(StatementBase statementBase) {
        Map<String, TableRelation> tableRelations = Maps.newHashMap();
        new AnalyzerUtils.TableRelationCollector(tableRelations).visit(statementBase);
        return tableRelations;
    }

    public static Map<TableName, SubqueryRelation> collectAllSubQueryRelation(QueryStatement queryStatement) {
        Map<TableName, SubqueryRelation> subQueryRelations = Maps.newHashMap();
        new AnalyzerUtils.SubQueryRelationCollector(subQueryRelations).visit(queryStatement);
        return subQueryRelations;
    }

    public static Map<TableName, Table> collectAllTableAndView(StatementBase statementBase) {
        Map<TableName, Table> tables = Maps.newHashMap();
        new AnalyzerUtils.TableAndViewCollector(tables).visit(statementBase);
        return tables;
    }

    private static class TableAndViewCollector extends TableCollector {
        public TableAndViewCollector(Map<TableName, Table> dbs) {
            super(dbs);
        }

        public Void visitView(ViewRelation node, Void context) {
            Table table = node.getView();
            tables.put(node.getResolveTableName(), table);
            return null;
        }
    }

    private static class TableCollectorWithAlias extends TableCollector {
        public TableCollectorWithAlias(Map<TableName, Table> dbs) {
            super(dbs);
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            Table table = node.getTable();
            tables.put(node.getResolveTableName(), table);
            return null;
        }
    }

    private static class TableAndViewCollectorWithAlias extends TableCollectorWithAlias {
        public TableAndViewCollectorWithAlias(Map<TableName, Table> dbs) {
            super(dbs);
        }

        public Void visitView(ViewRelation node, Void context) {
            Table table = node.getView();
            tables.put(node.getResolveTableName(), table);
            return null;
        }
    }

    private static class TableRelationCollector extends TableCollector {

        private final Map<String, TableRelation> tableRelations;

        public TableRelationCollector(Map<String, TableRelation> tableRelations) {
            super(null);
            this.tableRelations = tableRelations;
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            tableRelations.put(node.getName().getTbl(), node);
            return null;
        }
    }

    private static class SubQueryRelationCollector extends TableCollector {
        Map<TableName, SubqueryRelation> subQueryRelations;

        public SubQueryRelationCollector(Map<TableName, SubqueryRelation> subQueryRelations) {
            super(null);
            this.subQueryRelations = subQueryRelations;
        }

        @Override
        public Void visitSubquery(SubqueryRelation node, Void context) {
            subQueryRelations.put(node.getResolveTableName(), node);
            return visit(node.getQueryStatement());
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            return null;
        }
    }

    // For char and varchar types, use the inferred length if the length can be inferred,
    // otherwise (include null type) use the longest varchar value.
    // For double and float types, since they may be selected as key columns,
    // the key column must be an exact value, so we unified into a default decimal type.
    public static Type transformType(Type srcType) {
        Type newType;
        if (srcType.isScalarType()) {
            if (PrimitiveType.VARCHAR == srcType.getPrimitiveType() ||
                    PrimitiveType.CHAR == srcType.getPrimitiveType() ||
                    PrimitiveType.NULL_TYPE == srcType.getPrimitiveType()) {
                int len = ScalarType.MAX_VARCHAR_LENGTH;
                if (srcType instanceof ScalarType) {
                    ScalarType scalarType = (ScalarType) srcType;
                    if (scalarType.getLength() > 0 && scalarType.isAssignedStrLenInColDefinition()) {
                        len = scalarType.getLength();
                    }
                }
                ScalarType stringType = ScalarType.createVarcharType(len);
                stringType.setAssignedStrLenInColDefinition();
                newType = stringType;
            } else if (PrimitiveType.FLOAT == srcType.getPrimitiveType() ||
                    PrimitiveType.DOUBLE == srcType.getPrimitiveType()) {
                newType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 9);
            } else if (PrimitiveType.DECIMAL128 == srcType.getPrimitiveType() ||
                    PrimitiveType.DECIMAL64 == srcType.getPrimitiveType() ||
                    PrimitiveType.DECIMAL32 == srcType.getPrimitiveType()) {
                newType = ScalarType.createDecimalV3Type(srcType.getPrimitiveType(),
                        srcType.getPrecision(), srcType.getDecimalDigits());
            } else {
                newType = ScalarType.createType(srcType.getPrimitiveType());
            }
        } else if (srcType.isArrayType()) {
            newType = new ArrayType(transformType(((ArrayType) srcType).getItemType()));
        } else {
            throw new SemanticException("Unsupported CTAS transform type: %s", srcType);
        }
        return newType;
    }

    public static Type transformTypeForMv(Type srcType) {
        Type newType;
        if (srcType.isScalarType()) {
            if (PrimitiveType.VARCHAR == srcType.getPrimitiveType() ||
                    PrimitiveType.CHAR == srcType.getPrimitiveType() ||
                    PrimitiveType.NULL_TYPE == srcType.getPrimitiveType()) {
                int len = ScalarType.MAX_VARCHAR_LENGTH;
                if (srcType instanceof ScalarType) {
                    ScalarType scalarType = (ScalarType) srcType;
                    if (scalarType.getLength() > 0 && scalarType.isAssignedStrLenInColDefinition()) {
                        len = scalarType.getLength();
                    }
                }
                ScalarType stringType = ScalarType.createVarcharType(len);
                stringType.setAssignedStrLenInColDefinition();
                newType = stringType;
            } else if (PrimitiveType.DECIMAL128 == srcType.getPrimitiveType() ||
                    PrimitiveType.DECIMAL64 == srcType.getPrimitiveType() ||
                    PrimitiveType.DECIMAL32 == srcType.getPrimitiveType()) {
                newType = ScalarType.createDecimalV3Type(srcType.getPrimitiveType(),
                        srcType.getPrecision(), srcType.getDecimalDigits());
            } else {
                newType = ScalarType.createType(srcType.getPrimitiveType());
            }
        } else if (srcType.isArrayType()) {
            newType = new ArrayType(transformTypeForMv(((ArrayType) srcType).getItemType()));
        } else {
            throw new SemanticException("Unsupported Mv transform type: %s", srcType);
        }
        return newType;
    }

    public static TableName stringToTableName(String qualifiedName) {
        // Hierarchy: catalog.database.table
        List<String> parts = Splitter.on(".").omitEmptyStrings().trimResults().splitToList(qualifiedName);
        if (parts.size() == 3) {
            return new TableName(parts.get(0), parts.get(1), parts.get(2));
        } else if (parts.size() == 2) {
            return new TableName(null, parts.get(0), parts.get(1));
        } else if (parts.size() == 1) {
            return new TableName(null, null, parts.get(0));
        } else {
            throw new ParsingException("error table name ");
        }
    }

    public static String parseLiteralExprToDateString(LiteralExpr expr, int offset) {
        if (expr instanceof DateLiteral) {
            DateLiteral lowerDate = (DateLiteral) expr;
            return DateUtils.DATE_FORMATTER.format(lowerDate.toLocalDateTime().plusDays(offset));
        } else if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            return String.valueOf(intLiteral.getLongValue() + offset);
        } else {
            return null;
        }
    }

    public static Map<String, AddPartitionClause> getAddPartitionClauseFromPartitionValues(
            List<String> partitionValues, String granularity, OlapTable olapTable) throws AnalysisException {
        Map<String, AddPartitionClause> result = Maps.newHashMap();
        for (String partitionValue : partitionValues) {
            DateTimeFormatter beginDateTimeFormat;
            LocalDateTime beginTime;
            LocalDateTime endTime;

            try {
                beginDateTimeFormat = DateUtils.probeFormat(partitionValue);
                beginTime = DateUtils.parseStringWithDefaultHSM(partitionValue, beginDateTimeFormat);
                switch (granularity.toLowerCase()) {
                    case "day":
                        endTime = beginTime.plusDays(1);
                        break;
                    case "hour":
                        endTime = beginTime.plusHours(1);
                        break;
                    case "month":
                        endTime = beginTime.plusMonths(1);
                        break;
                    case "year":
                        endTime = beginTime.plusYears(1);
                        break;
                    default:
                        throw new AnalysisException("unsupported partition granularity:" + granularity);
                }
                String lowerBound = beginTime.format(DateUtils.DATEKEY_FORMATTER);
                String upperBound = endTime.format(DateUtils.DATEKEY_FORMATTER);
                String partitionName = "p" + lowerBound;
                PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(
                        Collections.singletonList(new PartitionValue(lowerBound)),
                        Collections.singletonList(new PartitionValue(upperBound)));
                Map<String, String> partitionProperties = Maps.newHashMap();
                Short replicationNum = olapTable.getTableProperty().getReplicationNum();
                partitionProperties.put("replication_num", String.valueOf(replicationNum));
                DistributionDesc distributionDesc = olapTable.getDefaultDistributionInfo().toDistributionDesc();

                SingleRangePartitionDesc singleRangePartitionDesc =
                        new SingleRangePartitionDesc(true, partitionName, partitionKeyDesc, partitionProperties);

                AddPartitionClause addPartitionClause =
                        new AddPartitionClause(singleRangePartitionDesc, distributionDesc,
                                partitionProperties, false);
                result.put(partitionName, addPartitionClause);
            } catch (AnalysisException e) {
                throw new AnalysisException(String.format("failed to analyse partition value:%s", partitionValue));
            }
        }
        return result;
    }

    public static SlotRef getSlotRefFromFunctionCall(Expr expr) {
        if (expr instanceof FunctionCallExpr) {
            ArrayList<Expr> children = expr.getChildren();
            for (Expr child : children) {
                if (child instanceof SlotRef) {
                    return (SlotRef) child;
                }
            }
        }
        return null;
    }
}
