// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.UpdateStmt;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.Config;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class AnalyzerUtils {
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

    public static Function getUdfFunction(ConnectContext session, FunctionName fnName, Type[] argTypes) {
        String dbName = fnName.getDb();
        if (StringUtils.isEmpty(dbName)) {
            dbName = session.getDatabase();
        } else {
            dbName = ClusterNamespace.getFullName(session.getClusterName(), dbName);
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

        if (!session.getGlobalStateMgr().getAuth().checkDbPriv(session, dbName, PrivPredicate.SELECT)) {
            throw new StarRocksPlannerException(String.format("Access denied. " +
                    "Found UDF: %s and need the SELECT priv for %s", fnName, dbName),
                    ErrorType.USER_ERROR);
        }

        if (!Config.enable_udf) {
            throw new StarRocksPlannerException("CBO Optimizer don't support UDF function: " + fnName,
                    ErrorType.USER_ERROR);
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
            String catalog = Strings.isNullOrEmpty(tableName.getCatalog()) ? session.getCurrentCatalog() : tableName.getCatalog();
            String dbName = Strings.isNullOrEmpty(tableName.getDb()) ? session.getDatabase() : tableName.getDb();

            if (Strings.isNullOrEmpty(catalog)) {
                return;
            }

            if (Strings.isNullOrEmpty(dbName)) {
                return;
            }

            if (!CatalogMgr.isInternalCatalog(catalog)) {
                return;
            } else {
                dbName = ClusterNamespace.getFullName(session.getClusterName(), dbName);
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

<<<<<<< HEAD
=======
    public static Map<TableName, Table> collectAllTableAndViewWithAlias(SelectRelation statementBase) {
        Map<TableName, Table> tables = Maps.newHashMap();
        new AnalyzerUtils.TableAndViewCollectorWithAlias(tables).visit(statementBase);
        return tables;
    }

    public static Multimap<String, TableRelation> collectAllTableRelation(StatementBase statementBase) {
        Multimap<String, TableRelation> tableRelations = ArrayListMultimap.create();
        new AnalyzerUtils.TableRelationCollector(tableRelations).visit(statementBase);
        return tableRelations;
    }

    public static List<TableRelation> collectTableRelations(StatementBase statementBase) {
        List<TableRelation> tableRelations = Lists.newArrayList();
        new AnalyzerUtils.TableRelationsCollector(tableRelations).visit(statementBase);
        return tableRelations;
    }

    public static List<ViewRelation> collectViewRelations(StatementBase statementBase) {
        List<ViewRelation> viewRelations = Lists.newArrayList();
        new AnalyzerUtils.ViewRelationsCollector(viewRelations).visit(statementBase);
        return viewRelations;
    }

    public static boolean isOnlyHasOlapTables(StatementBase statementBase) {
        Map<TableName, Table> nonOlapTables = Maps.newHashMap();
        new AnalyzerUtils.NonOlapTableCollector(nonOlapTables).visit(statementBase);
        return nonOlapTables.isEmpty();
    }

    public static void copyOlapTable(StatementBase statementBase, Set<OlapTable> olapTables) {
        new AnalyzerUtils.OlapTableCollector(olapTables).visit(statementBase);
    }

    public static void collectSpecifyExternalTables(StatementBase statementBase, List<Table> tables, Predicate<Table> filter) {
        new ExternalTableCollector(tables, filter).visit(statementBase);
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

    public static Map<TableName, Table> collectAllConnectorTableAndView(StatementBase statementBase) {
        Map<TableName, Table> tables = Maps.newHashMap();
        new AnalyzerUtils.ConnectorTableAndViewCollector(tables).visit(statementBase);
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

    private static class ConnectorTableAndViewCollector extends TableAndViewCollector {
        public ConnectorTableAndViewCollector(Map<TableName, Table> dbs) {
            super(dbs);
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            Table table = node.getTable();
            if (table == null) {
                tables.put(node.getName(), null);
                return null;
            }
            // For external tables, their db/table names are case-insensitive, need to get real names of them.
            if (table.isHiveTable() || table.isHudiTable()) {
                HiveMetaStoreTable hiveMetaStoreTable = (HiveMetaStoreTable) table;
                TableName tableName = new TableName(hiveMetaStoreTable.getCatalogName(), hiveMetaStoreTable.getDbName(),
                        hiveMetaStoreTable.getTableName());
                tables.put(tableName, table);
            } else if (table.isIcebergTable()) {
                IcebergTable icebergTable = (IcebergTable) table;
                TableName tableName = new TableName(icebergTable.getCatalogName(), icebergTable.getRemoteDbName(),
                        icebergTable.getRemoteTableName());
                tables.put(tableName, table);
            } else {
                tables.put(node.getName(), table);
            }
            return null;
        }
    }

    private static class NonOlapTableCollector extends TableCollector {
        public NonOlapTableCollector(Map<TableName, Table> tables) {
            super(tables);
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            if (!tables.isEmpty()) {
                return null;
            }
            // if olap table has MV, we remove it
            if (!node.getTable().isOlapTable() ||
                    !node.getTable().getRelatedMaterializedViews().isEmpty()) {
                tables.put(node.getName(), node.getTable());
            }
            return null;
        }
    }

    private static class OlapTableCollector extends TableCollector {
        Set<OlapTable> olapTables;

        public OlapTableCollector(Set<OlapTable> tables) {
            this.olapTables = tables;
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            if (node.getTable().isOlapTable()) {
                OlapTable table = (OlapTable) node.getTable();
                olapTables.add(table);
                // Only copy the necessary olap table meta to avoid the lock when plan query
                OlapTable copied = new OlapTable();
                table.copyOnlyForQuery(copied);
                node.setTable(copied);
            }
            return null;
        }
    }

    private static class ExternalTableCollector extends TableCollector {
        List<Table> tables;
        Predicate<Table> predicate;

        public ExternalTableCollector(List<Table> tables, Predicate<Table> filter) {
            this.tables = tables;
            this.predicate = filter;
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            Table table = node.getTable();
            if (predicate.test(table)) {
                tables.add(table);
            }
            return null;
        }
    }

>>>>>>> ef1fc6a40 ([Refactor] Synchronize OLAP external table metadata when loading data (#24739))
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

    private static class TableAndViewCollectorWithAlias extends TableCollector {
        public TableAndViewCollectorWithAlias(Map<TableName, Table> dbs) {
            super(dbs);
        }

        public Void visitView(ViewRelation node, Void context) {
            Table table = node.getView();
            tables.put(node.getResolveTableName(), table);
            return null;
        }
    }
}
