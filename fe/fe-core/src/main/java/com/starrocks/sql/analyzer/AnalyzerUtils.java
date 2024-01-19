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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.PartitionMeasure;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.Relation;
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
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.statistic.StatsConstants;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;
import static com.starrocks.statistic.StatsConstants.STATISTICS_DB_NAME;

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
            throw new SemanticException(clause + " clause cannot contain aggregations", expression.getPos());
        }
    }

    public static void verifyNoWindowFunctions(Expr expression, String clause) {
        List<AnalyticExpr> functions = Lists.newArrayList();
        expression.collectAll((Predicate<Expr>) arg -> arg instanceof AnalyticExpr, functions);
        if (!functions.isEmpty()) {
            throw new SemanticException(clause + " clause cannot contain window function", expression.getPos());
        }
    }

    public static void verifyNoGroupingFunctions(Expr expression, String clause) {
        List<GroupingFunctionCallExpr> calls = Lists.newArrayList();
        expression.collectAll((Predicate<Expr>) arg -> arg instanceof GroupingFunctionCallExpr, calls);
        if (!calls.isEmpty()) {
            throw new SemanticException(clause + " clause cannot contain grouping", expression.getPos());
        }
    }

    public static void verifyNoSubQuery(Expr expression, String clause) {
        List<Subquery> calls = Lists.newArrayList();
        expression.collectAll((Predicate<Expr>) arg -> arg instanceof Subquery, calls);
        if (!calls.isEmpty()) {
            throw new SemanticException(clause + " clause cannot contain subquery", expression.getPos());
        }
    }

    public static boolean isAggregate(List<FunctionCallExpr> aggregates, List<Expr> groupByExpressions) {
        return !aggregates.isEmpty() || !groupByExpressions.isEmpty();
    }

    public static Function getDBUdfFunction(ConnectContext context, FunctionName fnName,
                                            Type[] argTypes) {
        String dbName = fnName.getDb();
        if (StringUtils.isEmpty(dbName)) {
            dbName = context.getDatabase();
        }

        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            return null;
        }

        try {
            db.readLock();
            Function search = new Function(fnName, argTypes, Type.INVALID, false);
            Function fn = db.getFunction(search, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

            if (fn != null) {
                try {
                    Authorizer.checkFunctionAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(), db, fn,
                            PrivilegeType.USAGE);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(
                            InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.USAGE.name(), ObjectType.FUNCTION.name(), fn.getSignature());
                }
            }

            return fn;
        } finally {
            db.readUnlock();
        }
    }

    private static Function getGlobalUdfFunction(ConnectContext context, FunctionName fnName, Type[] argTypes) {
        Function search = new Function(fnName, argTypes, Type.INVALID, false);
        Function fn = context.getGlobalStateMgr().getGlobalFunctionMgr()
                .getFunction(search, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (fn != null) {
            try {
                Authorizer.checkGlobalFunctionAction(context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        fn, PrivilegeType.USAGE);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.USAGE.name(), ObjectType.GLOBAL_FUNCTION.name(), fn.getSignature());
            }
        }

        return fn;
    }

    public static Function getUdfFunction(ConnectContext context, FunctionName fnName, Type[] argTypes) {
        Function fn = getDBUdfFunction(context, fnName, argTypes);
        if (fn == null && context.getGlobalStateMgr() != null) {
            fn = getGlobalUdfFunction(context, fnName, argTypes);
        }
        if (fn != null) {
            if (!Config.enable_udf) {
                throw new StarRocksPlannerException(
                        "UDF is not enabled in FE, please configure enable_udf=true in fe/conf/fe.conf",
                        ErrorType.USER_ERROR);
            }
        }
        return fn;
    }

    // Get all the db used, the query needs to add locks to them
    public static Map<String, Database> collectAllDatabase(ConnectContext context, StatementBase statementBase) {
        Map<String, Database> dbs = Maps.newHashMap();
        new AnalyzerUtils.DBCollector(dbs, context).visit(statementBase);
        return dbs;
    }

    public static boolean isStatisticsJob(ConnectContext context, StatementBase stmt) {
        Map<String, Database> dbs = collectAllDatabase(context, stmt);
        return dbs.values().stream().anyMatch(db -> STATISTICS_DB_NAME.equals(db.getFullName()));
    }

    public static CallOperator getCallOperator(ScalarOperator operator) {
        if (operator instanceof CastOperator) {
            return getCallOperator(operator.getChild(0));
        } else if (operator instanceof CallOperator) {
            return (CallOperator) operator;
        }
        return null;
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

    private static class TableCollector extends AstTraverser<Void, Void> {
        protected Map<TableName, Table> tables;

        public TableCollector() {
        }

        public TableCollector(Map<TableName, Table> dbs) {
            this.tables = dbs;
        }

        // ---------------------------------------- Query Statement --------------------------------------------------------------

        @Override
        public Void visitQueryStatement(QueryStatement statement, Void context) {
            return visit(statement.getQueryRelation());
        }

        // ------------------------------------------- DML Statement -------------------------------------------------------

        @Override
        public Void visitInsertStatement(InsertStmt node, Void context) {
            Table table = node.getTargetTable();
            tables.put(node.getTableName(), table);
            return super.visitInsertStatement(node, context);
        }

        @Override
        public Void visitUpdateStatement(UpdateStmt node, Void context) {
            Table table = node.getTable();
            tables.put(node.getTableName(), table);
            return super.visitUpdateStatement(node, context);
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt node, Void context) {
            Table table = node.getTable();
            tables.put(node.getTableName(), table);
            return super.visitDeleteStatement(node, context);
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

    public static Map<TableName, Relation> collectAllTableAndViewRelations(ParseNode parseNode) {
        Map<TableName, Relation>  allTableAndViewRelations = Maps.newHashMap();
        new TableAndViewRelationsCollector(allTableAndViewRelations).visit(parseNode);
        return allTableAndViewRelations;
    }

    public static boolean isOnlyHasOlapTables(StatementBase statementBase) {
        Map<TableName, Table> nonOlapTables = Maps.newHashMap();
        new AnalyzerUtils.NonOlapTableCollector(nonOlapTables).visit(statementBase);
        return nonOlapTables.isEmpty();
    }

    public static void copyOlapTable(StatementBase statementBase, Set<OlapTable> olapTables) {
        new AnalyzerUtils.OlapTableCollector(olapTables).visit(statementBase);
    }

    public static void collectSpecifyExternalTables(StatementBase statementBase, List<Table> tables,
                                                    Predicate<Table> filter) {
        new ExternalTableCollector(tables, filter).visit(statementBase);
    }

    public static Map<TableName, SubqueryRelation> collectAllSubQueryRelation(QueryStatement queryStatement) {
        Map<TableName, SubqueryRelation> subQueryRelations = Maps.newHashMap();
        new AnalyzerUtils.SubQueryRelationCollector(subQueryRelations).visit(queryStatement);
        return subQueryRelations;
    }

    /**
     * Only collect one level table name and the corresponding sub query relation.
     */
    public static Map<TableName, SubqueryRelation> collectOneLevelSubQueryRelation(QueryStatement queryStatement) {
        Map<TableName, SubqueryRelation> subQueryRelations = Maps.newHashMap();
        new AnalyzerUtils.SubQueryOneLevelRelationCollector(subQueryRelations).visit(queryStatement);
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
            } else if (table.isPaimonTable()) {
                PaimonTable paimonTable = (PaimonTable) table;
                TableName tableName = new TableName(paimonTable.getCatalogName(), paimonTable.getDbName(),
                        paimonTable.getTableName());
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

            int relatedMVCount = node.getTable().getRelatedMaterializedViews().size();
            boolean useNonLockOptimization = Config.skip_whole_phase_lock_mv_limit < 0 ||
                    relatedMVCount <= Config.skip_whole_phase_lock_mv_limit;
            if (!(node.getTable().isOlapTableOrMaterializedView() && useNonLockOptimization)) {
                tables.put(node.getName(), node.getTable());
            }
            return null;
        }
    }

    private static class OlapTableCollector extends TableCollector {
        private Set<OlapTable> olapTables;
        private Map<Long, OlapTable> idMap;

        public OlapTableCollector(Set<OlapTable> tables) {
            this.olapTables = tables;
            this.idMap = new HashMap<>();
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            if (node.getTable().isOlapTable()) {
                OlapTable table = (OlapTable) node.getTable();
                if (!idMap.containsKey(table.getId())) {
                    olapTables.add(table);
                    idMap.put(table.getId(), table);
                    // Only copy the necessary olap table meta to avoid the lock when plan query
                    OlapTable copied = new OlapTable();
                    table.copyOnlyForQuery(copied);
                    node.setTable(copied);
                } else {
                    node.setTable(idMap.get(table.getId()));
                }
            } else if (node.getTable().isOlapMaterializedView()) {
                MaterializedView table = (MaterializedView) node.getTable();
                if (!idMap.containsKey(table.getId())) {
                    olapTables.add(table);
                    idMap.put(table.getId(), table);
                    // Only copy the necessary olap table meta to avoid the lock when plan query
                    MaterializedView copied = new MaterializedView();
                    table.copyOnlyForQuery(copied);
                    node.setTable(copied);
                } else {
                    node.setTable(idMap.get(table.getId()));
                }
            }
            // TODO: support cloud native table and mv
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

        private final Multimap<String, TableRelation> tableRelations;

        public TableRelationCollector(Multimap<String, TableRelation> tableRelations) {
            super(null);
            this.tableRelations = tableRelations;
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            String tblName = node.getTable() != null ? node.getTable().getName() : node.getName().getTbl();
            tableRelations.put(tblName, node);
            return null;
        }
    }

    private static class ViewRelationsCollector extends AstTraverser<Void, Void> {

        private final List<ViewRelation> viewRelations;

        public ViewRelationsCollector(List<ViewRelation> viewRelations) {
            this.viewRelations = viewRelations;
        }

        @Override
        public Void visitView(ViewRelation node, Void context) {
            viewRelations.add(node);
            return null;
        }
    }

    private static class TableRelationsCollector extends TableCollector {

        private final List<TableRelation> tableRelations;

        public TableRelationsCollector(List<TableRelation> tableRelations) {
            super(null);
            this.tableRelations = tableRelations;
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            tableRelations.add(node);
            return null;
        }
    }

    private static class TableAndViewRelationsCollector extends AstTraverser<Void, Void> {

        private final Map<TableName, Relation> allTableAndViewRelations;

        public TableAndViewRelationsCollector(Map<TableName, Relation> tableRelations) {
            this.allTableAndViewRelations = tableRelations;
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            if (node.isCreateByPolicyRewritten()) {
                return null;
            }
            allTableAndViewRelations.put(node.getName(), node);
            return null;
        }

        @Override
        public Void visitView(ViewRelation node, Void context) {
            if (node.isCreateByPolicyRewritten()) {
                return null;
            }
            allTableAndViewRelations.put(node.getName(), node);
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

    private static class SubQueryOneLevelRelationCollector extends TableCollector {
        Map<TableName, SubqueryRelation> subQueryRelations;

        public SubQueryOneLevelRelationCollector(Map<TableName, SubqueryRelation> subQueryRelations) {
            super(null);
            this.subQueryRelations = subQueryRelations;
        }

        @Override
        public Void visitSubquery(SubqueryRelation node, Void context) {
            // no recursive
            subQueryRelations.put(node.getResolveTableName(), node);
            return null;
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            return null;
        }
    }

    public static Set<TableName> getAllTableNamesForAnalyzeJobStmt(long dbId, long tableId) {
        Set<TableName> tableNames = Sets.newHashSet();
        if (StatsConstants.DEFAULT_ALL_ID != tableId && StatsConstants.DEFAULT_ALL_ID != dbId) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db != null && !db.isSystemDatabase()) {
                Table table = db.getTable(tableId);
                if (table != null && table.isOlapOrCloudNativeTable()) {
                    tableNames.add(new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            db.getFullName(), table.getName()));
                }
            }
        } else if (StatsConstants.DEFAULT_ALL_ID == tableId && StatsConstants.DEFAULT_ALL_ID != dbId) {
            getTableNamesInDb(tableNames, dbId);
        } else {
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
            for (Long id : dbIds) {
                getTableNamesInDb(tableNames, id);
            }
        }

        return tableNames;
    }

    private static void getTableNamesInDb(Set<TableName> tableNames, Long id) {
        Database db = GlobalStateMgr.getCurrentState().getDb(id);
        if (db != null && !db.isSystemDatabase()) {
            for (Table table : db.getTables()) {
                if (table == null || !table.isOlapOrCloudNativeTable()) {
                    continue;
                }
                TableName tableNameNew = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        db.getFullName(), table.getName());
                tableNames.add(tableNameNew);
            }
        }
    }

    public static Type transformTableColumnType(Type srcType) {
        return transformTableColumnType(srcType, true);
    }

    // For char and varchar types, use the inferred length if the length can be inferred,
    // otherwise (include null type) use the longest varchar value.
    // For double and float types, since they may be selected as key columns,
    // the key column must be an exact value, so we unified into a default decimal type.
    public static Type transformTableColumnType(Type srcType, boolean convertDouble) {
        Type newType = srcType;
        if (srcType.isScalarType()) {
            if (PrimitiveType.VARCHAR == srcType.getPrimitiveType() ||
                    PrimitiveType.CHAR == srcType.getPrimitiveType() ||
                    PrimitiveType.NULL_TYPE == srcType.getPrimitiveType()) {
                int len = ScalarType.MAX_VARCHAR_LENGTH;
                if (srcType instanceof ScalarType) {
                    ScalarType scalarType = (ScalarType) srcType;
                    if (scalarType.getLength() > 0) {
                        len = scalarType.getLength();
                    }
                }
                ScalarType stringType = ScalarType.createVarcharType(len);
                newType = stringType;
            } else if (PrimitiveType.FLOAT == srcType.getPrimitiveType() ||
                    PrimitiveType.DOUBLE == srcType.getPrimitiveType()) {
                if (convertDouble) {
                    newType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 9);
                }
            } else if (PrimitiveType.DECIMAL128 == srcType.getPrimitiveType() ||
                    PrimitiveType.DECIMAL64 == srcType.getPrimitiveType() ||
                    PrimitiveType.DECIMAL32 == srcType.getPrimitiveType()) {
                newType = ScalarType.createDecimalV3Type(srcType.getPrimitiveType(),
                        srcType.getPrecision(), srcType.getDecimalDigits());
            } else {
                newType = ScalarType.createType(srcType.getPrimitiveType());
            }
        } else if (srcType.isArrayType()) {
            newType = new ArrayType(transformTableColumnType(((ArrayType) srcType).getItemType(), convertDouble));
        } else if (srcType.isMapType()) {
            newType = new MapType(transformTableColumnType(((MapType) srcType).getKeyType(), convertDouble),
                    transformTableColumnType(((MapType) srcType).getValueType(), convertDouble));
        } else if (srcType.isStructType()) {
            StructType structType = (StructType) srcType;
            List<StructField> mappingFields = structType.getFields().stream()
                    .map(x -> new StructField(x.getName(), transformTableColumnType(x.getType(), convertDouble),
                            x.getComment()))
                    .collect(Collectors.toList());
            newType = new StructType(mappingFields, structType.isNamed());
        } else {
            // Default behavior is keep source type
            return newType;
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

    public static String parseLiteralExprToDateString(PartitionKey partitionKey, int offset) {
        LiteralExpr expr = partitionKey.getKeys().get(0);
        PrimitiveType type = partitionKey.getTypes().get(0);
        return parseLiteralExprToDateString(expr, type, offset);
    }

    public static String parseLiteralExprToDateString(LiteralExpr expr, PrimitiveType type, int offset) {
        if (expr instanceof DateLiteral) {
            DateLiteral lowerDate = (DateLiteral) expr;
            switch (type) {
                case DATE:
                    return DateUtils.DATE_FORMATTER_UNIX.format(lowerDate.toLocalDateTime().plusDays(offset));
                case DATETIME:
                    return DateUtils.DATE_TIME_FORMATTER_UNIX.format(lowerDate.toLocalDateTime().plusDays(offset));
                default:
                    return null;
            }
        } else if (expr instanceof IntLiteral) {
            IntLiteral intLiteral = (IntLiteral) expr;
            return String.valueOf(intLiteral.getLongValue() + offset);
        } else if (expr instanceof MaxLiteral) {
            return MaxLiteral.MAX_VALUE.toString();
        } else {
            return null;
        }
    }

    public static PartitionMeasure checkAndGetPartitionMeasure(
            ExpressionRangePartitionInfo expressionRangePartitionInfo)
            throws AnalysisException {
        long interval = 1;
        String granularity;
        List<Expr> partitionExprs = expressionRangePartitionInfo.getPartitionExprs();

        if (partitionExprs.size() != 1) {
            throw new AnalysisException("automatic partition only support one expression partitionExpr.");
        }
        Expr expr = partitionExprs.get(0);
        if (!(expr instanceof FunctionCallExpr)) {
            throw new AnalysisException("automatic partition only support FunctionCallExpr");
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        String fnName = functionCallExpr.getFnName().getFunction();
        if (fnName.equals(FunctionSet.DATE_TRUNC)) {
            List<Expr> paramsExprs = functionCallExpr.getParams().exprs();
            if (paramsExprs.size() != 2) {
                throw new AnalysisException("date_trunc params exprs size should be 2.");
            }
            Expr granularityExpr = paramsExprs.get(0);
            if (!(granularityExpr instanceof StringLiteral)) {
                throw new AnalysisException("date_trunc granularity is not string literal.");
            }
            StringLiteral granularityLiteral = (StringLiteral) granularityExpr;
            granularity = granularityLiteral.getStringValue();
        } else if (fnName.equals(FunctionSet.TIME_SLICE)) {
            List<Expr> paramsExprs = functionCallExpr.getParams().exprs();
            if (paramsExprs.size() != 4) {
                throw new AnalysisException("time_slice params exprs size should be 4.");
            }
            Expr intervalExpr = paramsExprs.get(1);
            if (!(intervalExpr instanceof IntLiteral)) {
                throw new AnalysisException("time_slice interval is not int literal.");
            }
            Expr granularityExpr = paramsExprs.get(2);
            if (!(granularityExpr instanceof StringLiteral)) {
                throw new AnalysisException("time_slice granularity is not string literal.");
            }
            StringLiteral granularityLiteral = (StringLiteral) granularityExpr;
            IntLiteral intervalLiteral = (IntLiteral) intervalExpr;
            granularity = granularityLiteral.getStringValue();
            interval = intervalLiteral.getLongValue();
        } else {
            throw new AnalysisException("automatic partition only support data_trunc function.");
        }
        return new PartitionMeasure(interval, granularity);
    }

    public static AddPartitionClause getAddPartitionClauseFromPartitionValues(OlapTable olapTable,
                                                                                           List<List<String>> partitionValues)
            throws AnalysisException {
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            PartitionMeasure measure = checkAndGetPartitionMeasure((ExpressionRangePartitionInfo) partitionInfo);
            return getAddPartitionClauseForRangePartition(olapTable, partitionValues, measure,
                    (ExpressionRangePartitionInfo) partitionInfo);
        } else if (partitionInfo instanceof ListPartitionInfo) {
            Short replicationNum = olapTable.getTableProperty().getReplicationNum();
            DistributionDesc distributionDesc = olapTable.getDefaultDistributionInfo().toDistributionDesc();
            Map<String, String> partitionProperties =
                    ImmutableMap.of("replication_num", String.valueOf(replicationNum));
            String partitionPrefix = "p";

            List<String> partitionColNames = Lists.newArrayList();
            List<PartitionDesc> partitionDescs = Lists.newArrayList();
            for (List<String> partitionValue : partitionValues) {
                List<String> formattedPartitionValue = Lists.newArrayList();
                for (String value : partitionValue) {
                    String formatValue = getFormatPartitionValue(value);
                    formattedPartitionValue.add(formatValue);
                }
                String partitionName = partitionPrefix + Joiner.on("_").join(formattedPartitionValue);
                if (partitionName.length() > 50) {
                    partitionName = partitionName.substring(0, 50) + "_" + System.currentTimeMillis();
                }
                if (!partitionColNames.contains(partitionName)) {
                    MultiItemListPartitionDesc multiItemListPartitionDesc = new MultiItemListPartitionDesc(true,
                            partitionName, Collections.singletonList(partitionValue), partitionProperties);
                    multiItemListPartitionDesc.setSystem(true);
                    partitionDescs.add(multiItemListPartitionDesc);
                    partitionColNames.add(partitionName);
                }
            }
            ListPartitionDesc listPartitionDesc = new ListPartitionDesc(partitionColNames, partitionDescs);
            listPartitionDesc.setSystem(true);
            return new AddPartitionClause(listPartitionDesc, distributionDesc,
                    partitionProperties, false);
        } else {
            throw new AnalysisException("automatic partition only support partition by value.");
        }
    }

    @VisibleForTesting
    public static String getFormatPartitionValue(String value) {
        StringBuilder sb = new StringBuilder();
        // When the value is negative
        if (value.length() > 0 && value.charAt(0) == '-') {
            sb.append("_");
        }
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) {
                sb.append(ch);
            } else if (ch == '-' || ch == ':' || ch == ' ') {
                // Main user remove characters in time
            } else {
                int unicodeValue = value.codePointAt(i);
                String unicodeString = Integer.toHexString(unicodeValue);
                sb.append(unicodeString);
            }
        }
        return sb.toString();
    }

    private static AddPartitionClause getAddPartitionClauseForRangePartition(OlapTable olapTable,
                                                                             List<List<String>> partitionValues,
                                                                             PartitionMeasure measure,
                                                               ExpressionRangePartitionInfo expressionRangePartitionInfo)
            throws AnalysisException {
        String granularity = measure.getGranularity();
        long interval = measure.getInterval();
        Type firstPartitionColumnType = expressionRangePartitionInfo.getPartitionColumns().get(0).getType();
        String partitionPrefix = "p";
        Short replicationNum = olapTable.getTableProperty().getReplicationNum();
        DistributionDesc distributionDesc = olapTable.getDefaultDistributionInfo().toDistributionDesc();
        Map<String, String> partitionProperties = ImmutableMap.of("replication_num", String.valueOf(replicationNum));

        List<PartitionDesc> partitionDescs = Lists.newArrayList();
        List<String> partitionColNames = Lists.newArrayList();
        for (List<String> partitionValue : partitionValues) {
            if (partitionValue.size() != 1) {
                throw new AnalysisException("automatic partition only support single column for range partition.");
            }
            String partitionItem = partitionValue.get(0);
            DateTimeFormatter beginDateTimeFormat;
            LocalDateTime beginTime;
            LocalDateTime endTime;
            String partitionName;
            try {
                if ("NULL".equalsIgnoreCase(partitionItem)) {
                    partitionItem = "0000-01-01";
                }
                beginDateTimeFormat = DateUtils.probeFormat(partitionItem);
                beginTime = DateUtils.parseStringWithDefaultHSM(partitionItem, beginDateTimeFormat);
                // The start date here is passed by BE through function calculation,
                // so it must be the start date of a certain partition.
                switch (granularity.toLowerCase()) {
                    case "hour":
                        beginTime = beginTime.withMinute(0).withSecond(0).withNano(0);
                        partitionName = partitionPrefix + beginTime.format(DateUtils.HOUR_FORMATTER_UNIX);
                        endTime = beginTime.plusHours(interval);
                        break;
                    case "day":
                        beginTime = beginTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
                        partitionName = partitionPrefix + beginTime.format(DateUtils.DATEKEY_FORMATTER_UNIX);
                        endTime = beginTime.plusDays(interval);
                        break;
                    case "month":
                        beginTime = beginTime.withDayOfMonth(1);
                        partitionName = partitionPrefix + beginTime.format(DateUtils.MONTH_FORMATTER_UNIX);
                        endTime = beginTime.plusMonths(interval);
                        break;
                    case "year":
                        beginTime = beginTime.withDayOfYear(1);
                        partitionName = partitionPrefix + beginTime.format(DateUtils.YEAR_FORMATTER_UNIX);
                        endTime = beginTime.plusYears(interval);
                        break;
                    default:
                        throw new AnalysisException("unsupported automatic partition granularity:" + granularity);
                }
                PartitionKeyDesc partitionKeyDesc =
                        createPartitionKeyDesc(firstPartitionColumnType, beginTime, endTime);

                if (!partitionColNames.contains(partitionName)) {
                    SingleRangePartitionDesc singleRangePartitionDesc =
                            new SingleRangePartitionDesc(true, partitionName, partitionKeyDesc, partitionProperties);
                    singleRangePartitionDesc.setSystem(true);
                    partitionDescs.add(singleRangePartitionDesc);
                    partitionColNames.add(partitionName);
                }
            } catch (AnalysisException e) {
                throw new AnalysisException(String.format("failed to analyse partition value:%s", partitionValue));
            }
        }
        RangePartitionDesc rangePartitionDesc = new RangePartitionDesc(partitionColNames, partitionDescs);
        rangePartitionDesc.setSystem(true);
        return new AddPartitionClause(rangePartitionDesc, distributionDesc, partitionProperties, false);
    }

    private static PartitionKeyDesc createPartitionKeyDesc(Type partitionType, LocalDateTime beginTime,
                                                           LocalDateTime endTime) throws AnalysisException {
        boolean isMaxValue;
        DateTimeFormatter outputDateFormat;
        if (partitionType.isDate()) {
            outputDateFormat = DateUtils.DATE_FORMATTER_UNIX;
            isMaxValue =
                    endTime.isAfter(TimeUtils.MAX_DATE.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
        } else if (partitionType.isDatetime()) {
            outputDateFormat = DateUtils.DATE_TIME_FORMATTER_UNIX;
            isMaxValue = endTime.isAfter(
                    TimeUtils.MAX_DATETIME.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
        } else {
            throw new AnalysisException(String.format("failed to analyse partition value:%s", partitionType));
        }
        String lowerBound = beginTime.format(outputDateFormat);

        PartitionValue upperPartitionValue;
        if (isMaxValue) {
            upperPartitionValue = PartitionValue.MAX_VALUE;
        } else {
            String upperBound = endTime.format(outputDateFormat);
            upperPartitionValue = new PartitionValue(upperBound);
        }

        return new PartitionKeyDesc(
                Collections.singletonList(new PartitionValue(lowerBound)),
                Collections.singletonList(upperPartitionValue));
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

    public static SlotRef getSlotRefFromCast(Expr expr) {
        if (expr instanceof CastExpr) {
            CastExpr castExpr = (CastExpr) expr;
            ArrayList<Expr> children = castExpr.getChildren();
            for (Expr child : children) {
                SlotRef slotRef = null;
                if (child instanceof SlotRef) {
                    slotRef = (SlotRef) child;
                } else if (child instanceof CastExpr) {
                    slotRef = getSlotRefFromCast(child);
                } else if (child instanceof FunctionCallExpr) {
                    slotRef = getSlotRefFromFunctionCall(child);
                }
                if (slotRef != null) {
                    return slotRef;
                }
            }
        }
        return null;
    }

    public static Type[] replaceNullTypes2Booleans(Type[] types) {
        return Arrays.stream(types).map(type -> replaceNullType2Boolean(type)).toArray(Type[]::new);
    }

    public static Type replaceNullType2Boolean(Type type) {
        if (type.isNull()) {
            return Type.BOOLEAN;
        } else if (type.isArrayType()) {
            Type childType = ((ArrayType) type).getItemType();
            Type newType = replaceNullType2Boolean(childType);
            if (!childType.equals(newType)) {
                return new ArrayType(newType);
            }
        } else if (type.isMapType()) {
            Type keyType = ((MapType) type).getKeyType();
            Type valueType = ((MapType) type).getValueType();
            Type nkt = replaceNullType2Boolean(keyType);
            Type nvt = replaceNullType2Boolean(valueType);
            if (!keyType.equals(nkt) || !valueType.equals(nvt)) {
                return new MapType(nkt, nvt);
            }
        } else if (type.isStructType()) {
            ArrayList<StructField> newFields = Lists.newArrayList();
            for (StructField sf : ((StructType) type).getFields()) {
                newFields.add(new StructField(sf.getName(), replaceNullType2Boolean(sf.getType()), sf.getComment()));
            }
            return new StructType(newFields);
        }
        return type;
    }

    public static void checkNondeterministicFunction(ParseNode node) {
        new AstTraverser<Void, Void>() {
            @Override
            public Void visitFunctionCall(FunctionCallExpr expr, Void context) {
                if (expr.isNondeterministicBuiltinFnName()) {
                    throw new SemanticException("Materialized view query statement select item " +
                            expr.toSql() + " not supported nondeterministic function", expr.getPos());
                }
                return null;
            }
        }.visit(node);
    }

    public static void checkAutoPartitionTableLimit(FunctionCallExpr functionCallExpr, String prePartitionGranularity) {
        if (prePartitionGranularity == null) {
            return;
        }
        String functionName = functionCallExpr.getFnName().getFunction();
        if (FunctionSet.DATE_TRUNC.equalsIgnoreCase(functionName)) {
            Expr expr = functionCallExpr.getParams().exprs().get(0);
            String functionGranularity = ((StringLiteral) expr).getStringValue();
            if (!prePartitionGranularity.equalsIgnoreCase(functionGranularity)) {
                throw new ParsingException("The partition granularity of automatic partition table " +
                        "batch creation in advance should be consistent", functionCallExpr.getPos());
            }
        } else if (FunctionSet.TIME_SLICE.equalsIgnoreCase(functionName)) {
            throw new ParsingException("time_slice does not support pre-created partitions", functionCallExpr.getPos());
        }
    }

    // check the partition expr is legal and extract partition columns
    public static List<String> checkAndExtractPartitionCol(FunctionCallExpr expr, List<ColumnDef> columnDefs) {
        String functionName = expr.getFnName().getFunction();
        NodePosition pos = expr.getPos();
        List<String> columnList = Lists.newArrayList();
        List<Expr> paramsExpr = expr.getParams().exprs();
        if (FunctionSet.DATE_TRUNC.equals(functionName)) {
            if (paramsExpr.size() != 2) {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"), pos);
            }
            Expr firstExpr = paramsExpr.get(0);
            Expr secondExpr = paramsExpr.get(1);
            String partitionColumnName;
            if (secondExpr instanceof SlotRef) {
                partitionColumnName = ((SlotRef) secondExpr).getColumnName();
                columnList.add(partitionColumnName);
            } else {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"), pos);
            }

            if (firstExpr instanceof StringLiteral) {
                StringLiteral stringLiteral = (StringLiteral) firstExpr;
                String fmt = stringLiteral.getValue();
                if (!AnalyzerUtils.SUPPORTED_PARTITION_FORMAT.contains(fmt.toLowerCase())) {
                    throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"),
                            pos);
                }
                checkPartitionColumnTypeValid(expr, columnDefs, pos, partitionColumnName, fmt);
            } else {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"), pos);
            }

        } else if (FunctionSet.TIME_SLICE.equals(functionName)) {
            if (paramsExpr.size() != 4) {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"), pos);
            }
            Expr firstExpr = paramsExpr.get(0);
            String partitionColumnName;
            if (firstExpr instanceof SlotRef) {
                partitionColumnName = ((SlotRef) firstExpr).getColumnName();
                columnList.add(partitionColumnName);
            } else {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"), pos);
            }
            Expr secondExpr = paramsExpr.get(1);
            Expr thirdExpr = paramsExpr.get(2);
            if (secondExpr instanceof IntLiteral && thirdExpr instanceof StringLiteral) {
                StringLiteral stringLiteral = (StringLiteral) thirdExpr;
                String fmt = stringLiteral.getValue();
                if (!AnalyzerUtils.SUPPORTED_PARTITION_FORMAT.contains(fmt.toLowerCase())) {
                    throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"),
                            pos);
                }
                // For materialized views currently columnDefs == null
                checkPartitionColumnTypeValid(expr, columnDefs, pos, partitionColumnName, fmt);
            } else {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"), pos);
            }
            Expr fourthExpr = paramsExpr.get(3);
            if (fourthExpr instanceof StringLiteral) {
                StringLiteral boundaryLiteral = (StringLiteral) fourthExpr;
                String boundary = boundaryLiteral.getValue();
                if (!"floor".equalsIgnoreCase(boundary)) {
                    throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfoAndExplain(expr.toSql(),
                            "PARTITION BY", "Automatic partitioning does not support the ceil parameter"), pos);
                }
            } else {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"), pos);
            }
        } else if (FunctionSet.STR2DATE.equals(functionName)) {
            if (paramsExpr.size() != 2) {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"), pos);
            }
            Expr firstExpr = paramsExpr.get(0);
            Expr secondExpr = paramsExpr.get(1);
            if (firstExpr instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) firstExpr;
                columnList.add(slotRef.getColumnName());
            } else {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"), pos);
            }

            if (!(secondExpr instanceof StringLiteral)) {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"), pos);
            }

        } else {
            throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"), pos);
        }
        return columnList;
    }

    private static void checkPartitionColumnTypeValid(FunctionCallExpr expr, List<ColumnDef> columnDefs,
                                                      NodePosition pos, String partitionColumnName, String fmt) {
        // For materialized views currently columnDefs == null
        if (columnDefs != null && "hour".equalsIgnoreCase(fmt)) {
            ColumnDef partitionDef = findPartitionDefByName(columnDefs, partitionColumnName);
            if (partitionDef == null) {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"), pos);
            }
            if (partitionDef.getType() != Type.DATETIME) {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfoAndExplain(expr.toSql(),
                        "PARTITION BY", "The hour parameter only supports datetime type"), pos);
            }
        }
    }

    private static ColumnDef findPartitionDefByName(List<ColumnDef> columnDefs, String partitionName) {
        for (ColumnDef columnDef : columnDefs) {
            if (partitionName.equalsIgnoreCase(columnDef.getName())) {
                return columnDef;
            }
        }
        return null;
    }

    public static boolean containsIgnoreCase(List<String> list, String soughtFor) {
        for (String current : list) {
            if (current.equalsIgnoreCase(soughtFor)) {
                return true;
            }
        }
        return false;
    }
}
