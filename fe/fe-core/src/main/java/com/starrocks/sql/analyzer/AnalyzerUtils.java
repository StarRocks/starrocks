// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.DateUtils;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.ParsingException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AnalyzerUtils {

    public static String getOrDefaultDatabase(String dbName, ConnectContext context) {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = context.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        return dbName;
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
        protected Map<TableName, Table> tables;

        public TableCollector() {
        }

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

    public static boolean isOnlyHasOlapTables(StatementBase statementBase) {
        Map<TableName, Table> nonOlapTables = Maps.newHashMap();
        new AnalyzerUtils.NonOlapTableCollector(nonOlapTables).visit(statementBase);
        return nonOlapTables.isEmpty();
    }

    public static void copyOlapTable(StatementBase statementBase, Set<OlapTable> olapTables) {
        new AnalyzerUtils.OlapTableCollector(olapTables).visit(statementBase);
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
                TableName tableName = new TableName(icebergTable.getCatalog(), icebergTable.getDb(),
                        icebergTable.getTable());
                tables.put(tableName, table);
            } else {
                tables.put(node.getName(), table);
            }
            return null;
        }
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

        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            if (node.hasWithClause()) {
                node.getCteRelations().forEach(this::visit);
            }
            if (node.getPredicate() != null && CollectionUtils.isNotEmpty(node.getPredicate().getChildren())) {
                node.getPredicate().getChildren().forEach(this::visit);
            }
            return visit(node.getRelation());
        }

        @Override
        public Void visitInPredicate(InPredicate node, Void context) {
            if (CollectionUtils.isNotEmpty(node.getChildren())) {
                node.getChildren().forEach(this::visit);
            }
            return null;
        }

        public Void visitSubquery(Subquery node, Void context) {
            return visit(node.getQueryStatement());
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
        } else if (expr instanceof MaxLiteral) {
            return MaxLiteral.MAX_VALUE.toString();
        } else {
            return null;
        }
    }

    public static Expr resolveSlotRef(SlotRef slotRef, QueryStatement queryStatement) {
        AstVisitor<Expr, Void> slotRefResolver = new AstVisitor<Expr, Void>() {
            @Override
            public Expr visitSelect(SelectRelation node, Void context) {
                for (SelectListItem selectListItem : node.getSelectList().getItems()) {
                    if (selectListItem.getAlias() == null) {
                        if (selectListItem.getExpr() instanceof SlotRef) {
                            SlotRef slot = (SlotRef) selectListItem.getExpr();
                            if (slot.getColumnName().equalsIgnoreCase(slotRef.getColumnName())) {
                                return slot;
                            }
                        }
                    } else {
                        if (selectListItem.getAlias().equalsIgnoreCase(slotRef.getColumnName())) {
                            return selectListItem.getExpr();
                        }
                    }
                }
                return null;
            }

            @Override
            public Expr visitSetOp(SetOperationRelation node, Void context) {
                for (Relation relation : node.getRelations()) {
                    Expr resolved = relation.accept(this, null);
                    if (resolved != null) {
                        return resolved;
                    }
                }
                return null;
            }

            @Override
            public SlotRef visitValues(ValuesRelation node, Void context) {
                return null;
            }
        };
        return queryStatement.getQueryRelation().accept(slotRefResolver, null);
    }

    public static void checkNondeterministicFunction(ParseNode node) {
        new AstTraverser<Void, Void>() {
            @Override
            public Void visitFunctionCall(FunctionCallExpr expr, Void context) {
                if (expr.isNondeterministicBuiltinFnName()) {
                    throw new SemanticException("Materialized view query statement select item " +
                            expr.toSql() + " not supported nondeterministic function.");
                }
                return null;
            }
        }.visit(node);
    }
}
