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
import com.google.common.base.Predicates;
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
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.Subquery;
import com.starrocks.analysis.TableName;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
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
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;
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
import com.starrocks.sql.ast.FieldReference;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.statistic.StatsConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;
import static com.starrocks.statistic.StatsConstants.STATISTICS_DB_NAME;

public class AnalyzerUtils {
    private static final Logger LOG = LogManager.getLogger(AnalyzerUtils.class);

    // The partition format supported by date_trunc
    public static final Set<String> DATE_TRUNC_SUPPORTED_PARTITION_FORMAT =
            ImmutableSet.of("hour", "day", "month", "year");
    // The partition format supported by time_slice
    public static final Set<String> TIME_SLICE_SUPPORTED_PARTITION_FORMAT =
            ImmutableSet.of("minute", "hour", "day", "month", "year");
    // The partition format supported by mv date_trunc
    public static final Set<String> MV_DATE_TRUNC_SUPPORTED_PARTITION_FORMAT =
            ImmutableSet.of("hour", "day", "week", "month", "year");

    public static final String DEFAULT_PARTITION_NAME_PREFIX = "p";

    public static final String PARTITION_NAME_PREFIX_SPLIT = "_";

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

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            return null;
        }

        Function search = new Function(fnName, argTypes, Type.INVALID, false);
        Function fn = db.getFunction(search, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

        if (fn != null) {
            try {
                Authorizer.checkFunctionAction(context, db, fn, PrivilegeType.USAGE);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                        PrivilegeType.USAGE.name(), ObjectType.FUNCTION.name(), fn.getSignature());
            }
        }

        return fn;
    }

    private static Function getGlobalUdfFunction(ConnectContext context, FunctionName fnName, Type[] argTypes) {
        Function search = new Function(fnName, argTypes, Type.INVALID, false);
        Function fn = context.getGlobalStateMgr().getGlobalFunctionMgr()
                .getFunction(search, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (fn != null) {
            try {
                Authorizer.checkGlobalFunctionAction(context, fn, PrivilegeType.USAGE);
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

    /**
     * Gather conjuncts from this expr and return them in a list.
     * A conjunct is an expr that returns a boolean, e.g., Predicates, function calls,
     * SlotRefs, etc. Hence, this method is placed here and not in Predicate.
     */
    public static List<Expr> extractConjuncts(Expr root) {
        List<Expr> conjuncts = Lists.newArrayList();
        if (null == root) {
            return conjuncts;
        }

        extractConjunctsImpl(root, conjuncts);
        return conjuncts;
    }

    private static void extractConjunctsImpl(Expr root, List<Expr> conjuncts) {
        if (!(root instanceof CompoundPredicate)) {
            conjuncts.add(root);
            return;
        }

        CompoundPredicate cpe = (CompoundPredicate) root;
        if (!CompoundPredicate.Operator.AND.equals(cpe.getOp())) {
            conjuncts.add(root);
            return;
        }

        extractConjunctsImpl(cpe.getChild(0), conjuncts);
        extractConjunctsImpl(cpe.getChild(1), conjuncts);
    }

    /**
     * Flatten AND/OR tree
     */
    public static List<Expr> flattenPredicate(Expr root) {
        List<Expr> children = Lists.newArrayList();
        if (null == root) {
            return children;
        }

        flattenPredicate(root, children);
        return children;
    }

    private static void flattenPredicate(Expr root, List<Expr> children) {
        Queue<Expr> q = Lists.newLinkedList();
        q.add(root);
        while (!q.isEmpty()) {
            Expr head = q.poll();
            if (head instanceof CompoundPredicate &&
                    (((CompoundPredicate) head).getOp() == CompoundPredicate.Operator.AND ||
                            ((CompoundPredicate) head).getOp() == CompoundPredicate.Operator.OR)) {
                q.addAll(head.getChildren());
            } else {
                children.add(head);
            }
        }
    }

    private static class DBCollector implements AstVisitor<Void, Void> {
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
        public Void visitSubqueryRelation(SubqueryRelation node, Void context) {
            return visit(node.getQueryStatement());
        }

        public Void visitView(ViewRelation node, Void context) {
            getDB(node.getName());
            return visit(node.getQueryStatement(), context);
        }

        @Override
        public Void visitSetOp(SetOperationRelation node, Void context) {
            if (node.hasWithClause()) {
                node.getCteRelations().forEach(this::visit);
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

            Database db = session.getGlobalStateMgr().getLocalMetastore().getDb(dbName);
            if (db == null) {
                return;
            }
            dbs.put(db.getFullName(), db);
        }
    }

    // Get all the selected table columns
    public static Map<TableName, Set<String>> collectAllSelectTableColumns(StatementBase statementBase) {
        SelectTableColumnCollector selectTableColumnCollector = null;
        if (statementBase instanceof UpdateStmt) {
            selectTableColumnCollector = new UpdateStmtSelectTableColumnCollector((UpdateStmt) statementBase);
        } else {
            selectTableColumnCollector = new SelectTableColumnCollector();
        }
        selectTableColumnCollector.visit(statementBase);
        return selectTableColumnCollector.getTableColumns();
    }

    private static class UpdateStmtSelectTableColumnCollector extends SelectTableColumnCollector {

        private Table updateTable;
        private TableName updateTableName;

        public UpdateStmtSelectTableColumnCollector(UpdateStmt updateStmt) {
            this.updateTable = updateStmt.getTable();
            this.updateTableName = updateStmt.getTableName();
        }

        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            Relation relation = node.getRelation();
            doVisitSelect(node);
            if (relation instanceof ValuesRelation) {
                return null;
            }
            if (node.hasWithClause()) {
                node.getCteRelations().forEach(this::visit);
            }

            if (node.getOrderBy() != null) {
                for (OrderByElement orderByElement : node.getOrderBy()) {
                    visit(orderByElement.getExpr());
                }
            }

            // In the UpdateStatement, columns other than those in the assignment are taken as outputExpression for
            // the SelectRelation (even if these columns are not mentioned in the SQL),
            // which is not inline with expectations.
            boolean skipOutputExpr = (relation instanceof JoinRelation) ||
                    (relation instanceof TableRelation && ((TableRelation) relation).getTable().equals(updateTable));
            if (node.getOutputExpression() != null) {
                if (skipOutputExpr) {
                    node.getOutputExpression().stream()
                            .filter(expr -> !(expr instanceof SlotRef && ((SlotRef) expr).getQualifiedName() == null &&
                                    ((SlotRef) expr).getTblNameWithoutAnalyzed().equals(updateTableName)))
                            .forEach(this::visit);
                } else {
                    node.getOutputExpression().forEach(this::visit);
                }
            }

            if (node.getPredicate() != null) {
                visit(node.getPredicate());
            }

            if (node.getGroupBy() != null) {
                node.getGroupBy().forEach(this::visit);
            }

            if (node.getAggregate() != null) {
                node.getAggregate().forEach(this::visit);
            }

            if (node.getHaving() != null) {
                visit(node.getHaving());
            }

            return visit(node.getRelation());
        }
    }

    private static class SelectTableColumnCollector extends AstTraverser<Void, Void> {
        protected Map<TableName, Set<String>> tableColumns;
        protected Map<String, TableName> aliasMap;
        protected Set<TableName> ctes;
        protected Set<TableName> realTableAndViews;

        public SelectTableColumnCollector() {
            this.tableColumns = Maps.newHashMap();
            this.aliasMap = Maps.newHashMap();
            this.ctes = Sets.newHashSet();
            this.realTableAndViews = Sets.newHashSet();
        }

        public Map<TableName, Set<String>> getTableColumns() {
            Map<TableName, Set<String>> added = Maps.newHashMap();
            Iterator<TableName> iterator = tableColumns.keySet().iterator();
            while (iterator.hasNext()) {
                TableName next = iterator.next();
                if (aliasMap.containsKey(next.getTbl())) {
                    Set<String> columns = tableColumns.get(next);
                    iterator.remove();
                    added.put(aliasMap.get(next.getTbl()), columns);
                }
            }
            tableColumns.putAll(added);
            for (TableName cte : ctes) {
                // policy rewrite node tblName maybe real Table/View
                if (!realTableAndViews.contains(cte)) {
                    tableColumns.remove(cte);
                }
            }
            return tableColumns;
        }

        @Override
        public Void visitCTE(CTERelation node, Void context) {
            ctes.add(node.getResolveTableName());
            return super.visitCTE(node, context);
        }

        @Override
        public Void visitSubqueryRelation(SubqueryRelation node, Void context) {
            ctes.add(node.getResolveTableName());
            return super.visitSubqueryRelation(node, context);
        }

        @Override
        public Void visitTableFunction(TableFunctionRelation node, Void context) {
            // TableFunctionRelation is also a subquery
            ctes.add(node.getAlias());
            node.getChildExpressions().forEach(this::visit);
            return super.visitTableFunction(node, context);
        }

        /**
         * treat {@link NormalizedTableFunctionRelation} as JoinRelation
         **/
        @Override
        public Void visitNormalizedTableFunction(NormalizedTableFunctionRelation node, Void context) {
            return super.visitJoin(node, context);
        }

        private void visitTableAndView(TableName alias, TableName realName, boolean createByPolicyRewritten) {
            // `select ... from t1 t2` TableRelation's name is t1, alias is t2
            // but in SlotRef, tblName would be table's alias instead of name.
            // So in last stage, should find slotRef's realTableName
            if (alias != null) {
                aliasMap.put(alias.getTbl(), realName);
            }
            if (!createByPolicyRewritten) {
                realTableAndViews.add(realName);
            } else {
                // policy rewritten node should be invisible for user (treated as cte)
                ctes.add(realName);
            }
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            visitTableAndView(node.getAlias(), node.getName(), node.isCreateByPolicyRewritten());
            return super.visitTable(node, context);
        }

        @Override
        public Void visitView(ViewRelation node, Void context) {
            visitTableAndView(node.getAlias(), node.getName(), node.isCreateByPolicyRewritten());
            // block view definition drill-down
            return null;
        }

        // find all TableRelations or ViewRelations in 'select * from ...'
        private void fillStarRelation(Relation relation, Set<TableName> starRelationNames) {
            if (relation instanceof TableRelation || relation instanceof ViewRelation) {
                // except virtual relation like FileTableFunctionRelation
                if (!(relation instanceof FileTableFunctionRelation)) {
                    starRelationNames.add(relation.getResolveTableName());
                }
            } else if (relation instanceof JoinRelation) {
                JoinRelation joinRelation = (JoinRelation) relation;
                fillStarRelation(joinRelation.getLeft(), starRelationNames);
                fillStarRelation(joinRelation.getRight(), starRelationNames);
            }
        }

        protected void doVisitSelect(SelectRelation node) {
            Relation relation = node.getRelation();
            if (node.getOutputExpression().stream().allMatch(FieldReference.class::isInstance)) {
                Set<TableName> starRelationNames = Sets.newHashSet();
                fillStarRelation(relation, starRelationNames);
                starRelationNames.forEach(name -> put(name, "*"));
            }
        }

        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            doVisitSelect(node);
            if (node.getRelation() instanceof ValuesRelation) {
                return null;
            }
            return super.visitSelect(node, context);
        }

        protected void put(TableName tableName, String column) {
            tableColumns.compute(tableName, (k, v) -> {
                if (v == null) {
                    HashSet<String> strings = new HashSet<>();
                    strings.add(column);
                    return strings;
                } else {
                    v.add(column);
                    return v;
                }
            });
        }

        @Override
        public Void visitSlot(SlotRef slotRef, Void context) {
            // filter _LAMBDA_TABLE && alias SlotRef
            if (!slotRef.isFromLambda() && slotRef.getTblNameWithoutAnalyzed() != null) {
                // when used `slotRef.getColumnName()`, it would like c2.c2_sub1 instead of c2 for struct data type
                // so finally use `slotRef.getLabel()`
                put(slotRef.getTblNameWithoutAnalyzed(), slotRef.getLabel().replace("`", ""));
            }
            return null;
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
            this.tables = Maps.newHashMap();
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
        Map<TableName, Relation> allTableAndViewRelations = Maps.newHashMap();
        new TableAndViewRelationsCollector(allTableAndViewRelations).visit(parseNode);
        return allTableAndViewRelations;
    }

    public static List<FileTableFunctionRelation> collectFileTableFunctionRelation(StatementBase statementBase) {
        List<FileTableFunctionRelation> fileTableFunctionRelations = Lists.newArrayList();
        new AnalyzerUtils.FileTableFunctionRelationsCollector(fileTableFunctionRelations).visit(statementBase);
        return fileTableFunctionRelations;
    }

    /**
     * CopySafe:
     * 1. OlapTable & MaterializedView, that support the copyOnlyForQuery interface
     * 2. External tables with immutable memory-structure
     */
    public static boolean areTablesCopySafe(StatementBase statementBase) {
        Map<TableName, Table> nonOlapTables = Maps.newHashMap();
        new CopyUnsafeTablesCollector(nonOlapTables).visit(statementBase);
        return nonOlapTables.isEmpty();
    }

    public static boolean hasTemporaryTables(StatementBase statementBase) {
        Map<TableName, Table> tables = new HashMap<>();
        new AnalyzerUtils.TableCollector(tables).visit(statementBase);
        return tables.values().stream().anyMatch(t -> t.isTemporaryTable());
    }

    /**
     * Whether this statement access external tables
     */
    public static boolean hasExternalTables(StatementBase statementBase) {
        List<Table> tables = Lists.newArrayList();
        collectSpecifyExternalTables(statementBase, tables, Predicates.alwaysTrue());
        return !tables.isEmpty();
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
                TableName tableName =
                        new TableName(table.getCatalogName(), table.getCatalogDBName(), table.getCatalogTableName());
                tables.put(tableName, table);
            } else if (table.isIcebergTable()) {
                IcebergTable icebergTable = (IcebergTable) table;
                TableName tableName = new TableName(icebergTable.getCatalogName(), icebergTable.getCatalogDBName(),
                        icebergTable.getCatalogTableName());
                tables.put(tableName, table);
            } else if (table.isPaimonTable()) {
                PaimonTable paimonTable = (PaimonTable) table;
                TableName tableName = new TableName(paimonTable.getCatalogName(), paimonTable.getCatalogDBName(),
                        paimonTable.getCatalogTableName());
                tables.put(tableName, table);
            } else {
                tables.put(node.getName(), table);
            }
            return null;
        }
    }

    private static class CopyUnsafeTablesCollector extends TableCollector {

        private static final ImmutableSet<Table.TableType> IMMUTABLE_EXTERNAL_TABLES =
                ImmutableSet.of(Table.TableType.HIVE, Table.TableType.ICEBERG);

        public CopyUnsafeTablesCollector(Map<TableName, Table> tables) {
            super(tables);
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            if (!tables.isEmpty()) {
                return null;
            }

            Table table = node.getTable();
            int relatedMVCount = node.getTable().getRelatedMaterializedViews().size();
            boolean useNonLockOptimization = Config.skip_whole_phase_lock_mv_limit < 0 ||
                    relatedMVCount <= Config.skip_whole_phase_lock_mv_limit;
            // TODO: not support LakeTable right now
            if ((table.isOlapTableOrMaterializedView() && useNonLockOptimization)) {
                // OlapTable can be copied via copyOnlyForQuery
                return null;
            } else if (IMMUTABLE_EXTERNAL_TABLES.contains(table.getType())) {
                // Immutable table
                return null;
            } else {
                tables.put(node.getName(), node.getTable());
            }
            return null;
        }
    }

    private static class OlapTableCollector extends TableCollector {
        private Set<OlapTable> olapTables;
        private Map<TableIndexId, OlapTable> idMap;

        public OlapTableCollector(Set<OlapTable> tables) {
            this.olapTables = tables;
            this.idMap = new HashMap<>();
        }

        /**
         * One table may contain multi MaterializeIndexMetas. and it's different if one has tableA and baseIndex a
         * and one has tableA and baseIndex b. So use TableId and its base table index as the key to distinguish a TableRelation.
         */
        class TableIndexId {
            long tableId;
            long baseIndexId;

            public TableIndexId(long tableId, long indexId) {
                this.tableId = tableId;
                this.baseIndexId = indexId;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                TableIndexId that = (TableIndexId) o;
                return tableId == that.tableId && baseIndexId == that.baseIndexId;
            }

            @Override
            public int hashCode() {
                return Objects.hash(tableId, baseIndexId);
            }
        }

        @Override
        public Void visitInsertStatement(InsertStmt node, Void context) {
            super.visitInsertStatement(node, context);
            Table copied = copyTable(node.getTargetTable());
            if (copied != null) {
                node.setTargetTable(copied);
            }
            return null;
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            Table copied = copyTable(node.getTable());
            if (copied != null) {
                node.setTable(copied);
            }
            return null;
        }

        // TODO: support cloud native table and mv
        private Table copyTable(Table originalTable) {
            if (!(originalTable instanceof OlapTable)) {
                return null;
            }
            OlapTable table = (OlapTable) originalTable;
            TableIndexId tableIndexId = new TableIndexId(table.getId(), table.getBaseIndexId());
            OlapTable existed = idMap.get(tableIndexId);
            if (existed != null) {
                return existed;
            }

            OlapTable copied = null;
            if (originalTable.isOlapTable()) {
                copied = new OlapTable();
            } else if (originalTable.isOlapMaterializedView()) {
                copied = new MaterializedView();
            } else {
                return null;
            }

            olapTables.add(table);
            table.copyOnlyForQuery(copied);
            idMap.put(tableIndexId, copied);
            return copied;
        }
    }

    // The conception is not very clear, be careful when use it.
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
            boolean internal = table.isNativeTableOrMaterializedView() || table.isOlapView();
            if (!internal && predicate.test(table)) {
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
        public Void visitSubqueryRelation(SubqueryRelation node, Void context) {
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
        public Void visitSubqueryRelation(SubqueryRelation node, Void context) {
            // no recursive
            subQueryRelations.put(node.getResolveTableName(), node);
            return null;
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            return null;
        }
    }

    private static class FileTableFunctionRelationsCollector extends AstTraverser<Void, Void> {

        private final List<FileTableFunctionRelation> fileTableFunctionRelations;

        public FileTableFunctionRelationsCollector(List<FileTableFunctionRelation> fileTableFunctionRelations) {
            this.fileTableFunctionRelations = fileTableFunctionRelations;
        }

        @Override
        public Void visitFileTableFunction(FileTableFunctionRelation node, Void context) {
            fileTableFunctionRelations.add(node);
            return null;
        }
    }

    public static Set<TableName> getAllTableNamesForAnalyzeJobStmt(long dbId, long tableId) {
        Set<TableName> tableNames = Sets.newHashSet();
        if (StatsConstants.DEFAULT_ALL_ID != tableId && StatsConstants.DEFAULT_ALL_ID != dbId) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db != null && !db.isSystemDatabase()) {
                Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
                if (table != null && table.isOlapOrCloudNativeTable()) {
                    tableNames.add(new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            db.getFullName(), table.getName()));
                }
            }
        } else if (StatsConstants.DEFAULT_ALL_ID == tableId && StatsConstants.DEFAULT_ALL_ID != dbId) {
            getTableNamesInDb(tableNames, dbId);
        } else {
            List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();
            for (Long id : dbIds) {
                getTableNamesInDb(tableNames, id);
            }
        }

        return tableNames;
    }

    private static void getTableNamesInDb(Set<TableName> tableNames, Long id) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(id);
        if (db != null && !db.isSystemDatabase()) {
            for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
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
                int len = ScalarType.getOlapMaxVarcharLength();
                if (srcType instanceof ScalarType) {
                    ScalarType scalarType = (ScalarType) srcType;
                    if (scalarType.getLength() > 0) {
                        // Catalog's varchar length may larger than olap's max varchar length
                        len = Integer.min(scalarType.getLength(), ScalarType.getOlapMaxVarcharLength());
                    }
                }
                newType = ScalarType.createVarcharType(len);
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
            Map<ColumnId, Column> idToColumn,
            ExpressionRangePartitionInfo expressionRangePartitionInfo)
            throws AnalysisException {
        long interval = 1;
        String granularity;
        List<Expr> partitionExprs = expressionRangePartitionInfo.getPartitionExprs(idToColumn);

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
                                                                              List<List<String>> partitionValues,
                                                                              boolean isTemp,
                                                                              String partitionNamePrefix)
            throws AnalysisException {
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo instanceof ExpressionRangePartitionInfo) {
            PartitionMeasure measure = checkAndGetPartitionMeasure(olapTable.getIdToColumn(),
                    (ExpressionRangePartitionInfo) partitionInfo);
            return getAddPartitionClauseForRangePartition(olapTable, partitionValues, isTemp, partitionNamePrefix, measure,
                    (ExpressionRangePartitionInfo) partitionInfo);
        } else if (partitionInfo instanceof ListPartitionInfo) {
            Short replicationNum = olapTable.getTableProperty().getReplicationNum();
            DistributionDesc distributionDesc = olapTable.getDefaultDistributionInfo()
                    .toDistributionDesc(olapTable.getIdToColumn());
            Map<String, String> partitionProperties =
                    ImmutableMap.of("replication_num", String.valueOf(replicationNum));

            // table partitions for check
            TreeMap<String, PListCell> tablePartitions = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            tablePartitions.putAll(olapTable.getListPartitionItems());
            List<String> partitionColNames = Lists.newArrayList();
            List<PartitionDesc> partitionDescs = Lists.newArrayList();
            for (List<String> partitionValue : partitionValues) {
                List<String> formattedPartitionValue = Lists.newArrayList();
                for (String value : partitionValue) {
                    String formatValue = getFormatPartitionValue(value);
                    formattedPartitionValue.add(formatValue);
                }

                String partitionName = DEFAULT_PARTITION_NAME_PREFIX + Joiner.on("_").join(formattedPartitionValue);
                if (partitionName.length() > FeConstants.MAX_LIST_PARTITION_NAME_LENGTH) {
                    partitionName = partitionName.substring(0, FeConstants.MAX_LIST_PARTITION_NAME_LENGTH)
                            + "_" + Integer.toHexString(partitionName.hashCode());
                }
                if (partitionNamePrefix != null) {
                    if (partitionNamePrefix.contains(PARTITION_NAME_PREFIX_SPLIT)) {
                        throw new AnalysisException("partition name prefix can not contain " + PARTITION_NAME_PREFIX_SPLIT);
                    }
                    partitionName = partitionNamePrefix + PARTITION_NAME_PREFIX_SPLIT + partitionName;
                }
                if (!partitionColNames.contains(partitionName)) {
                    List<List<String>> partitionItems = Collections.singletonList(partitionValue);
                    PListCell cell = new PListCell(partitionItems);
                    // If the partition name already exists and their partition values are different, change the partition name.
                    if (tablePartitions.containsKey(partitionName) && !tablePartitions.get(partitionName).equals(cell)) {
                        partitionName = calculateUniquePartitionName(partitionName, tablePartitions);
                        if (tablePartitions.containsKey(partitionName)) {
                            throw new AnalysisException(String.format("partition name %s already exists in table " +
                                    "%s.", partitionName, olapTable.getName()));
                        }
                    }
                    MultiItemListPartitionDesc multiItemListPartitionDesc = new MultiItemListPartitionDesc(true,
                            partitionName, partitionItems, partitionProperties);
                    multiItemListPartitionDesc.setSystem(true);
                    partitionDescs.add(multiItemListPartitionDesc);
                    partitionColNames.add(partitionName);

                    // update table partition
                    tablePartitions.put(partitionName, cell);
                }
            }
            ListPartitionDesc listPartitionDesc = new ListPartitionDesc(partitionColNames, partitionDescs);
            listPartitionDesc.setSystem(true);
            return new AddPartitionClause(listPartitionDesc, distributionDesc,
                    partitionProperties, isTemp);
        } else {
            throw new AnalysisException("automatic partition only support partition by value.");
        }
    }

    /**
     * Calculate the unique partition name for list partition.
     */
    private static String calculateUniquePartitionName(String partitionName,
                                                       Map<String, PListCell> tablePartitions) {
        // ensure partition name is unique with case-insensitive
        int diff = partitionName.hashCode();
        String newPartitionName = partitionName + "_" + Integer.toHexString(diff);
        if (tablePartitions.containsKey(newPartitionName)) {
            int i = 0;
            do {
                diff += 1;
                newPartitionName = partitionName + "_" + Integer.toHexString(diff);
            } while (i++ < 100 && tablePartitions.containsKey(newPartitionName));
        }
        return newPartitionName;
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

    private static AddPartitionClause getAddPartitionClauseForRangePartition(
            OlapTable olapTable,
            List<List<String>> partitionValues,
            boolean isTemp,
            String partitionPrefix,
            PartitionMeasure measure,
            ExpressionRangePartitionInfo expressionRangePartitionInfo) throws AnalysisException {
        String granularity = measure.getGranularity();
        long interval = measure.getInterval();
        Type firstPartitionColumnType = expressionRangePartitionInfo.getPartitionColumns(olapTable.getIdToColumn())
                .get(0).getType();
        Short replicationNum = olapTable.getTableProperty().getReplicationNum();
        DistributionDesc distributionDesc = olapTable.getDefaultDistributionInfo()
                .toDistributionDesc(olapTable.getIdToColumn());
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
                    case "minute":
                        beginTime = beginTime.withSecond(0).withNano(0);
                        partitionName = DEFAULT_PARTITION_NAME_PREFIX + beginTime.format(DateUtils.MINUTE_FORMATTER_UNIX);
                        endTime = beginTime.plusMinutes(interval);
                        break;
                    case "hour":
                        beginTime = beginTime.withMinute(0).withSecond(0).withNano(0);
                        partitionName = DEFAULT_PARTITION_NAME_PREFIX + beginTime.format(DateUtils.HOUR_FORMATTER_UNIX);
                        endTime = beginTime.plusHours(interval);
                        break;
                    case "day":
                        beginTime = beginTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
                        partitionName = DEFAULT_PARTITION_NAME_PREFIX + beginTime.format(DateUtils.DATEKEY_FORMATTER_UNIX);
                        endTime = beginTime.plusDays(interval);
                        break;
                    case "month":
                        beginTime = beginTime.withDayOfMonth(1);
                        partitionName = DEFAULT_PARTITION_NAME_PREFIX + beginTime.format(DateUtils.MONTH_FORMATTER_UNIX);
                        endTime = beginTime.plusMonths(interval);
                        break;
                    case "year":
                        beginTime = beginTime.withDayOfYear(1);
                        partitionName = DEFAULT_PARTITION_NAME_PREFIX + beginTime.format(DateUtils.YEAR_FORMATTER_UNIX);
                        endTime = beginTime.plusYears(interval);
                        break;
                    default:
                        throw new AnalysisException("unsupported automatic partition granularity:" + granularity);
                }
                PartitionKeyDesc partitionKeyDesc =
                        createPartitionKeyDesc(firstPartitionColumnType, beginTime, endTime);

                if (partitionPrefix != null) {
                    if (partitionPrefix.contains(PARTITION_NAME_PREFIX_SPLIT)) {
                        throw new AnalysisException("partition name prefix can not contain " + PARTITION_NAME_PREFIX_SPLIT);
                    }
                    partitionName = partitionPrefix + PARTITION_NAME_PREFIX_SPLIT + partitionName;
                }

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
        return new AddPartitionClause(rangePartitionDesc, distributionDesc, partitionProperties, isTemp);
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
                newFields.add(new StructField(sf.getName(), sf.getFieldId(),
                        replaceNullType2Boolean(sf.getType()), sf.getComment()));
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

    static class NonDeterministicAnalyzeVisitor extends AstTraverser<Void, Void> {
        Optional<String> nonDeterministicFunctionOpt = Optional.empty();

        public Pair<Boolean, String> getResult() {
            return Pair.create(nonDeterministicFunctionOpt.isPresent(), nonDeterministicFunctionOpt.orElse(null));
        }

        private boolean containsNonDeterministicFunction(FunctionCallExpr expr) {
            FunctionName functionName = expr.getFnName();
            if (functionName == null) {
                return false;
            }
            return FunctionSet.allNonDeterministicFunctions.contains(functionName.getFunction());
        }

        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            if (node.getSelectList() != null) {
                for (SelectListItem item : node.getSelectList().getItems()) {
                    Expr expr = item.getExpr();
                    if (expr instanceof FunctionCallExpr) {
                        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
                        if (containsNonDeterministicFunction(functionCallExpr)) {
                            nonDeterministicFunctionOpt = Optional.of(functionCallExpr.getFnName().getFunction());
                            return null;
                        }
                    }
                }
            }
            return super.visitSelect(node, context);
        }

        @Override
        public Void visitFunctionCall(FunctionCallExpr expr, Void context) {
            if (containsNonDeterministicFunction(expr)) {
                nonDeterministicFunctionOpt = Optional.ofNullable(expr.getFnName()).map(FunctionName::getFunction);
                return null;
            }
            for (Expr param : expr.getChildren()) {
                visit(param);
                if (nonDeterministicFunctionOpt.isPresent()) {
                    return null;
                }
            }
            return null;
        }
    }

    /**
     * Check if the function is a non-deterministic function with strict mode, eg current_date/current_timestamp also
     * is treated as non-deterministic function too.
     *
     * @param node the node to check
     * @return true if node contains non-deterministic functions, false otherwise.
     */
    public static Pair<Boolean, String> containsNonDeterministicFunction(ParseNode node) {
        NonDeterministicAnalyzeVisitor visitor = new NonDeterministicAnalyzeVisitor();
        visitor.visit(node);
        return visitor.getResult();
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

    public static List<String> checkAndExtractPartitionCol(FunctionCallExpr expr, List<ColumnDef> columnDefs) {
        return checkAndExtractPartitionCol(expr, columnDefs, AnalyzerUtils.DATE_TRUNC_SUPPORTED_PARTITION_FORMAT);
    }

    /**
     * Check the partition expr is legal and extract partition columns
     * TODO: support date_trunc('week', dt) for normal olap table.
     *
     * @param expr                      partition expr
     * @param columnDefs                partition column defs
     * @param supportedDateTruncFormats date trunc supported formats which are a bit different bewtween mv and olap table
     * @return partition column names
     */
    public static List<String> checkAndExtractPartitionCol(FunctionCallExpr expr, List<ColumnDef> columnDefs,
                                                           Set<String> supportedDateTruncFormats) {
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
                if (!supportedDateTruncFormats.contains(fmt.toLowerCase())) {
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
                if (!AnalyzerUtils.TIME_SLICE_SUPPORTED_PARTITION_FORMAT.contains(fmt.toLowerCase())) {
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
        if (columnDefs != null && ("hour".equalsIgnoreCase(fmt) || "minute".equalsIgnoreCase(fmt))) {
            ColumnDef partitionDef = findPartitionDefByName(columnDefs, partitionColumnName);
            if (partitionDef == null) {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfo(expr.toSql(), "PARTITION BY"), pos);
            }
            if (partitionDef.getType() != Type.DATETIME) {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedExprWithInfoAndExplain(expr.toSql(),
                        "PARTITION BY", "The hour/minute parameter only supports datetime type"), pos);
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

    // rename slotRef in expr to newColName
    public static Expr renameSlotRef(Expr expr, String newColName) {
        if (expr instanceof FunctionCallExpr) {
            for (int i = 0; i < expr.getChildren().size(); i++) {
                Expr child = expr.getChildren().get(i);
                if (child instanceof SlotRef) {
                    expr.setChild(i, new SlotRef(null, newColName));
                    break;
                }
            }
            return expr;
        } else if (expr instanceof CastExpr) {
            CastExpr castExpr = (CastExpr) expr;
            ArrayList<Expr> children = castExpr.getChildren();
            for (Expr child : children) {
                return renameSlotRef(child, newColName);
            }
        }
        return expr;
    }
}
