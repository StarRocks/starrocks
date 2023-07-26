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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.ColWithComment;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.PartitionRangeDesc;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;
import static com.starrocks.server.CatalogMgr.isInternalCatalog;

public class MaterializedViewAnalyzer {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewAnalyzer.class);

    private static final Set<Table.TableType> SUPPORTED_TABLE_TYPE =
            ImmutableSet.of(Table.TableType.OLAP,
                    Table.TableType.HIVE,
                    Table.TableType.HUDI,
                    Table.TableType.ICEBERG,
                    Table.TableType.JDBC,
                    Table.TableType.MYSQL,
                    Table.TableType.VIEW);

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new MaterializedViewAnalyzerVisitor().visit(stmt, session);
    }

<<<<<<< HEAD
    public static List<BaseTableInfo> getBaseTableInfos(Map<TableName, Table> tableNameTableMap) {
        List<BaseTableInfo> baseTableInfos = Lists.newArrayList();

        if (tableNameTableMap.isEmpty()) {
            throw new SemanticException("Can not find base table in query statement");
        }
        tableNameTableMap.forEach((tableNameInfo, table) -> {
            Preconditions.checkState(table != null, "Materialized view base table is null");
            if (!isSupportBasedOnTable(table)) {
                throw new SemanticException("Create materialized view do not support the table type: " +
                        table.getType());
            }
            if (table instanceof MaterializedView && !((MaterializedView) table).isActive()) {
                throw new SemanticException(
                        "Create materialized view from inactive materialized view: " + table.getName());
            }
            if (isExternalTableFromResource(table)) {
=======
    public static List<BaseTableInfo> getBaseTableInfos(QueryStatement queryStatement, boolean withCheck) {
        List<BaseTableInfo> baseTableInfos = Lists.newArrayList();
        processBaseTables(queryStatement, baseTableInfos, withCheck);
        Set<BaseTableInfo> baseTableInfoSet = Sets.newHashSet(baseTableInfos);
        baseTableInfos.clear();
        baseTableInfos.addAll(baseTableInfoSet);
        return baseTableInfos;
    }

    private static void processBaseTables(QueryStatement queryStatement, List<BaseTableInfo> baseTableInfos,
                                          boolean withCheck) {
        Map<TableName, Table> tableNameTableMap = AnalyzerUtils.collectAllConnectorTableAndView(queryStatement);
        for (Map.Entry<TableName, Table> entry : tableNameTableMap.entrySet()) {
            TableName tableNameInfo = entry.getKey();
            Table table = entry.getValue();
            if (withCheck) {
                Preconditions.checkState(table != null, "Materialized view base table is null");
                if (!isSupportBasedOnTable(table)) {
                    throw new SemanticException("Create/Rebuild materialized view do not support the table type: " +
                            table.getType(), tableNameInfo.getPos());
                }
                if (table instanceof MaterializedView && !((MaterializedView) table).isActive()) {
                    throw new SemanticException(
                            "Create/Rebuild materialized view from inactive materialized view: " + table.getName(),
                            tableNameInfo.getPos());
                }
            }

            if (table.isView()) {
                continue;
            }

            if (!FeConstants.isReplayFromQueryDump && isExternalTableFromResource(table)) {
>>>>>>> fc1d7a6987 ([BugFix] Fix bug replay failed when check base mv active (#27959))
                throw new SemanticException(
                        "Only supports creating materialized views based on the external table " +
                                "which created by catalog");
            }
            Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(tableNameInfo.getCatalog(),
                    tableNameInfo.getDb());
            if (isInternalCatalog(tableNameInfo.getCatalog())) {
                baseTableInfos.add(new BaseTableInfo(database.getId(), database.getFullName(),
                        table.getId()));
            } else {
                baseTableInfos.add(new BaseTableInfo(tableNameInfo.getCatalog(),
                        tableNameInfo.getDb(), table.getTableIdentifier()));
            }
<<<<<<< HEAD
        });
        return baseTableInfos;
=======
            baseTableInfos.add(BaseTableInfo.fromTableName(tableNameInfo, table));
        }
        processViews(queryStatement, baseTableInfos, withCheck);
>>>>>>> fc1d7a6987 ([BugFix] Fix bug replay failed when check base mv active (#27959))
    }

    private static boolean isSupportBasedOnTable(Table table) {
        return SUPPORTED_TABLE_TYPE.contains(table.getType()) || table instanceof OlapTable;
    }

    private static boolean isExternalTableFromResource(Table table) {
        if (table instanceof OlapTable) {
            return false;
        } else if (table instanceof JDBCTable || table instanceof MysqlTable) {
            return false;
        } else if (table instanceof HiveTable || table instanceof HudiTable) {
            HiveMetaStoreTable hiveMetaStoreTable = (HiveMetaStoreTable) table;
            String catalogName = hiveMetaStoreTable.getCatalogName();
            return Strings.isBlank(catalogName) || isResourceMappingCatalog(catalogName);
        } else if (table instanceof IcebergTable) {
            IcebergTable icebergTable = (IcebergTable) table;
            String catalogName = icebergTable.getCatalogName();
            return Strings.isBlank(catalogName) || isResourceMappingCatalog(catalogName);
        } else {
            return true;
        }
    }

<<<<<<< HEAD
=======
    private static void processViews(QueryStatement queryStatement, List<BaseTableInfo> baseTableInfos,
                                     boolean withCheck) {
        List<ViewRelation> viewRelations = AnalyzerUtils.collectViewRelations(queryStatement);
        if (viewRelations.isEmpty()) {
            return;
        }
        Set<ViewRelation> viewRelationSet = Sets.newHashSet(viewRelations);
        for (ViewRelation viewRelation : viewRelationSet) {
            // base tables of view
            processBaseTables(viewRelation.getQueryStatement(), baseTableInfos, withCheck);

            // view itself is considered as base-table
            baseTableInfos.add(BaseTableInfo.fromTableName(viewRelation.getName(), viewRelation.getView()));
        }
    }
>>>>>>> fc1d7a6987 ([BugFix] Fix bug replay failed when check base mv active (#27959))

    static class MaterializedViewAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        public enum RefreshTimeUnit {
            DAY,
            HOUR,
            MINUTE,
            SECOND
        }

        static class SelectRelationCollector extends AstVisitor<Void, Void> {
            private final List<SelectRelation> selectRelations = new ArrayList<>();

            public static List<SelectRelation> collectBaseRelations(QueryRelation queryRelation) {
                SelectRelationCollector collector = new SelectRelationCollector();
                queryRelation.accept(collector, null);
                return collector.selectRelations;
            }

            @Override
            public Void visitRelation(Relation node, Void context) {
                return null;
            }

            @Override
            public Void visitSelect(SelectRelation node, Void context) {
                selectRelations.add(node);
                return null;
            }

            @Override
            public Void visitSetOp(SetOperationRelation node, Void context) {
                for (QueryRelation sub : node.getRelations()) {
                    selectRelations.addAll(collectBaseRelations(sub));
                }
                return null;
            }
        }

        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement,
                                                         ConnectContext context) {
            final TableName tableNameObject = statement.getTableName();
            MetaUtils.normalizationTableName(context, tableNameObject);
            final String tableName = tableNameObject.getTbl();
            FeNameFormat.checkTableName(tableName);
            QueryStatement queryStatement = statement.getQueryStatement();
            // check query relation is select relation
            if (!(queryStatement.getQueryRelation() instanceof SelectRelation) &&
                    !(queryStatement.getQueryRelation() instanceof SetOperationRelation)) {
                throw new SemanticException("Materialized view query statement only support select or set operation");
            }

            List<SelectRelation> selectRelations =
                    SelectRelationCollector.collectBaseRelations(queryStatement.getQueryRelation());
            if (CollectionUtils.isEmpty(selectRelations)) {
                throw new SemanticException("Materialized view query statement must contain at least one select");
            }

            // analyze query statement, can check whether tables and columns exist in catalog
            Analyzer.analyze(queryStatement, context);
            AnalyzerUtils.checkNondeterministicFunction(queryStatement);

            // convert queryStatement to sql and set
            statement.setInlineViewDef(AstToSQLBuilder.toSQL(queryStatement));
            statement.setSimpleViewDef(AstToSQLBuilder.buildSimple(queryStatement));
            Database db = context.getGlobalStateMgr().getDb(statement.getTableName().getDb());
            if (db == null) {
                throw new SemanticException("Can not find database:" + statement.getTableName().getDb());
            }
            List<BaseTableInfo> baseTableInfos = getBaseTableInfos(queryStatement, true);
            // now do not support empty base tables
            // will be relaxed after test
            if (baseTableInfos.isEmpty()) {
                throw new SemanticException("Can not find base table in query statement");
            }
            statement.setBaseTableInfos(baseTableInfos);

            // set the columns into createMaterializedViewStatement
            List<Column> mvColumns = genMaterializedViewColumns(statement);
            statement.setMvColumnItems(mvColumns);

            Map<TableName, Table> aliasTableMap = getAllBaseTables(queryStatement, context);
            Map<Column, Expr> columnExprMap = Maps.newHashMap();
            List<Expr> outputExpressions = queryStatement.getQueryRelation().getOutputExpression();
            for (int i = 0; i < outputExpressions.size(); ++i) {
                columnExprMap.put(mvColumns.get(i), outputExpressions.get(i));
            }
            // some check if partition exp exists
            if (statement.getPartitionExpDesc() != null) {
                // check partition expression all in column list and
                // write the expr into partitionExpDesc if partition expression exists
                checkExpInColumn(statement, columnExprMap, context, aliasTableMap);
                // check whether partition expression functions are allowed if it exists
                checkPartitionExpParams(statement);
                // check partition column must be base table's partition column
                checkPartitionColumnWithBaseTable(statement, aliasTableMap);
            }
            // check and analyze distribution
            checkDistribution(statement, aliasTableMap);

            planMVQuery(statement, queryStatement, context);
            return null;
        }

        private Map<TableName, Table> getAllBaseTables(QueryStatement queryStatement, ConnectContext context) {
            Map<TableName, Table> aliasTableMap = AnalyzerUtils.collectAllTableAndViewWithAlias(queryStatement);
            List<ViewRelation> viewRelations = AnalyzerUtils.collectViewRelations(queryStatement);
            if (viewRelations.isEmpty()) {
                return aliasTableMap;
            }
            for (ViewRelation viewRelation : viewRelations) {
                Map<TableName, Table> viewTableMap = getAllBaseTables(viewRelation.getQueryStatement(), context);
                aliasTableMap.putAll(viewTableMap);
            }
            Map<TableName, Table> result = Maps.newHashMap();
            for (Map.Entry<TableName, Table> entry : aliasTableMap.entrySet()) {
                // catalog may be null, should do normalization
                entry.getKey().normalization(context);
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }

        private List<BaseTableInfo> getAndCheckBaseTables(QueryStatement queryStatement) {
            List<BaseTableInfo> baseTableInfos = Lists.newArrayList();
            processBaseTables(queryStatement, baseTableInfos);
            Set<BaseTableInfo> baseTableInfoSet = Sets.newHashSet(baseTableInfos);
            baseTableInfos.clear();
            baseTableInfos.addAll(baseTableInfoSet);
            return baseTableInfos;
        }

        private void processBaseTables(QueryStatement queryStatement, List<BaseTableInfo> baseTableInfos) {
            Map<TableName, Table> tableNameTableMap = AnalyzerUtils.collectAllConnectorTableAndView(queryStatement);
            tableNameTableMap.forEach((tableNameInfo, table) -> {
                Preconditions.checkState(table != null, "Materialized view base table is null");
                if (!isSupportBasedOnTable(table)) {
                    throw new SemanticException("Create materialized view do not support the table type: " + table.getType());
                }
                if (table instanceof MaterializedView && !((MaterializedView) table).isActive()) {
                    throw new SemanticException("Create materialized view from inactive materialized view: " + table.getName());
                }

                if (table.isView()) {
                    return;
                }

                if (isExternalTableFromResource(table)) {
                    throw new SemanticException(
                            "Only supports creating materialized views based on the external table " +
                                    "which created by catalog");
                }
                Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(tableNameInfo.getCatalog(),
                        tableNameInfo.getDb());
                if (isInternalCatalog(tableNameInfo.getCatalog())) {
                    baseTableInfos.add(new BaseTableInfo(database.getId(), database.getFullName(),
                            table.getId()));
                } else {
                    baseTableInfos.add(new BaseTableInfo(tableNameInfo.getCatalog(),
                            tableNameInfo.getDb(), table.getTableIdentifier()));
                }
            });
            processViews(queryStatement, baseTableInfos);
        }

        private void processViews(QueryStatement queryStatement, List<BaseTableInfo> baseTableInfos) {
            List<ViewRelation> viewRelations = AnalyzerUtils.collectViewRelations(queryStatement);
            if (viewRelations.isEmpty()) {
                return;
            }
            Set<ViewRelation> viewRelationSet = Sets.newHashSet(viewRelations);
            for (ViewRelation viewRelation : viewRelationSet) {
                processBaseTables(viewRelation.getQueryStatement(), baseTableInfos);
            }
        }

        // TODO(murphy) implement
        // Plan the query statement and store in memory
        private void planMVQuery(CreateMaterializedViewStatement createStmt, QueryStatement query, ConnectContext ctx) {
            if (!ctx.getSessionVariable().isEnableIncrementalRefreshMV()) {
                return;
            }
            if (!createStmt.getRefreshSchemeDesc().getType().equals(MaterializedView.RefreshType.INCREMENTAL)) {
                return;
            }

            try {
                ctx.getSessionVariable().setMVPlanner(true);

                QueryRelation queryRelation = query.getQueryRelation();
                ColumnRefFactory columnRefFactory = new ColumnRefFactory();
                LogicalPlan logicalPlan = new RelationTransformer(columnRefFactory, ctx).transform(queryRelation);
                Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();
                List<ColumnRefOperator> outputColumns = new ArrayList<>();
                for (int colIdx = 0; colIdx < logicalPlan.getOutputColumn().size(); colIdx++) {
                    ColumnRefOperator ref = logicalPlan.getOutputColumn().get(colIdx);
                    outputColumns.add(ref);
                    columnRefMap.put(ref, ref);
                }

                // Build logical plan for view query
                OptExprBuilder optExprBuilder = logicalPlan.getRootBuilder();
                logicalPlan = new LogicalPlan(optExprBuilder, outputColumns, logicalPlan.getCorrelation());
                Optimizer optimizer = new Optimizer();
                PhysicalPropertySet requiredPropertySet = PhysicalPropertySet.EMPTY;
                OptExpression optimizedPlan = optimizer.optimize(
                        ctx,
                        logicalPlan.getRoot(),
                        requiredPropertySet,
                        new ColumnRefSet(logicalPlan.getOutputColumn()),
                        columnRefFactory);
                optimizedPlan.deriveMVProperty();

                // TODO: refine rules for mv plan
                // TODO: infer state
                // TODO: store the plan in create-mv statement and persist it at executor
                ExecPlan execPlan =
                        PlanFragmentBuilder.createPhysicalPlanForMV(ctx, createStmt, optimizedPlan, logicalPlan,
                                queryRelation, columnRefFactory);
            } catch (DdlException ex) {
                throw new RuntimeException(ex);
            } finally {
                ctx.getSessionVariable().setMVPlanner(false);
            }
        }

        private void checkNondeterministicFunction(Expr expr) {
            if (expr instanceof FunctionCallExpr) {
                if (((FunctionCallExpr) expr).isNondeterministicBuiltinFnName()) {
                    throw new SemanticException("Materialized view query statement select item " +
                            expr.toSql() + " not supported nondeterministic function");
                }
            }
            ArrayList<Expr> children = expr.getChildren();
            for (Expr child : children) {
                checkNondeterministicFunction(child);
            }
        }

        private List<Column> genMaterializedViewColumns(CreateMaterializedViewStatement statement) {
            List<String> columnNames = statement.getQueryStatement().getQueryRelation()
                    .getRelationFields().getAllFields().stream()
                    .map(Field::getName).collect(Collectors.toList());
            Scope queryScope = statement.getQueryStatement().getQueryRelation().getScope();
            List<Field> relationFields = queryScope.getRelationFields().getAllFields();
            List<Column> mvColumns = Lists.newArrayList();

            for (int i = 0; i < relationFields.size(); ++i) {
                Type type = AnalyzerUtils.transformTypeForMv(relationFields.get(i).getType());
                Column column = new Column(columnNames.get(i), type, relationFields.get(i).isNullable());
                // set default aggregate type, look comments in class Column
                column.setAggregationType(AggregateType.NONE, false);
                mvColumns.add(column);
            }

            if (statement.getColWithComments() != null) {
                List<ColWithComment> colWithComments = statement.getColWithComments();
                if (colWithComments.size() != mvColumns.size()) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_VIEW_WRONG_LIST);
                }
                for (int i = 0; i < colWithComments.size(); ++i) {
                    Column column = mvColumns.get(i);
                    ColWithComment colWithComment = colWithComments.get(i);
                    colWithComment.analyze();
                    column.setName(colWithComment.getColName());
                    column.setComment(colWithComment.getComment());
                }
            }

            // set duplicate key, when sort key is set, it is dup key col.
            List<String> keyCols = statement.getSortKeys();
            if (keyCols == null) {
                keyCols = Lists.newArrayList();
                int theBeginIndexOfValue = 0;
                int keySizeByte = 0;
                for (; theBeginIndexOfValue < mvColumns.size(); theBeginIndexOfValue++) {
                    Column column = mvColumns.get(theBeginIndexOfValue);
                    keySizeByte += column.getType().getIndexSize();
                    if (theBeginIndexOfValue + 1 > FeConstants.SHORTKEY_MAX_COLUMN_COUNT ||
                            keySizeByte > FeConstants.SHORTKEY_MAXSIZE_BYTES) {
                        if (theBeginIndexOfValue == 0 && column.getType().getPrimitiveType().isCharFamily()) {
                            keyCols.add(column.getName());
                            theBeginIndexOfValue++;
                        }
                        break;
                    }
                    if (!column.getType().canBeMVKey()) {
                        break;
                    }
                    if (column.getType().getPrimitiveType() == PrimitiveType.VARCHAR) {
                        keyCols.add(column.getName());
                        theBeginIndexOfValue++;
                        break;
                    }
                    keyCols.add(column.getName());
                }
                if (theBeginIndexOfValue == 0) {
                    throw new SemanticException("Data type of first column cannot be " + mvColumns.get(0).getType());
                }
            }

            if (keyCols.isEmpty()) {
                throw new SemanticException("The number of sort key is 0");
            }

            if (keyCols.size() > mvColumns.size()) {
                throw new SemanticException("The number of sort key should be less than the number of columns.");
            }

            for (int i = 0; i < keyCols.size(); i++) {
                Column column = mvColumns.get(i);
                if (!column.getName().equalsIgnoreCase(keyCols.get(i))) {
                    String keyName = keyCols.get(i);
                    if (mvColumns.stream().noneMatch(col -> col.getName().equalsIgnoreCase(keyName))) {
                        throw new SemanticException("Sort key(%s) doesn't exist.", keyCols.get(i));
                    }
                    throw new SemanticException("Sort key should be a ordered prefix of select cols.");
                }

                if (!column.getType().canBeMVKey()) {
                    throw new SemanticException("This col(%s) can't be mv sort key", keyCols.get(i));
                }
                column.setIsKey(true);
                column.setAggregationType(null, false);
            }
            return mvColumns;
        }

        private void checkExpInColumn(CreateMaterializedViewStatement statement,
                                      Map<Column, Expr> columnExprMap, ConnectContext connectContext,
                                      Map<TableName, Table> aliasTableMap) {
            ExpressionPartitionDesc expressionPartitionDesc = statement.getPartitionExpDesc();
            List<Column> columns = statement.getMvColumnItems();
            SlotRef slotRef = getSlotRef(expressionPartitionDesc.getExpr());
            if (slotRef.getTblNameWithoutAnalyzed() != null) {
                throw new SemanticException("Materialized view partition exp: "
                        + slotRef.toSql() + " must related to column");
            }
            int columnId = 0;
            for (Column column : columns) {
                if (slotRef.getColumnName().equalsIgnoreCase(column.getName())) {
                    statement.setPartitionColumn(column);
                    SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(columnId), slotRef.getColumnName(),
                            column.getType(), column.isAllowNull());
                    slotRef.setDesc(slotDescriptor);
                    break;
                }
                columnId++;
            }
            if (statement.getPartitionColumn() == null) {
                throw new SemanticException("Materialized view partition exp column:"
                        + slotRef.getColumnName() + " is not found in query statement");
            }
            checkExpWithRefExpr(statement, columnExprMap, connectContext, aliasTableMap);
        }

        private void checkExpWithRefExpr(CreateMaterializedViewStatement statement,
                                         Map<Column, Expr> columnExprMap, ConnectContext connectContext,
                                         Map<TableName, Table> aliasTableMap) {
            ExpressionPartitionDesc expressionPartitionDesc = statement.getPartitionExpDesc();
            Column partitionColumn = statement.getPartitionColumn();
            Expr refExpr = columnExprMap.get(partitionColumn);
            try {
                refExpr = resolvePartitionExpr(refExpr, connectContext, aliasTableMap);
            } catch (Exception e) {
                LOG.warn("resolve partition column failed", e);
                throw new SemanticException("resolve partition column failed");
            }
            if (expressionPartitionDesc.isFunction()) {
                // e.g. partition by date_trunc('month', ss)
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) expressionPartitionDesc.getExpr();
                if (!(refExpr instanceof SlotRef)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionCallExpr.getFnName().getFunction() +
                            " must related with column");
                }
                SlotRef slotRef = getSlotRef(functionCallExpr);
                slotRef.setType(partitionColumn.getType());
                // copy function and set it into partitionRefTableExpr
                Expr partitionRefTableExpr = functionCallExpr.clone();
                List<Expr> children = partitionRefTableExpr.getChildren();
                for (int i = 0; i < children.size(); i++) {
                    if (children.get(i) instanceof SlotRef) {
                        partitionRefTableExpr.setChild(i, refExpr);
                    }
                }
                statement.setPartitionRefTableExpr(partitionRefTableExpr);
            } else {
                // e.g. partition by date_trunc('day',ss) or partition by ss
                if (refExpr instanceof FunctionCallExpr || refExpr instanceof SlotRef) {
                    statement.setPartitionRefTableExpr(refExpr);
                } else {
                    throw new SemanticException("Materialized view partition function must related with column");
                }
            }
        }

        private static class ExprShuttleContext {
            private Expr parent;
            private int childIdx;

            public ExprShuttleContext(Expr parent, int childIdx) {
                this.parent = parent;
                this.childIdx = childIdx;
            }
        }

        private Expr resolvePartitionExpr(
                Expr partitionColumnExpr, ConnectContext connectContext, Map<TableName, Table> aliasTableMap) {
            if (partitionColumnExpr instanceof SlotRef) {
                return resolveSlotRefForView((SlotRef) partitionColumnExpr, connectContext, aliasTableMap);
            } else {
                AstVisitor exprShuttle = new AstVisitor<Void, ExprShuttleContext>() {
                    @Override
                    public Void visitExpression(Expr expr, ExprShuttleContext context) {
                        for (int i = 0; i < expr.getChildren().size(); i++) {
                            expr.getChild(i).accept(this, new ExprShuttleContext(expr, i));
                        }
                        return null;
                    }

                    @Override
                    public Void visitSlot(SlotRef slotRef, ExprShuttleContext context) {
                        Expr resolved = resolveSlotRefForView(slotRef, connectContext, aliasTableMap);
                        if (resolved == null) {
                            throw new RuntimeException(String.format("can not resolve slotRef: %s", slotRef.debugString()));
                        }
                        if (resolved != slotRef) {
                            if (context.parent != null) {
                                context.parent.setChild(context.childIdx, resolved);
                            }
                        }
                        return null;
                    }
                };
                partitionColumnExpr.accept(exprShuttle, new ExprShuttleContext(null, -1));
                return partitionColumnExpr;
            }
        }

        private Expr resolveSlotRefForView(
                SlotRef slotRef, ConnectContext connectContext, Map<TableName, Table> aliasTableMap) {
            TableName tableName = slotRef.getTblNameWithoutAnalyzed();
            tableName.normalization(connectContext);

            Table table = aliasTableMap.get(tableName);
            if (table == null) {
                String catalog = tableName.getCatalog() == null ?
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
                table = connectContext.getGlobalStateMgr()
                        .getMetadataMgr().getTable(catalog, tableName.getDb(), tableName.getTbl());
                if (table == null) {
                    throw new SemanticException("Materialized view partition expression %s could only ref to base table",
                            slotRef.toSql());
                }
            }
            if (!table.isView()) {
                // for table, it must be slotRef
                if (!table.getName().equalsIgnoreCase(tableName.getTbl())) {
                    slotRef.setType(table.getColumn(slotRef.getColumnName()).getType());
                }
                return slotRef;
            }
            View view = (View) table;
            QueryStatement queryStatement = view.getQueryStatement();
            Expr resolved = AnalyzerUtils.resolveSlotRef(slotRef, queryStatement);
            if (resolved == null) {
                return null;
            }
            SlotRef slot = getSlotRef(resolved);
            // TableName's catalog may be null, so normalization it
            slot.getTblNameWithoutAnalyzed().normalization(connectContext);
            // resolved may be view's column, resolve it recursively
            // return resolveSlotRefForView(resolved, connectContext, aliasTableMap);
            return resolvePartitionExpr(resolved, connectContext, aliasTableMap);
        }

        private void checkPartitionExpParams(CreateMaterializedViewStatement statement) {
            Expr expr = statement.getPartitionRefTableExpr();
            if (expr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = ((FunctionCallExpr) expr);
                String functionName = functionCallExpr.getFnName().getFunction();
                CheckPartitionFunction checkPartitionFunction =
                        PartitionFunctionChecker.FN_NAME_TO_PATTERN.get(functionName);
                if (checkPartitionFunction == null) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " is not support");
                }
                if (!checkPartitionFunction.check(functionCallExpr)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " check failed");
                }
            }
        }

        private void checkPartitionColumnWithBaseTable(CreateMaterializedViewStatement statement,
                                                       Map<TableName, Table> tableNameTableMap) {
            SlotRef slotRef = getSlotRef(statement.getPartitionRefTableExpr());
            Table table = tableNameTableMap.get(slotRef.getTblNameWithoutAnalyzed());

            if (table == null) {
                throw new SemanticException("Materialized view partition expression %s could only ref to base table",
                        slotRef.toSql());
            } else if (table.isNativeTableOrMaterializedView()) {
                checkPartitionColumnWithBaseOlapTable(slotRef, (OlapTable) table);
            } else if (table.isHiveTable() || table.isHudiTable()) {
                checkPartitionColumnWithBaseHMSTable(slotRef, (HiveMetaStoreTable) table);
            } else if (table.isIcebergTable()) {
                checkPartitionColumnWithBaseIcebergTable(slotRef, (IcebergTable) table);
            } else {
                throw new SemanticException("Materialized view with partition does not support base table type : %s",
                        table.getType());
            }
            replaceTableAlias(slotRef, statement, tableNameTableMap);
        }

        private void checkPartitionColumnWithBaseOlapTable(SlotRef slotRef, OlapTable table) {
            PartitionInfo partitionInfo = table.getPartitionInfo();
            if (partitionInfo instanceof SinglePartitionInfo) {
                throw new SemanticException("Materialized view partition column in partition exp " +
                        "must be base table partition column");
            } else if (partitionInfo instanceof RangePartitionInfo) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
                if (partitionColumns.size() != 1) {
                    throw new SemanticException("Materialized view related base table partition columns " +
                            "only supports single column");
                }
                String partitionColumn = partitionColumns.get(0).getName();
                if (!partitionColumn.equalsIgnoreCase(slotRef.getColumnName())) {
                    throw new SemanticException("Materialized view partition column in partition exp " +
                            "must be base table partition column");
                }
                partitionColumns.forEach(partitionColumn1 -> checkPartitionColumnType(partitionColumn1));
            } else {
                throw new SemanticException("Materialized view related base table partition type:" +
                        partitionInfo.getType().name() + "not supports");
            }
        }

        private void checkPartitionColumnWithBaseHMSTable(SlotRef slotRef, HiveMetaStoreTable table) {
            List<Column> partitionColumns = table.getPartitionColumns();
            if (table.isUnPartitioned()) {
                throw new SemanticException("Materialized view partition column in partition exp " +
                        "must be base table partition column");
            } else {
                boolean found = false;
                for (Column partitionColumn : partitionColumns) {
                    if (partitionColumn.getName().equalsIgnoreCase(slotRef.getColumnName())) {
                        checkPartitionColumnType(partitionColumn);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new SemanticException("Materialized view partition column in partition exp " +
                            "must be base table partition column");
                }
            }
        }

        private void checkPartitionColumnWithBaseIcebergTable(SlotRef slotRef, IcebergTable table) {
            org.apache.iceberg.Table icebergTable = table.getNativeTable();
            PartitionSpec partitionSpec = icebergTable.spec();
            if (partitionSpec.isUnpartitioned()) {
                throw new SemanticException("Materialized view partition column in partition exp " +
                        "must be base table partition column");
            } else {
                boolean found = false;
                for (PartitionField partitionField : partitionSpec.fields()) {
                    String partitionColumnName = partitionField.name();
                    if (partitionColumnName.equalsIgnoreCase(slotRef.getColumnName())) {
                        checkPartitionColumnType(table.getColumn(partitionColumnName));
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new SemanticException("Materialized view partition column in partition exp " +
                            "must be base table partition column");
                }
            }
        }

        private SlotRef getSlotRef(Expr expr) {
            if (expr instanceof SlotRef) {
                return ((SlotRef) expr);
            } else {
                List<SlotRef> slotRefs = Lists.newArrayList();
                expr.collect(SlotRef.class, slotRefs);
                Preconditions.checkState(slotRefs.size() == 1);
                return slotRefs.get(0);
            }
        }

        private void replaceTableAlias(SlotRef slotRef,
                                       CreateMaterializedViewStatement statement,
                                       Map<TableName, Table> tableNameTableMap) {
            TableName tableName = slotRef.getTblNameWithoutAnalyzed();
            Table table = tableNameTableMap.get(tableName);
            List<BaseTableInfo> baseTableInfos = statement.getBaseTableInfos();
            for (BaseTableInfo baseTableInfo : baseTableInfos) {
                if (table.isNativeTableOrMaterializedView()) {
                    if (baseTableInfo.getTable().equals(table)) {
                        slotRef.setTblName(new TableName(baseTableInfo.getCatalogName(),
                                baseTableInfo.getDbName(), table.getName()));
                        break;
                    }
                } else if (table.isHiveTable() || table.isHudiTable()) {
                    HiveMetaStoreTable hiveMetaStoreTable = (HiveMetaStoreTable) table;
                    if (hiveMetaStoreTable.getCatalogName().equals(baseTableInfo.getCatalogName()) &&
                            hiveMetaStoreTable.getDbName().equals(baseTableInfo.getDbName()) &&
                            table.getTableIdentifier().equals(baseTableInfo.getTableIdentifier())) {
                        slotRef.setTblName(new TableName(baseTableInfo.getCatalogName(),
                                baseTableInfo.getDbName(), table.getName()));
                        break;
                    }
                } else if (table.isIcebergTable()) {
                    IcebergTable icebergTable = (IcebergTable) table;
                    if (icebergTable.getCatalogName().equals(baseTableInfo.getCatalogName()) &&
                            icebergTable.getRemoteDbName().equals(baseTableInfo.getDbName()) &&
                            table.getTableIdentifier().equals(baseTableInfo.getTableIdentifier())) {
                        slotRef.setTblName(new TableName(baseTableInfo.getCatalogName(),
                                baseTableInfo.getDbName(), table.getName()));
                        break;
                    }
                }
            }
        }

        private void checkPartitionColumnType(Column partitionColumn) {
            PrimitiveType type = partitionColumn.getPrimitiveType();
            if (!type.isFixedPointType() && !type.isDateType()) {
                throw new SemanticException("Materialized view partition exp column:"
                        + partitionColumn.getName() + " with type " + type + " not supported");
            }
        }

        private void checkDistribution(CreateMaterializedViewStatement statement,
                                       Map<TableName, Table> tableNameTableMap) {
            DistributionDesc distributionDesc = statement.getDistributionDesc();
            List<Column> mvColumnItems = statement.getMvColumnItems();
            Map<String, String> properties = statement.getProperties();
            if (properties == null) {
                properties = Maps.newHashMap();
                statement.setProperties(properties);
            }
            if (!properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
                properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM,
                        autoInferReplicationNum(tableNameTableMap).toString());
            }
            if (distributionDesc == null) {
                if (ConnectContext.get().getSessionVariable().isAllowDefaultPartition()) {
                    distributionDesc = new HashDistributionDesc(0,
                            Lists.newArrayList(mvColumnItems.get(0).getName()));
                    statement.setDistributionDesc(distributionDesc);
                } else {
                    throw new SemanticException("Materialized view should contain distribution desc");
                }
            }
            Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            for (Column columnDef : mvColumnItems) {
                if (!columnSet.add(columnDef.getName())) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_DUP_FIELDNAME, columnDef.getName());
                }
            }
            distributionDesc.analyze(columnSet);
        }

        private Short autoInferReplicationNum(Map<TableName, Table> tableNameTableMap) {
            // For replication_num, we select the maximum value of all tables replication_num
            Short defaultReplicationNum = 1;
            for (Table table : tableNameTableMap.values()) {
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    Short replicationNum = olapTable.getDefaultReplicationNum();
                    if (replicationNum > defaultReplicationNum) {
                        defaultReplicationNum = replicationNum;
                    }
                }
            }
            return defaultReplicationNum;
        }

        @Override
        public Void visitDropMaterializedViewStatement(DropMaterializedViewStmt stmt, ConnectContext context) {
            stmt.getDbMvName().normalization(context);
            return null;
        }

        @Override
        public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement,
                                                        ConnectContext context) {
            statement.getMvName().normalization(context);
            final RefreshSchemeDesc refreshSchemeDesc = statement.getRefreshSchemeDesc();
            final String newMvName = statement.getNewMvName();
            if (newMvName != null) {
                if (statement.getMvName().getTbl().equals(newMvName)) {
                    throw new SemanticException("Same materialized view name %s", newMvName);
                }
            } else if (refreshSchemeDesc != null) {
                if (refreshSchemeDesc.getType() == MaterializedView.RefreshType.SYNC) {
                    throw new SemanticException("Unsupported change to SYNC refresh type");
                }
                if (refreshSchemeDesc instanceof AsyncRefreshSchemeDesc) {
                    AsyncRefreshSchemeDesc async = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
                    final IntervalLiteral intervalLiteral = async.getIntervalLiteral();
                    if (intervalLiteral != null) {
                        long step = ((IntLiteral) intervalLiteral.getValue()).getLongValue();
                        if (step <= 0) {
                            throw new SemanticException("Unsupported negative or zero step value: %s", step);
                        }
                        final String unit = intervalLiteral.getUnitIdentifier().getDescription().toUpperCase();
                        try {
                            RefreshTimeUnit.valueOf(unit);
                        } catch (IllegalArgumentException e) {
                            throw new SemanticException(
                                    "Unsupported interval unit: %s, only timeunit %s are supported.", unit,
                                    Arrays.asList(RefreshTimeUnit.values()));
                        }
                    }
                }
            } else if (statement.getModifyTablePropertiesClause() != null) {
                TableName mvName = statement.getMvName();
                Database db = context.getGlobalStateMgr().getDb(mvName.getDb());
                if (db == null) {
                    throw new SemanticException("Can not find database:" + mvName.getDb());
                }
                OlapTable table = (OlapTable) db.getTable(mvName.getTbl());
                if (table == null) {
                    throw new SemanticException("Can not find materialized view:" + mvName.getTbl());
                }
                if (!(table instanceof MaterializedView)) {
                    throw new SemanticException(mvName.getTbl() + " is not async materialized view");
                }
            } else if (statement.getStatus() != null) {
                String status = statement.getStatus();
                if (!AlterMaterializedViewStmt.SUPPORTED_MV_STATUS.contains(status)) {
                    throw new SemanticException("Unsupported modification for materialized view status:" + status);
                }
            } else {
                throw new SemanticException("Unsupported modification for materialized view");
            }
            return null;
        }

        @Override
        public Void visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement,
                                                          ConnectContext context) {
            statement.getMvName().normalization(context);
            TableName mvName = statement.getMvName();
            Database db = context.getGlobalStateMgr().getDb(mvName.getDb());
            if (db == null) {
                throw new SemanticException("Can not find database:" + mvName.getDb());
            }
            OlapTable table = (OlapTable) db.getTable(mvName.getTbl());
            if (table == null) {
                throw new SemanticException("Can not find materialized view:" + mvName.getTbl());
            }
            Preconditions.checkState(table instanceof MaterializedView);
            MaterializedView mv = (MaterializedView) table;
            if (!mv.isActive()) {
                throw new SemanticException(
                        "Refresh materialized view failed because " + mv.getName() + " is not active. " +
                                "You can try to active it with ALTER MATERIALIZED VIEW " + mv.getName() + " ACTIVE.");
            }
            if (statement.getPartitionRangeDesc() == null) {
                return null;
            }
            if (!(table.getPartitionInfo() instanceof RangePartitionInfo)) {
                throw new SemanticException("Not support refresh by partition for single partition mv.");
            }
            Column partitionColumn =
                    ((RangePartitionInfo) table.getPartitionInfo()).getPartitionColumns().get(0);
            if (partitionColumn.getType().isDateType()) {
                validateDateTypePartition(statement.getPartitionRangeDesc());
            } else if (partitionColumn.getType().isIntegerType()) {
                validateNumberTypePartition(statement.getPartitionRangeDesc());
            } else {
                throw new SemanticException(
                        "Unsupported batch partition build type:" + partitionColumn.getType() + ".");
            }
            return null;
        }

        private void validateNumberTypePartition(PartitionRangeDesc partitionRangeDesc)
                throws SemanticException {
            String start = partitionRangeDesc.getPartitionStart();
            String end = partitionRangeDesc.getPartitionEnd();
            long startNum;
            long endNum;
            try {
                startNum = Long.parseLong(start);
                endNum = Long.parseLong(end);
            } catch (NumberFormatException ex) {
                throw new SemanticException("Batch build partition EVERY is number type " +
                        "but START or END does not type match.");
            }
            if (startNum >= endNum) {
                throw new SemanticException("Batch build partition start value should less then end value.");
            }
        }

        private void validateDateTypePartition(PartitionRangeDesc partitionRangeDesc)
                throws SemanticException {
            String start = partitionRangeDesc.getPartitionStart();
            String end = partitionRangeDesc.getPartitionEnd();
            LocalDateTime startTime;
            LocalDateTime endTime;
            DateTimeFormatter startDateTimeFormat;
            DateTimeFormatter endDateTimeFormat;
            try {
                startDateTimeFormat = DateUtils.probeFormat(start);
                endDateTimeFormat = DateUtils.probeFormat(end);
                startTime = DateUtils.parseStringWithDefaultHSM(start, startDateTimeFormat);
                endTime = DateUtils.parseStringWithDefaultHSM(end, endDateTimeFormat);
            } catch (Exception ex) {
                throw new SemanticException("Batch build partition EVERY is date type " +
                        "but START or END does not type match.");
            }
            if (!startTime.isBefore(endTime)) {
                throw new SemanticException("Batch build partition start date should less than end date.");
            }
        }

        @Override
        public Void visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStmt statement,
                                                                ConnectContext context) {
            statement.getMvName().normalization(context);
            return null;
        }
    }

    @FunctionalInterface
    public interface CheckPartitionFunction {

        boolean check(Expr expr);
    }

    static class PartitionFunctionChecker {

        public static final Map<String, CheckPartitionFunction> FN_NAME_TO_PATTERN;

        static {
            FN_NAME_TO_PATTERN = Maps.newHashMap();
            // can add some other functions
            FN_NAME_TO_PATTERN.put("date_trunc", PartitionFunctionChecker::checkDateTrunc);
        }

        public static boolean checkDateTrunc(Expr expr) {
            if (!(expr instanceof FunctionCallExpr)) {
                return false;
            }
            FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
            String fnNameString = fnExpr.getFnName().getFunction();
            if (!fnNameString.equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
                return false;
            }
            if (fnExpr.getChild(0) instanceof StringLiteral && fnExpr.getChild(1) instanceof SlotRef) {
                String fmt = ((StringLiteral) fnExpr.getChild(0)).getValue();
                if (fmt.equalsIgnoreCase("week")) {
                    throw new SemanticException("The function date_trunc used by the materialized view for partition" +
                            " does not support week formatting");
                }
                SlotRef slotRef = (SlotRef) fnExpr.getChild(1);
                PrimitiveType primitiveType = slotRef.getType().getPrimitiveType();
                // must check slotRef type, because function analyze don't check it.
                return primitiveType == PrimitiveType.DATETIME || primitiveType == PrimitiveType.DATE;
            } else {
                return false;
            }
        }
    }

}
