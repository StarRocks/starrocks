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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
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
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.ColWithComment;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.PartitionRangeDesc;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

public class MaterializedViewAnalyzer {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewAnalyzer.class);

    private static final Set<JDBCTable.ProtocolType> SUPPORTED_JDBC_PARTITION_TYPE =
            ImmutableSet.of(JDBCTable.ProtocolType.MYSQL);

    private static final Set<Table.TableType> SUPPORTED_TABLE_TYPE =
            ImmutableSet.of(Table.TableType.OLAP,
                    Table.TableType.HIVE,
                    Table.TableType.HUDI,
                    Table.TableType.ICEBERG,
                    Table.TableType.JDBC,
                    Table.TableType.MYSQL,
                    Table.TableType.PAIMON,
                    Table.TableType.VIEW);

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new MaterializedViewAnalyzerVisitor().visit(stmt, session);
    }

    public static Set<BaseTableInfo> getBaseTableInfos(QueryStatement queryStatement, boolean withCheck) {
        Set<BaseTableInfo> baseTableInfos = Sets.newHashSet();
        processBaseTables(queryStatement, baseTableInfos, withCheck);
        return baseTableInfos;
    }

    private static void processBaseTables(QueryStatement queryStatement, Set<BaseTableInfo> baseTableInfos,
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
                throw new SemanticException(
                        "Only supports creating materialized views based on the external table " +
                                "which created by catalog", tableNameInfo.getPos());
            }
            baseTableInfos.add(BaseTableInfo.fromTableName(tableNameInfo, table));
        }
        processViews(queryStatement, baseTableInfos, withCheck);
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
        } else if (table instanceof PaimonTable) {
            PaimonTable paimonTable = (PaimonTable) table;
            String catalogName = paimonTable.getCatalogName();
            return Strings.isBlank(catalogName) || isResourceMappingCatalog(catalogName);
        } else {
            return true;
        }
    }

    private static void processViews(QueryStatement queryStatement, Set<BaseTableInfo> baseTableInfos,
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

    static class MaterializedViewAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        public enum RefreshTimeUnit {
            DAY,
            HOUR,
            MINUTE,
            SECOND
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
                throw new SemanticException("Materialized view query statement only support select or set operation",
                        queryStatement.getQueryRelation().getPos());
            }

            // analyze query statement, can check whether tables and columns exist in catalog
            Analyzer.analyze(queryStatement, context);
            AnalyzerUtils.checkNondeterministicFunction(queryStatement);

            // convert queryStatement to sql and set
            statement.setInlineViewDef(AstToSQLBuilder.toSQL(queryStatement));
            statement.setSimpleViewDef(AstToSQLBuilder.buildSimple(queryStatement));
            // collect table from query statement

            Database db = context.getGlobalStateMgr().getDb(statement.getTableName().getDb());
            if (db == null) {
                throw new SemanticException("Can not find database:" + statement.getTableName().getDb(),
                        statement.getTableName().getPos());
            }
            Set<BaseTableInfo> baseTableInfos = getBaseTableInfos(queryStatement, true);
            // now do not support empty base tables
            // will be relaxed after test
            if (baseTableInfos.isEmpty()) {
                throw new SemanticException("Can not find base table in query statement");
            }
            statement.setBaseTableInfos(Lists.newArrayList(baseTableInfos));

            // set the columns into createMaterializedViewStatement
            List<Pair<Column, Integer>> mvColumnPairs = genMaterializedViewColumns(statement);
            List<Column> mvColumns = mvColumnPairs.stream().map(pair -> pair.first).collect(Collectors.toList());
            statement.setMvColumnItems(mvColumns);

            Map<TableName, Table> aliasTableMap = getNormalizedBaseTables(queryStatement, context);
            Map<Column, Expr> columnExprMap = Maps.newHashMap();

            // columns' order may be changed for sort keys' reorder, need to
            // change `queryStatement.getQueryRelation`'s outputs at the same time.
            List<Expr> outputExpressions = queryStatement.getQueryRelation().getOutputExpression();
            for (Pair<Column, Integer> pair : mvColumnPairs) {
                Preconditions.checkState(pair.second < outputExpressions.size());
                columnExprMap.put(pair.first, outputExpressions.get(pair.second));
            }
            // some check if partition exp exists
            if (statement.getPartitionExpDesc() != null) {
                // check partition expression all in column list and
                // write the expr into partitionExpDesc if partition expression exists
                checkExpInColumn(statement);
                // check partition expression is supported
                checkPartitionColumnExprs(statement, columnExprMap, context);
                // check whether partition expression functions are allowed if it exists
                checkPartitionExpPatterns(statement);
                // check partition column must be base table's partition column
                checkPartitionColumnWithBaseTable(statement, aliasTableMap);
                checkWindowFunctions(statement, columnExprMap);
            }
            // check and analyze distribution
            checkDistribution(statement, aliasTableMap);

            planMVQuery(statement, queryStatement, context);
            return null;
        }

        /**
         * Retrieve all the tables from the input query statement and do normalization: if the query statement
         * contains views, retrieve the contained tables in the view recursively.
         * @param queryStatement : the input statement which need retrieve
         * @param context        : the session connect context
         * @return               : Retrieve all the tables from the input query statement and do normalization.
         */
        private Map<TableName, Table> getNormalizedBaseTables(QueryStatement queryStatement, ConnectContext context) {
            Map<TableName, Table> aliasTableMap = getAllBaseTables(queryStatement, context);

            // do normalization if catalog is null
            Map<TableName, Table> result = Maps.newHashMap();
            for (Map.Entry<TableName, Table> entry : aliasTableMap.entrySet()) {
                entry.getKey().normalization(context);
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }

        /**
         * Retrieve all the tables from the input query statement :
         * - if the query statement contains views, retrieve the contained tables in the view recursively.
         * @param queryStatement : the input statement which need retrieve
         * @param context        : the session connect context
         * @return
         */
        private Map<TableName, Table> getAllBaseTables(QueryStatement queryStatement, ConnectContext context) {
            Map<TableName, Table> aliasTableMap = AnalyzerUtils.collectAllTableAndView(queryStatement);
            List<ViewRelation> viewRelations = AnalyzerUtils.collectViewRelations(queryStatement);
            if (viewRelations.isEmpty()) {
                return aliasTableMap;
            }

            for (ViewRelation viewRelation : viewRelations) {
                Map<TableName, Table> viewTableMap = getAllBaseTables(viewRelation.getQueryStatement(), context);
                aliasTableMap.putAll(viewTableMap);
            }
            return aliasTableMap;
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

        /**
         * when the materialized view's sort keys are not set by hand, choose the sort keys by iterating the columns
         *  and find the satisfied columns as sort keys.
         */
        List<String> chooseSortKeysByDefault(List<Column> mvColumns) {
            List<String> keyCols = Lists.newArrayList();
            int theBeginIndexOfValue = 0;
            int keySizeByte = 0;

            // skip the firth nth columns which can not be used for keys.
            for (; theBeginIndexOfValue < mvColumns.size(); theBeginIndexOfValue++) {
                Column column = mvColumns.get(theBeginIndexOfValue);
                if (column.getType().canBeMVKey()) {
                    break;
                }
            }
            if (theBeginIndexOfValue == mvColumns.size()) {
                throw new SemanticException("All columns of materialized view cannot be used for keys");
            }

            int skip = theBeginIndexOfValue;
            for (; theBeginIndexOfValue < mvColumns.size(); theBeginIndexOfValue++) {
                Column column = mvColumns.get(theBeginIndexOfValue);
                keySizeByte += column.getType().getIndexSize();
                if (keySizeByte > FeConstants.SHORTKEY_MAXSIZE_BYTES) {
                    if (column.getType().getPrimitiveType().isCharFamily()) {
                        keyCols.add(column.getName());
                        theBeginIndexOfValue++;
                    }
                    break;
                }

                if (!column.getType().canBeMVKey()) {
                    continue;
                }

                if (column.getType().getPrimitiveType() == PrimitiveType.VARCHAR) {
                    keyCols.add(column.getName());
                    theBeginIndexOfValue++;
                    break;
                }

                keyCols.add(column.getName());
            }
            if (theBeginIndexOfValue == skip) {
                throw new SemanticException("Data type of first column cannot be " +
                        mvColumns.get(theBeginIndexOfValue).getType());
            }
            return keyCols;
        }

        /**
         * NOTE: order or column names of the defined query may be changed when generating.
         * @param statement : creating materialized view statement
         * @return          : Generate materialized view's columns and corresponding output expression index pair
         *                      from creating materialized view statement.
         */
        private List<Pair<Column, Integer>> genMaterializedViewColumns(CreateMaterializedViewStatement statement) {
            List<String> columnNames = statement.getQueryStatement().getQueryRelation()
                    .getRelationFields().getAllFields().stream()
                    .map(Field::getName).collect(Collectors.toList());
            Scope queryScope = statement.getQueryStatement().getQueryRelation().getScope();
            List<Field> relationFields = queryScope.getRelationFields().getAllFields();
            List<Column> mvColumns = Lists.newArrayList();
            for (int i = 0; i < relationFields.size(); ++i) {
                Type type = AnalyzerUtils.transformTableColumnType(relationFields.get(i).getType(), false);
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
            if (CollectionUtils.isEmpty(keyCols)) {
                keyCols = chooseSortKeysByDefault(mvColumns);
            }

            if (keyCols.isEmpty()) {
                throw new SemanticException("Sort key of materialized view is empty");
            }

            if (keyCols.size() > mvColumns.size()) {
                throw new SemanticException("The number of sort key should be less than the number of columns.");
            }

            // Reorder the MV columns according to sort-key
            Map<String, Pair<Column, Integer>> columnMap = new HashMap<>();
            for (int i = 0; i < mvColumns.size(); i++) {
                Column col = mvColumns.get(i);
                if (columnMap.putIfAbsent(col.getName(), Pair.create(col, i)) != null) {
                    throw new SemanticException("Duplicate column name " + Strings.quote(col.getName()));
                }
            }

            List<Pair<Column, Integer>> reorderedColumns = new ArrayList<>();
            Set<String> usedColumns = new LinkedHashSet<>();
            for (String columnName : keyCols) {
                Pair<Column, Integer> columnPair = columnMap.get(columnName);
                if (columnPair == null || columnPair.first == null) {
                    throw new SemanticException("Sort key not exists: " + columnName);
                }
                Column keyColumn = columnPair.first;
                Type keyColType = keyColumn.getType();
                if (!keyColType.canBeMVKey()) {
                    throw new SemanticException("Type %s cannot be sort key: %s", keyColType, columnName);
                }
                keyColumn.setIsKey(true);
                keyColumn.setAggregationType(null, false);

                reorderedColumns.add(columnPair);
                usedColumns.add(columnName);
            }
            for (Column col : mvColumns) {
                String colName = col.getName();
                if (!usedColumns.contains(colName)) {
                    Preconditions.checkState(columnMap.containsKey(colName));
                    reorderedColumns.add(columnMap.get(colName));
                }
            }
            return reorderedColumns;
        }

        private void checkExpInColumn(CreateMaterializedViewStatement statement) {
            ExpressionPartitionDesc expressionPartitionDesc = statement.getPartitionExpDesc();
            List<Column> columns = statement.getMvColumnItems();
            SlotRef slotRef = getSlotRef(expressionPartitionDesc.getExpr());
            if (slotRef.getTblNameWithoutAnalyzed() != null) {
                throw new SemanticException("Materialized view partition exp: "
                        + slotRef.toSql() + " must related to column", expressionPartitionDesc.getExpr().getPos());
            }
            int columnId = 0;
            for (Column column : columns) {
                if (slotRef.getColumnName().equalsIgnoreCase(column.getName())) {
                    statement.setPartitionColumn(column);
                    SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(columnId), slotRef.getColumnName(),
                            column.getType(), column.isAllowNull());
                    slotRef.setDesc(slotDescriptor);
                    slotRef.setType(column.getType());
                    slotRef.setNullable(column.isAllowNull());
                    break;
                }
                columnId++;
            }
            if (statement.getPartitionColumn() == null) {
                throw new SemanticException("Materialized view partition exp column:"
                        + slotRef.getColumnName() + " is not found in query statement");
            }
        }

        private boolean isValidPartitionExpr(Expr partitionExpr) {
            if (partitionExpr instanceof FunctionCallExpr) {
                FunctionCallExpr partitionColumnExpr = (FunctionCallExpr) partitionExpr;
                String fnName = partitionColumnExpr.getFnName().getFunction();
                if (fnName.equalsIgnoreCase(FunctionSet.TIME_SLICE) || fnName.equalsIgnoreCase(FunctionSet.STR2DATE)) {
                    return partitionColumnExpr.getChild(0) instanceof SlotRef;
                }
            }
            return partitionExpr instanceof SlotRef;
        }

        private void checkPartitionColumnExprs(CreateMaterializedViewStatement statement,
                                               Map<Column, Expr> columnExprMap,
                                               ConnectContext connectContext) {
            ExpressionPartitionDesc expressionPartitionDesc = statement.getPartitionExpDesc();
            Column partitionColumn = statement.getPartitionColumn();

            // partition column expr from input query
            Expr partitionColumnExpr = columnExprMap.get(partitionColumn);
            try {
                partitionColumnExpr = resolvePartitionExpr(partitionColumnExpr, connectContext, statement.getQueryStatement());
            } catch (Exception e) {
                LOG.warn("resolve partition column failed", e);
                throw new SemanticException("resolve partition column failed", statement.getPartitionExpDesc().getPos());
            }

            if (expressionPartitionDesc.isFunction()) {
                // e.g. partition by date_trunc('month', dt)
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) expressionPartitionDesc.getExpr();
                String functionName = functionCallExpr.getFnName().getFunction();
                if (!MaterializedViewPartitionFunctionChecker.FN_NAME_TO_PATTERN.containsKey(functionName)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionCallExpr.getFnName().getFunction() +
                            " is not supported yet.", functionCallExpr.getPos());
                }

                if (!isValidPartitionExpr(partitionColumnExpr)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionCallExpr.getFnName().getFunction() +
                            " must related with column", functionCallExpr.getPos());
                }
                SlotRef slotRef = getSlotRef(functionCallExpr);
                slotRef.setType(partitionColumn.getType());
                // copy function and set it into partitionRefTableExpr
                Expr partitionRefTableExpr = functionCallExpr.clone();
                List<Expr> children = partitionRefTableExpr.getChildren();
                for (int i = 0; i < children.size(); i++) {
                    if (children.get(i) instanceof SlotRef) {
                        partitionRefTableExpr.setChild(i, partitionColumnExpr);
                    }
                }
                statement.setPartitionRefTableExpr(partitionRefTableExpr);
            } else {
                if (partitionColumnExpr instanceof FunctionCallExpr || partitionColumnExpr instanceof SlotRef) {
                    // e.g. partition by date_trunc('day',ss) or time_slice(dt, interval 1 day) or partition by ss
                    statement.setPartitionRefTableExpr(partitionColumnExpr);
                } else {
                    throw new SemanticException("Materialized view partition function must related with column",
                            expressionPartitionDesc.getPos());
                }
            }
        }

        /**
         * Resolve the materialized view's partition expr's slot ref.
         * @param partitionColumnExpr : the materialized view's partition expr
         * @param connectContext      : connect context of the current session.
         * @param queryStatement      : the sub query statment that contains the partition column slot ref
         * @return                    : return the resolved partition expr.
         */
        private Expr resolvePartitionExpr(Expr partitionColumnExpr,
                                          ConnectContext connectContext,
                                          QueryStatement queryStatement) {
            Expr expr = AnalyzerUtils.resolveExpr(partitionColumnExpr, queryStatement);
            SlotRef slot;
            if (expr instanceof SlotRef) {
                slot = (SlotRef) expr;
            } else {
                slot = getSlotRef(expr);
            }
            Map<TableName, Table> aliasTableMap = getNormalizedBaseTables(queryStatement, connectContext);
            TableName tableName = slot.getTblNameWithoutAnalyzed();
            tableName.normalization(connectContext);

            Table table = aliasTableMap.get(tableName);
            if (table == null) {
                String catalog = tableName.getCatalog() == null ?
                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
                table = connectContext.getGlobalStateMgr()
                        .getMetadataMgr().getTable(catalog, tableName.getDb(), tableName.getTbl());
                if (table == null) {
                    throw new SemanticException("Materialized view partition expression %s could only ref to base table",
                            slot.toSql());
                }
            }
            // TableName's catalog may be null, so normalization it
            slot.getTblNameWithoutAnalyzed().normalization(connectContext);
            slot.setType(table.getColumn(slot.getColumnName()).getType());
            return expr;
        }

        private void checkPartitionExpPatterns(CreateMaterializedViewStatement statement) {
            Expr expr = statement.getPartitionRefTableExpr();
            if (expr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = ((FunctionCallExpr) expr);
                String functionName = functionCallExpr.getFnName().getFunction();
                MaterializedViewPartitionFunctionChecker.CheckPartitionFunction checkPartitionFunction =
                        MaterializedViewPartitionFunctionChecker.FN_NAME_TO_PATTERN.get(functionName);
                if (checkPartitionFunction == null) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " is not support", functionCallExpr.getPos());
                }
                if (!checkPartitionFunction.check(functionCallExpr)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " check failed", functionCallExpr.getPos());
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
            } else if (table.isJDBCTable()) {
                checkPartitionColumnWithBaseJDBCTable(slotRef, (JDBCTable) table);
            } else if (table.isPaimonTable()) {
                checkPartitionColumnWithBasePaimonTable(slotRef, (PaimonTable) table);
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
                throw new SemanticException("Materialized view related base table partition type: " +
                        partitionInfo.getType().name() + " not supports");
            }
        }

        private void checkPartitionColumnWithBaseTable(SlotRef slotRef, List<Column> partitionColumns, boolean unPartitioned) {
            if (unPartitioned) {
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

        private void checkPartitionColumnWithBaseHMSTable(SlotRef slotRef, HiveMetaStoreTable table) {
            checkPartitionColumnWithBaseTable(slotRef, table.getPartitionColumns(), table.isUnPartitioned());
        }

        private void checkPartitionColumnWithBaseJDBCTable(SlotRef slotRef, JDBCTable table) {
            checkPartitionColumnWithBaseTable(slotRef, table.getPartitionColumns(), table.isUnPartitioned());
            if (!SUPPORTED_JDBC_PARTITION_TYPE.contains(table.getProtocolType())) {
                throw new SemanticException(String.format("Materialized view PARTITION BY for JDBC %s is not " +
                        "supported, you could remove the PARTITION BY clause", table.getProtocolType()));
            }
        }

        // if mv is partitioned, mv will be refreshed by partition.
        // if mv has window functions, it should also be partitioned by and the partition by columns
        // should contain the partition column of mv
        private void checkWindowFunctions(
                CreateMaterializedViewStatement statement,
                Map<Column, Expr> columnExprMap) {
            SlotRef partitionSlotRef = getSlotRef(statement.getPartitionRefTableExpr());
            // should analyze the partition expr to get type info
            PartitionExprAnalyzer.analyzePartitionExpr(statement.getPartitionRefTableExpr(), partitionSlotRef);
            for (Expr columnExpr : columnExprMap.values()) {
                if (columnExpr instanceof AnalyticExpr) {
                    AnalyticExpr analyticExpr = columnExpr.cast();
                    if (analyticExpr.getPartitionExprs() == null
                            || !analyticExpr.getPartitionExprs().contains(statement.getPartitionRefTableExpr())) {
                        throw new SemanticException("window function %s ’s partition expressions" +
                                " should contain the partition column %s of materialized view",
                                analyticExpr.getFnCall().getFnName().getFunction(), statement.getPartitionColumn().getName());
                    }
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

        @VisibleForTesting
        public void checkPartitionColumnWithBasePaimonTable(SlotRef slotRef, PaimonTable table) {
            if (table.isUnPartitioned()) {
                throw new SemanticException("Materialized view partition column in partition exp " +
                        "must be base table partition column");
            } else {
                boolean found = false;
                for (String partitionColumnName : table.getPartitionColumnNames()) {
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
                } else if (table.isPaimonTable()) {
                    PaimonTable paimonTable = (PaimonTable) table;
                    if (replacePaimonTableAlias(slotRef, paimonTable, baseTableInfo)) {
                        break;
                    }
                }
            }
        }

        boolean replacePaimonTableAlias(SlotRef slotRef, PaimonTable paimonTable, BaseTableInfo baseTableInfo) {
            if (paimonTable.getCatalogName().equals(baseTableInfo.getCatalogName()) &&
                    paimonTable.getDbName().equals(baseTableInfo.getDbName()) &&
                    paimonTable.getTableIdentifier().equals(baseTableInfo.getTableIdentifier())) {
                slotRef.setTblName(new TableName(baseTableInfo.getCatalogName(),
                        baseTableInfo.getDbName(), paimonTable.getName()));
                return true;
            }
            return false;
        }

        private void checkPartitionColumnType(Column partitionColumn) {
            PrimitiveType type = partitionColumn.getPrimitiveType();
            if (!type.isFixedPointType() && !type.isDateType() && type != PrimitiveType.CHAR && type != PrimitiveType.VARCHAR) {
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
                } else {
                    distributionDesc = new RandomDistributionDesc();
                }
                statement.setDistributionDesc(distributionDesc);
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
        public Void visitAlterMaterializedViewStatement(AlterMaterializedViewStmt statement, ConnectContext context) {
            TableName mvName = statement.getMvName();
            MetaUtils.normalizationTableName(context, mvName);
            Table table = MetaUtils.getTable(statement.getMvName());
            if (!(table instanceof MaterializedView)) {
                throw new SemanticException(mvName.getTbl() + " is not async materialized view", mvName.getPos());
            }

            AlterMVClauseAnalyzerVisitor alterTableClauseAnalyzerVisitor = new AlterMVClauseAnalyzerVisitor();
            alterTableClauseAnalyzerVisitor.setTable(table);
            alterTableClauseAnalyzerVisitor.analyze(statement.getAlterTableClause(), context);
            return null;
        }

        @Override
        public Void visitRefreshMaterializedViewStatement(RefreshMaterializedViewStatement statement,
                                                          ConnectContext context) {
            statement.getMvName().normalization(context);
            TableName mvName = statement.getMvName();
            Database db = context.getGlobalStateMgr().getDb(mvName.getDb());
            if (db == null) {
                throw new SemanticException("Can not find database:" + mvName.getDb(), mvName.getPos());
            }
            OlapTable table = (OlapTable) db.getTable(mvName.getTbl());
            if (table == null) {
                throw new SemanticException("Can not find materialized view:" + mvName.getTbl(), mvName.getPos());
            }
            if (!(table instanceof MaterializedView)) {
                throw new SemanticException("Can not refresh non materialized view:" + table.getName(), mvName.getPos());
            }
            MaterializedView mv = (MaterializedView) table;
            if (!mv.isActive()) {
                throw new SemanticException("Refresh materialized view failed because [" + mv.getName() +
                        "] is not active. You can try to active it with ALTER MATERIALIZED VIEW " + mv.getName()
                        + " ACTIVE; ", mvName.getPos());
            }
            if (statement.getPartitionRangeDesc() == null) {
                return null;
            }
            if (!(table.getPartitionInfo() instanceof RangePartitionInfo)) {
                throw new SemanticException("Not support refresh by partition for single partition mv",
                        mvName.getPos());
            }
            Column partitionColumn =
                    ((RangePartitionInfo) table.getPartitionInfo()).getPartitionColumns().get(0);
            if (partitionColumn.getType().isDateType()) {
                validateDateTypePartition(statement.getPartitionRangeDesc());
            } else if (partitionColumn.getType().isIntegerType()) {
                validateNumberTypePartition(statement.getPartitionRangeDesc());
            } else {
                throw new SemanticException(
                        "Unsupported batch partition build type:" + partitionColumn.getType());
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
                        "but START or END does not type match", partitionRangeDesc.getPos());
            }
            if (startNum >= endNum) {
                throw new SemanticException("Batch build partition start value should less then end value",
                        partitionRangeDesc.getPos());
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
                        "but START or END does not type match", partitionRangeDesc.getPos());
            }
            if (!startTime.isBefore(endTime)) {
                throw new SemanticException("Batch build partition start date should less than end date",
                        partitionRangeDesc.getPos());
            }
        }

        @Override
        public Void visitCancelRefreshMaterializedViewStatement(CancelRefreshMaterializedViewStmt statement,
                                                                ConnectContext context) {
            statement.getMvName().normalization(context);
            return null;
        }
    }
}
