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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.mv.analyzer.MVPartitionSlotRefResolver;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.ColWithComment;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.IndexDef;
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
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;
import static com.starrocks.server.CatalogMgr.isInternalCatalog;

public class MaterializedViewAnalyzer {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewAnalyzer.class);

    private static final Set<JDBCTable.ProtocolType> SUPPORTED_JDBC_PARTITION_TYPE =
            ImmutableSet.of(JDBCTable.ProtocolType.MYSQL, JDBCTable.ProtocolType.MARIADB);

    private static final Set<Table.TableType> SUPPORTED_TABLE_TYPE =
            ImmutableSet.of(Table.TableType.OLAP,
                    Table.TableType.HIVE,
                    Table.TableType.HUDI,
                    Table.TableType.ICEBERG,
                    Table.TableType.JDBC,
                    Table.TableType.MYSQL,
                    Table.TableType.PAIMON,
                    Table.TableType.ODPS,
                    Table.TableType.KUDU,
                    Table.TableType.DELTALAKE,
                    Table.TableType.VIEW,
                    Table.TableType.HIVE_VIEW,
                    Table.TableType.ICEBERG_VIEW);

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
            baseTableInfos.add(fromTableName(tableNameInfo, table));
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
        }
        String catalog = table.getCatalogName();
        return Strings.isBlank(catalog) || isResourceMappingCatalog(catalog);
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
            baseTableInfos.add(fromTableName(viewRelation.getName(), viewRelation.getView()));
        }
    }

    private static BaseTableInfo fromTableName(TableName name, Table table) {
        if (isInternalCatalog(name.getCatalog())) {
            Database database = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(name.getCatalog(), name.getDb());
            return new BaseTableInfo(database.getId(), database.getFullName(), table.getName(), table.getId());
        } else {
            return new BaseTableInfo(name.getCatalog(), name.getDb(), table.getName(), table.getTableIdentifier());
        }
    }

    @VisibleForTesting
    protected static List<Integer> getQueryOutputIndices(List<Pair<Column, Integer>> mvColumnPairs) {
        return Streams
                .mapWithIndex(mvColumnPairs.stream(), (pair, idx) -> Pair.create(pair.second, (int) idx))
                .sorted(Comparator.comparingInt(x -> x.first))
                .map(x -> x.second)
                .collect(Collectors.toList());
    }

    static class MaterializedViewAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {

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
            MetaUtils.normalizeMVName(context, tableNameObject);
            final String tableName = tableNameObject.getTbl();
            FeNameFormat.checkTableName(tableName);
            QueryStatement queryStatement = statement.getQueryStatement();
            // check query relation is select relation
            if (!(queryStatement.getQueryRelation() instanceof SelectRelation) &&
                    !(queryStatement.getQueryRelation() instanceof SetOperationRelation)) {
                throw new SemanticException("Materialized view query statement only supports a single query block or " +
                        "multiple query blocks in set operations",
                        queryStatement.getQueryRelation().getPos());
            }

            // analyze query statement, can check whether tables and columns exist in catalog
            Analyzer.analyze(queryStatement, context);
            AnalyzerUtils.checkNondeterministicFunction(queryStatement);

            boolean hasTemporaryTable = AnalyzerUtils.hasTemporaryTables(queryStatement);
            if (hasTemporaryTable) {
                throw new SemanticException("Materialized view can't base on temporary table");
            }

            // convert queryStatement to sql and set
            statement.setInlineViewDef(AstToSQLBuilder.toSQL(queryStatement));
            statement.setSimpleViewDef(AstToSQLBuilder.buildSimple(queryStatement));
            Preconditions.checkArgument(statement.getOrigStmt() != null, "MV's original statement is null");
            String originalViewDef = statement.getOrigStmt().originStmt;
            Preconditions.checkArgument(originalViewDef != null,
                    "MV's original view definition is null");
            statement.setOriginalViewDefineSql(originalViewDef.substring(statement.getQueryStartIndex()));

            // collect table from query statement

            if (!InternalCatalog.isFromDefault(statement.getTableName())) {
                throw new SemanticException("Materialized view can only be created in default_catalog. " +
                        "You could either create it with default_catalog.<database>.<mv>, or switch to " +
                        "default_catalog through `set catalog <default_catalog>` statement",
                        statement.getTableName().getPos());
            }
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

            List<Integer> queryOutputIndices = getQueryOutputIndices(mvColumnPairs);
            // to avoid disturbing original codes, only set query output indices when column orders have changed.
            if (IntStream.range(0, queryOutputIndices.size()).anyMatch(i -> i != queryOutputIndices.get(i))) {
                statement.setQueryOutputIndices(queryOutputIndices);
            }

            // set the Indexes into createMaterializedViewStatement
            List<Index> mvIndexes = genMaterializedViewIndexes(statement);
            statement.setMvIndexes(mvIndexes);

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
            if (statement.getPartitionByExpr() != null) {
                // check partition expression all in column list and
                // write the expr into partitionExpDesc if partition expression exists
                checkExpInColumn(statement);
                // check partition expression is supported
                checkPartitionColumnExprs(statement, columnExprMap, context);
                // check whether partition expression functions are allowed if it exists
                checkPartitionExpPatterns(statement);
                // check partition column must be base table's partition column
                checkPartitionColumnWithBaseTable(statement, aliasTableMap);
                // check window function can be used in partitioned mv
                checkWindowFunctions(statement, columnExprMap);
                // determine mv partition 's type: list or range
                determinePartitionType(statement, aliasTableMap);
            }
            // check and analyze distribution
            checkDistribution(statement, aliasTableMap);

            planMVQuery(statement, queryStatement, context);
            return null;
        }

        /**
         * Retrieve all the tables from the input query statement and do normalization: if the query statement
         * contains views, retrieve the contained tables in the view recursively.
         *
         * @param queryStatement : the input statement which need retrieve
         * @param context        : the session connect context
         * @return : Retrieve all the tables from the input query statement and do normalization.
         */
        private Map<TableName, Table> getNormalizedBaseTables(QueryStatement queryStatement, ConnectContext context) {
            Map<TableName, Table> aliasTableMap = getAllBaseTables(queryStatement, context);
            // do normalization if catalog is null
            Map<TableName, Table> result = new CaseInsensitiveMap();
            for (Map.Entry<TableName, Table> entry : aliasTableMap.entrySet()) {
                entry.getKey().normalization(context);
                result.put(entry.getKey(), entry.getValue());
            }
            return result;
        }

        /**
         * Retrieve all the tables from the input query statement :
         * - if the query statement contains views, retrieve the contained tables in the view recursively.
         *
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
         * and find the satisfied columns as sort keys.
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
                throw new SemanticException("Data type of {}th column cannot be " +
                        mvColumns.get(theBeginIndexOfValue).getType(), theBeginIndexOfValue);
            }
            return keyCols;
        }

        /**
         * NOTE: order or column names of the defined query may be changed when generating.
         *
         * @param statement : creating materialized view statement
         * @return : Generate materialized view's columns and corresponding output expression index pair
         * from creating materialized view statement.
         */
        private List<Pair<Column, Integer>> genMaterializedViewColumns(CreateMaterializedViewStatement statement) {
            List<String> columnNames = statement.getQueryStatement().getQueryRelation()
                    .getRelationFields().getAllFields().stream()
                    .map(Field::getName).collect(Collectors.toList());
            Scope queryScope = statement.getQueryStatement().getQueryRelation().getScope();
            List<Field> relationFields = queryScope.getRelationFields().getAllFields();
            List<ColWithComment> colWithComments = statement.getColWithComments();
            if (colWithComments != null) {
                if (colWithComments.size() != relationFields.size()) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_VIEW_WRONG_LIST);
                }
                for (ColWithComment colWithComment : colWithComments) {
                    colWithComment.analyze();
                }
            }
            List<Column> mvColumns = Lists.newArrayList();
            for (int i = 0; i < relationFields.size(); ++i) {
                Type type = AnalyzerUtils.transformTableColumnType(relationFields.get(i).getType(), false);
                String colName = columnNames.get(i);
                if (colWithComments != null) {
                    colName = colWithComments.get(i).getColName();
                }
                Column column = new Column(colName, type, relationFields.get(i).isNullable());
                if (colWithComments != null) {
                    column.setComment(colWithComments.get(i).getComment());
                }
                // set default aggregate type, look comments in class Column
                column.setAggregationType(AggregateType.NONE, false);
                mvColumns.add(column);
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

        private List<Index> genMaterializedViewIndexes(CreateMaterializedViewStatement statement) {
            List<IndexDef> indexDefs = statement.getIndexDefs();
            List<Index> indexes = new ArrayList<>();
            List<Column> columns = statement.getMvColumnItems();

            if (CollectionUtils.isNotEmpty(indexDefs)) {
                Multimap<String, Integer> indexMultiMap = ArrayListMultimap.create();
                Multimap<String, Integer> colMultiMap = ArrayListMultimap.create();

                for (IndexDef indexDef : indexDefs) {
                    indexDef.analyze();
                    List<ColumnId> columnIds = new ArrayList<>(indexDef.getColumns().size());
                    for (String indexColName : indexDef.getColumns()) {
                        boolean found = false;
                        for (Column column : columns) {
                            if (column.getName().equalsIgnoreCase(indexColName)) {
                                indexDef.checkColumn(column, statement.getKeysType());
                                found = true;
                                columnIds.add(column.getColumnId());
                                break;
                            }
                        }
                        if (!found) {
                            throw new SemanticException("BITMAP column does not exist in table. invalid column: " + indexColName,
                                    indexDef.getPos());
                        }
                    }
                    indexes.add(new Index(indexDef.getIndexName(), columnIds, indexDef.getIndexType(),
                            indexDef.getComment()));
                    indexMultiMap.put(indexDef.getIndexName().toLowerCase(), 1);
                    colMultiMap.put(String.join(",", indexDef.getColumns()), 1);
                }
                for (String indexName : indexMultiMap.asMap().keySet()) {
                    if (indexMultiMap.get(indexName).size() > 1) {
                        throw new SemanticException("Duplicate index name '%s'", indexName);
                    }
                }
                for (String colName : colMultiMap.asMap().keySet()) {
                    if (colMultiMap.get(colName).size() > 1) {
                        throw new SemanticException("Duplicate column name '%s' in index", colName);
                    }
                }
            }
            return indexes;
        }

        private void checkExpInColumn(CreateMaterializedViewStatement statement) {
            Expr partitionByExpr = statement.getPartitionByExpr();
            List<Column> columns = statement.getMvColumnItems();
            SlotRef slotRef = getSlotRef(partitionByExpr);
            if (slotRef.getTblNameWithoutAnalyzed() != null) {
                throw new SemanticException("Materialized view partition exp: "
                        + slotRef.toSql() + " must related to column", partitionByExpr.getPos());
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
                                               ConnectContext connectContext) throws SemanticException {
            Expr partitionByExpr = statement.getPartitionByExpr();
            Column partitionColumn = statement.getPartitionColumn();
            if (partitionByExpr == null) {
                return;
            }

            // partition column expr from input query
            Expr partitionColumnExpr = columnExprMap.get(partitionColumn);
            try {
                partitionColumnExpr = resolvePartitionExpr(partitionColumnExpr, connectContext, statement.getQueryStatement());
            } catch (Exception e) {
                LOG.warn("resolve partition column failed", e);
                throw new SemanticException("Resolve partition column failed:" + e.getMessage(),
                        statement.getPartitionByExpr().getPos());
            }

            // set partition-ref into statement
            if (partitionByExpr instanceof FunctionCallExpr) {
                // e.g. partition by date_trunc('month', dt)
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionByExpr;
                String functionName = functionCallExpr.getFnName().getFunction();
                if (!PartitionFunctionChecker.FN_NAME_TO_PATTERN.containsKey(functionName)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionCallExpr.getFnName().getFunction() +
                            " is not supported yet.", functionCallExpr.getPos());
                }

                if (!isValidPartitionExpr(partitionColumnExpr)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionCallExpr.getFnName().getFunction() +
                            " must related with column", functionCallExpr.getPos());
                }
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
                            partitionByExpr.getPos());
                }
            }
        }

        /**
         * Resolve the materialized view's partition expr's slot ref.
         *
         * @param partitionColumnExpr : the materialized view's partition expr
         * @param connectContext      : connect context of the current session.
         * @param queryStatement      : the sub query statment that contains the partition column slot ref
         * @return : return the resolved partition expr.
         */
        private Expr resolvePartitionExpr(Expr partitionColumnExpr,
                                          ConnectContext connectContext,
                                          QueryStatement queryStatement) {
            Expr expr = MVPartitionSlotRefResolver.resolveExpr(partitionColumnExpr, queryStatement);
            if (expr == null) {
                throw new SemanticException("Cannot resolve materialized view's partition expression:%s",
                        partitionColumnExpr.toSql());
            }
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
                PartitionFunctionChecker.CheckPartitionFunction checkPartitionFunction =
                        PartitionFunctionChecker.FN_NAME_TO_PATTERN.get(functionName);
                if (checkPartitionFunction == null) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " is not support: " + expr.toSqlWithoutTbl(), functionCallExpr.getPos());
                }
                if (!checkPartitionFunction.check(functionCallExpr)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " check failed: " + expr.toSqlWithoutTbl(), functionCallExpr.getPos());
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
            } else if (table.isHiveTable() || table.isHudiTable() || table.isOdpsTable()) {
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

        private void determinePartitionType(CreateMaterializedViewStatement statement,
                                            Map<TableName, Table> tableNameTableMap) {
            Expr partitionRefTableExpr = statement.getPartitionRefTableExpr();
            if (partitionRefTableExpr == null) {
                statement.setPartitionType(PartitionType.UNPARTITIONED);
                return;
            }

            SlotRef slotRef = getSlotRef(partitionRefTableExpr);
            Table refBaseTable = tableNameTableMap.get(slotRef.getTblNameWithoutAnalyzed());
            if (refBaseTable == null) {
                LOG.warn("Materialized view partition expression %s could only ref to base table",
                        slotRef.toSql());
                statement.setPartitionType(PartitionType.UNPARTITIONED);
                return;
            }

            if (refBaseTable.isNativeTableOrMaterializedView()) {
                // To olap table, determine mv's partition by its ref base table's partition type.
                OlapTable olapTable = (OlapTable) refBaseTable;
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                if (partitionInfo.isRangePartition()) {
                    // set the partition type
                    statement.setPartitionType(PartitionType.RANGE);
                } else if (partitionInfo.isListPartition()) {
                    // set the partition type
                    statement.setPartitionType(PartitionType.LIST);
                }
            } else {
                List<Column> refPartitionCols = refBaseTable.getPartitionColumns();
                Optional<Column> refPartitionColOpt = refPartitionCols.stream()
                        .filter(col -> col.getName().equals(slotRef.getColumnName()))
                        .findFirst();
                if (refPartitionColOpt.isEmpty()) {
                    throw new SemanticException("Materialized view partition column in partition exp " +
                            "must be base table partition column", partitionRefTableExpr.getPos());
                }
                Column refPartitionCol = refPartitionColOpt.get();
                Type partitionExprType = refPartitionCol.getType();
                Expr partitionByExpr = statement.getPartitionByExpr();
                // To olap table, determine mv's partition by its ref base table's partition column type:
                // - if the partition column is string type && no use `str2date`, use list partition.
                // - otherwise use range partition as before.
                // To be compatible with old implementations, if the partition column is not a string type,
                // still use range partition.
                // TODO: remove this compatibility code in the future, use list partition directly later.
                if (partitionExprType.isStringType() && (partitionByExpr instanceof SlotRef) &&
                        !(partitionRefTableExpr instanceof FunctionCallExpr)) {
                    statement.setPartitionType(PartitionType.LIST);
                } else {
                    statement.setPartitionType(PartitionType.RANGE);
                }
            }
        }

        private void checkPartitionColumnWithBaseOlapTable(SlotRef slotRef, OlapTable table) {
            PartitionInfo partitionInfo = table.getPartitionInfo();
            if (partitionInfo.isUnPartitioned()) {
                throw new SemanticException("Materialized view partition column in partition exp " +
                        "must be base table partition column");
            } else if (partitionInfo.isRangePartition()) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns(table.getIdToColumn());
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
            } else if (partitionInfo.isListPartition()) {
                ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
                Set<String> partitionColumns = listPartitionInfo.getPartitionColumns(table.getIdToColumn()).stream()
                        .map(col -> col.getName())
                        .collect(Collectors.toSet());
                // mv's partition columns should be subset of the base table's partition columns
                if (!partitionColumns.contains(slotRef.getColumnName())) {
                    throw new SemanticException("Materialized view partition column in partition exp " +
                            "must be base table partition column");
                }
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
        private void checkWindowFunctions(CreateMaterializedViewStatement statement, Map<Column, Expr> columnExprMap) {
            SlotRef partitionSlotRef = getSlotRef(statement.getPartitionRefTableExpr());
            // should analyze the partition expr to get type info
            PartitionExprAnalyzer.analyzePartitionExpr(statement.getPartitionRefTableExpr(), partitionSlotRef);

            MVPartitionSlotRefResolver.checkWindowFunction(statement, statement.getPartitionRefTableExpr());
        }

        private void checkPartitionColumnWithBaseIcebergTable(SlotRef slotRef, IcebergTable table) {
            org.apache.iceberg.Table icebergTable = table.getNativeTable();
            PartitionSpec partitionSpec = icebergTable.spec();
            if (partitionSpec.isUnpartitioned()) {
                throw new SemanticException("Materialized view partition column in partition exp " +
                        "must be base table partition column");
            } else {
                if (icebergTable.specs().size() > 1) {
                    throw new SemanticException("Do not support create materialized view when " +
                            "base iceberg table has partition evolution");
                }
                boolean found = false;
                for (PartitionField partitionField : partitionSpec.fields()) {
                    String transformName = partitionField.transform().toString();
                    String partitionColumnName = icebergTable.schema().findColumnName(partitionField.sourceId());
                    if (partitionColumnName.equalsIgnoreCase(slotRef.getColumnName())) {
                        checkPartitionColumnType(table.getColumn(partitionColumnName));
                        if (transformName.startsWith("bucket") || transformName.startsWith("truncate")) {
                            throw new SemanticException("Do not support create materialized view when " +
                                    "base iceberg table partition transform has bucket or truncate");
                        }
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
                    Optional<Table> tableOptional = MvUtils.getTableWithIdentifier(baseTableInfo);
                    if (tableOptional.isEmpty()) {
                        continue;
                    }
                    if (tableOptional.get().equals(table)) {
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
            if (!type.isFixedPointType() && !type.isDateType() && !type.isStringType()) {
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

            AlterMVClauseAnalyzerVisitor alterTableClauseAnalyzerVisitor = new AlterMVClauseAnalyzerVisitor(table);
            alterTableClauseAnalyzerVisitor.analyze(context, statement.getAlterTableClause());
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
            if (!mv.isActive() && AlterJobMgr.MANUAL_INACTIVE_MV_REASON.equalsIgnoreCase(mv.getInactiveReason())) {
                throw new SemanticException("Refresh materialized view failed because [" + mv.getName() +
                        "] is not active. You can try to active it with ALTER MATERIALIZED VIEW " + mv.getName()
                        + " ACTIVE; ", mvName.getPos());
            }
            if (statement.getPartitionRangeDesc() == null) {
                return null;
            }
            if (table.getPartitionInfo().isUnPartitioned()) {
                throw new SemanticException("Not support refresh by partition for single partition mv",
                        mvName.getPos());
            }
            Column partitionColumn = table.getPartitionInfo().getPartitionColumns(table.getIdToColumn()).get(0);
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
