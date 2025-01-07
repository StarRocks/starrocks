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
import com.starrocks.analysis.IndexDef;
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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Index;
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
import com.starrocks.server.RunMode;
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
                    Table.TableType.ODPS,
                    Table.TableType.DELTALAKE,
                    Table.TableType.VIEW,
                    Table.TableType.HIVE_VIEW);

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
            baseTableInfos.add(BaseTableInfo.fromTableName(viewRelation.getName(), viewRelation.getView()));
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
                throw new SemanticException("Data type of {}th column cannot be " +
                        mvColumns.get(theBeginIndexOfValue).getType(), theBeginIndexOfValue);
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

        private List<Index> genMaterializedViewIndexes(CreateMaterializedViewStatement statement) {
            List<IndexDef> indexDefs = statement.getIndexDefs();
            List<Index> indexes = new ArrayList<>();
            List<Column> columns = statement.getMvColumnItems();

            if (CollectionUtils.isNotEmpty(indexDefs)) {
                Multimap<String, Integer> indexMultiMap = ArrayListMultimap.create();
                Multimap<String, Integer> colMultiMap = ArrayListMultimap.create();

                for (IndexDef indexDef : indexDefs) {
                    indexDef.analyze();
                    for (String indexColName : indexDef.getColumns()) {
                        boolean found = false;
                        for (Column column : columns) {
                            if (column.getName().equalsIgnoreCase(indexColName)) {
                                indexDef.checkColumn(column, statement.getKeysType());
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            throw new SemanticException("BITMAP column does not exist in table. invalid column: " + indexColName,
                                    indexDef.getPos());
                        }
                    }
                    indexes.add(new Index(indexDef.getIndexName(), indexDef.getColumns(), indexDef.getIndexType(),
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
                                               ConnectContext connectContext) throws SemanticException {
            ExpressionPartitionDesc expressionPartitionDesc = statement.getPartitionExpDesc();
            Column partitionColumn = statement.getPartitionColumn();

            // partition column expr from input query
            Expr partitionColumnExpr = columnExprMap.get(partitionColumn);
            try {
                partitionColumnExpr = resolvePartitionExpr(partitionColumnExpr, connectContext, statement.getQueryStatement());
            } catch (Exception e) {
                LOG.warn("resolve partition column failed", e);
                throw new SemanticException("Resolve partition column failed:" + e.getMessage(),
                        statement.getPartitionExpDesc().getPos());
            }

            // set partition-ref into statement
            if (expressionPartitionDesc.isFunction()) {
                // e.g. partition by date_trunc('month', dt)
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) expressionPartitionDesc.getExpr();
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
            Expr expr = SlotRefResolver.resolveExpr(partitionColumnExpr, queryStatement);
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
        private void checkWindowFunctions(CreateMaterializedViewStatement statement, Map<Column, Expr> columnExprMap) {
            SlotRef partitionSlotRef = getSlotRef(statement.getPartitionRefTableExpr());
            // should analyze the partition expr to get type info
            PartitionExprAnalyzer.analyzePartitionExpr(statement.getPartitionRefTableExpr(), partitionSlotRef);

            SlotRefResolver.checkWindowFunction(statement, statement.getPartitionRefTableExpr());
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
            Optional<Short> maxReplicationFromTable = Optional.empty();
            for (Table table : tableNameTableMap.values()) {
                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    Short replicationNum = olapTable.getDefaultReplicationNum();
                    if (!maxReplicationFromTable.isPresent() || replicationNum > maxReplicationFromTable.get()) {
                        maxReplicationFromTable = Optional.of(replicationNum);
                    }
                }
            }
            return maxReplicationFromTable.orElseGet(RunMode::defaultReplicationNum);
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
            if (!mv.isActive() && AlterJobMgr.MANUAL_INACTIVE_MV_REASON.equalsIgnoreCase(mv.getInactiveReason())) {
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
<<<<<<< HEAD
}
=======

    private static @NotNull Column getPartitionColumn(List<Column> columns, SlotRef slotRef) {
        Column mvPartitionColumn = null;
        int columnId = 0;
        for (Column column : columns) {
            if (slotRef.getColumnName().equalsIgnoreCase(column.getName())) {
                mvPartitionColumn = column;
                break;
            }
            columnId++;
        }
        if (mvPartitionColumn == null) {
            throw new SemanticException("Materialized view partition exp column:"
                    + slotRef.getColumnName() + " is not found in query statement");
        }
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(columnId), slotRef.getColumnName(),
                mvPartitionColumn.getType(), mvPartitionColumn.isAllowNull());
        slotRef.setDesc(slotDescriptor);
        slotRef.setType(mvPartitionColumn.getType());
        slotRef.setNullable(mvPartitionColumn.isAllowNull());
        slotRef.setType(mvPartitionColumn.getType());
        return mvPartitionColumn;
    }

    /**
     * For list partitioned mv and its partition expr is a function call, deduce generated partition columns.
     * For iceberg table with timestamp-with-zone type, we need to adjust partition expr timezone.
     * NOTE:
     * - Iceberg table's timestamp-with-zone's default timezone is UTC
     * - Starrocks table's timestamp type is no timezone and its time is converted to local timezone.
     */
    private static Column getGeneratedPartitionColumn(Expr adjustedPartitionByExpr,
                                                      int placeHolderSlotId) {

        Type type = adjustedPartitionByExpr.getType();
        if (type.isScalarType()) {
            ScalarType scalarType = (ScalarType) type;
            if (scalarType.isWildcardChar()) {
                type = ScalarType.createCharType(ScalarType.getOlapMaxVarcharLength());
            } else if (scalarType.isWildcardVarchar()) {
                type = ScalarType.createVarcharType(ScalarType.getOlapMaxVarcharLength());
            }
        }
        String columnName = FeConstants.GENERATED_PARTITION_COLUMN_PREFIX + placeHolderSlotId;
        TypeDef typeDef = new TypeDef(type);
        try {
            typeDef.analyze();
        } catch (Exception e) {
            throw new ParsingException("Generate partition column " + columnName
                    + " for multi expression partition error: " + e.getMessage());
        }
        ColumnDef generatedPartitionColumn = new ColumnDef(
                columnName, typeDef, null, false, null, null, true,
                ColumnDef.DefaultValueDef.NOT_SET, null, adjustedPartitionByExpr, "");
        return generatedPartitionColumn.toColumn(null);
    }

    /**
     * Support to adjust partition-by-expression if ref-base-table is an Iceberg Table or OlapTable
     * - Adjust partition expr timezone for iceberg table with timestamp-with-zone type.
     * - Adjust partition expr if olap table's with common partition expression.
     */
    public static Expr getMVAdjustedPartitionByExpr(Integer i,
                                                    Expr partitionByExpr,
                                                    List<Column> mvColumns,
                                                    TableName mvTableName,
                                                    Map<TableName, Table> refTableNameTableMap,
                                                    Map<Integer, Expr> changedPartitionByExprs,
                                                    Map<Expr, Expr> partitionByExprToAdjustExprMap) {
        List<SlotRef> slotRefs = partitionByExpr.collectAllSlotRefs();
        if (slotRefs.size() != 1) {
            throw new SemanticException("Partition expr only can ref one slot refs:",
                    partitionByExpr.toSql());
        }
        SlotRef slotRef = slotRefs.get(0);
        TableName refTableName = slotRef.getTblNameWithoutAnalyzed();
        Table refBaseTable = refTableNameTableMap.get(refTableName);
        if (refBaseTable == null) {
            throw new SemanticException("Materialized view partition expression %s could only ref to base table",
                    slotRef.toSql());
        }
        return getMVAdjustedPartitionByExpr(i, partitionByExpr, mvColumns, mvTableName, slotRef, refBaseTable,
                changedPartitionByExprs, partitionByExprToAdjustExprMap);
    }

    /**
     * Why need to maintain partitionByExprToAdjustExprMap?
     * If a mv has `partition_retention_condition` property, since the partition expr is changed, we need to
     * adjust the partition expr in `partition_retention_condition` to the new partition expr.
     */
    public static Expr getMVAdjustedPartitionByExpr(Integer i,
                                                    Expr partitionByExpr,
                                                    List<Column> mvColumns,
                                                    TableName mvTableName,
                                                    SlotRef slotRef,
                                                    Table refBaseTable,
                                                    Map<Integer, Expr> changedPartitionByExprs,
                                                    Map<Expr, Expr> partitionByExprToAdjustExprMap) {
        Expr originalPartitionByExpr = partitionByExpr.clone();
        // change timezone, date_trunc('day', dt) -> date_trunc('day', date_sub(dt, interval 8 hour))
        Expr adjustedPartitionByExpr;
        if (refBaseTable instanceof IcebergTable) {
            IcebergTable icebergTable = (IcebergTable) refBaseTable;
            adjustedPartitionByExpr = getAdjustedIcebergPartitionExpr(mvColumns, mvTableName, slotRef, icebergTable,
                    partitionByExpr);
        } else if (refBaseTable instanceof OlapTable) {
            adjustedPartitionByExpr = getAdjustedOlapTablePartitionExpr(changedPartitionByExprs, i, mvColumns, mvTableName,
                    slotRef, partitionByExpr);
        } else {
            return partitionByExpr;
        }
        if (adjustedPartitionByExpr.equals(partitionByExpr)) {
            return adjustedPartitionByExpr;
        }
        ExpressionAnalyzer.analyzeExpression(adjustedPartitionByExpr, new AnalyzeState(), new Scope(RelationId.anonymous(),
                        new RelationFields(mvColumns.stream().map(col -> new Field(col.getName(),
                                col.getType(), mvTableName, null)).collect(Collectors.toList()))),
                ConnectContext.buildInner());
        // update partitionByExprToAdjustExprMap
        recordPartitionByExprToAdjustExprMap(mvColumns, mvTableName, originalPartitionByExpr, adjustedPartitionByExpr,
                partitionByExprToAdjustExprMap);
        return adjustedPartitionByExpr;
    }

    /**
     * Use mv's partition column to adjust original partition expr to new partition expr which is used for
     * `partition_retention_condition` property that partition expression refs to mv's partition column.
     */
    private static void recordPartitionByExprToAdjustExprMap(List<Column> mvColumns,
                                                             TableName mvTableName,
                                                             Expr originalPartitionByExpr,
                                                             Expr adjustedPartitionByExpr,
                                                             Map<Expr, Expr> partitionByExprToAdjustExprMap) {
        List<SlotRef> slotRefs = originalPartitionByExpr.collectAllSlotRefs();
        if (slotRefs.size() != 1) {
            throw new SemanticException("Partition expr only can ref one slot refs:",
                    originalPartitionByExpr.toSql());
        }
        SlotRef slotRef = slotRefs.get(0);
        resolveRefToMVColumns(mvColumns, slotRef, mvTableName);
        partitionByExprToAdjustExprMap.put(originalPartitionByExpr, adjustedPartitionByExpr);
    }

    /**
     * For generated column, change its referred slot ref from original ref-base-table's output columns to
     * MV's output columns.
     */
    public static void resolveRefToMVColumns(List<Column> columns, SlotRef slotRef, TableName mvTableName) {
        Column mvPartitionColumn = null;
        int columnId = 0;
        for (Column column : columns) {
            if (slotRef.getColumnName().equalsIgnoreCase(column.getName())) {
                mvPartitionColumn = column;
                break;
            }
            columnId++;
        }
        if (mvPartitionColumn == null) {
            throw new SemanticException("Materialized view partition exp column:"
                    + slotRef.getColumnName() + " is not found in query statement");
        }
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(columnId), slotRef.getColumnName(),
                mvPartitionColumn.getType(), mvPartitionColumn.isAllowNull());
        slotRef.setDesc(slotDescriptor);
        slotRef.setType(mvPartitionColumn.getType());
        slotRef.setNullable(mvPartitionColumn.isAllowNull());
        slotRef.setType(mvPartitionColumn.getType());
        slotRef.setColumnName(mvPartitionColumn.getName());
        slotRef.setTblName(mvTableName);
    }

    public static Map<Expr, Expr> getMVPartitionByExprToAdjustMap(TableName mvTableName,
                                                                  MaterializedView mv) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (!partitionInfo.isListPartition()) {
            return null;
        }

        Map<Table, List<Expr>> refBaseTablePartitionExprMap = mv.getRefBaseTablePartitionExprs();
        if (refBaseTablePartitionExprMap.size() != 1) {
            return null;
        }
        Map.Entry<Table, List<Expr>> entry = refBaseTablePartitionExprMap.entrySet().iterator().next();
        Table refBaseTable = entry.getKey();
        List<Expr> refBaseTablePartitionExprs = entry.getValue();
        Map<Expr, Expr> mvPartitionByExprToAdjustMap = Maps.newHashMap();
        for (int i = 0; i < refBaseTablePartitionExprs.size(); i++) {
            Expr expr = refBaseTablePartitionExprs.get(i);
            Expr cloned = expr.clone();
            List<SlotRef> slotRefs = cloned.collectAllSlotRefs();
            if (slotRefs.size() != 1) {
                continue;
            }
            SlotRef slotRef = slotRefs.get(0);
            MaterializedViewAnalyzer.getMVAdjustedPartitionByExpr(i, cloned, mv.getColumns(),
                    mvTableName, slotRef, refBaseTable, Maps.newHashMap(), mvPartitionByExprToAdjustMap);
        }

        return mvPartitionByExprToAdjustMap;
    }

    /**
     * If partition by expression has changed (eg. timezone offset changed), adjust where expr to new partition expr.
     */
    public static Expr adjustWhereExprIfNeeded(Map<Expr, Expr> partitionByExprMap,
                                               Expr expr,
                                               Scope scope,
                                               ConnectContext context) {
        if (CollectionUtils.sizeIsEmpty(partitionByExprMap)) {
            return expr;
        }
        ExprSubstitutionMap exprSubstitutionMap = new ExprSubstitutionMap(false);
        partitionByExprMap.entrySet().forEach(e -> {
            Expr lExpr = e.getKey();
            Expr rExpr = e.getValue();
            ExpressionAnalyzer.analyzeExpression(lExpr, new AnalyzeState(), scope, context);
            ExpressionAnalyzer.analyzeExpression(rExpr, new AnalyzeState(), scope, context);
            exprSubstitutionMap.put(lExpr, rExpr);
        });
        return expr.substitute(exprSubstitutionMap);
    }

    /**
     * IcebergTable partition column is timestamp with UTC time zone, adjust partition-by-expression's timezone to local timezone,
     * otherwise MV refresh cannot match target partition by referring specific Iceberg input partition.
     */
    public static int getIcebergPartitionColumnTimeZoneOffset(IcebergTable icebergTable,
                                                              SlotRef slotRef) {
        List<Column> refPartitionCols = icebergTable.getPartitionColumns();
        Optional<Column> refPartitionColOpt = refPartitionCols.stream()
                .filter(col -> col.getName().equals(slotRef.getColumnName()))
                .findFirst();
        if (refPartitionColOpt.isEmpty()) {
            throw new SemanticException("Materialized view partition column in partition exp " +
                    "must be base table partition column");
        }
        PartitionField partitionField = icebergTable.getPartitionField(refPartitionColOpt.get().getName());
        if (partitionField.transform().dedupName().equalsIgnoreCase("time")) {
            org.apache.iceberg.Schema icebegSchema = icebergTable.getNativeTable().schema();
            if (icebegSchema.findType(partitionField.sourceId()).equals(Types.TimestampType.withZone())) {
                ZoneId zoneId = TimeUtils.getTimeZone().toZoneId();
                return getZoneHourOffset(java.time.ZoneOffset.UTC, zoneId);
            } else {
                return 0;
            }
        }
        return 0;
    }

    private static int getZoneHourOffset(ZoneId zoneId1, ZoneId zoneId2) {
        ZonedDateTime nowInZone1 = ZonedDateTime.now(zoneId1);
        ZonedDateTime nowInZone2 = ZonedDateTime.now(zoneId2);
        ZoneOffset offset1 = nowInZone1.getOffset();
        ZoneOffset offset2 = nowInZone2.getOffset();
        return (offset2.getTotalSeconds() - offset1.getTotalSeconds()) / 3600;
    }

    /**
     * For mv's partition function experssion with timezone, adjust timezone to local timezone; otherwise MV refresh
     * cannot match target partition by referring specific Iceberg input partition.
     */
    public static Expr getIcebergAdjustPartitionExpr(Expr partitionByExpr,
                                                     SlotRef slotRef,
                                                     int zoneHourOffset) {
        if (zoneHourOffset == 0) {
            return partitionByExpr;
        }
        // date_trunc('day', dt) -> date_trunc('day', date_sub(dt, interval 8 hour))
        {
            IntLiteral interval = new IntLiteral(zoneHourOffset, Type.INT);
            Type[] argTypes = {slotRef.getType(), interval.getType()};
            Function dateSubFn = Expr.getBuiltinFunction(FunctionSet.DATE_SUB,
                    argTypes, Function.CompareMode.IS_IDENTICAL);
            Preconditions.checkNotNull(dateSubFn, "date_sub function is not found");
            TimestampArithmeticExpr newChild = new TimestampArithmeticExpr(FunctionSet.DATE_SUB, slotRef,
                    interval, "HOUR");
            newChild.setFn(dateSubFn);
            newChild.setType(slotRef.getType());
            partitionByExpr.setChild(1, newChild);
        }

        // date_trunc('day', date_sub(dt, interval 8 hour)) -> date_add(date_trunc('day', date_sub(dt, interval 8 hour)), 8)
        {
            IntLiteral interval = new IntLiteral(zoneHourOffset, Type.INT);
            Type[] argTypes = {slotRef.getType(), interval.getType()};
            Function dateAddFn = Expr.getBuiltinFunction(FunctionSet.DATE_ADD,
                    argTypes, Function.CompareMode.IS_IDENTICAL);
            Preconditions.checkNotNull(dateAddFn, "date_add function is not found");
            TimestampArithmeticExpr dateAddFunc = new TimestampArithmeticExpr(FunctionSet.DATE_ADD, partitionByExpr,
                    interval, "HOUR");
            dateAddFunc.setFn(dateAddFn);
            dateAddFunc.setType(slotRef.getType());

            return dateAddFunc;
        }
    }

    /**
     * If iceberg table partition column is timestamp with time zone, adjust timezone to local timezone.
     */
    public static Expr getAdjustedIcebergPartitionExpr(List<Column> mvColumns, TableName mvTableName,
                                                       SlotRef slotRef, IcebergTable icebergTable,
                                                       Expr partitionByExpr) {
        if (!MvUtils.isFuncCallExpr(partitionByExpr, FunctionSet.DATE_TRUNC)) {
            return partitionByExpr;
        }
        // why resolve slot ref to mv columns?
        // generated column should refer mv's defined column, but partitionByExpr is ref to base table's column,
        // so resolve slot ref to mv columns.
        resolveRefToMVColumns(mvColumns, slotRef, mvTableName);
        int zoneHourOffset = getIcebergPartitionColumnTimeZoneOffset(icebergTable, slotRef);
        return getIcebergAdjustPartitionExpr(partitionByExpr, slotRef, zoneHourOffset);
    }

    /**
     * For MV with olap tables that contain common partition expressions which uses a generated column inner,
     * we need to adjust the partition expression to the associated generated column.
     */
    public static Expr getAdjustedOlapTablePartitionExpr(Map<Integer, Expr> changedPartitionByExprs,
                                                         Integer i,
                                                         List<Column> mvColumns, TableName mvTableName,
                                                         SlotRef slotRef,
                                                         Expr partitionByExpr) {
        if (changedPartitionByExprs.containsKey(i)) {
            // why resolve slot ref to mv columns?
            // generated column should refer mv's defined column, but partitionByExpr is ref to base table's column,
            // so resolve slot ref to mv columns.
            resolveRefToMVColumns(mvColumns, slotRef, mvTableName);
            return partitionByExpr;
        }
        return partitionByExpr;
    }

    public static Optional<Expr> analyzeMVRetentionCondition(ConnectContext connectContext,
                                                             MaterializedView mv,
                                                             Table refBaseTable,
                                                             String retentionCondition) {
        Expr retentionCondtiionExpr = null;
        try {
            retentionCondtiionExpr = SqlParser.parseSqlToExpr(retentionCondition, SqlModeHelper.MODE_DEFAULT);
        } catch (Exception e) {
            throw new SemanticException("Failed to parse retention condition: " + retentionCondition);
        }
        if (retentionCondtiionExpr == null) {
            return Optional.empty();
        }
        Scope scope = new Scope(RelationId.anonymous(), new RelationFields(
                refBaseTable.getBaseSchema().stream()
                        .map(col -> new Field(col.getName(), col.getType(), null, null))
                        .collect(Collectors.toList())));
        ExpressionAnalyzer.analyzeExpression(retentionCondtiionExpr, new AnalyzeState(), scope, connectContext);
        Map<Expr, Expr> partitionByExprMap = getMVPartitionByExprToAdjustMap(null, mv);
        retentionCondtiionExpr = MaterializedViewAnalyzer.adjustWhereExprIfNeeded(partitionByExprMap, retentionCondtiionExpr,
                scope, connectContext);
        return Optional.of(retentionCondtiionExpr);
    }

    public static Optional<ScalarOperator> analyzeMVRetentionConditionOperator(ConnectContext connectContext,
                                                                               MaterializedView mv,
                                                                               Table refBaseTable,
                                                                               Optional<Expr> exprOpt) {
        if (exprOpt.isEmpty()) {
            return Optional.empty();
        }
        Expr retentionCondtiionExpr = exprOpt.get();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        List<ColumnRefOperator> columnRefOperators = refBaseTable.getBaseSchema()
                .stream()
                .map(col -> columnRefFactory.create(col.getName(), col.getType(), col.isAllowNull()))
                .collect(Collectors.toList());
        TableName tableName = new TableName(null, refBaseTable.getName());
        Scope scope = new Scope(RelationId.anonymous(), new RelationFields(
                columnRefOperators.stream()
                        .map(col -> new Field(col.getName(), col.getType(), tableName, null))
                        .collect(Collectors.toList())));
        ExpressionMapping expressionMapping = new ExpressionMapping(scope, columnRefOperators);
        // substitute generated column expr if whereExpr is a mv which contains iceberg transform expr.
        // translate whereExpr to scalarOperator and replace whereExpr's generatedColumnExpr to partition slotRef.
        ScalarOperator scalarOperator =
                SqlToScalarOperatorTranslator.translate(retentionCondtiionExpr, expressionMapping, Lists.newArrayList(),
                        columnRefFactory, connectContext, null,
                        null, null, false);
        if (scalarOperator == null) {
            return Optional.empty();
        }
        ScalarOperatorRewriter scalarOpRewriter = new ScalarOperatorRewriter();
        scalarOperator = scalarOpRewriter.rewrite(scalarOperator, DEFAULT_TYPE_CAST_RULE);
        return Optional.of(scalarOperator);
    }
}
>>>>>>> 6a0fd5dd7b ([BugFix] Build a ConnectContext for inner query which is used for StarRocks internal query (#54737))
