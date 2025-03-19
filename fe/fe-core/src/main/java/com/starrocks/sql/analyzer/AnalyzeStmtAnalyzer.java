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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AnalyzeHistogramDesc;
import com.starrocks.sql.ast.AnalyzeMultiColumnDesc;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AnalyzeTypeDesc;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropStatsStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.statistic.columns.ColumnUsage;
import com.starrocks.statistic.columns.PredicateColumnsMgr;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.createPartitionKey;
import static com.starrocks.connector.PartitionUtil.toPartitionValues;

public class AnalyzeStmtAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new AnalyzeStatementAnalyzerVisitor().analyze(statement, session);
    }

    private static final List<String> VALID_PROPERTIES = Lists.newArrayList(
            StatsConstants.STATISTIC_AUTO_COLLECT_RATIO,
            StatsConstants.STATISTIC_AUTO_COLLECT_INTERVAL,
            StatsConstants.STATISTIC_SAMPLE_COLLECT_ROWS,
            StatsConstants.STATISTIC_EXCLUDE_PATTERN,

            StatsConstants.HIGH_WEIGHT_SAMPLE_RATIO,
            StatsConstants.MEDIUM_HIGH_WEIGHT_SAMPLE_RATIO,
            StatsConstants.MEDIUM_LOW_WEIGHT_SAMPLE_RATIO,
            StatsConstants.LOW_WEIGHT_SAMPLE_RATIO,

            StatsConstants.HISTOGRAM_BUCKET_NUM,
            StatsConstants.HISTOGRAM_MCV_SIZE,
            StatsConstants.HISTOGRAM_SAMPLE_RATIO,
            StatsConstants.INIT_SAMPLE_STATS_JOB,

            //Deprecated , just not throw exception
            StatsConstants.PRO_SAMPLE_RATIO,
            StatsConstants.PROP_UPDATE_INTERVAL_SEC_KEY,
            StatsConstants.PROP_COLLECT_INTERVAL_SEC_KEY
    );

    public static final List<String> NUMBER_PROP_KEY_LIST = ImmutableList.<String>builder().addAll(
            Lists.newArrayList(StatsConstants.STATISTIC_SAMPLE_COLLECT_ROWS,
                    StatsConstants.HISTOGRAM_BUCKET_NUM,
                    StatsConstants.HISTOGRAM_MCV_SIZE,
                    StatsConstants.HISTOGRAM_SAMPLE_RATIO)).build();

    public static boolean isSupportedHistogramAnalyzeTableType(Table table) {
        return table.isNativeTableOrMaterializedView() || table.isHiveTable();
    }

    static class AnalyzeStatementAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitAnalyzeStatement(AnalyzeStmt statement, ConnectContext session) {
            statement.getTableName().normalization(session);
            Table analyzeTable = MetaUtils.getSessionAwareTable(session, null, statement.getTableName());
            AnalyzeTypeDesc analyzeTypeDesc = statement.getAnalyzeTypeDesc();
            if (StatisticUtils.statisticDatabaseBlackListCheck(statement.getTableName().getDb())) {
                throw new SemanticException("Forbidden collect database: %s", statement.getTableName().getDb());
            }

            // ANALYZE TABLE xxx (col1, col2, ...)
            List<Expr> columns = statement.getColumns();

            if (analyzeTypeDesc instanceof AnalyzeMultiColumnDesc) {
                if (columns.size() <= 1) {
                    throw new SemanticException("must greater than 1 column on multi-column combined analyze statement");
                }

                if (columns.size() > Config.statistics_max_multi_column_combined_num) {
                    throw new SemanticException("column size " + columns.size() + " exceeded max size of " +
                            Config.statistics_max_multi_column_combined_num + " on multi-column combined analyze statement");
                }

                if (statement.getPartitionNames() != null) {
                    throw new SemanticException("not support specify partition names on multi-column analyze statement");
                }

                if (statement.isAsync()) {
                    throw new SemanticException("not support async analyze on multi-column analyze statement");
                }
            }

            if (CollectionUtils.isNotEmpty(columns)) {
                Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
                // The actual column name, avoiding case sensitivity issues
                List<String> realColumnNames = Lists.newArrayList();
                for (Expr column : columns) {
                    ExpressionAnalyzer.analyzeExpression(column, new AnalyzeState(), new Scope(RelationId.anonymous(),
                            new RelationFields(analyzeTable.getBaseSchema().stream().map(col -> new Field(col.getName(),
                                            col.getType(), statement.getTableName(), null))
                                    .collect(Collectors.toList()))), session);
                    String colName = StatisticUtils.getColumnName(analyzeTable, column);
                    if (!mentionedColumns.add(colName)) {
                        throw new SemanticException("Column '%s' specified twice", colName);
                    }
                    realColumnNames.add(colName);
                }
                statement.setColumnNames(realColumnNames);
            }

            if (statement.getPartitionNames() != null) {
                if (!analyzeTable.isNativeTableOrMaterializedView()) {
                    throw new SemanticException("Analyze partition only support olap table");
                }
                List<Long> pidList = Lists.newArrayList();
                for (String partitionName : statement.getPartitionNames().getPartitionNames()) {
                    Partition p = analyzeTable.getPartition(partitionName);
                    if (p == null) {
                        throw new SemanticException("Partition '%s' not found", partitionName);
                    }
                    pidList.add(p.getId());
                }
                statement.setPartitionIds(pidList);
            }

            // ANALYZE TABLE xxx
            // ANALYZE TABLE xxx ALL COLUMNS
            if (statement.isAllColumns() && CollectionUtils.isEmpty(columns)) {
                List<String> collectibleColumns = StatisticUtils.getCollectibleColumns(analyzeTable);
                statement.setColumnNames(collectibleColumns);
            }

            // ANALYZE TABLE xxx PREDICATE COLUMNS
            if (statement.isUsePredicateColumns()) {
                // check if the table type is supported
                if (!analyzeTable.isNativeTableOrMaterializedView()) {
                    throw new SemanticException("Only OLAP table can support ANALYZE PREDICATE COLUMNS");
                }

                List<String> targetColumns = Lists.newArrayList();

                List<ColumnUsage> predicateColumns =
                        PredicateColumnsMgr.getInstance().queryPredicateColumns(statement.getTableName());
                for (ColumnUsage col : ListUtils.emptyIfNull(predicateColumns)) {
                    Column realColumn = analyzeTable.getColumnByUniqueId(col.getColumnFullId().getColumnUniqueId());
                    if (realColumn != null) {
                        targetColumns.add(realColumn.getName());
                    }
                }

                statement.setColumnNames(targetColumns);
            }

            analyzeProperties(statement.getProperties());
            analyzeAnalyzeTypeDesc(session, statement, statement.getAnalyzeTypeDesc());

            if (CatalogMgr.isExternalCatalog(statement.getTableName().getCatalog())) {
                if (!analyzeTable.isAnalyzableExternalTable()) {
                    throw new SemanticException(
                            "Analyze external table only support hive, iceberg, deltalake, paimon and odps table",
                            statement.getTableName().toString());
                } else if (analyzeTypeDesc instanceof AnalyzeMultiColumnDesc) {
                    throw new SemanticException("Don't support analyze multi-columns combined statistics on external table");
                }

                statement.setExternal(true);
            } else if (CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog(analyzeTable.getCatalogName())) {
                throw new SemanticException("Don't support analyze external table created by resource mapping");
            }
            return null;
        }

        @Override
        public Void visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, ConnectContext session) {
            if (null != statement.getTableName()) {
                TableName tbl = statement.getTableName();

                if ((Strings.isNullOrEmpty(tbl.getCatalog()) &&
                        CatalogMgr.isExternalCatalog(session.getCurrentCatalog())) ||
                        CatalogMgr.isExternalCatalog(tbl.getCatalog())) {
                    if (tbl.getTbl() == null) {
                        throw new SemanticException("External catalog don't support analyze all tables, please give a" +
                                " specific table");
                    }
                    String catalogName = Strings.isNullOrEmpty(tbl.getCatalog()) ?
                            session.getCurrentCatalog() : tbl.getCatalog();
                    tbl.setCatalog(catalogName);
                    statement.setCatalogName(catalogName);
                    String dbName = Strings.isNullOrEmpty(tbl.getDb()) ?
                            session.getDatabase() : tbl.getDb();
                    tbl.setDb(dbName);
                    Table analyzeTable = MetaUtils.getSessionAwareTable(session, null, statement.getTableName());
                    if (!analyzeTable.isAnalyzableExternalTable()) {
                        throw new SemanticException("Analyze external table only support hive, iceberg, deltalake, " +
                                "paimon and odps table", statement.getTableName().toString());
                    }
                }

                if (null != tbl.getDb() && null == tbl.getTbl()) {
                    Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(tbl.getCatalog(), tbl.getDb());
                    if (db == null) {
                        throw new SemanticException("Database %s is not found", tbl.getCatalogAndDb());
                    }

                    if (statement.isNative() &&
                            StatisticUtils.statisticDatabaseBlackListCheck(statement.getTableName().getDb())) {
                        throw new SemanticException("Forbidden collect database: %s", statement.getTableName().getDb());
                    }

                    statement.setDbId(db.getId());
                } else if (null != statement.getTableName().getTbl()) {
                    statement.getTableName().normalization(session);
                    Database db = GlobalStateMgr.getCurrentState().getMetadataMgr()
                            .getDb(statement.getTableName().getCatalog(), statement.getTableName().getDb());
                    if (db == null) {
                        throw new SemanticException("Database %s is not found", statement.getTableName().getCatalogAndDb());
                    }
                    Table analyzeTable = MetaUtils.getSessionAwareTable(session, db, statement.getTableName());

                    if (analyzeTable.isTemporaryTable()) {
                        throw new SemanticException("Don't support create analyze job for temporary table");
                    }

                    if (CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog(analyzeTable.getCatalogName())) {
                        throw new SemanticException("Don't support analyze external table created by resource mapping");
                    }

                    // Analyze columns mentioned in the statement.
                    Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

                    List<Expr> columns = statement.getColumns();
                    // The actual column name, avoiding case sensitivity issues
                    List<String> realColumnNames = Lists.newArrayList();
                    if (columns != null && !columns.isEmpty()) {
                        for (Expr column : columns) {
                            ExpressionAnalyzer.analyzeExpression(column, new AnalyzeState(), new Scope(RelationId.anonymous(),
                                    new RelationFields(analyzeTable.getBaseSchema().stream().map(col -> new Field(col.getName(),
                                                    col.getType(), statement.getTableName(), null))
                                            .collect(Collectors.toList()))), session);
                            String colName = StatisticUtils.getColumnName(analyzeTable, column);
                            if (!mentionedColumns.add(colName)) {
                                throw new SemanticException("Column '%s' specified twice", colName);
                            }
                            realColumnNames.add(colName);
                        }
                        statement.setColumnNames(realColumnNames);
                    }

                    statement.setDbId(db.getId());
                    statement.setTableId(analyzeTable.getId());
                }
            } else {
                if (CatalogMgr.isExternalCatalog(session.getCurrentCatalog())) {
                    throw new SemanticException("External catalog %s don't support analyze all databases",
                            session.getCurrentCatalog());
                }
            }
            analyzeProperties(statement.getProperties());
            analyzeAnalyzeTypeDesc(session, statement, statement.getAnalyzeTypeDesc());
            return null;
        }

        private void analyzeProperties(Map<String, String> properties) {
            for (String property : properties.keySet()) {
                if (!VALID_PROPERTIES.contains(property)) {
                    throw new SemanticException("Property '%s' is not valid", property);
                }
            }

            for (String key : NUMBER_PROP_KEY_LIST) {
                if (properties.containsKey(key) && !NumberUtils.isCreatable(properties.get(key))) {
                    throw new SemanticException("Property '%s' value must be numeric", key);
                }
            }

            if (properties.containsKey(StatsConstants.STATISTIC_EXCLUDE_PATTERN)) {
                String pattern = properties.get(StatsConstants.STATISTIC_EXCLUDE_PATTERN);
                // check regex
                try {
                    Pattern.compile(pattern);
                } catch (PatternSyntaxException e) {
                    throw new SemanticException("Property %s value is error, msg: %s",
                            StatsConstants.STATISTIC_EXCLUDE_PATTERN, e.getMessage());
                }
            }
        }

        private void analyzeAnalyzeTypeDesc(ConnectContext session, StatementBase statement,
                                            AnalyzeTypeDesc analyzeTypeDesc) {
            if (analyzeTypeDesc instanceof AnalyzeHistogramDesc) {
                List<Expr> columns;
                Map<String, String> properties;
                TableName tableName;
                if (statement instanceof AnalyzeStmt) {
                    AnalyzeStmt analyzeStmt = (AnalyzeStmt) statement;
                    columns = analyzeStmt.getColumns();
                    properties = analyzeStmt.getProperties();
                    tableName = analyzeStmt.getTableName();
                } else if (statement instanceof CreateAnalyzeJobStmt) {
                    CreateAnalyzeJobStmt createAnalyzeJobStmt = (CreateAnalyzeJobStmt) statement;
                    columns = createAnalyzeJobStmt.getColumns();
                    properties = createAnalyzeJobStmt.getProperties();
                    tableName = createAnalyzeJobStmt.getTableName();
                } else {
                    throw new NotImplementedException("unreachable");
                }

                Table analyzeTable = MetaUtils.getSessionAwareTable(session, null, tableName);
                if (!isSupportedHistogramAnalyzeTableType(analyzeTable)) {
                    throw new SemanticException("Can't create histogram statistics on table type is %s",
                            analyzeTable.getType().name());
                }
                for (Expr column : columns) {
                    if (column.getType().isComplexType()
                            || column.getType().isJsonType()
                            || column.getType().isOnlyMetricType()) {
                        throw new SemanticException("Can't create histogram statistics on column type is %s",
                                column.getType().toSql());
                    }
                }

                long bucket = ((AnalyzeHistogramDesc) analyzeTypeDesc).getBuckets();
                if (bucket <= 0) {
                    throw new SemanticException("Bucket number can't less than 1");
                }
                properties.put(StatsConstants.HISTOGRAM_BUCKET_NUM, String.valueOf(bucket));

                properties.computeIfAbsent(StatsConstants.HISTOGRAM_MCV_SIZE,
                        p -> String.valueOf(Config.histogram_mcv_size));
                properties.computeIfAbsent(StatsConstants.HISTOGRAM_SAMPLE_RATIO,
                        p -> String.valueOf(Config.histogram_sample_ratio));

                double totalRows = 0;
                if (analyzeTable.isNativeTableOrMaterializedView()) {
                    OlapTable analyzedOlapTable = (OlapTable) analyzeTable;
                    totalRows = analyzedOlapTable.getRowCount();
                } else {
                    List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                            .listPartitionNames(tableName.getCatalog(), tableName.getDb(),
                                    tableName.getTbl(), ConnectorMetadatRequestContext.DEFAULT);
                    List<PartitionKey> keys = new ArrayList<>();
                    try {
                        for (String partName : partitionNames) {
                            List<String> values = toPartitionValues(partName);
                            PartitionKey partitionKey = createPartitionKey(values, analyzeTable.getPartitionColumns(),
                                    analyzeTable);
                            keys.add(partitionKey);
                        }
                    } catch (AnalysisException e) {
                        throw new SemanticException("can not get partition keys for table : %s.%s.%s, %s",
                                tableName.getCatalog(), tableName.getDb(), tableName.getTbl(), e.getMessage());
                    }

                    Statistics tableStats = session.getGlobalStateMgr().getMetadataMgr().
                            getTableStatistics(OptimizerFactory.initContext(session, new ColumnRefFactory()),
                                    tableName.getCatalog(), analyzeTable, Maps.newHashMap(), keys, null);
                    totalRows = tableStats.getOutputRowCount();
                }
                double sampleRows = totalRows *
                        Double.parseDouble(properties.get(StatsConstants.HISTOGRAM_SAMPLE_RATIO));
                if (sampleRows < Config.statistic_sample_collect_rows && totalRows != 0) {
                    if (Config.statistic_sample_collect_rows > totalRows) {
                        properties.put(StatsConstants.HISTOGRAM_SAMPLE_RATIO, "1");
                    } else {
                        properties.put(StatsConstants.HISTOGRAM_SAMPLE_RATIO, String.valueOf(
                                BigDecimal.valueOf(
                                                (double) Config.statistic_sample_collect_rows / totalRows)
                                        .setScale(8, RoundingMode.HALF_UP).doubleValue()));
                    }
                } else if (sampleRows > Config.histogram_max_sample_row_count) {
                    properties.put(StatsConstants.HISTOGRAM_SAMPLE_RATIO, String.valueOf(
                            BigDecimal.valueOf((double) Config.histogram_max_sample_row_count /
                                            (totalRows == 0L ? 1L : totalRows))
                                    .setScale(8, RoundingMode.HALF_UP).doubleValue()));
                }
            }
        }

        @Override
        public Void visitDropStatsStatement(DropStatsStmt statement, ConnectContext session) {
            statement.getTableName().normalization(session);
            if (CatalogMgr.isExternalCatalog(statement.getTableName().getCatalog())) {
                statement.setExternal(true);
            }
            return null;
        }

        @Override
        public Void visitDropHistogramStatement(DropHistogramStmt statement, ConnectContext session) {
            statement.getTableName().normalization(session);
            if (CatalogMgr.isExternalCatalog(statement.getTableName().getCatalog())) {
                statement.setExternal(true);
            }

            Table analyzeTable = MetaUtils.getSessionAwareTable(session, null, statement.getTableName());
            List<Expr> columns = statement.getColumns();
            List<String> realColumnNames = Lists.newArrayList();
            for (Expr column : columns) {
                ExpressionAnalyzer.analyzeExpression(column, new AnalyzeState(), new Scope(RelationId.anonymous(),
                        new RelationFields(analyzeTable.getBaseSchema().stream().map(col -> new Field(col.getName(),
                                        col.getType(), statement.getTableName(), null))
                                .collect(Collectors.toList()))), session);
                String colName = StatisticUtils.getColumnName(analyzeTable, column);
                realColumnNames.add(colName);
            }
            statement.setColumnNames(realColumnNames);
            return null;
        }
    }
}
