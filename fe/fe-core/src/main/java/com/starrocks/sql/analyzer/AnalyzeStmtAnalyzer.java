// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AnalyzeHistogramDesc;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AnalyzeTypeDesc;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.ast.DropStatsStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AnalyzeStmtAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new AnalyzeStatementAnalyzerVisitor().analyze(statement, session);
    }

    private static final List<String> VALID_PROPERTIES = Lists.newArrayList(
            StatsConstants.STATISTIC_AUTO_COLLECT_RATIO,
            StatsConstants.STATISTIC_SAMPLE_COLLECT_ROWS,

            StatsConstants.HISTOGRAM_BUCKET_NUM,
            StatsConstants.HISTOGRAM_MCV_SIZE,
            StatsConstants.HISTOGRAM_SAMPLE_RATIO,

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

    static class AnalyzeStatementAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitAnalyzeStatement(AnalyzeStmt statement, ConnectContext session) {
            MetaUtils.normalizationTableName(session, statement.getTableName());
            Table analyzeTable = MetaUtils.getTable(session, statement.getTableName());

            if (StatisticUtils.statisticDatabaseBlackListCheck(statement.getTableName().getDb())) {
                throw new SemanticException("Forbidden collect database: %s", statement.getTableName().getDb());
            }

            // Analyze columns mentioned in the statement.
            Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

            List<String> columnNames = statement.getColumnNames();
            if (columnNames != null) {
                for (String colName : columnNames) {
                    Column col = analyzeTable.getColumn(colName);
                    if (col == null) {
                        throw new SemanticException("Unknown column '%s' in '%s'", colName, analyzeTable.getName());
                    }
                    if (!mentionedColumns.add(colName)) {
                        throw new SemanticException("Column '%s' specified twice", colName);
                    }
                }
            }

            analyzeProperties(statement.getProperties());
            analyzeAnalyzeTypeDesc(session, statement, statement.getAnalyzeTypeDesc());
            return null;
        }

        @Override
        public Void visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, ConnectContext session) {
            if (null != statement.getTableName()) {
                TableName tbl = statement.getTableName();

                if (null != tbl.getDb() && null == tbl.getTbl()) {
                    Database db = MetaUtils.getDatabase(session, statement.getTableName());

                    if (StatisticUtils.statisticDatabaseBlackListCheck(statement.getTableName().getDb())) {
                        throw new SemanticException("Forbidden collect database: %s", statement.getTableName().getDb());
                    }

                    statement.setDbId(db.getId());
                } else if (null != statement.getTableName().getTbl()) {
                    MetaUtils.normalizationTableName(session, statement.getTableName());
                    Database db = MetaUtils.getDatabase(session, statement.getTableName());
                    Table analyzeTable = MetaUtils.getTable(session, statement.getTableName());

                    // Analyze columns mentioned in the statement.
                    Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

                    List<String> columnNames = statement.getColumnNames();
                    if (columnNames != null && !columnNames.isEmpty()) {
                        for (String colName : columnNames) {
                            Column col = analyzeTable.getColumn(colName);
                            if (col == null) {
                                throw new SemanticException("Unknown column '%s' in '%s'", colName,
                                        analyzeTable.getName());
                            }
                            if (!mentionedColumns.add(colName)) {
                                throw new SemanticException("Column '%s' specified twice", colName);
                            }
                        }
                    }

                    statement.setDbId(db.getId());
                    statement.setTableId(analyzeTable.getId());
                }
            }
            analyzeProperties(statement.getProperties());
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
        }

        private void analyzeAnalyzeTypeDesc(ConnectContext session, AnalyzeStmt statement,
                                            AnalyzeTypeDesc analyzeTypeDesc) {
            if (analyzeTypeDesc instanceof AnalyzeHistogramDesc) {
                List<String> columns = statement.getColumnNames();
                OlapTable analyzeTable = (OlapTable) MetaUtils.getTable(session, statement.getTableName());

                for (String columnName : columns) {
                    Column column = analyzeTable.getColumn(columnName);
                    if (column.getType().isComplexType()
                            || column.getType().isJsonType()
                            || column.getType().isOnlyMetricType()) {
                        throw new SemanticException("Can't create histogram statistics on column type is %s",
                                column.getType().toSql());
                    }
                }

                Map<String, String> properties = statement.getProperties();

                long bucket = ((AnalyzeHistogramDesc) analyzeTypeDesc).getBuckets();
                if (bucket <= 0) {
                    throw new SemanticException("Bucket number can't less than 1");
                }
                statement.getProperties().put(StatsConstants.HISTOGRAM_BUCKET_NUM, String.valueOf(bucket));

                properties.computeIfAbsent(StatsConstants.HISTOGRAM_MCV_SIZE,
                        p -> String.valueOf(Config.histogram_mcv_size));
                properties.computeIfAbsent(StatsConstants.HISTOGRAM_SAMPLE_RATIO,
                        p -> String.valueOf(Config.histogram_sample_ratio));

                long totalRows = analyzeTable.getRowCount();
                long sampleRows = (long) (totalRows *
                        Double.parseDouble(properties.get(StatsConstants.HISTOGRAM_SAMPLE_RATIO)));

                if (sampleRows < Config.statistic_sample_collect_rows && totalRows != 0) {
                    if (Config.statistic_sample_collect_rows > totalRows) {
                        properties.put(StatsConstants.HISTOGRAM_SAMPLE_RATIO, "1");
                    } else {
                        properties.put(StatsConstants.HISTOGRAM_SAMPLE_RATIO, String.valueOf(
                                BigDecimal.valueOf((double) Config.statistic_sample_collect_rows / (double) totalRows)
                                        .setScale(8, RoundingMode.HALF_UP).doubleValue()));
                    }
                } else if (sampleRows > Config.histogram_max_sample_row_count) {
                    properties.put(StatsConstants.HISTOGRAM_SAMPLE_RATIO, String.valueOf(
                            BigDecimal.valueOf((double) Config.histogram_max_sample_row_count / (double) totalRows)
                                    .setScale(8, RoundingMode.HALF_UP).doubleValue()));
                }
            }
        }

        @Override
        public Void visitDropStatsStatement(DropStatsStmt statement, ConnectContext session) {
            MetaUtils.normalizationTableName(session, statement.getTableName());
            return null;
        }

        @Override
        public Void visitDropHistogramStatement(DropHistogramStmt statement, ConnectContext session) {
            MetaUtils.normalizationTableName(session, statement.getTableName());
            Table analyzeTable = MetaUtils.getTable(session, statement.getTableName());
            List<String> columnNames = statement.getColumnNames();
            for (String colName : columnNames) {
                Column col = analyzeTable.getColumn(colName);
                if (col == null) {
                    throw new SemanticException("Unknown column '%s' in '%s'", colName, analyzeTable.getName());
                }
            }
            return null;
        }
    }
}
