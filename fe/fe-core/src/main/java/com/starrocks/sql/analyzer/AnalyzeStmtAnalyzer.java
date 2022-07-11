// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AnalyzeHistogramDesc;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AnalyzeTypeDesc;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.DropHistogramStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AnalyzeStmtAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new AnalyzeStatementAnalyzerVisitor().analyze(statement, session);
    }

    private static final List<String> VALID_PROPERTIES = Lists.newArrayList(
            StatsConstants.PRO_SAMPLE_RATIO,
            StatsConstants.PRO_AUTO_COLLECT_STATISTICS_RATIO,
            StatsConstants.PROP_UPDATE_INTERVAL_SEC_KEY,
            StatsConstants.PROP_SAMPLE_COLLECT_ROWS_KEY,

            //Deprecated , just not throw exception
            StatsConstants.PROP_COLLECT_INTERVAL_SEC_KEY
    );

    public static final List<String> NUMBER_PROP_KEY_LIST = ImmutableList.<String>builder()
            .add(StatsConstants.PROP_UPDATE_INTERVAL_SEC_KEY)
            .add(StatsConstants.PROP_SAMPLE_COLLECT_ROWS_KEY).build();

    static class AnalyzeStatementAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitAnalyzeStatement(AnalyzeStmt statement, ConnectContext session) {
            MetaUtils.normalizationTableName(session, statement.getTableName());
            Table analyzeTable = MetaUtils.getStarRocksTable(session, statement.getTableName());

            if (StatisticUtils.statisticDatabaseBlackListCheck(statement.getTableName().getDb())) {
                throw new SemanticException("Forbidden collect database: %s", statement.getTableName().getDb());
            }
            if (analyzeTable.getType() != Table.TableType.OLAP) {
                throw new SemanticException("Table '%s' is not a OLAP table", analyzeTable.getName());
            }

            // Analyze columns mentioned in the statement.
            Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

            List<String> columnNames = statement.getColumnNames();
            if (columnNames == null || columnNames.isEmpty()) {
                statement.setColumnNames(
                        analyzeTable.getBaseSchema().stream().map(Column::getName).collect(Collectors.toList()));
            } else {
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
                    tbl.setDb(ClusterNamespace.getFullName(statement.getClusterName(), tbl.getDb()));
                    Database db = MetaUtils.getStarRocks(session, statement.getTableName());

                    if (StatisticUtils.statisticDatabaseBlackListCheck(statement.getTableName().getDb())) {
                        throw new SemanticException("Forbidden collect database: %s", statement.getTableName().getDb());
                    }

                    statement.setDbId(db.getId());
                } else if (null != statement.getTableName().getTbl()) {
                    MetaUtils.normalizationTableName(session, statement.getTableName());
                    Database db = MetaUtils.getStarRocks(session, statement.getTableName());
                    Table analyzeTable = MetaUtils.getStarRocksTable(session, statement.getTableName());

                    if (!(analyzeTable instanceof OlapTable)) {
                        throw new SemanticException("Table '%s' is not a OLAP table", analyzeTable.getName());
                    }

                    // Analyze columns mentioned in the statement.
                    Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

                    List<String> columnNames = statement.getColumnNames();
                    if (columnNames != null && !columnNames.isEmpty()) {
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
                if (properties.containsKey(key) && !StringUtils.isNumeric(properties.get(key))) {
                    throw new SemanticException("Property '%s' value must be numeric", key);
                }
            }
        }

        private void analyzeAnalyzeTypeDesc(ConnectContext session, AnalyzeStmt statement, AnalyzeTypeDesc analyzeTypeDesc) {
            if (analyzeTypeDesc instanceof AnalyzeHistogramDesc) {
                List<String> columns = statement.getColumnNames();
                OlapTable analyzeTable = (OlapTable) MetaUtils.getStarRocksTable(session, statement.getTableName());

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
                statement.getProperties().put(StatsConstants.PRO_BUCKET_NUM, String.valueOf(bucket));

                properties.computeIfAbsent(StatsConstants.PRO_SAMPLE_RATIO, p -> String.valueOf(Config.histogram_sample_ratio));

                long minSampleRows;
                if (properties.get(StatsConstants.PROP_SAMPLE_COLLECT_ROWS_KEY) == null) {
                    minSampleRows = Config.statistic_sample_collect_rows;
                    properties.put(StatsConstants.PROP_SAMPLE_COLLECT_ROWS_KEY, String.valueOf(minSampleRows));
                } else {
                    minSampleRows = Long.parseLong(StatsConstants.PROP_SAMPLE_COLLECT_ROWS_KEY);
                }

                long totalRows = analyzeTable.getRowCount();
                long sampleRows = (long) (totalRows * Double.parseDouble(properties.get(StatsConstants.PRO_SAMPLE_RATIO)));
                if (sampleRows < minSampleRows) {
                    sampleRows = Math.min(minSampleRows, totalRows);
                }
                properties.put(StatsConstants.PROP_SAMPLE_COLLECT_ROWS_KEY, String.valueOf(sampleRows));
            }
        }

        @Override
        public Void visitDropHistogramStatement(DropHistogramStmt statement, ConnectContext session) {
            MetaUtils.normalizationTableName(session, statement.getTableName());
            Table analyzeTable = MetaUtils.getStarRocksTable(session, statement.getTableName());
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
