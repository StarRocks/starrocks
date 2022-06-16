// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AnalyzeHistogramDesc;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.ast.AnalyzeTypeDesc;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.StatisticUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AnalyzeStmtAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new AnalyzeStatementAnalyzerVisitor().analyze(statement, session);
    }

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

            Map<String, String> properties = statement.getProperties();

            if (null == properties) {
                statement.setProperties(Maps.newHashMap());
                return null;
            }

            for (String key : AnalyzeJob.NUMBER_PROP_KEY_LIST) {
                if (properties.containsKey(key) && !StringUtils.isNumeric(properties.get(key))) {
                    throw new SemanticException("Property '%s' value must be numeric", key);
                }
            }

            analyzeAnalyzeTypeDesc(session, statement, statement.getAnalyzeTypeDesc());
            return null;
        }

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
                    Table table = MetaUtils.getStarRocksTable(session, statement.getTableName());

                    if (!(table instanceof OlapTable)) {
                        throw new SemanticException("Table '%s' is not a OLAP table", table.getName());
                    }

                    // Analyze columns mentioned in the statement.
                    Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

                    List<String> columnNames = statement.getColumnNames();
                    if (columnNames != null && !columnNames.isEmpty()) {
                        for (String colName : columnNames) {
                            Column col = table.getColumn(colName);
                            if (col == null) {
                                throw new SemanticException("Unknown column '%s' in '%s'", colName, table.getName());
                            }
                            if (!mentionedColumns.add(colName)) {
                                throw new SemanticException("Column '%s' specified twice", colName);
                            }
                        }
                    }

                    statement.setDbId(db.getId());
                    statement.setTableId(table.getId());
                }
            }

            Map<String, String> properties = statement.getProperties();

            if (null == properties) {
                statement.setProperties(Maps.newHashMap());
                return null;
            }

            for (String key : AnalyzeJob.NUMBER_PROP_KEY_LIST) {
                if (properties.containsKey(key) && !StringUtils.isNumeric(properties.get(key))) {
                    throw new SemanticException("Property '%s' value must be numeric", key);
                }
            }
            return null;
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
            }
        }
    }
}
