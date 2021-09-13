// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AnalyzeStmt;
import com.starrocks.analysis.CreateAnalyzeJobStmt;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.QueryStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.relation.Relation;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.StatisticUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class Analyzer {
    private static final Logger LOG = LogManager.getLogger(Analyzer.class);

    private final Catalog catalog;
    private final ConnectContext session;

    public Analyzer(Catalog catalog, ConnectContext session) {
        this.catalog = catalog;
        this.session = session;
    }

    public Relation analyze(StatementBase node) {
        node.setClusterName(session.getClusterName());
        if (node instanceof QueryStmt) {
            return new QueryAnalyzer(catalog, session)
                    .transformQueryStmt((QueryStmt) node, new Scope(RelationId.anonymous(), new RelationFields()));
        } else if (node instanceof InsertStmt) {
            return new InsertAnalyzer(catalog, session).transformInsertStmt((InsertStmt) node);
        } else if (node instanceof AnalyzeStmt) {
            return analyzeAnalyzeStmt((AnalyzeStmt) node);
        } else if (node instanceof CreateAnalyzeJobStmt) {
            return analyzeCreateAnalyzeStmt((CreateAnalyzeJobStmt) node);
        } else {
            throw unsupportedException("New Planner only support Query Statement");
        }
    }

    private Relation analyzeAnalyzeStmt(AnalyzeStmt node) {
        MetaUtils.normalizationTableName(session, node.getTableName());
        Table analyzeTable = MetaUtils.getStarRocksTable(session, node.getTableName());

        if (StatisticUtils.statisticDatabaseBlackListCheck(node.getTableName().getDb())) {
            throw new SemanticException("Forbidden collect database: %s", node.getTableName().getDb());
        }
        if (analyzeTable.getType() != Table.TableType.OLAP) {
            throw new SemanticException("Table '%s' is not a OLAP table", analyzeTable.getName());
        }

        // Analyze columns mentioned in the statement.
        Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

        List<String> columnNames = node.getColumnNames();
        if (columnNames == null || columnNames.isEmpty()) {
            node.setColumnNames(
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

        Map<String, String> properties = node.getProperties();

        if (null == properties) {
            node.setProperties(Maps.newHashMap());
            return null;
        }

        for (String key : AnalyzeJob.NUMBER_PROP_KEY_LIST) {
            if (properties.containsKey(key) && !StringUtils.isNumeric(properties.get(key))) {
                throw new SemanticException("Property '%s' value must be numeric", key);
            }
        }

        return null;
    }

    private Relation analyzeCreateAnalyzeStmt(CreateAnalyzeJobStmt node) {
        if (null != node.getTableName()) {
            TableName tbl = node.getTableName();

            if (null != tbl.getDb() && null == tbl.getTbl()) {
                tbl.setDb(ClusterNamespace.getFullName(node.getClusterName(), tbl.getDb()));
                Database db = MetaUtils.getStarRocks(session, node.getTableName());

                if (StatisticUtils.statisticDatabaseBlackListCheck(node.getTableName().getDb())) {
                    throw new SemanticException("Forbidden collect database: %s", node.getTableName().getDb());
                }

                node.setDbId(db.getId());
            } else if (null != node.getTableName().getTbl()) {
                MetaUtils.normalizationTableName(session, node.getTableName());
                Database db = MetaUtils.getStarRocks(session, node.getTableName());
                Table table = MetaUtils.getStarRocksTable(session, node.getTableName());

                if (!(table instanceof OlapTable)) {
                    throw new SemanticException("Table '%s' is not a OLAP table", table.getName());
                }

                // Analyze columns mentioned in the statement.
                Set<String> mentionedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

                List<String> columnNames = node.getColumnNames();
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

                node.setDbId(db.getId());
                node.setTableId(table.getId());
            }
        }

        Map<String, String> properties = node.getProperties();

        if (null == properties) {
            node.setProperties(Maps.newHashMap());
            return null;
        }

        for (String key : AnalyzeJob.NUMBER_PROP_KEY_LIST) {
            if (properties.containsKey(key) && !StringUtils.isNumeric(properties.get(key))) {
                throw new SemanticException("Property '%s' value must be numeric", key);
            }
        }

        return null;
    }
}
