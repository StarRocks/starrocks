// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.AnalyzeStmt;
import com.starrocks.analysis.BaseViewStmt;
import com.starrocks.analysis.CreateAnalyzeJobStmt;
import com.starrocks.analysis.CreateTableAsSelectStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DeleteStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.UpdateStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.StatisticUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Analyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new AnalyzerVisitor().analyze(statement, session);
    }

    private static class AnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(StatementBase statement, ConnectContext session) {
            statement.setClusterName(session.getClusterName());
            visit(statement, session);
        }

        @Override
        public Void visitAlterTableStatement(AlterTableStmt statement, ConnectContext context) {
            AlterTableStatementAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitAlterWorkGroupStatement(AlterWorkGroupStmt statement, ConnectContext session) {
            statement.analyze();
            return null;
        }

        @Override
        public Void visitAnalyzeStatement(AnalyzeStmt statement, ConnectContext session) {
            analyzeAnalyzeStmt(statement, session);
            return null;
        }

        @Override
        public Void visitBaseViewStatement(BaseViewStmt statement, ConnectContext session) {
            ViewAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitCreateAnalyzeJobStatement(CreateAnalyzeJobStmt statement, ConnectContext session) {
            analyzeCreateAnalyzeStmt(statement, session);
            return null;
        }

        @Override
        public Void visitCreateTableAsSelectStatement(CreateTableAsSelectStmt statement, ConnectContext session) {
            // this phrase do not analyze insertStmt, insertStmt will analyze in
            // StmtExecutor.handleCreateTableAsSelectStmt because planner will not do meta operations
            CTASAnalyzer.transformCTASStmt((CreateTableAsSelectStmt) statement, session);
            return null;
        }

        @Override
        public Void visitCreateWorkGroupStatement(CreateWorkGroupStmt statement, ConnectContext session) {
            statement.analyze();
            return null;
        }

        @Override
        public Void visitInsertStatement(InsertStmt statement, ConnectContext session) {
            InsertAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitShowStatement(ShowStmt statement, ConnectContext session) {
            ShowStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitDropTableStmt(DropTableStmt statement, ConnectContext session) {
            DropStmtAnalyzer.analyze(statement, session);
            return null;
        }

        @Override
        public Void visitQueryStatement(QueryStatement stmt, ConnectContext session) {
            new QueryAnalyzer(session).analyze(stmt);

            QueryRelation queryRelation = stmt.getQueryRelation();
            long selectLimit = ConnectContext.get().getSessionVariable().getSqlSelectLimit();
            if (!queryRelation.hasLimit() && selectLimit != SessionVariable.DEFAULT_SELECT_LIMIT) {
                queryRelation.setLimit(new LimitElement(selectLimit));
            }
            return null;
        }

        @Override
        public Void visitUpdateStatement(UpdateStmt node, ConnectContext context) {
            UpdateAnalyzer.analyze(node, context);
            return null;
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt node, ConnectContext context) {
            DeleteAnalyzer.analyze(node, context);
            return null;
        }
    }

    private static void analyzeAnalyzeStmt(AnalyzeStmt node, ConnectContext session) {
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
            return;
        }

        for (String key : AnalyzeJob.NUMBER_PROP_KEY_LIST) {
            if (properties.containsKey(key) && !StringUtils.isNumeric(properties.get(key))) {
                throw new SemanticException("Property '%s' value must be numeric", key);
            }
        }
    }

    private static void analyzeCreateAnalyzeStmt(CreateAnalyzeJobStmt node, ConnectContext session) {
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
            return;
        }

        for (String key : AnalyzeJob.NUMBER_PROP_KEY_LIST) {
            if (properties.containsKey(key) && !StringUtils.isNumeric(properties.get(key))) {
                throw new SemanticException("Property '%s' value must be numeric", key);
            }
        }
    }
}
