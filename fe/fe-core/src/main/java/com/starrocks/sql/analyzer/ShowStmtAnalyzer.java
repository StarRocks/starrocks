// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.DescribeStmt;
import com.starrocks.analysis.SetType;
import com.starrocks.analysis.ShowAlterStmt;
import com.starrocks.analysis.ShowColumnStmt;
import com.starrocks.analysis.ShowCreateDbStmt;
import com.starrocks.analysis.ShowCreateTableStmt;
import com.starrocks.analysis.ShowDataStmt;
import com.starrocks.analysis.ShowDbStmt;
import com.starrocks.analysis.ShowDeleteStmt;
import com.starrocks.analysis.ShowIndexStmt;
import com.starrocks.analysis.ShowMaterializedViewStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.ShowTableStatusStmt;
import com.starrocks.analysis.ShowTableStmt;
import com.starrocks.analysis.ShowVariablesStmt;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.common.proc.ProcService;
import com.starrocks.common.proc.TableProcDir;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ShowStmtAnalyzer {

    public static void analyze(ShowStmt stmt, ConnectContext session) {
        new ShowStmtAnalyzerVisitor().visit(stmt, session);
    }

    static class ShowStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(ShowStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitShowTableStmt(ShowTableStmt node, ConnectContext context) {
            String db = node.getDb();
            db = getFullDatabaseName(db, context);
            node.setDb(db);
            return null;
        }

        @Override
        public Void visitShowVariablesStmt(ShowVariablesStmt node, ConnectContext context) {
            if (node.getType() == null) {
                node.setType(SetType.DEFAULT);
            }
            return null;
        }

        @Override
        public Void visitShowColumnStmt(ShowColumnStmt node, ConnectContext context) {
            node.init();
            String db = node.getTableName().getDb();
            db = getFullDatabaseName(db, context);
            node.getTableName().setDb(db);
            return null;
        }

        @Override
        public Void visitShowTableStatusStmt(ShowTableStatusStmt node, ConnectContext context) {
            String db = node.getDb();
            db = getFullDatabaseName(db, context);
            node.setDb(db);
            return null;
        }

        @Override
        public Void visitShowMaterializedViewStmt(ShowMaterializedViewStmt node, ConnectContext context) {
            String db = node.getDb();
            db = getFullDatabaseName(db, context);
            node.setDb(db);
            return null;
        }

        @Override
        public Void visitShowCreateTableStmt(ShowCreateTableStmt node, ConnectContext context) {
            if (node.getTbl() == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_TABLES_USED);
            }
            node.getTbl().normalization(context);
            return null;
        }

        @Override
        public Void visitShowDatabasesStmt(ShowDbStmt node, ConnectContext context) {
            String catalogName;
            if (node.getCatalogName() != null) {
                catalogName = node.getCatalogName();
            } else {
                catalogName = context.getCurrentCatalog();
            }
            if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
            }
            return null;
        }

        @Override
        public Void visitShowAlterStmt(ShowAlterStmt node, ConnectContext context) {
            ShowAlterStmtAnalyzer.analyze(node, context);
            return null;
        }

        @Override
        public Void visitShowDeleteStmt(ShowDeleteStmt node, ConnectContext context) {
            String dbName = node.getDbName();
            dbName = getFullDatabaseName(dbName, context);
            node.setDbName(dbName);
            return null;
        }

        @Override
        public Void visitShowIndexStmt(ShowIndexStmt node, ConnectContext context) {
            node.init();
            String db = node.getTableName().getDb();
            db = getFullDatabaseName(db, context);
            node.getTableName().setDb(db);
            if (Strings.isNullOrEmpty(node.getTableName().getCatalog())) {
                node.getTableName().setCatalog(context.getCurrentCatalog());
            }
            return null;
        }

        String getFullDatabaseName(String db, ConnectContext session) {
            String catalog = session.getCurrentCatalog();
            if (Strings.isNullOrEmpty(db)) {
                db = session.getDatabase();
                if (CatalogMgr.isInternalCatalog(catalog)) {
                    db = ClusterNamespace.getFullName(db);
                }
                if (Strings.isNullOrEmpty(db)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
            } else {
                if (CatalogMgr.isInternalCatalog(catalog)) {
                    db = ClusterNamespace.getFullName(db);
                }
            }
            return db;
        }

        @Override
        public Void visitShowCreateDbStatement(ShowCreateDbStmt node, ConnectContext context) {
            String dbName = node.getDb();
            dbName = getFullDatabaseName(dbName, context);
            node.setDb(dbName);
            return null;
        }

        @Override
        public Void visitShowDataStmt(ShowDataStmt node, ConnectContext context) {
            String dbName = node.getDbName();
            dbName = getFullDatabaseName(dbName, context);
            node.setDbName(dbName);
            return null;
        }

        @Override
        public Void visitDescTableStmt(DescribeStmt node, ConnectContext context) {
            node.getDbTableName().normalization(context);
            Database db = GlobalStateMgr.getCurrentState().getDb(node.getDb());
            if (db == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, node.getDb());
            }
            db.readLock();
            try {
                Table table = db.getTable(node.getTableName());
                //if getTable not find table, may be is statement "desc materialized-view-name"
                if (table == null) {
                    for (Table tb : db.getTables()) {
                        if (tb.getType() == Table.TableType.OLAP) {
                            OlapTable olapTable = (OlapTable) tb;
                            for (MaterializedIndex mvIdx : olapTable.getVisibleIndex()) {
                                if (olapTable.getIndexNameById(mvIdx.getId()).equalsIgnoreCase(node.getTableName())) {
                                    List<Column> columns = olapTable.getIndexIdToSchema().get(mvIdx.getId());
                                    for (Column column : columns) {
                                        // Extra string (aggregation and bloom filter)
                                        List<String> extras = Lists.newArrayList();
                                        if (column.getAggregationType() != null &&
                                                olapTable.getKeysType() != KeysType.PRIMARY_KEYS) {
                                            extras.add(column.getAggregationType().name());
                                        }
                                        String defaultStr = column.getMetaDefaultValue(extras);
                                        String extraStr = StringUtils.join(extras, ",");
                                        List<String> row = Arrays.asList(
                                                column.getDisplayName(),
                                                column.getType().canonicalName(),
                                                column.isAllowNull() ? "Yes" : "No",
                                                ((Boolean) column.isKey()).toString(),
                                                defaultStr,
                                                extraStr);
                                        node.getTotalRows().add(row);
                                    }
                                    node.setMaterializedView(true);
                                    return null;
                                }
                            }
                        }
                    }
                    ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, node.getTableName());
                }

                if (table.getType() == Table.TableType.HIVE || table.getType() == Table.TableType.HUDI
                        || table.getType() == Table.TableType.ICEBERG) {
                    // Reuse the logic of `desc <table_name>` because hive/hudi/iceberg external table doesn't support view.
                    node.setAllTables(false);
                }

                if (!node.isAllTables()) {
                    // show base table schema only
                    String procString = "/dbs/" + db.getId() + "/" + table.getId() + "/" + TableProcDir.INDEX_SCHEMA
                            + "/";
                    if (table.getType() == Table.TableType.OLAP) {
                        procString += ((OlapTable) table).getBaseIndexId();
                    } else {
                        procString += table.getId();
                    }

                    try {
                        node.setNode(ProcService.getInstance().open(procString));
                    } catch (AnalysisException e) {
                        throw new SemanticException(String.format("Unknown proc node path: ", procString));
                    }
                } else {
                    if (table.isNativeTable()) {
                        node.setOlapTable(true);
                        OlapTable olapTable = (OlapTable) table;
                        Set<String> bfColumns = olapTable.getCopiedBfColumns();
                        Map<Long, List<Column>> indexIdToSchema = olapTable.getIndexIdToSchema();

                        // indices order
                        List<Long> indices = Lists.newArrayList();
                        indices.add(olapTable.getBaseIndexId());
                        for (Long indexId : indexIdToSchema.keySet()) {
                            if (indexId != olapTable.getBaseIndexId()) {
                                indices.add(indexId);
                            }
                        }

                        // add all indices
                        for (int i = 0; i < indices.size(); ++i) {
                            long indexId = indices.get(i);
                            List<Column> columns = indexIdToSchema.get(indexId);
                            String indexName = olapTable.getIndexNameById(indexId);
                            MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(indexId);
                            for (int j = 0; j < columns.size(); ++j) {
                                Column column = columns.get(j);

                                // Extra string (aggregation and bloom filter)
                                List<String> extras = Lists.newArrayList();
                                if (column.getAggregationType() != null &&
                                        olapTable.getKeysType() != KeysType.PRIMARY_KEYS) {
                                    extras.add(column.getAggregationType().name());
                                }
                                if (bfColumns != null && bfColumns.contains(column.getName())) {
                                    extras.add("BLOOM_FILTER");
                                }
                                String defaultStr = column.getMetaDefaultValue(extras);
                                String extraStr = StringUtils.join(extras, ",");
                                List<String> row = Arrays.asList("",
                                        "",
                                        column.getDisplayName(),
                                        column.getType().canonicalName(),
                                        column.isAllowNull() ? "Yes" : "No",
                                        ((Boolean) column.isKey()).toString(),
                                        defaultStr,
                                        extraStr);

                                if (j == 0) {
                                    row.set(0, indexName);
                                    row.set(1, indexMeta.getKeysType().name());
                                }

                                node.getTotalRows().add(row);
                            } // end for columns

                            if (i != indices.size() - 1) {
                                node.getTotalRows().add(node.EMPTY_ROW);
                            }
                        } // end for indices
                    } else if (table.getType() == Table.TableType.MYSQL) {
                        node.setOlapTable(false);
                        MysqlTable mysqlTable = (MysqlTable) table;
                        List<String> row = Arrays.asList(mysqlTable.getHost(),
                                mysqlTable.getPort(),
                                mysqlTable.getUserName(),
                                mysqlTable.getPasswd(),
                                mysqlTable.getMysqlDatabaseName(),
                                mysqlTable.getMysqlTableName());
                        node.getTotalRows().add(row);
                    } else {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_UNKNOWN_STORAGE_ENGINE, table.getType());
                    }
                }
            } finally {
                db.readUnlock();
            }
            return null;
        }
    }
}
