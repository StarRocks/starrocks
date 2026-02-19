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
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.TableName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.SchemaConstants;
import com.starrocks.common.proc.ExternalTableProcDir;
import com.starrocks.common.proc.TableProcDir;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.DescribeStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.OrderByPair;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.ShowAnalyzeJobStmt;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.sql.ast.ShowBasicStatsMetaStmt;
import com.starrocks.sql.ast.ShowColumnStmt;
import com.starrocks.sql.ast.ShowCreateExternalCatalogStmt;
import com.starrocks.sql.ast.ShowCreateRoutineLoadStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.ShowDataDistributionStmt;
import com.starrocks.sql.ast.ShowDataStmt;
import com.starrocks.sql.ast.ShowDbStmt;
import com.starrocks.sql.ast.ShowDeleteStmt;
import com.starrocks.sql.ast.ShowDynamicPartitionStmt;
import com.starrocks.sql.ast.ShowFunctionsStmt;
import com.starrocks.sql.ast.ShowHistogramStatsMetaStmt;
import com.starrocks.sql.ast.ShowIndexStmt;
import com.starrocks.sql.ast.ShowLoadStmt;
import com.starrocks.sql.ast.ShowLoadWarningsStmt;
import com.starrocks.sql.ast.ShowMaterializedViewsStmt;
import com.starrocks.sql.ast.ShowMultiColumnStatsMetaStmt;
import com.starrocks.sql.ast.ShowPartitionsStmt;
import com.starrocks.sql.ast.ShowProcStmt;
import com.starrocks.sql.ast.ShowRoutineLoadStmt;
import com.starrocks.sql.ast.ShowRoutineLoadTaskStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.ShowStreamLoadStmt;
import com.starrocks.sql.ast.ShowTableStatusStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.ShowTabletStmt;
import com.starrocks.sql.ast.ShowTemporaryTableStmt;
import com.starrocks.sql.ast.ShowTransactionStmt;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprCastFunction;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.LikePredicate;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.spm.ShowBaselinePlanStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.type.DateType;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.ErrorCode.ERR_UNSUPPORTED_SQL_PATTERN;

public class ShowStmtAnalyzer {

    public static void analyze(ShowStmt stmt, ConnectContext session) {
        new ShowStmtAnalyzerVisitor().analyze(stmt, session);
    }

    static class ShowStmtAnalyzerVisitor implements AstVisitorExtendInterface<Void, ConnectContext> {

        private static final Logger LOGGER = LoggerFactory.getLogger(ShowStmtAnalyzerVisitor.class);

        public void analyze(ShowStmt statement, ConnectContext session) {
            analyzeShowPredicateClause(statement, session);
            analyzeShowPredicate(statement);
            visit(statement, session);
        }

        @Override
        public Void visitShowTableStatement(ShowTableStmt node, ConnectContext context) {
            String catalogName;
            if (node.getCatalogName() != null) {
                catalogName = node.getCatalogName();
            } else {
                catalogName = context.getCurrentCatalog();
            }

            if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
            }
            String db = node.getDb();
            db = getDatabaseName(db, context);
            node.setDb(db);
            return null;
        }

        @Override
        public Void visitShowTemporaryTablesStatement(ShowTemporaryTableStmt node, ConnectContext context) {
            String catalogName;
            if (node.getCatalogName() != null) {
                catalogName = node.getCatalogName();
            } else {
                catalogName = context.getCurrentCatalog();
            }
            if (!CatalogMgr.isInternalCatalog(catalogName)) {
                throw new SemanticException("show temporary table is not supported under non-default catalog");
            }
            return visitShowTableStatement(node, context);
        }

        @Override
        public Void visitShowTabletStatement(ShowTabletStmt node, ConnectContext context) {
            ShowTabletStmtAnalyzer.analyze(node, context);
            return null;
        }

        @Override
        public Void visitShowColumnStatement(ShowColumnStmt node, ConnectContext context) {
            node.setTableRef(AnalyzerUtils.normalizedTableRef(node.getTableRef(), context));
            return null;
        }

        @Override
        public Void visitShowTableStatusStatement(ShowTableStatusStmt node, ConnectContext context) {
            String db = node.getDb();
            db = getDatabaseName(db, context);
            node.setDb(db);
            return null;
        }

        @Override
        public Void visitShowFunctionsStatement(ShowFunctionsStmt node, ConnectContext context) {
            if (!node.getIsGlobal() && !node.getIsBuiltin()) {
                String dbName = node.getDbName();
                if (Strings.isNullOrEmpty(dbName)) {
                    dbName = context.getDatabase();
                    if (Strings.isNullOrEmpty(dbName)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                    }
                }
                node.setDbName(dbName);
            }

            if (node.getExpr() != null) {
                ErrorReport.reportSemanticException(ERR_UNSUPPORTED_SQL_PATTERN);
            }
            return null;
        }

        @Override
        public Void visitShowMaterializedViewStatement(ShowMaterializedViewsStmt node, ConnectContext context) {
            String db = node.getDb();
            db = getDatabaseName(db, context);
            node.setDb(db);
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
        public Void visitShowCreateTableStatement(ShowCreateTableStmt node, ConnectContext context) {
            if (node.getTableRef() == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_TABLES_USED);
            }
            node.setTableRef(AnalyzerUtils.normalizedTableRef(node.getTableRef(), context));
            return null;
        }

        @Override
        public Void visitShowDatabasesStatement(ShowDbStmt node, ConnectContext context) {
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
        public Void visitShowRoutineLoadStatement(ShowRoutineLoadStmt node, ConnectContext context) {
            String dbName = node.getDbFullName();
            dbName = getDatabaseName(dbName, context);
            node.setDb(dbName);
            return null;
        }

        @Override
        public Void visitShowCreateRoutineLoadStatement(ShowCreateRoutineLoadStmt node, ConnectContext context) {
            String dbName = node.getDbFullName();
            dbName = getDatabaseName(dbName, context);
            node.setDb(dbName);
            return null;
        }

        @Override
        public Void visitShowRoutineLoadTaskStatement(ShowRoutineLoadTaskStmt node, ConnectContext context) {
            String dbName = node.getDbFullName();
            dbName = getDatabaseName(dbName, context);
            node.setDbFullName(dbName);
            try {
                node.checkJobNameExpr();
            } catch (AnalysisException e) {
                LOGGER.error("analysis show routine load task error:", e);
                throw new SemanticException("analysis show routine load task error: %s", e.getMessage());
            }
            return null;
        }

        @Override
        public Void visitShowStreamLoadStatement(ShowStreamLoadStmt node, ConnectContext context) {
            String dbName = node.getDbFullName();
            dbName = getDatabaseName(dbName, context);
            node.setDb(dbName);
            return null;
        }

        @Override
        public Void visitShowAlterStatement(ShowAlterStmt statement, ConnectContext context) {
            ShowAlterStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowDeleteStatement(ShowDeleteStmt node, ConnectContext context) {
            String dbName = node.getDbName();
            dbName = getDatabaseName(dbName, context);
            node.setDbName(dbName);
            return null;
        }

        @Override
        public Void visitShowDynamicPartitionStatement(ShowDynamicPartitionStmt node, ConnectContext context) {
            String dbName = node.getDb();
            dbName = getDatabaseName(dbName, context);
            node.setDb(dbName);
            return null;
        }

        @Override
        public Void visitShowIndexStatement(ShowIndexStmt node, ConnectContext context) {
            node.setTableRef(AnalyzerUtils.normalizedTableRef(node.getTableRef(), context));
            return null;
        }

        @Override
        public Void visitShowTransactionStatement(ShowTransactionStmt statement, ConnectContext context) {
            ShowTransactionStmtAnalyzer.analyze(statement, context);
            return null;
        }

        String getDatabaseName(String db, ConnectContext session) {
            if (Strings.isNullOrEmpty(db)) {
                db = session.getDatabase();
                if (Strings.isNullOrEmpty(db)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
            }
            return db;
        }

        @Override
        public Void visitShowDataStatement(ShowDataStmt node, ConnectContext context) {
            String dbName = node.getDbName();
            dbName = getDatabaseName(dbName, context);
            node.setDbName(dbName);
            return null;
        }

        @Override
        public Void visitShowDataDistributionStatement(ShowDataDistributionStmt node, ConnectContext context) {
            String db = context.getDatabase();
            if (Strings.isNullOrEmpty(db)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
            return null;
        }

        @Override
        public Void visitDescTableStmt(DescribeStmt node, ConnectContext context) {
            if (node.isTableFunctionTable()) {
                descTableFunctionTable(node, context);
                return null;
            }

            TableRef tableRef = node.getTableRef();
            if (tableRef == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_TABLES_USED);
            }
            tableRef = AnalyzerUtils.normalizedTableRef(tableRef, context);
            node.setTableRef(tableRef);
            String catalogName = tableRef.getCatalogName();
            String dbName = tableRef.getDbName();
            String tbl = tableRef.getTableName();
            if (catalogName == null) {
                catalogName = context.getCurrentCatalog();
            }

            CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();

            if (!catalogMgr.catalogExists(catalogName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
            }

            if (CatalogMgr.isInternalCatalog(catalogName)) {
                descInternalCatalogTable(node, context);
            } else {
                descExternalCatalogTable(node, catalogName, dbName, tbl);
            }
            return null;
        }

        private void descTableFunctionTable(DescribeStmt node, ConnectContext context) {
            Table table = null;
            try {
                table = new TableFunctionTable(node.getTableFunctionProperties());
            } catch (DdlException e) {
                throw new StorageAccessException(e);
            }

            List<Column> columns = table.getFullSchema();
            for (Column column : columns) {
                List<String> row = Arrays.asList(
                        column.getName(),
                        column.getType().canonicalName().toLowerCase(),
                        column.isAllowNull() ? SchemaConstants.YES : SchemaConstants.NO);
                node.getTotalRows().add(row);
            }
        }

        private void descInternalCatalogTable(DescribeStmt node, ConnectContext context) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(node.getDb());
            if (db == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, node.getDb());
            }
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                Table table = null;
                try {
                    TableName tableName = new TableName(node.getCatalogName(), node.getDb(), node.getTableName());
                    table = MetaUtils.getSessionAwareTable(context, db, tableName);
                } catch (Exception e) {
                    // if table is not found, may be is statement "desc materialized-view-name",
                    // ignore this exception.
                }
                //if getTable not find table, may be is statement "desc materialized-view-name"
                if (table == null) {
                    for (Table tb : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                        if (tb.getType() == Table.TableType.OLAP) {
                            OlapTable olapTable = (OlapTable) tb;
                            for (MaterializedIndexMeta mvMeta : olapTable.getVisibleIndexMetas()) {
                                if (olapTable.getIndexNameByMetaId(mvMeta.getIndexMetaId())
                                        .equalsIgnoreCase(node.getTableName())) {
                                    List<Column> columns = olapTable.getSchemaByIndexMetaId(mvMeta.getIndexMetaId());
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
                                                column.getName(),
                                                // In Mysql, the Type column should lowercase, and the Null column should uppercase.
                                                // If you do not follow this specification, it may cause the BI system,
                                                // such as superset, to fail to recognize the column type.
                                                column.getType().canonicalName().toLowerCase(),
                                                column.isAllowNull() ? SchemaConstants.YES : SchemaConstants.NO,
                                                ((Boolean) column.isKey()).toString(),
                                                defaultStr,
                                                extraStr);
                                        node.getTotalRows().add(row);
                                    }
                                    node.setMaterializedView(true);
                                    return;
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
                        procString += ((OlapTable) table).getBaseIndexMetaId();
                    } else {
                        procString += table.getId();
                    }

                    node.setProcPath(procString);
                } else {
                    if (table.isNativeTableOrMaterializedView()) {
                        node.setOlapTable(true);
                        OlapTable olapTable = (OlapTable) table;
                        Set<String> bfColumns = olapTable.getBfColumnNames();
                        Map<Long, List<Column>> indexMetaIdToSchema = olapTable.getIndexMetaIdToSchema();

                        // indices order
                        List<Long> indices = Lists.newArrayList();
                        indices.add(olapTable.getBaseIndexMetaId());
                        for (Long indexMetaId : indexMetaIdToSchema.keySet()) {
                            if (indexMetaId != olapTable.getBaseIndexMetaId()) {
                                indices.add(indexMetaId);
                            }
                        }

                        // add all indices
                        for (int i = 0; i < indices.size(); ++i) {
                            long indexMetaId = indices.get(i);
                            List<Column> columns = indexMetaIdToSchema.get(indexMetaId);
                            String indexName = olapTable.getIndexNameByMetaId(indexMetaId);
                            MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByMetaId(indexMetaId);
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
                                        column.getName(),
                                        // In Mysql, the Type column should lowercase, and the Null column should uppercase.
                                        // If you do not follow this specification, it may cause the BI system,
                                        // such as superset, to fail to recognize the column type.
                                        column.getType().canonicalName().toLowerCase(),
                                        column.isAllowNull() ? SchemaConstants.YES : SchemaConstants.NO,
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
                                mysqlTable.getCatalogDBName(),
                                mysqlTable.getCatalogTableName());
                        node.getTotalRows().add(row);
                    } else {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_UNKNOWN_STORAGE_ENGINE, table.getType());
                    }
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        }

        private void descExternalCatalogTable(DescribeStmt node, String catalogName, String dbName, String tbl) {
            // show external table schema only
            String procString =
                    "/catalog/" + catalogName + "/" + dbName + "/" + tbl + "/" + ExternalTableProcDir.SCHEMA;
            node.setProcPath(procString);
        }

        @Override
        public Void visitShowProcStmt(ShowProcStmt node, ConnectContext context) {
            String path = node.getPath();
            if (Strings.isNullOrEmpty(path)) {
                throw new SemanticException("Path is null");
            }
            return null;
        }

        private void analyzeShowPredicate(ShowStmt showStmt) {
            Expr predicate = showStmt.getPredicate();
            if (!showStmt.isSelfPredicate() || predicate == null) {
                return;
            }

            List<Expr> exprs = AnalyzerUtils.extractConjuncts(predicate);
            for (Expr expr : exprs) {
                if (!(expr instanceof BinaryPredicate && ((BinaryPredicate) expr).getOp().isEquivalence()) &&
                        !(expr instanceof LikePredicate)) {
                    throw new SemanticException(
                            "Invalid predicate in SHOW statement. Only '=' and 'LIKE' operators are supported. " +
                                    "Found: '" + ExprToSql.toSql(expr) + "'");
                }

                if (!(expr.getChild(0) instanceof SlotRef)) {
                    throw new SemanticException(
                            "Invalid left operator in predicate '" + ExprToSql.toSql(expr) + "'. " +
                                    "Left side must be a column reference");
                }

                if (!(expr.getChild(1) instanceof StringLiteral)) {
                    throw new SemanticException(
                            "Invalid right operator in predicate '" + ExprToSql.toSql(expr) + "'. " +
                                    "Right side must be a string literal. " +
                                    "Example: column = 'value' or column LIKE 'pattern%'");
                }
            }
        }

        private void analyzeShowPredicateClause(ShowStmt showStmt, ConnectContext context) {
            Expr predicate = showStmt.getPredicate();
            if (showStmt.isSelfPredicate() || predicate == null) {
                return;
            }

            // check where columns
            ShowResultMetaFactory factory = new ShowResultMetaFactory();
            ShowResultSetMetaData meta = factory.getMetadata(showStmt);
            ExpressionAnalyzer.analyzeExpressionResolveSlot(showStmt.getPredicate(), context, slotRef -> {
                int index = meta.getColumnIdx(slotRef.getColumnName().toLowerCase());
                slotRef.setType(meta.getColumn(index).getType());
            });

            // check order by columns
            List<OrderByElement> orderByElements = showStmt.getOrderByElements();
            if (!showStmt.isSelfOrderBy() && orderByElements != null) {
                List<OrderByPair> orderByPairs = new ArrayList<>();
                for (OrderByElement orderByElement : orderByElements) {
                    if (!(orderByElement.getExpr() instanceof SlotRef)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Show stmt only support order by column");
                    }
                    SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                    int index = meta.getColumnIdx(slotRef.getColumnName().toLowerCase());
                    orderByPairs.add(new OrderByPair(index, orderByElement.getIsAsc()));
                }
                showStmt.setOrderByPairs(orderByPairs);
            }
            // check limit
            if (!showStmt.isSelfLimit() && showStmt.getLimitElement() != null) {
                if (!showStmt.getLimitElement().getLimitExpr().isConstant()) {
                    throw new SemanticException("Limit expr must be constant");
                }
                if (!showStmt.getLimitElement().getOffsetExpr().isConstant()) {
                    throw new SemanticException("Offset expr must be constant");
                }
            }
        }


        @Override
        public Void visitShowPartitionsStatement(ShowPartitionsStmt statement, ConnectContext context) {
            TableRef tableRef = AnalyzerUtils.normalizedTableRef(statement.getTableRef(), context);
            statement.setTableRef(tableRef);
            String catalogName = tableRef.getCatalogName();
            String dbName = tableRef.getDbName();
            final Map<String, Expr> filterMap = statement.getFilterMap();
            if (statement.getWhereClause() != null) {
                analyzeSubPredicate(filterMap, statement.getWhereClause());
            }
            Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, catalogName, dbName);
            if (db == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
            }

            final String tableName = tableRef.getTableName();
            final boolean isTempPartition = statement.isTempPartition();
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                Table table =
                        MetaUtils.getSessionAwareTable(context, db, new TableName(catalogName, dbName, tableName));
                if (!(table instanceof OlapTable) && !(table instanceof PaimonTable)) {
                    throw new SemanticException("Table[" + tableName + "] does not exists or is not OLAP/Paimon table");
                }
                // build proc path
                StringBuilder stringBuilder = new StringBuilder();
                if (table instanceof OlapTable) {
                    stringBuilder.append("/dbs/");
                    stringBuilder.append(db.getId());
                    stringBuilder.append("/").append(table.getId());
                    if (isTempPartition) {
                        stringBuilder.append("/temp_partitions");
                    } else {
                        stringBuilder.append("/partitions");
                    }
                } else if (table instanceof PaimonTable) {
                    stringBuilder.append("/catalog/");
                    stringBuilder.append(catalogName);
                    stringBuilder.append("/").append(dbName);
                    stringBuilder.append("/").append(tableName);
                    stringBuilder.append("/partitions");
                }

                LOGGER.debug("process SHOW PROC '{}';", stringBuilder);

                statement.setProcPath(stringBuilder.toString());
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
            return null;
        }

        private void analyzeSubPredicate(Map<String, Expr> filterMap, Expr subExpr) {
            if (subExpr == null) {
                return;
            }
            if (subExpr instanceof CompoundPredicate cp) {
                if (cp.getOp() != CompoundPredicate.Operator.AND) {
                    throw new SemanticException("Only allow compound predicate with operator AND");
                }
                analyzeSubPredicate(filterMap, cp.getChild(0));
                analyzeSubPredicate(filterMap, cp.getChild(1));
                return;
            }

            if (!(subExpr.getChild(0) instanceof SlotRef)) {
                throw new SemanticException("Show filter by column");
            }

            String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName();
            boolean filter = leftKey.equalsIgnoreCase(ShowPartitionsStmt.FILTER_PARTITION_NAME) ||
                    leftKey.equalsIgnoreCase(ShowPartitionsStmt.FILTER_STATE);
            if (subExpr instanceof BinaryPredicate) {
                binaryPredicateHandler(subExpr, leftKey, filter);
            } else if (subExpr instanceof LikePredicate) {
                likePredicateHandler((LikePredicate) subExpr, filter);
            } else {
                throw new SemanticException("Only operator =|>=|<=|>|<|!=|like are supported.");
            }
            filterMap.put(leftKey.toLowerCase(), subExpr);
        }

        private void likePredicateHandler(LikePredicate subExpr, boolean filter) {
            if (filter && subExpr.getOp() != LikePredicate.Operator.LIKE) {
                throw new SemanticException("Where clause : PartitionName|State like \"p20191012|NORMAL\"");
            }
            if (!filter) {
                throw new SemanticException("Where clause : PartitionName|State like \"p20191012|NORMAL\"");
            }
        }

        private void binaryPredicateHandler(Expr subExpr, String leftKey, boolean filter) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
            if (filter && binaryPredicate.getOp() != BinaryType.EQ) {
                throw new SemanticException(String.format("Only operator =|like are supported for %s", leftKey));
            }
            if (leftKey.equalsIgnoreCase(ShowPartitionsStmt.FILTER_LAST_CONSISTENCY_CHECK_TIME)) {
                if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                    throw new SemanticException("Where clause : LastConsistencyCheckTime =|>=|<=|>|<|!= "
                            + "\"2019-12-22|2019-12-22 22:22:00\"");
                }
                try {
                    subExpr.setChild(1,
                            ExprCastFunction.castTo(subExpr.getChild(1), DateType.DATETIME));
                } catch (AnalysisException e) {
                    throw new SemanticException("expression %s cast to datetime error: %s",
                            subExpr.getChild(1).toString(), e.getMessage());
                }
            } else if (ShowPartitionsStmt.FILTER_COLUMNS.stream()
                    .noneMatch(column -> column.equalsIgnoreCase(leftKey))) {
                throw new SemanticException("Only the columns of PartitionId/PartitionName/" +
                        "State/Buckets/ReplicationNum/LastConsistencyCheckTime are supported.");
            }
        }

        public Void visitShowLoadStatement(ShowLoadStmt statement, ConnectContext context) {
            ShowLoadStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowLoadWarningsStatement(ShowLoadWarningsStmt statement, ConnectContext context) {
            ShowLoadWarningsStmtAnalyzer.analyze(statement, context);
            return null;
        }

        @Override
        public Void visitShowCreateExternalCatalogStatement(ShowCreateExternalCatalogStmt node, ConnectContext context) {
            String catalogName = node.getCatalogName();
            if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_CATALOG_ERROR, catalogName);
            }
            return null;
        }

        @Override
        public Void visitShowBasicStatsMetaStatement(ShowBasicStatsMetaStmt node, ConnectContext context) {
            analyzeOrderByItems(node);
            return null;
        }

        @Override
        public Void visitShowMultiColumnsStatsMetaStatement(ShowMultiColumnStatsMetaStmt node, ConnectContext context) {
            analyzeOrderByItems(node);
            return null;
        }

        @Override
        public Void visitShowHistogramStatsMetaStatement(ShowHistogramStatsMetaStmt node, ConnectContext context) {
            analyzeOrderByItems(node);
            return null;
        }

        @Override
        public Void visitShowAnalyzeStatusStatement(ShowAnalyzeStatusStmt node, ConnectContext context) {
            analyzeOrderByItems(node);
            return null;
        }

        @Override
        public Void visitShowAnalyzeJobStatement(ShowAnalyzeJobStmt node, ConnectContext context) {
            analyzeOrderByItems(node);
            return null;
        }

        @Override
        public Void visitShowBaselinePlanStatement(ShowBaselinePlanStmt statement, ConnectContext context) {
            if (statement.getWhere() != null) {
                // check where columns
                ExpressionAnalyzer.analyzeExpressionResolveSlot(statement.getWhere(), context, slotRef -> {
                    if (!ShowBaselinePlanStmt.BASELINE_FIELD_META.containsKey(slotRef.getColumnName().toLowerCase())) {
                        throw new SemanticException("Where clause : " + slotRef.getColumnName() + " is not supported.");
                    }
                    slotRef.setType(
                            ShowBaselinePlanStmt.BASELINE_FIELD_META.get(slotRef.getColumnName().toLowerCase()));
                });
            }
            return null;
        }

        public void analyzeOrderByItems(ShowStmt node) {
            ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(node);
            List<OrderByElement> orderByElements = node.getOrderByElements();
            if (orderByElements != null && !orderByElements.isEmpty()) {
                List<OrderByPair> orderByPairs = new ArrayList<>();
                for (OrderByElement orderByElement : orderByElements) {
                    if (!(orderByElement.getExpr() instanceof SlotRef)) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Should order by column");
                    }
                    SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                    int index = metaData.getColumnIdx(slotRef.getColumnName());
                    OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                    orderByPairs.add(orderByPair);
                }
                node.setOrderByPairs(orderByPairs);
            }
        }
    }
}
