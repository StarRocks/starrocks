// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.pipe;

import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Database;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Persist the file-list in an OLAP table
 */
public class FileListTableRepo {

    private static final Logger LOG = LogManager.getLogger(FileListTableRepo.class);

    private static final String FILE_LIST_DB_NAME = StatsConstants.STATISTICS_DB_NAME;
    private static final String FILE_LIST_TABLE_NAME = "pipe_file_list";

    private static final String FILE_LIST_TABLE_CREATE =
            "CREATE TABLE IF NOT EXISTS %s" +
                    "(" +
                    "pipe_id bigint, " +
                    "filename string, " +
                    "file_version int, " +
                    "file_size bigint, " +
                    "state string, " +
                    "last_modified  datetime, " +
                    "staged_time datetime, " +
                    "start_load datetime, " +
                    "finish_load datetime" +
                    ")" +
                    "PRIMARY KEY() " +
                    "DISTRIBUTED BY HASH() BUCKETS 8 " +
                    "properties('replication_num' = '%d') ";

    private static final String CORRECT_FILE_LIST_REPLICATION_NUM =
            "ALTER TABLE %s SET ('replication_num'='3')";

    /**
     * Execute SQL
     */
    static class RepoExecutor {

        public static List<TResultBatch> executeDQL(ConnectContext context, String sql) {
            try {
                // TODO: use json sink protocol, instead of statistic protocol
                StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
                ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, TResultSinkType.STATISTIC);
                StmtExecutor executor = new StmtExecutor(context, parsedStmt);
                context.setExecutor(executor);
                context.setQueryId(UUIDUtil.genUUID());
                Pair<List<TResultBatch>, Status> sqlResult = executor.executeStmtWithExecPlan(context, execPlan);
                if (!sqlResult.second.ok()) {
                    throw new SemanticException("execute sql failed with status: " + sqlResult.second.getErrorMsg());
                }
                return sqlResult.first;
            } catch (Exception e) {
                throw new SemanticException("execute sql failed with exception", e);
            }
        }

        public static void executeDDL(ConnectContext context, String sql) {

        }

    }

    /**
     * Create the database and table
     */
    public static class RepoCreator {

        private static final RepoCreator INSTANCE = new RepoCreator();

        private static boolean databaseExists = false;
        private boolean tableExists = false;
        private boolean tableCorrected = false;

        public static RepoCreator getInstance() {
            return INSTANCE;
        }

        public void run() {
            try {
                if (!databaseExists) {
                    databaseExists = checkDatabaseExists();
                    if (!databaseExists) {
                        LOG.warn("database not exists: " + FILE_LIST_DB_NAME);
                        return;
                    }
                }
                if (!tableExists) {
                    createTable();
                    LOG.info("table created: " + FILE_LIST_TABLE_NAME);
                    tableExists = true;
                }
                if (!tableCorrected) {
                    correctTable();
                    LOG.info("table corrected: " + FILE_LIST_TABLE_NAME);
                    tableCorrected = true;
                }
            } catch (Exception e) {
                LOG.error("error happens in RepoCreator: ", e);
            }
        }

        public boolean checkDatabaseExists() {
            return GlobalStateMgr.getCurrentState().getDb(FILE_LIST_DB_NAME) != null;
        }

        public static boolean checkTableExists() {
            Database db = GlobalStateMgr.getCurrentState().getDb(FILE_LIST_DB_NAME);
            if (db == null) {
                return false;
            }
            return db.getTable(FILE_LIST_TABLE_NAME) != null;
        }

        public static void createTable() {
            String sql = SQLBuilder.buildCreateTableSql();
            ConnectContext context = StatisticUtils.buildConnectContext();
            RepoExecutor.executeDDL(context, sql);
        }

        public static void correctTable() {
            String sql = SQLBuilder.buildAlterTableSql();
            ConnectContext context = StatisticUtils.buildConnectContext();
            RepoExecutor.executeDDL(context, sql);
        }

    }

    /**
     * Generate SQL for operations
     */
    static class SQLBuilder {

        public static String buildCreateTableSql() {
            int replica = Math.min(3, GlobalStateMgr.getCurrentSystemInfo().getTotalBackendNumber());
            return String.format(FILE_LIST_TABLE_CREATE,
                    CatalogUtils.normalizeTableName(FILE_LIST_DB_NAME, FILE_LIST_TABLE_NAME), replica);
        }

        public static String buildAlterTableSql() {
            return String.format(CORRECT_FILE_LIST_REPLICATION_NUM,
                    CatalogUtils.normalizeTableName(FILE_LIST_DB_NAME, FILE_LIST_TABLE_NAME));
        }

        public static String buildInsertSql(List<PipeFile> files) {
            return "";
        }

        public static String buildUpdateStateSql(List<PipeFile> files, FileListRepo.PipeFileState state) {
            return "";
        }

        public static String buildDeleteFileSql(List<PipeFile> files) {
            return "";
        }
    }

}