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
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Persist the file-list in an OLAP table
 */
public class FileListTableRepo extends FileListRepo {

    private static final Logger LOG = LogManager.getLogger(FileListTableRepo.class);

    private static final String FILE_LIST_DB_NAME = StatsConstants.STATISTICS_DB_NAME;
    private static final String FILE_LIST_TABLE_NAME = "pipe_file_list";
    private static final String FILE_LIST_FULL_NAME = FILE_LIST_DB_NAME + "." + FILE_LIST_TABLE_NAME;

    private static final String FILE_LIST_TABLE_CREATE =
            // TODO: add md5/etag into the file list
            "CREATE TABLE IF NOT EXISTS %s (" +
                    "pipe_id bigint, " +
                    "filename string, " +
                    "file_version int, " +
                    "file_size bigint, " +
                    "state string, " +
                    "last_modified  datetime, " +
                    "staged_time datetime, " +
                    "start_load datetime, " +
                    "finish_load datetime" +
                    " ) PRIMARY KEY(pipe_id, filename, file_version) " +
                    "DISTRIBUTED BY HASH(pipe_id, filename) BUCKETS 8 " +
                    "properties('replication_num' = '%d') ";

    private static final String CORRECT_FILE_LIST_REPLICATION_NUM =
            "ALTER TABLE %s SET ('replication_num'='3')";

    private static final String SELECT_FILES =
            "SELECT pipe_id, filename, file_version, file_size, state, last_modified, staged_time," +
                    " start_load, finish_load" +
                    " FROM " + FILE_LIST_FULL_NAME;

    @Override
    public List<PipeFile> listUnloadedFiles() {
        return null;
    }

    @Override
    public void addFiles(List<TBrokerFileStatus> files) {

    }

    @Override
    public void updateFileState(List<PipeFile> files, PipeFileState state) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void destroy() {

    }

    /**
     * Query the repo
     */
    public static class RepoAccessor {

        private final static RepoAccessor INSTANCE = new RepoAccessor();

        public static RepoAccessor getInstance() {
            return INSTANCE;
        }

        public List<PipeFile> listAllFiles() {
            ConnectContext connect = StatisticUtils.buildConnectContext();
            connect.setThreadLocalInfo();
            List<PipeFile> res = new ArrayList<>();
            try {
                List<TResultBatch> batch = RepoExecutor.executeDQL(connect, SELECT_FILES);
                for (TResultBatch rows : ListUtils.emptyIfNull(batch)) {
                    for (ByteBuffer buffer : rows.getRows()) {
                        ByteBuf copied = Unpooled.copiedBuffer(buffer);
                        String jsonString = copied.toString(Charset.defaultCharset());
                        res.add(PipeFile.fromJson(jsonString));
                    }
                }
            } catch (Exception e) {
                LOG.error("listAllFiles failed", e);
            } finally {
                ConnectContext.remove();
            }
            return res;
        }
    }

    /**
     * Execute SQL
     */
    static class RepoExecutor {

        public static List<TResultBatch> executeDQL(ConnectContext context, String sql) {
            try {
                // TODO: use json sink protocol, instead of statistic protocol
                StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
                ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, TResultSinkType.HTTP_PROTOCAL);
                StmtExecutor executor = new StmtExecutor(context, parsedStmt);
                context.setExecutor(executor);
                context.setQueryId(UUIDUtil.genUUID());
                Pair<List<TResultBatch>, Status> sqlResult = executor.executeStmtWithExecPlan(context, execPlan);
                if (!sqlResult.second.ok()) {
                    throw new SemanticException("execute sql failed with status: " + sqlResult.second.getErrorMsg());
                }
                return sqlResult.first;
            } catch (Exception e) {
                LOG.error("Repo execute SQL failed {}", sql, e);
                throw new SemanticException("execute sql failed with exception", e);
            }
        }

        public static void executeDDL(ConnectContext context, String sql) {
            try {
                StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
                Analyzer.analyze(parsedStmt, context);
                DDLStmtExecutor.execute(parsedStmt, context);
            } catch (Exception e) {
                LOG.error("execute DDL error: {}", sql, e);
                throw new RuntimeException(e);
            }
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

        public static void createTable() {
            String sql = SQLBuilder.buildCreateTableSql();
            ConnectContext context = StatisticUtils.buildConnectContext();
            RepoExecutor.executeDDL(context, sql);
        }

        public static void correctTable() {
            int numBackends = GlobalStateMgr.getCurrentSystemInfo().getTotalBackendNumber();
            int replica = GlobalStateMgr.getCurrentState()
                    .mayGetDb(FILE_LIST_DB_NAME)
                    .flatMap(db -> db.mayGetTable(FILE_LIST_TABLE_NAME))
                    .map(tbl -> ((OlapTable) tbl).getPartitionInfo().getMinReplicationNum())
                    .orElse((short) 1);
            if (numBackends >= 3 && replica < 3) {
                String sql = SQLBuilder.buildAlterTableSql();
                ConnectContext context = StatisticUtils.buildConnectContext();
                RepoExecutor.executeDDL(context, sql);
            } else {
                LOG.info("table {} already has {} replicas, no need to alter replication_num",
                        FILE_LIST_FULL_NAME, replica);
            }
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
            String tableName = CatalogUtils.normalizeTableName(FILE_LIST_DB_NAME, FILE_LIST_TABLE_NAME);
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("INSERT INTO %s VALUES ", tableName));
            for (PipeFile file : files) {
                // FIXME
                sb.append(String.format("(%d,'%s',%d,%d,'%s', '%s','%s','%s','%s' )", null));
            }
            return sb.toString();
        }

        public static String buildUpdateStateSql(List<PipeFile> files, FileListRepo.PipeFileState state) {
            return "";
        }

        public static String buildDeleteFileSql(List<PipeFile> files) {
            return "";
        }
    }

}