//  Copyright 2021-present StarRocks, Inc. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.starrocks.load.pipe;

import com.google.common.base.Preconditions;
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
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Persist the file-list in an OLAP table
 */
public class FileListTableRepo extends FileListRepo {

    private static final Logger LOG = LogManager.getLogger(FileListTableRepo.class);

    private static final String FILE_LIST_DB_NAME = StatsConstants.STATISTICS_DB_NAME;
    private static final String FILE_LIST_TABLE_NAME = "pipe_file_list";
    private static final String FILE_LIST_FULL_NAME = FILE_LIST_DB_NAME + "." + FILE_LIST_TABLE_NAME;

    private static final String FILE_LIST_TABLE_CREATE =
            "CREATE TABLE IF NOT EXISTS %s (" +
                    "pipe_id bigint, " +
                    "file_name string, " +
                    "file_version string, " +
                    "file_size bigint, " +
                    "state string, " +
                    "last_modified  datetime, " +
                    "staged_time datetime, " +
                    "start_load datetime, " +
                    "finish_load datetime" +
                    " ) PRIMARY KEY(pipe_id, file_name, file_version) " +
                    "DISTRIBUTED BY HASH(pipe_id, file_name) BUCKETS 8 " +
                    "properties('replication_num' = '%d') ";

    private static final String CORRECT_FILE_LIST_REPLICATION_NUM =
            "ALTER TABLE %s SET ('replication_num'='3')";

    private static final String ALL_COLUMNS =
            "pipe_id, file_name, file_version, file_size, state, last_modified, staged_time," +
                    " start_load, finish_load";

    private static final String SELECT_FILES =
            "SELECT " + ALL_COLUMNS + " FROM " + FILE_LIST_FULL_NAME;

    private static final String SELECT_FILES_BY_STATE = SELECT_FILES + " WHERE pipe_id = %d AND state = %s";

    private static final String UPDATE_FILE_STATE =
            "UPDATE " + FILE_LIST_FULL_NAME + " SET state = %s WHERE ";

    private static final String UPDATE_FILE_STATE_START_LOAD =
            "UPDATE " + FILE_LIST_FULL_NAME + " SET state = %s, start_load = now() WHERE ";

    private static final String UPDATE_FILE_STATE_FINISH_LOAD =
            "UPDATE " + FILE_LIST_FULL_NAME + " SET state = %s, finish_load = now() WHERE ";

    private static final String INSERT_FILES =
            "INSERT INTO " + FILE_LIST_FULL_NAME + " VALUES ";

    private static final String SELECTED_STAGED_FILES =
            "SELECT " + ALL_COLUMNS + " FROM " + FILE_LIST_FULL_NAME + " WHERE ";

    private static final String DELETE_BY_PIPE = "DELETE FROM " + FILE_LIST_FULL_NAME + " WHERE pipe_id = %d";

    @Override
    public List<PipeFileRecord> listUnloadedFiles() {
        return RepoAccessor.getInstance().listUnloadedFiles(pipeId.getId());
    }

    @Override
    public void addFiles(List<PipeFileRecord> records) {
        records.forEach(file -> file.pipeId = pipeId.getId());
        List<PipeFileRecord> stagedFiles = RepoAccessor.getInstance().selectStagedFiles(records);
        List<PipeFileRecord> newFiles = ListUtils.subtract(records, stagedFiles);
        if (CollectionUtils.isEmpty(newFiles)) {
            return;
        }
        RepoAccessor.getInstance().addFiles(records);
        LOG.info("add files into file-list, pipe={}, alreadyStagedFile={}, newFiles={}", pipeId, stagedFiles, newFiles);
    }

    @Override
    public void updateFileState(List<PipeFileRecord> files, PipeFileState state) {
        files.forEach(x -> x.pipeId = pipeId.getId());
        RepoAccessor.getInstance().updateFilesState(files, state);
    }

    @Override
    public void cleanup() {
        // TODO
    }

    @Override
    public void destroy() {
        RepoAccessor.getInstance().deleteByPipe(pipeId.getId());
    }

    /**
     * Query the repo
     */
    public static class RepoAccessor {

        private static final RepoAccessor INSTANCE = new RepoAccessor();

        public static RepoAccessor getInstance() {
            return INSTANCE;
        }

        public List<PipeFileRecord> listAllFiles() {
            try {
                List<TResultBatch> batch = RepoExecutor.getInstance().executeDQL(SELECT_FILES);
                return PipeFileRecord.fromResultBatch(batch);
            } catch (Exception e) {
                LOG.error("listAllFiles failed", e);
                throw e;
            }
        }

        public List<PipeFileRecord> listUnloadedFiles(long pipeId) {
            List<PipeFileRecord> res = null;
            try {
                String sql = buildListUnloadedFile(pipeId);
                List<TResultBatch> batch = RepoExecutor.getInstance().executeDQL(sql);
                res = PipeFileRecord.fromResultBatch(batch);
            } catch (Exception e) {
                LOG.error("listUnloadedFiles failed", e);
                throw e;
            }
            return res;
        }

        public List<PipeFileRecord> selectStagedFiles(List<PipeFileRecord> records) {
            try {
                String sql = buildSelectStagedFiles(records);
                List<TResultBatch> batch = RepoExecutor.getInstance().executeDQL(sql);
                return PipeFileRecord.fromResultBatch(batch);
            } catch (Exception e) {
                LOG.error("selectStagedFiles failed", e);
                throw e;
            }
        }

        public void addFiles(List<PipeFileRecord> records) {
            try {
                String sql = buildSqlAddFiles(records);
                RepoExecutor.getInstance().executeDML(sql);
                LOG.info("addFiles into repo: {}", records);
            } catch (Exception e) {
                LOG.error("addFiles {} failed", records, e);
                throw e;
            }
        }

        /**
         * pipe_id, file_name, file_version are required to locate unique file
         */
        public void updateFilesState(List<PipeFileRecord> records, PipeFileState state) {
            try {
                String sql = null;
                switch (state) {
                    case UNLOADED:
                    case ERROR:
                    case SKIPPED:
                        sql = buildSqlUpdateState(records, state);
                        break;
                    case LOADING:
                        sql = buildSqlStartLoad(records, state);
                        break;
                    case LOADED:
                        sql = buildSqlFinishLoad(records, state);
                        break;
                    default:
                        Preconditions.checkState(false, "not supported");
                        break;
                }
                RepoExecutor.getInstance().executeDML(sql);
                LOG.info("update files state to {}: {}", state, records);
            } catch (Exception e) {
                LOG.error("update files state failed: {}", records, e);
                throw e;
            }
        }

        public void deleteByPipe(long pipeId) {
            try {
                String sql = String.format(DELETE_BY_PIPE, pipeId);
                RepoExecutor.getInstance().executeDML(sql);
                LOG.info("delete pipe files {}", pipeId);
            } catch (Exception e) {
                LOG.error("delete file of pipe {} failed", pipeId, e);
                throw e;
            }
        }

        protected String buildSelectStagedFiles(List<PipeFileRecord> files) {
            String where = files.stream().map(PipeFileRecord::toUniqueLocator).collect(Collectors.joining(" OR "));
            return SELECTED_STAGED_FILES + where;
        }

        protected String buildListUnloadedFile(long pipeId) {
            String sql = String.format(SELECT_FILES_BY_STATE,
                    pipeId, Strings.quote(PipeFileState.UNLOADED.toString()));
            return sql;
        }

        protected String buildDeleteByPipe(long pipeId) {
            return String.format(DELETE_BY_PIPE, pipeId);
        }

        protected String buildSqlAddFiles(List<PipeFileRecord> records) {
            StringBuilder sb = new StringBuilder();
            sb.append(INSERT_FILES);
            sb.append(records.stream().map(PipeFileRecord::toValueList).collect(Collectors.joining(",")));
            return sb.toString();
        }

        protected String buildSqlUpdateState(List<PipeFileRecord> records, PipeFileState state) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format(UPDATE_FILE_STATE, Strings.quote(state.toString())));
            sb.append(records.stream().map(PipeFileRecord::toUniqueLocator).collect(Collectors.joining(" OR ")));
            return sb.toString();
        }

        protected String buildSqlStartLoad(List<PipeFileRecord> records, PipeFileState state) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format(UPDATE_FILE_STATE_START_LOAD, Strings.quote(state.toString())));
            sb.append(records.stream().map(PipeFileRecord::toUniqueLocator).collect(Collectors.joining(" OR ")));
            return sb.toString();
        }

        protected String buildSqlFinishLoad(List<PipeFileRecord> records, PipeFileState state) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format(UPDATE_FILE_STATE_FINISH_LOAD, Strings.quote(state.toString())));
            sb.append(records.stream().map(PipeFileRecord::toUniqueLocator).collect(Collectors.joining(" OR ")));
            return sb.toString();
        }
    }

    /**
     * Execute SQL
     */
    static class RepoExecutor {

        private static final RepoExecutor INSTANCE = new RepoExecutor();

        public static RepoExecutor getInstance() {
            return INSTANCE;
        }

        public void executeDML(String sql) {
            try {
                ConnectContext context = StatisticUtils.buildConnectContext();
                context.setThreadLocalInfo();
                StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
                Preconditions.checkState(parsedStmt instanceof DmlStmt, "the statement should be dml");
                DmlStmt dmlStmt = (DmlStmt) parsedStmt;
                ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, TResultSinkType.HTTP_PROTOCAL);
                StmtExecutor executor = new StmtExecutor(context, parsedStmt);
                context.setExecutor(executor);
                context.setQueryId(UUIDUtil.genUUID());
                executor.handleDMLStmt(execPlan, dmlStmt);
            } catch (Exception e) {
                LOG.error("Repo execute SQL failed {}", sql, e);
                throw new SemanticException("execute sql failed with exception", e);
            } finally {
                ConnectContext.remove();
            }
        }

        public List<TResultBatch> executeDQL(String sql) {
            try {
                ConnectContext context = StatisticUtils.buildConnectContext();
                context.setThreadLocalInfo();

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
                throw new SemanticException("execute sql failed: " + sql, e);
            } finally {
                ConnectContext.remove();
            }
        }

        public void executeDDL(String sql) {
            try {
                ConnectContext context = StatisticUtils.buildConnectContext();
                context.setThreadLocalInfo();

                StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
                Analyzer.analyze(parsedStmt, context);
                DDLStmtExecutor.execute(parsedStmt, context);
            } catch (Exception e) {
                LOG.error("execute DDL error: {}", sql, e);
                throw new RuntimeException(e);
            } finally {
                ConnectContext.remove();
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
            RepoExecutor.getInstance().executeDDL(sql);
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
                RepoExecutor.getInstance().executeDDL(sql);
            } else {
                LOG.info("table {} already has {} replicas, no need to alter replication_num",
                        FILE_LIST_FULL_NAME, replica);
            }
        }

        public boolean isDatabaseExists() {
            return databaseExists;
        }

        public boolean isTableExists() {
            return tableExists;
        }

        public boolean isTableCorrected() {
            return tableCorrected;
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
    }

}