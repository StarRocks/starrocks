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

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.DateUtils;
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
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
            // TODO: add md5/etag into the file list
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

    private static final String UPDATE_FILES_STATE =
            "UPDATE " + FILE_LIST_FULL_NAME +
                    " SET state = %s WHERE ";

    private static final String FILE_LOCATOR = "(pipe_id = %s AND file_name = %s AND file_version = %s)";

    private static final String INSERT_FILES =
            "INSERT INTO " + FILE_LIST_FULL_NAME + " VALUES ";

    private static final String SELECTED_STAGED_FILES =
            "SELECT " + ALL_COLUMNS + " FROM " + FILE_LIST_FULL_NAME + " WHERE ";

    private static final String FILE_RECORD_VALUES = "(%d, %s, %s, %d, %s, %s, %s, %s, %s)";

    private static final String DELETE_BY_PIPE = "DELETE FROM " + FILE_LIST_FULL_NAME + " WHERE pipe_id = %d";

    @Override
    public List<PipeFile> listUnloadedFiles() {
        List<PipeFileRecord> records = RepoAccessor.getInstance().listUnloadedFiles(pipeId.getId());
        return records.stream().map(PipeFileRecord::toPipeFile).collect(Collectors.toList());
    }

    @Override
    public void addFiles(List<TBrokerFileStatus> files) {
        List<PipeFileRecord> records = new ArrayList<>();
        for (TBrokerFileStatus file : files) {
            PipeFileRecord record = PipeFileRecord.fromRawFile(file);
            record.pipeId = pipeId.getId();
            records.add(record);
        }
        List<PipeFileRecord> stagedFiles = RepoAccessor.getInstance().selectStagedFiles(records);
        List<PipeFileRecord> newFiles = ListUtils.subtract(records, stagedFiles);
        if (CollectionUtils.isEmpty(newFiles)) {
            return;
        }
        RepoAccessor.getInstance().addFiles(records);
        LOG.info("add files into file-list, pipe={}, alreadyStagedFile={}, newFiles={}", pipeId, stagedFiles, newFiles);
    }

    @Override
    public void updateFileState(List<PipeFile> files, PipeFileState state) {
        List<PipeFileRecord> records = new ArrayList<>();
        for (PipeFile file : files) {
            PipeFileRecord record = PipeFileRecord.fromFile(file);
            record.pipeId = pipeId.getId();
            records.add(record);
        }
        RepoAccessor.getInstance().updateFilesState(records, state);
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
     * Record stored in the pipe_files table
     */
    public static class PipeFileRecord {
        public long pipeId;
        public String fileName;
        public String fileVersion;
        public long fileSize;

        public PipeFileState loadState;
        public LocalDateTime lastModified;
        public LocalDateTime stagedTime;
        public LocalDateTime startLoadTime;
        public LocalDateTime finishLoadTime;

        public static PipeFileRecord fromFile(PipeFile file) {
            PipeFileRecord record = new PipeFileRecord();
            record.fileName = file.path;
            record.fileVersion = "";
            return record;
        }

        public static PipeFileRecord fromRawFile(TBrokerFileStatus file) {
            PipeFileRecord record = new PipeFileRecord();
            record.fileName = file.getPath();
            record.fileSize = file.getSize();
            record.stagedTime = LocalDateTime.now();
            record.loadState = PipeFileState.UNLOADED;
            return record;
        }

        public static List<PipeFileRecord> fromResultBatch(List<TResultBatch> batches) {
            List<PipeFileRecord> res = new ArrayList<>();
            for (TResultBatch batch : ListUtils.emptyIfNull(batches)) {
                for (ByteBuffer buffer : batch.getRows()) {
                    ByteBuf copied = Unpooled.copiedBuffer(buffer);
                    String jsonString = copied.toString(Charset.defaultCharset());
                    res.add(PipeFileRecord.fromJson(jsonString));
                }
            }
            return res;
        }

        /**
         * The json should come from the HTTP/JSON protocol, which looks like {"data": [col1, col2, col3]}
         */
        public static PipeFileRecord fromJson(String json) {
            try {
                JsonElement object = JsonParser.parseString(json);
                JsonArray dataArray = object.getAsJsonObject().get("data").getAsJsonArray();

                PipeFileRecord file = new PipeFileRecord();
                file.pipeId = dataArray.get(0).getAsLong();
                file.fileName = dataArray.get(1).getAsString();
                file.fileVersion = dataArray.get(2).getAsString();
                file.fileSize = dataArray.get(3).getAsLong();
                file.loadState = EnumUtils.getEnumIgnoreCase(PipeFileState.class, dataArray.get(4).getAsString());
                file.lastModified = parseJsonDateTime(dataArray.get(5));
                file.stagedTime = parseJsonDateTime(dataArray.get(6));
                file.startLoadTime = parseJsonDateTime(dataArray.get(7));
                file.finishLoadTime = parseJsonDateTime(dataArray.get(8));
                return file;
            } catch (Exception e) {
                throw new RuntimeException("convert json to PipeFile failed due to malformed json data: " + json, e);
            }
        }

        /**
         * Usually datetime in JSON should be string. but null datetime is a JSON null instead of empty string
         */
        public static LocalDateTime parseJsonDateTime(JsonElement json) throws AnalysisException {
            if (json.isJsonNull()) {
                return null;
            }
            String str = json.getAsString();
            if (StringUtils.isEmpty(str)) {
                return null;
            }
            return DateUtils.parseDatTimeString(str);
        }

        public static String toSQLString(LocalDateTime dt) {
            if (dt == null) {
                return "NULL";
            }
            return Strings.quote(DateUtils.formatDateTimeUnix(dt));
        }

        public static String toSQLString(String str) {
            if (str == null) {
                return "NULL";
            } else {
                return Strings.quote(str);
            }
        }

        public static String toSQLStringNonnull(String str) {
            if (str == null) {
                return "''";
            } else {
                return Strings.quote(str);
            }
        }

        public String toValueList() {
            return String.format(FILE_RECORD_VALUES,
                    pipeId,
                    toSQLString(fileName),
                    toSQLStringNonnull(fileVersion),
                    fileSize,
                    toSQLString(loadState.toString()),
                    toSQLString(lastModified),
                    toSQLString(stagedTime),
                    toSQLString(startLoadTime),
                    toSQLString(finishLoadTime)
            );
        }

        public String toUniqueLocator() {
            return String.format(FILE_LOCATOR,
                    pipeId, Strings.quote(fileName), Strings.quote(fileVersion));
        }

        public PipeFile toPipeFile() {
            PipeFile file = new PipeFile();
            file.path = fileName;
            file.state = loadState;
            file.size = fileSize;
            return file;
        }

        @Override
        public String toString() {
            return "PipeFileRecord{" +
                    "pipeId=" + pipeId +
                    ", fileName='" + fileName + '\'' +
                    ", fileVersion='" + fileVersion + '\'' +
                    ", fileSize=" + fileSize +
                    ", loadState=" + loadState +
                    ", lastModified=" + lastModified +
                    ", stagedTime=" + stagedTime +
                    ", startLoadTime=" + startLoadTime +
                    ", finishLoadTime=" + finishLoadTime +
                    '}';
        }
    }

    /**
     * Query the repo
     */
    public static class RepoAccessor {

        private final static RepoAccessor INSTANCE = new RepoAccessor();

        public static RepoAccessor getInstance() {
            return INSTANCE;
        }

        public List<PipeFileRecord> listAllFiles() {
            try {
                List<TResultBatch> batch = RepoExecutor.executeDQL(SELECT_FILES);
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
                List<TResultBatch> batch = RepoExecutor.executeDQL(sql);
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
                List<TResultBatch> batch = RepoExecutor.executeDQL(sql);
                return PipeFileRecord.fromResultBatch(batch);
            } catch (Exception e) {
                LOG.error("selectStagedFiles failed", e);
                throw e;
            }
        }

        public void addFiles(List<PipeFileRecord> records) {
            try {
                String sql = buildSqlAddFiles(records);
                RepoExecutor.executeDML(sql);
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
                String sql = buildSqlUpdateFilesState(records, state);
                RepoExecutor.executeDML(sql);
                LOG.info("update files state to {}: {}", state, records);
            } catch (Exception e) {
                LOG.error("update files state failed: {}", records, e);
                throw e;
            }
        }

        public void deleteByPipe(long pipeId) {
            try {
                String sql = String.format(DELETE_BY_PIPE, pipeId);
                RepoExecutor.executeDML(sql);
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

        protected String buildSqlUpdateFilesState(List<PipeFileRecord> records, PipeFileState state) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format(UPDATE_FILES_STATE, Strings.quote(state.toString())));
            sb.append(records.stream().map(PipeFileRecord::toUniqueLocator).collect(Collectors.joining(" OR ")));
            return sb.toString();
        }
    }

    /**
     * Execute SQL
     */
    static class RepoExecutor {

        public static void executeDML(String sql) {
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

        public static List<TResultBatch> executeDQL(String sql) {
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
                throw new SemanticException("execute sql failed with exception", e);
            } finally {
                ConnectContext.remove();
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
    }

}