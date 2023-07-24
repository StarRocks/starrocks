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
                    " ) PRIMARY KEY(pipe_id, filename, file_version) " +
                    "DISTRIBUTED BY HASH(pipe_id, filename) BUCKETS 8 " +
                    "properties('replication_num' = '%d') ";

    private static final String CORRECT_FILE_LIST_REPLICATION_NUM =
            "ALTER TABLE %s SET ('replication_num'='3')";

    private static final String SELECT_FILES =
            "SELECT pipe_id, file_name, file_version, file_size, state, last_modified, staged_time," +
                    " start_load, finish_load" +
                    " FROM " + FILE_LIST_FULL_NAME;

    private static final String SELECT_FILES_BY_STATE = SELECT_FILES + " WHERE pipe_id = %d AND state = %s";

    private static final String UPDATE_FILES_STATE =
            "UPDATE " + FILE_LIST_FULL_NAME +
                    " SET state = %s " +
                    " WHERE %s";

    private static final String FILE_LOCATOR = "(pipe_id = %s AND file_name = %s AND file_version = %s)";

    private static final String INSERT_FILES =
            "INSERT INTO " + FILE_LIST_FULL_NAME + " VALUES ";

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
        RepoAccessor.getInstance().addFiles(records);
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
                file.lastModified = parseDataTime(dataArray.get(5).getAsString());
                file.stagedTime = parseDataTime(dataArray.get(6).getAsString());
                file.startLoadTime = parseDataTime(dataArray.get(7).getAsString());
                file.finishLoadTime = parseDataTime(dataArray.get(8).getAsString());
                return file;
            } catch (Exception e) {
                throw new RuntimeException("convert json to PipeFile failed due to malformed json data: " + json, e);
            }
        }

        public static LocalDateTime parseDataTime(String str) throws AnalysisException {
            if (StringUtils.isEmpty(str)) {
                return null;
            }
            return DateUtils.parseDatTimeString(str);
        }

        public static String dateTimeString(LocalDateTime dt) {
            if (dt == null) {
                return "";
            }
            return Strings.quote(DateUtils.formatDateTimeUnix(dt));
        }

        public String toValueList() {
            return String.format(FILE_RECORD_VALUES, pipeId,
                    Strings.quote(fileName),
                    Strings.quote(fileVersion),
                    fileSize,
                    Strings.quote(loadState.toString()),
                    dateTimeString(lastModified),
                    dateTimeString(stagedTime),
                    dateTimeString(startLoadTime),
                    dateTimeString(finishLoadTime)
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
            List<PipeFileRecord> res = new ArrayList<>();
            try {
                List<TResultBatch> batch = RepoExecutor.executeDQL(SELECT_FILES);
                for (TResultBatch rows : ListUtils.emptyIfNull(batch)) {
                    for (ByteBuffer buffer : rows.getRows()) {
                        ByteBuf copied = Unpooled.copiedBuffer(buffer);
                        String jsonString = copied.toString(Charset.defaultCharset());
                        res.add(PipeFileRecord.fromJson(jsonString));
                    }
                }
            } catch (Exception e) {
                LOG.error("listAllFiles failed", e);
                throw e;
            }
            return res;
        }

        public List<PipeFileRecord> listUnloadedFiles(long pipeId) {
            List<PipeFileRecord> res = new ArrayList<>();
            try {
                String sql = String.format(SELECT_FILES_BY_STATE,
                        pipeId, Strings.quote(PipeFileState.UNLOADED.toString()));
                List<TResultBatch> batch = RepoExecutor.executeDQL(sql);
                for (TResultBatch rows : ListUtils.emptyIfNull(batch)) {
                    for (ByteBuffer buffer : rows.getRows()) {
                        ByteBuf copied = Unpooled.copiedBuffer(buffer);
                        String jsonString = copied.toString(Charset.defaultCharset());
                        res.add(PipeFileRecord.fromJson(jsonString));
                    }
                }
            } catch (Exception e) {
                LOG.error("listUnloadedFiles failed", e);
                throw e;
            }
            return res;
        }

        public void addFiles(List<PipeFileRecord> records) {
            try {
                StringBuilder sb = new StringBuilder();
                sb.append(INSERT_FILES);
                sb.append(records.stream().map(PipeFileRecord::toValueList).collect(Collectors.joining(",")));
                RepoExecutor.executeDQL(sb.toString());
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
                String sql = UPDATE_FILES_STATE +
                        records.stream().map(PipeFileRecord::toUniqueLocator).collect(Collectors.joining(" OR "));
                RepoExecutor.executeDQL(sql);
                LOG.info("update files state to {}: {}", state, records);
            } catch (Exception e) {
                LOG.error("update files state failed: {}", records, e);
                throw e;
            }
        }

        public void deleteByPipe(long pipeId) {
            try {
                String sql = String.format(DELETE_BY_PIPE, pipeId);
                RepoExecutor.executeDQL(sql);
                LOG.info("delete pipe files {}", pipeId);
            } catch (Exception e) {
                LOG.error("delete file of pipe {} failed", pipeId, e);
                throw e;
            }
        }

    }

    /**
     * Execute SQL
     */
    static class RepoExecutor {

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