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
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;

import java.util.List;

/**
 * Persist the file-list in an OLAP table
 */
public class FileListTableRepo {

    private static final String FILE_LIST_DB_NAME = StatsConstants.STATISTICS_DB_NAME;
    private static final String FILE_LIST_TABLE_NAME = "pipe_file_list";

    private static final String FILE_LIST_TABLE_CREATE =
            "CREATE TABLE IF NOT EXISTS %s.%s" +
                    "(" +
                    " "
            ")"+
                    "PRIMARY KEY() "+
                    "DISTRIBUTED BY HASH() BUCKETS 8 "+
                    "properties('replication_num' = '%d') ";

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

    }

    /**
     * Create the database and table
     */
    static class RepoCreator {

        public static boolean checkDatabaseExists() {
            return GlobalStateMgr.getCurrentState().getDb(FILE_LIST_DB_NAME) != null;
        }

        public static boolean checkTableExists() {
            Database db = GlobalStateMgr.getCurrentState().getDb(FILE_LIST_DB_NAME);
            if (db == null) {
                return false;
            }
            return db.getTable(FILE_LIST_TABLE_NAME) != null;
        }

        public static boolean createDatabase() {

        }

        public static boolean createTable() {
            final String createSql = String.format("create table if not exists %s.%s (" +

                    ")");
        }

    }

    /**
     * Generate SQL for operations
     */
    static class SQLBuilder {

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