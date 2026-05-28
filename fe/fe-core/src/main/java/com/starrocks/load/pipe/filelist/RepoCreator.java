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

package com.starrocks.load.pipe.filelist;

import com.starrocks.common.StarRocksException;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatisticUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Create the database and table
 */
public class RepoCreator {

    private static final Logger LOG = LogManager.getLogger(RepoCreator.class);
    private static final RepoCreator INSTANCE = new RepoCreator();

    private static volatile boolean databaseExists = false;
    private static volatile boolean tableExists = false;

    public static RepoCreator getInstance() {
        return INSTANCE;
    }

    public synchronized void run() {
        try {
            databaseExists = checkDatabaseExists();
            if (!databaseExists) {
                tableExists = false;
                LOG.warn("database not exists: " + FileListTableRepo.FILE_LIST_DB_NAME);
                return;
            }
            tableExists = checkTableExists();
            if (!tableExists) {
                createTable();
                tableExists = true;
                LOG.info("table created: " + FileListTableRepo.FILE_LIST_TABLE_NAME);
            }
            correctTable();
        } catch (Exception e) {
            LOG.error("error happens in RepoCreator: ", e);
        }
    }

    public boolean checkDatabaseExists() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(FileListTableRepo.FILE_LIST_DB_NAME) != null;
    }

    public boolean checkTableExists() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore()
                .mayGetTable(FileListTableRepo.FILE_LIST_DB_NAME, FileListTableRepo.FILE_LIST_TABLE_NAME)
                .isPresent();
    }

    public static void createTable() throws StarRocksException {
        int expectedReplicationNum =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getSystemTableExpectedReplicationNum();
        String sql = FileListTableRepo.SQLBuilder.buildCreateTableSql(expectedReplicationNum);
        SimpleExecutor.getRepoExecutor().executeDDL(sql);
    }

    public static boolean correctTable() {
        return StatisticUtils.alterSystemTableReplicationNumIfNecessary(FileListTableRepo.FILE_LIST_TABLE_NAME);
    }

    public boolean isDatabaseExists() {
        return databaseExists;
    }

    public boolean isTableExists() {
        return tableExists;
    }
}
