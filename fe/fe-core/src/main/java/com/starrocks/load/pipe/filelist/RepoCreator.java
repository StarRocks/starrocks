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

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.UserException;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Create the database and table
 */
public class RepoCreator {

    private static final Logger LOG = LogManager.getLogger(RepoCreator.class);
    private static final RepoCreator INSTANCE = new RepoCreator();

    private static boolean databaseExists = false;
    private static boolean tableExists = false;
    private static boolean tableCorrected = false;

    public static RepoCreator getInstance() {
        return INSTANCE;
    }

    public void run() {
        try {
            if (!databaseExists) {
                databaseExists = checkDatabaseExists();
                if (!databaseExists) {
                    LOG.warn("database not exists: " + FileListTableRepo.FILE_LIST_DB_NAME);
                    return;
                }
            }
            if (!tableExists) {
                createTable();
                LOG.info("table created: " + FileListTableRepo.FILE_LIST_TABLE_NAME);
                tableExists = true;
            }
            if (!tableCorrected && correctTable()) {
                LOG.info("table corrected: " + FileListTableRepo.FILE_LIST_TABLE_NAME);
                tableCorrected = true;
            }
        } catch (Exception e) {
            LOG.error("error happens in RepoCreator: ", e);
        }
    }

    public boolean checkDatabaseExists() {
        return GlobalStateMgr.getCurrentState().getDb(FileListTableRepo.FILE_LIST_DB_NAME) != null;
    }

    public static void createTable() throws UserException {
        String sql = FileListTableRepo.SQLBuilder.buildCreateTableSql();
        RepoExecutor.getInstance().executeDDL(sql);
    }

    public static boolean correctTable() {
        int numBackends = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getTotalBackendNumber();
        int replica = GlobalStateMgr.getCurrentState()
                .mayGetDb(FileListTableRepo.FILE_LIST_DB_NAME)
                .flatMap(db -> db.mayGetTable(FileListTableRepo.FILE_LIST_TABLE_NAME))
                .map(tbl -> ((OlapTable) tbl).getPartitionInfo().getMinReplicationNum())
                .orElse((short) 1);
        if (numBackends < 3) {
            LOG.info("not enough backends in the cluster, expected 3 but got {}", numBackends);
            return false;
        }
        if (replica < 3) {
            String sql = FileListTableRepo.SQLBuilder.buildAlterTableSql();
            RepoExecutor.getInstance().executeDDL(sql);
        } else {
            LOG.info("table {} already has {} replicas, no need to alter replication_num",
                    FileListTableRepo.FILE_LIST_FULL_NAME, replica);
        }
        return true;
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
