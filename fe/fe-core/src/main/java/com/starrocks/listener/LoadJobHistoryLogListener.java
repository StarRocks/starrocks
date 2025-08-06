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


package com.starrocks.listener;

import com.google.gson.Gson;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.qe.DmlType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.InsertOverwriteJobStats;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Listener to trigger load_history log print after load finished
 */
public class LoadJobHistoryLogListener implements LoadJobListener {

    public static final LoadJobHistoryLogListener INSTANCE = new LoadJobHistoryLogListener();

    private static final Logger LOADS_HISTORY_LOG = LogManager.getLogger("loads_history");
    private static final Gson GSON = new Gson();

    @Override
    public void onStreamLoadTransactionFinish(TransactionState transactionState) {
        if (!Config.enable_loads_history_log) {
            return;
        }
        String label = transactionState.getLabel();
        List<StreamLoadTask> streamLoadTaskList = GlobalStateMgr.getCurrentState().getStreamLoadMgr()
                .getTaskByName(label);
        if (streamLoadTaskList != null) {
            streamLoadTaskList.parallelStream().forEach(streamLoadTask -> {
                String jsonString = GSON.toJson(streamLoadTask.toThrift());
                LOADS_HISTORY_LOG.info(jsonString);
            });
        }
    }

    @Override
    public void onLoadJobTransactionFinish(TransactionState transactionState) {
        if (!Config.enable_loads_history_log) {
            return;
        }
        String label = transactionState.getLabel();
        List<LoadJob> loadJobs = GlobalStateMgr.getCurrentState().getLoadMgr().getLoadJobs(label);
        if (loadJobs != null) {
            loadJobs.parallelStream().forEach(loadJob -> {
                String jsonString = GSON.toJson(loadJob.toThrift());
                LOADS_HISTORY_LOG.info(jsonString);
            });
        }
    }

    @Override
    public void onDMLStmtJobTransactionFinish(TransactionState transactionState, Database db, Table table,
                                              DmlType dmlType) {
        // do nothing
    }

    @Override
    public void onInsertOverwriteJobCommitFinish(Database db, Table table, InsertOverwriteJobStats stats) {
        // do nothing
    }

    @Override
    public void onDeleteJobTransactionFinish(Database db, Table table) {
        // do nothing
    }
}
