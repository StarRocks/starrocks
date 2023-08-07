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
import com.google.common.collect.Maps;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;

import java.util.Map;
import java.util.concurrent.Future;

public class PipeTaskDesc {

    private final long id;
    private final String dbName;
    private final String sqlTask;
    private final FilePipePiece piece;
    private PipeTaskState state = PipeTaskState.RUNNABLE;

    // TODO: error code and category
    // Error state
    private String errorMsg;
    private int errorLimit = 1;
    private int errorCount = 0;
    private int retryCount = 0;

    // Execution state
    private String uniqueTaskName;
    private Future<Constants.TaskRunState> future;

    public PipeTaskDesc(long id, String uniqueName, String dbName, String sqlTask, FilePipePiece piece) {
        this.id = id;
        this.uniqueTaskName = uniqueName;
        this.dbName = dbName;
        this.sqlTask = sqlTask;
        this.piece = piece;
    }

    public static String genUniqueTaskName(String pipeName, long taskId, int retrySeq) {
        return String.format("pipe-%s-task-%d-%d", pipeName, taskId, retrySeq);
    }

    public void onRunning() {
        this.state = PipeTaskState.RUNNING;
    }

    public void onFinished() {
        this.state = PipeTaskState.FINISHED;
    }

    public void onError(String errorMsg) {
        this.state = PipeTaskState.ERROR;
        this.errorMsg = errorMsg;
        this.errorCount++;
    }

    public void onRetry() {
        Preconditions.checkState(this.state == PipeTaskState.ERROR);
        this.retryCount++;
        this.state = PipeTaskState.RUNNABLE;
    }

    public boolean needSchedule() {
        return isRunning() || isRunnable() || (isError() && !tooManyErrors());
    }

    public boolean isRunnable() {
        return this.state.equals(PipeTaskState.RUNNABLE);
    }

    public boolean isRunning() {
        return this.state.equals(PipeTaskState.RUNNING);
    }

    public boolean isFinished() {
        return this.state.equals(PipeTaskState.FINISHED);
    }

    public boolean isError() {
        return this.state.equals(PipeTaskState.ERROR);
    }

    public boolean tooManyErrors() {
        return isError() && errorCount > errorLimit;
    }

    public void interrupt() {
        if (!this.state.equals(PipeTaskState.RUNNING)) {
            return;
        }
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        taskManager.killTask(uniqueTaskName, true);
    }

    public long getId() {
        return id;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public String getUniqueTaskName() {
        return uniqueTaskName;
    }

    public void setUniqueTaskName(String uniqueName) {
        this.uniqueTaskName = uniqueName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getSqlTask() {
        return sqlTask;
    }

    public Map<String, String> getProperties() {
        return Maps.newHashMap();
    }

    public PipeTaskState getState() {
        return state;
    }

    public FilePipePiece getPiece() {
        return piece;
    }

    public Future<Constants.TaskRunState> getFuture() {
        return future;
    }

    public void setFuture(Future<Constants.TaskRunState> future) {
        this.future = future;
    }

    public void setErrorLimit(int errorLimit) {
        this.errorLimit = errorLimit;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    @Override
    public String toString() {
        return "PipeTask " + uniqueTaskName + ", state=" + state + ", piece=" + piece;
    }

    enum PipeTaskState {
        RUNNABLE,
        RUNNING,
        FINISHED,
        ERROR,
    }
}
