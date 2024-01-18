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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/LoadTask.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.loadv2;

import com.starrocks.common.exception.LoadException;
import com.starrocks.common.exception.UserException;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.load.FailMsg;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.PriorityLeaderTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class LoadTask extends PriorityLeaderTask {
    public enum TaskType {
        PENDING,
        LOADING
    }

    private static final Logger LOG = LogManager.getLogger(LoadTask.class);

    protected TaskType taskType;
    protected LoadTaskCallback callback;
    protected TaskAttachment attachment;
    protected FailMsg failMsg = new FailMsg();
    protected int retryTime = 1;

    public LoadTask(LoadTaskCallback callback, TaskType taskType, int priority) {
        super(priority);
        this.taskType = taskType;
        this.signature = GlobalStateMgr.getCurrentState().getNextId();
        this.callback = callback;
    }

    @Override
    protected void exec() {
        boolean isFinished = false;
        try {
            // execute pending task
            executeTask();
            // callback on pending task finished
            callback.onTaskFinished(attachment);
            isFinished = true;
        } catch (UserException e) {
            failMsg.setMsg(e.getMessage() == null ? "" : e.getMessage());
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                    .add("error_msg", "Failed to execute load task").build(), e);
        } catch (Exception e) {
            failMsg.setMsg(e.getMessage() == null ? "" : e.getMessage());
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                    .add("error_msg", "Unexpected failed to execute load task").build(), e);
        } finally {
            if (!isFinished) {
                // callback on pending task failed
                callback.onTaskFailed(signature, failMsg);
            }
        }
    }

    /**
     * init load task
     *
     * @throws LoadException
     */
    public void init() throws LoadException {
    }

    /**
     * execute load task
     *
     * @throws UserException task is failed
     */
    abstract void executeTask() throws Exception;

    public int getRetryTime() {
        return retryTime;
    }

    // Derived class may need to override this.
    public void updateRetryInfo() {
        this.retryTime--;
        this.signature = GlobalStateMgr.getCurrentState().getNextId();
    }

    public TaskType getTaskType() {
        return taskType;
    }
}
