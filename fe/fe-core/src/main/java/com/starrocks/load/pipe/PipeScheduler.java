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

import com.starrocks.common.DdlException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.scheduler.SubmitResult;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * PipeScheduler: get tasks from pipe and execute them
 */
public class PipeScheduler extends FrontendDaemon {

    private static final Logger LOG = LogManager.getLogger(PipeScheduler.class);

    private PipeManager pipeManager;

    private final Map<Long, Integer> beSlotMap = new HashMap<>();
    private final ReentrantLock slotLock = new ReentrantLock();

    public PipeScheduler(PipeManager pm) {
        this.pipeManager = pm;
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of PipeScheduler", e);
        }
    }

    private void process() throws DdlException {
        List<Pipe> pipes = pipeManager.getRunnablePipes();
        for (Pipe pipe : pipes) {
            if (!hasAvailableSlot()) {
                break;
            }

            List<PipeTaskDesc> tasks = pipe.execute();
            if (CollectionUtils.isEmpty(tasks)) {
                continue;
            }
            for (PipeTaskDesc task : tasks) {
                if (!tryExecuteTask(task)) {
                    break;
                }
            }
        }
    }

    private boolean tryExecuteTask(PipeTaskDesc taskDesc) throws DdlException {
        // Acquire resource for this task
        boolean ready = true;
        if (MapUtils.isNotEmpty(taskDesc.getBeSlotRequirement())) {
            ready = acquireSlots(taskDesc.getBeSlotRequirement());
        }
        if (!ready) {
            return false;
        }

        try {
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            Task task = TaskBuilder.buildPipeTask(taskDesc);
            taskManager.createTask(task, false);
            // FIXME: execute task async
            taskDesc.onRunning();
            SubmitResult result = taskManager.executeTaskSync(task);
            if (result.getStatus().equals(SubmitResult.SubmitStatus.SUBMITTED)) {
                taskDesc.onFinished();
            } else {
                taskDesc.onError();
            }
        } catch (Throwable e) {
            taskDesc.onError();
        } finally {
            releaseSlots(taskDesc.getBeSlotRequirement());
        }

        return true;
    }

    private boolean acquireSlots(Map<Long, Integer> slots) {
        return true;
    }

    private void releaseSlots(Map<Long, Integer> slots) {

    }

    private boolean hasAvailableSlot() {
        // FIXME: consider available resource
        return true;
    }

}
