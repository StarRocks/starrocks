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

package com.starrocks.load.pipe;

import com.starrocks.common.Config;
import com.starrocks.common.util.LeaderDaemon;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * PipeScheduler: get tasks from pipe and execute them
 */
public class PipeScheduler extends LeaderDaemon {

    private static final Logger LOG = LogManager.getLogger(PipeScheduler.class);

    private final PipeManager pipeManager;

    private final Map<Long, Integer> beSlotMap = new HashMap<>();
    private final ReentrantLock slotLock = new ReentrantLock();
    private boolean recovered = false;

    public PipeScheduler(PipeManager pm) {
        super("pipe-scheduler", Config.pipe_scheduler_interval_millis);
        this.pipeManager = pm;
    }

    @Override
    protected void runAfterLeaseValid() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of PipeScheduler", e);
        }
    }

    @Override
    protected void onStopped() {
        // recovered and beSlotMap are leader-session bookkeeping: the next leader must
        // re-run pipe.recovery() before scheduling, and BE slot reservations made against a
        // sealed editlog must not leak across sessions.
        //
        // Each Pipe also carries its own transient `recovered` flag and `runningTasks` map.
        // Pipe.recovery() short-circuits when recovered is true and buildNewTasks() refuses
        // to enqueue new work while runningTasks is non-empty, so reset both per pipe;
        // otherwise the re-elected leader thinks every pipe is already recovered and never
        // schedules another task.
        slotLock.lock();
        try {
            beSlotMap.clear();
        } finally {
            slotLock.unlock();
        }
        recovered = false;
        for (Pipe pipe : CollectionUtils.emptyIfNull(pipeManager.getAllPipes())) {
            try {
                pipe.resetForLeaderHandoff();
            } catch (Throwable e) {
                LOG.warn("Failed to reset pipe {} for leader handoff", pipe, e);
            }
        }
    }

    private void process() {
        if (!recovered) {
            for (Pipe pipe : CollectionUtils.emptyIfNull(pipeManager.getAllPipes())) {
                try {
                    pipe.recovery();
                } catch (Throwable e) {
                    LOG.warn("Failed to recover pipe {} due to ", pipe, e);
                }
            }

            recovered = pipeManager.getAllPipes().stream().allMatch(Pipe::isRecovered);
            if (recovered) {
                LOG.warn("Successfully recover all pipes");
            }
        }

        List<Pipe> pipes = pipeManager.getRunnablePipes();
        for (Pipe pipe : pipes) {
            try {
                pipe.schedule();
            } catch (Throwable e) {
                LOG.warn("Failed to execute pipe {} due to ", pipe, e);
            }
        }
    }

    // TODO: manage the resource
    private boolean acquireSlots(Map<Long, Integer> slots) {
        return true;
    }

    private void releaseSlots(Map<Long, Integer> slots) {
    }

    private boolean hasAvailableSlot() {
        return true;
    }

}
