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

import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.FrontendDaemon;
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

    private final PipeManager pipeManager;

    private final Map<Long, Integer> beSlotMap = new HashMap<>();
    private final ReentrantLock slotLock = new ReentrantLock();

    public PipeScheduler(PipeManager pm) {
        super("PipeScheduler", Config.pipe_scheduler_interval_millis);
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
            try {
                pipe.schedule();
            } catch (Throwable e) {
                LOG.warn("Failed to execute due to ", e);
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
