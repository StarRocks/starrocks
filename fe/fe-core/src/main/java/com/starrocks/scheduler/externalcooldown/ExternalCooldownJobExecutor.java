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


package com.starrocks.scheduler.externalcooldown;

import com.starrocks.common.DdlException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;


public class ExternalCooldownJobExecutor extends FrontendDaemon {
    private static final long EXECUTOR_INTERVAL_MILLIS = 5000;
    private static final Logger LOG = LogManager.getLogger(ExternalCooldownJobExecutor.class);

    public ExternalCooldownJobExecutor() {
        super("External Cooldown Job Executor", EXECUTOR_INTERVAL_MILLIS);
    }

    @Override
    protected void runAfterCatalogReady() {
        // initialize if need
        GlobalStateMgr.getCurrentState().getExternalCooldownMgr().doInitializeIfNeed();

        try {
            runImpl();
        } catch (Throwable e) {
            LOG.error("Failed to run the ExternalCooldownJobExecutor ", e);
        }
    }

    private void runImpl() {
        List<ExternalCooldownMaintenanceJob> jobs = GlobalStateMgr.getCurrentState().getExternalCooldownMgr().getRunnableJobs();
        if (CollectionUtils.isEmpty(jobs)) {
            return;
        }

        long startMillis = System.currentTimeMillis();
        for (ExternalCooldownMaintenanceJob job : jobs) {
            if (!job.isRunnable()) {
                LOG.warn("Job {} external cooldown config not satisfied ", job);
                continue;
            }
            try {
                job.onSchedule();
            } catch (DdlException e) {
                LOG.warn("[ExternalCooldownJobExecutor] execute job got exception", e);
            }
        }

        String jobNameList = jobs.stream().map(x -> x.getOlapTable().getName()).collect(Collectors.joining(", "));
        long duration = System.currentTimeMillis() - startMillis;
        LOG.info("[ExternalCooldownJobExecutor] finish schedule batch of jobs in {}ms: {}", duration, jobNameList);
    }
}
