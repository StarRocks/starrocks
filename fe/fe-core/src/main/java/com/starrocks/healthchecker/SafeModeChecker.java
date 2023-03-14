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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/HealthAction.java

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
package com.starrocks.healthchecker;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class SafeModeChecker extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(SafeModeChecker.class);

    public SafeModeChecker() {
        super("safe mode checker", Config.safe_mode_checker_interval_sec * 1000);
    }

    @Override
    protected void runAfterCatalogReady() {
        // update interval
        if (getInterval() != Config.safe_mode_checker_interval_sec * 1000) {
            setInterval(Config.safe_mode_checker_interval_sec * 1000);
        }

        checkInternal();
    }

    @VisibleForTesting
    protected void checkInternal() {
        List<Backend> backendList = GlobalStateMgr.getCurrentSystemInfo().getBackends();
        for (Backend be : backendList) {
            // We assume that the cluster is always in balance, once we find that
            // the left space of one disk less than min(0.9 * disk_capacity, 50GB),
            // we should enter safe mode
            if (be.isAlive()) {
                for (DiskInfo diskInfo : be.getDisks().values()) {
                    double safeModeCheckDiskCapacity = Math.min(
                            0.9 * diskInfo.getTotalCapacityB(), 53687091200L);
                    if (diskInfo.getAvailableCapacityB() < safeModeCheckDiskCapacity) {
                        if (!GlobalStateMgr.getCurrentState().isSafeMode()) {
                            String warnMsg = String.format(
                                    "The cluster is entering safe mode since left disk space of %d" +
                                            " is %d. The load jobs will fail with exception.",
                                    be.getId(),
                                    diskInfo.getAvailableCapacityB());
                            LOG.warn(warnMsg);

                            // set safe mode flag to disable load jobs
                            GlobalStateMgr.getCurrentState().setSafeMode(true);

                            // abort all running transactions
                            try {
                                GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                                        .abortAllRunningTransactions();
                            } catch (UserException e) {
                                LOG.error("Abort transactions failed with exceptions!", e);
                            }
                        }
                        return;
                    }
                }
            }
        }

        // cluster state is healthy, exit safe mode
        if (GlobalStateMgr.getCurrentState().isSafeMode()) {
            LOG.info("The cluster exit safe mode");
            GlobalStateMgr.getCurrentState().setSafeMode(false);
        }
    }

}
