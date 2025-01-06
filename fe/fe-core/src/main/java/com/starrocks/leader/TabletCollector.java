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

package com.starrocks.leader;

import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TGetTabletsInfoRequest;
import com.starrocks.thrift.TGetTabletsInfoResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

public class TabletCollector extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletCollector.class);
    private static final long CHECK_INTERVAL_MS = 100;

    private final PriorityQueue<CollectStat> collectQueue;
    private final Set<Long> queuedBeIds;

    public TabletCollector() {
        super("TabletCollector", CHECK_INTERVAL_MS);
        collectQueue = new PriorityQueue<>();
        queuedBeIds = new HashSet<>();
    }

    @Override
    protected void runAfterCatalogReady() {
        if (RunMode.isSharedDataMode()) {
            return;
        }

        updateQueue();

        collect(collectQueue.poll());
    }

    private void updateQueue() {
        List<Backend> backends = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends();
        for (Backend backend : backends) {
            if (backend.isAlive() && !queuedBeIds.contains(backend.getId())) {
                queuedBeIds.add(backend.getId());
                collectQueue.add(new CollectStat(backend.getId(), -1L));
            }
        }
    }

    private void collect(CollectStat collectStat) {
        if (collectStat == null) {
            return;
        }

        // 1. If there are more than 1 pending report task in ReportHandler
        // or
        // 2. The time since the last collection is less than Config.tablet_collect_interval_seconds.
        // return back the stat to collectQueue and do nothing.
        if (GlobalStateMgr.getCurrentState().getReportHandler().getPendingTabletReportTaskCnt() > 1
                || System.currentTimeMillis() - collectStat.lastCollectTime < Config.tablet_collect_interval_seconds * 1000) {
            collectQueue.add(collectStat);
            return;
        }

        // If backend is invalid, remove this backend from queue.
        // If resumed, the backend will be added back to the queue via updateQueue().
        Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(collectStat.beId);
        if (backend == null || !backend.isAlive()) {
            queuedBeIds.remove(collectStat.beId);
            return;
        }

        try {
            long startMs = System.currentTimeMillis();
            TGetTabletsInfoResult result = ThriftRPCRequestExecutor.call(
                    ThriftConnectionPool.backendPool,
                    new TNetworkAddress(backend.getHost(), backend.getBePort()),
                    Config.tablet_collect_timeout_seconds * 1000,
                    2,
                    client -> client.get_tablets_info(new TGetTabletsInfoRequest()));

            if (result.getStatus().getStatus_code() == TStatusCode.OK) {
                GlobalStateMgr.getCurrentState().getReportHandler()
                        .putTabletReportTask(backend.getId(), result.getReport_version(), result.getTablets());
                LOG.debug("collect tablet from backend {} successfully, time used: {}ms", backend.getId(),
                        System.currentTimeMillis() - startMs);
            } else {
                String errMsg = "";
                if (result.getStatus().getError_msgs() != null) {
                    errMsg = String.join(",", result.getStatus().getError_msgs());
                }
                LOG.warn("collect tablet from backend {} failed, error: {}", backend.getId(), errMsg);
            }
        } catch (Exception e) {
            LOG.warn("collect tablets from backend {} failed", backend.getId(), e);
        }

        // Regardless of whether the collection succeeds or fails, lastCollectTime must be updated,
        // otherwise it will block the collection of other backends.
        collectStat.lastCollectTime = System.currentTimeMillis();
        collectQueue.add(collectStat);
    }

    public static class CollectStat implements Comparable<CollectStat> {
        long beId;
        long lastCollectTime;

        CollectStat(long beId, long lastCollectTime) {
            this.beId = beId;
            this.lastCollectTime = lastCollectTime;
        }

        public long getBeId() {
            return beId;
        }

        public long getLastCollectTime() {
            return lastCollectTime;
        }

        @Override
        public int compareTo(CollectStat other) {
            return Long.compare(lastCollectTime, other.lastCollectTime);
        }
    }
}
