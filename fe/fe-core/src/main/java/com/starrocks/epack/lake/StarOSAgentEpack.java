// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.lake;

import com.staros.client.StarClientException;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerGroupSpec;
import com.staros.util.LockCloseable;
import com.starrocks.common.DdlException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class StarOSAgentEpack extends StarOSAgent {
    private static final Logger LOG = LogManager.getLogger(StarOSAgentEpack.class);

    // remove previous worker with same backend id
    private void tryRemovePreviousWorkerGroup(long workerGroupId) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Iterator<Map.Entry<Long, Long>> iterator = workerToBackend.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, Long> entry = iterator.next();
                long nodeId = entry.getValue();
                long workerId = entry.getKey();
                ComputeNode node = GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(nodeId);
                if (node.getWorkerGroupId() == workerGroupId) {
                    iterator.remove();
                    workerToId.entrySet().removeIf(e -> e.getValue() == workerId);
                }
            }
        }
    }

    public long createWorkerGroup(String size) throws DdlException {
        prepare();

        // size should be x0, x1, x2, x4...
        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize(size).build();
        // owner means tenant, now there is only one tenant, so pass "Starrocks" to starMgr
        String owner = "Starrocks";
        WorkerGroupDetailInfo result = null;
        try {
            result = client.createWorkerGroup(serviceId, owner, spec, Collections.emptyMap(),
                    Collections.emptyMap());
        } catch (StarClientException e) {
            LOG.warn("Failed to create worker group. error: {}", e.getMessage());
            throw new DdlException("Failed to create worker group. error: " + e.getMessage());
        }
        return result.getGroupId();
    }

    public void deleteWorkerGroup(long groupId) throws DdlException {
        prepare();
        try {
            client.deleteWorkerGroup(serviceId, groupId);
        } catch (StarClientException e) {
            LOG.warn("Failed to delete worker group {}. error: {}", groupId, e.getMessage());
            throw new DdlException("Failed to delete worker group. error: " + e.getMessage());
        }

        tryRemovePreviousWorkerGroup(groupId);
    }

}
