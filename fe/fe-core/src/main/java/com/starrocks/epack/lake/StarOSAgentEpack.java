// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.lake;

import com.staros.client.StarClientException;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerGroupSpec;
import com.starrocks.common.DdlException;
import com.starrocks.lake.StarOSAgent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;

public class StarOSAgentEpack extends StarOSAgent {
    private static final Logger LOG = LogManager.getLogger(StarOSAgentEpack.class);

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
    }

}
