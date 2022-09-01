// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.system;

import com.starrocks.system.HeartbeatResponse.HbStatus;
import org.junit.Assert;
import org.junit.Test;

public class ComputeNodeTest {
    
    @Test
    public void testHbStatusBadNeedSync() {

        BackendHbResponse hbResponse = new BackendHbResponse();
        hbResponse.status = HbStatus.BAD;

        ComputeNode node = new ComputeNode();
        boolean needSync = node.handleHbResponse(hbResponse);
        Assert.assertTrue(needSync);
    }

    @Test
    public void testUpdateStartTime() {

        BackendHbResponse hbResponse = new BackendHbResponse();
        hbResponse.status = HbStatus.OK;
        hbResponse.setRebootTime(1000L);
        ComputeNode node = new ComputeNode();
        boolean needSync = node.handleHbResponse(hbResponse);
        Assert.assertTrue(node.getLastStartTime() == 1000000L);    
        Assert.assertTrue(needSync);
    }
}
