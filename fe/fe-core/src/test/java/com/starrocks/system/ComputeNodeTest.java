// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.system;

import org.junit.Assert;
import org.junit.Test;

import com.starrocks.system.HeartbeatResponse.HbStatus;

public class ComputeNodeTest {
    
    @Test
    public void testHbStatusBadNeedSync() {

        BackendHbResponse hbResponse = new BackendHbResponse();
        hbResponse.status = HbStatus.BAD;

        ComputeNode node = new ComputeNode();
        boolean needSync = node.handleHbResponse(hbResponse);
        Assert.assertTrue(needSync);
    }
}
