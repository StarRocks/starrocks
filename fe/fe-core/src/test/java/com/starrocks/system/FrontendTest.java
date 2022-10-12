// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.system;

import com.starrocks.ha.FrontendNodeType;
import org.junit.Assert;
import org.junit.Test;

public class FrontendTest {
    
    @Test
    public void testFeUpdate() {
        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "name", "testHost", 1110);
        fe.updateHostAndEditLogPort("modifiedHost", 2110);
        Assert.assertEquals("modifiedHost", fe.getHost());
        Assert.assertTrue(fe.getEditLogPort() == 2110);
    }

    @Test
    public void testHbStatusBadNeedSync() {
        FrontendHbResponse hbResponse = new FrontendHbResponse("BAD", "");
        
        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "name", "testHost", 1110);
        boolean needSync = fe.handleHbResponse(hbResponse, true);
        Assert.assertTrue(needSync);
    }
}
