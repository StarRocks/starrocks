// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import org.junit.Assert;
import org.junit.Test;

public class BrokerMgrTest {
    
    @Test    
    public void testIPTitle() {
        Assert.assertTrue(BrokerMgr.BROKER_PROC_NODE_TITLE_NAMES.get(1).equals("IP"));
    }
}
