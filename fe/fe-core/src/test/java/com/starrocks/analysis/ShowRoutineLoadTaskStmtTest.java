// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import org.junit.Assert;
import org.junit.Test;


public class ShowRoutineLoadTaskStmtTest {
    
    @Test 
    public void testGetRedirectStatus() {
        ShowRoutineLoadTaskStmt loadStmt = new ShowRoutineLoadTaskStmt("", null);
        Assert.assertTrue(loadStmt.getRedirectStatus().equals(RedirectStatus.FORWARD_WITH_SYNC));
    }
}
