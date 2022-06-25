// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ShowRestoreStmtTest {
    
    @Test
    public void checkShowRestoreStmtRedirectStatus() {
        ShowRestoreStmt stmt = new ShowRestoreStmt("", null);
        assertEquals(stmt.getRedirectStatus(), RedirectStatus.NO_FORWARD);
    }
}
