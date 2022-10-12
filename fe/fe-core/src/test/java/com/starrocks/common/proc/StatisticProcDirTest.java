// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.proc;

import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Test;

public class StatisticProcDirTest {

    @Test(expected = AnalysisException.class)
    public void testLookupInvalid() throws AnalysisException {
        new StatisticProcDir(GlobalStateMgr.getCurrentState()).lookup("12345");
    }
}
