// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.common.proc;

import com.starrocks.catalog.Catalog;
import com.starrocks.common.AnalysisException;
import org.junit.Test;

public class StatisticProcDirTest {

    @Test(expected = AnalysisException.class)
    public void testLookupInvalid() throws AnalysisException {
        new StatisticProcDir(Catalog.getCurrentCatalog()).lookup("12345");
    }
}
