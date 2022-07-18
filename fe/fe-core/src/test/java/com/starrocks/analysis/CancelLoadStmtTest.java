// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class CancelLoadStmtTest {
    @Before
    public void setUp() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNormal() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
        CancelLoadStmt stmt = (CancelLoadStmt) analyzeSuccess("CANCEL LOAD FROM test WHERE `label` = 'abc'");
        Assert.assertEquals("CANCEL LOAD FROM `default_cluster:test` WHERE label = 'abc'", stmt.toString());
        Assert.assertEquals("default_cluster:test", stmt.getDbName());
        Assert.assertEquals("abc", stmt.getLabel());
    }

    @Test
    public void testNoDb() throws UserException, AnalysisException {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase(null);
        analyzeFail("CANCEL LOAD", "No database selected");
    }

    @Test
    public void testInvalidWhere() {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
        analyzeFail("CANCEL LOAD", "Where clause should looks like: LABEL = \"your_load_label\"");
        analyzeFail("CANCEL LOAD WHERE STATE = 'RUNNING'", "Where clause should looks like: LABEL = \"your_load_label\"");
    }

    @Test
    public void testGetRedirectStatus() {
        CancelLoadStmt stmt = new CancelLoadStmt(null, null);
        Assert.assertEquals(stmt.getRedirectStatus(), RedirectStatus.FORWARD_WITH_SYNC);
    }
}
