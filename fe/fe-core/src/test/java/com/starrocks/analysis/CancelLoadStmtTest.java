// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CancelLoadStmt;
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
        Assert.assertEquals("test", stmt.getDbName());
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
        String failMessage = "Where clause should looks like: LABEL = \"your_load_label\"";
        analyzeFail("CANCEL LOAD", failMessage);
        analyzeFail("CANCEL LOAD WHERE STATE = 'RUNNING'", failMessage);
        analyzeFail("CANCEL LOAD WHERE LABEL != 'RUNNING'", failMessage);
        analyzeFail("CANCEL LOAD WHERE LABEL = 123", failMessage);
        analyzeFail("CANCEL LOAD WHERE LABEL = ''", failMessage);
        analyzeFail("CANCEL LOAD WHERE LABEL LIKE 'abc' AND true", failMessage);
    }

    @Test
    public void testGetRedirectStatus() {
        CancelLoadStmt stmt = new CancelLoadStmt(null, null);
        Assert.assertEquals(stmt.getRedirectStatus(), RedirectStatus.FORWARD_WITH_SYNC);
    }
}
