// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.analysis;

import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.exception.UserException;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.ShowLoadWarningsStmt;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class ShowLoadWarningsStmtTest {
    @Before
    public void setUp() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNormal() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
        ShowLoadWarningsStmt stmt = (ShowLoadWarningsStmt) analyzeSuccess("SHOW LOAD WARNINGS FROM test WHERE `label` = 'abc' LIMIT 10");
        Assert.assertEquals(10, stmt.getLimitNum());
        Assert.assertEquals(0, stmt.getLimitElement().getOffset());
        Assert.assertEquals("abc", stmt.getLabel());
        Assert.assertEquals("test", stmt.getDbName());

        stmt = (ShowLoadWarningsStmt) analyzeSuccess("SHOW LOAD WARNINGS FROM test WHERE `load_job_id` = 123");
        Assert.assertEquals(123, stmt.getJobId());

        stmt = (ShowLoadWarningsStmt) analyzeSuccess("SHOW LOAD WARNINGS ON 'http://127.0.0.1:8000'");
        Assert.assertEquals("http://127.0.0.1:8000", stmt.getRawUrl());
        ShowResultSetMetaData metaData = stmt.getMetaData();
        Assert.assertNotNull(metaData);
        Assert.assertEquals(3, metaData.getColumnCount());
        Assert.assertEquals("JobId", metaData.getColumn(0).getName());
        Assert.assertEquals("Label", metaData.getColumn(1).getName());
        Assert.assertEquals("ErrorMsgDetail", metaData.getColumn(2).getName());
    }

    @Test
    public void testNoDb() throws UserException, AnalysisException {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase(null);
        analyzeFail("SHOW LOAD WARNINGS", "No database selected");
    }

    @Test
    public void testNoWhere() throws UserException, AnalysisException {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
    }

    @Test
    public void testInvalidWhere() {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
        String failMessage = "Where clause should looks like: LABEL = \"your_load_label\", or LOAD_JOB_ID = $job_id";
        analyzeFail("SHOW LOAD WARNINGS", "should supply condition like: LABEL = \"your_load_label\", or LOAD_JOB_ID = $job_id");
        analyzeFail("SHOW LOAD WARNINGS WHERE STATE = 'RUNNING'", failMessage);
        analyzeFail("SHOW LOAD WARNINGS WHERE STATE LIKE 'RUNNING'", failMessage);
        analyzeFail("SHOW LOAD WARNINGS WHERE STATE != 'LOADING'", failMessage);
        analyzeFail("SHOW LOAD WARNINGS WHERE LABEL = 123", failMessage);
        analyzeFail("SHOW LOAD WARNINGS WHERE LABEL LIKE 'abc' AND true", failMessage);
        analyzeFail("SHOW LOAD WARNINGS WHERE LABEL = ''", failMessage);
        analyzeFail("SHOW LOAD WARNINGS WHERE LOAD_JOB_ID = ''", failMessage);
        analyzeFail("SHOW LOAD WARNINGS WHERE LOAD_JOB_ID = '123'", failMessage);
    }

    @Test
    public void testInvalidUrl() {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
        analyzeFail("SHOW LOAD WARNINGS ON 'xxx'", "Invalid url: no protocol: xxx");
        analyzeFail("SHOW LOAD WARNINGS ON ''", "Error load url is missing");
    }

    @Test
    public void testGetRedirectStatus() {
        ShowLoadWarningsStmt stmt = new ShowLoadWarningsStmt(null, null, null, null);
        Assert.assertEquals(stmt.getRedirectStatus(), RedirectStatus.NO_FORWARD);
    }
}
