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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.StarRocksException;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CancelLoadStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class CancelLoadStmtTest {
    @BeforeEach
    public void setUp() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNormal() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
        CancelLoadStmt stmt = (CancelLoadStmt) analyzeSuccess("CANCEL LOAD FROM test WHERE `label` = 'abc'");
        Assertions.assertEquals("test", stmt.getDbName());
        Assertions.assertEquals("abc", stmt.getLabel());
    }

    @Test
    public void testNoDb() throws StarRocksException, AnalysisException {
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
        Assertions.assertEquals(stmt.getRedirectStatus(), RedirectStatus.FORWARD_WITH_SYNC);
    }
}
