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

import com.google.common.collect.Lists;
import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AddSqlDigestBlackListStmt;
import com.starrocks.sql.ast.DelSqlDigestBlackListStmt;
import com.starrocks.sql.ast.ShowSqlDigestBlackListStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class SqlDigestBlacklistTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testAddDigestSqlBlacklist() {
        AddSqlDigestBlackListStmt stmt = (AddSqlDigestBlackListStmt) analyzeSuccess("ADD SQL DIGEST BLACKLIST abc");
        Assertions.assertNotNull(stmt.getDigest());
        // bad cases
        analyzeFail("ADD SQL DIGEST BLACKLIST \"x\";");
    }

    @Test
    public void testDelDigestSqlBlacklist() {
        DelSqlDigestBlackListStmt stmt =
                (DelSqlDigestBlackListStmt) analyzeSuccess("delete sql digest blacklist 2x,6c;");
        Assertions.assertEquals(Lists.asList("2x", new String[]{"6c"}), stmt.getDigests());
        // bad cases
        analyzeFail("delete sql digest blacklist");
    }

    @Test
    public void testShowDigestSqlBlacklist() {
        ShowSqlDigestBlackListStmt stmt = (ShowSqlDigestBlackListStmt) analyzeSuccess("show sql digest blacklist");
        Assertions.assertEquals(1, new ShowResultMetaFactory().getMetadata(stmt).getColumnCount());
        Assertions.assertEquals("Digests", new ShowResultMetaFactory().getMetadata(stmt).getColumn(0).getName());

        // bad cases
        analyzeFail("show blacklist");
    }
}
