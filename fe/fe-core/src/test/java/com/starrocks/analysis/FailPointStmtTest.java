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

import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UpdateFailPointStatusStatement;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FailPointStmtTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    public void testNormalCase(String sql) {
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof UpdateFailPointStatusStatement);
        Assert.assertEquals(sql, stmt.toSql());
    }

    @Test
    public void testUpdateFailPointStatus() {
        List<String> sqls = Arrays.asList(
                "ADMIN ENABLE FAILPOINT 'test'",
                "ADMIN ENABLE FAILPOINT 'test' WITH 1 TIMES",
                "ADMIN ENABLE FAILPOINT 'test' WITH 0.5 PROBABILITY",
                "ADMIN ENABLE FAILPOINT 'test' ON BACKEND '127.0.0.1:9000,127.0.0.2:9002'",
                "ADMIN DISABLE FAILPOINT 'test'"
        );
        for (String sql : sqls) {
            testNormalCase(sql);
        }
    }
}
