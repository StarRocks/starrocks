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

package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeSelectAllColumnExcludeTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testBasicExcludeSingleColumn() {
        String sql = "SELECT * EXCLUDE (age) FROM test.test_exclude";
        analyzeSuccess(sql);
    }

    @Test
    public void testExcludeMultipleColumns() {
        String sql = "SELECT * EXCLUDE (name, email) FROM test.test_exclude";
        analyzeSuccess(sql);
    }

    @Test
    public void testExcludeNonExistingColumn() {
        String sql = "SELECT * EXCLUDE (invalid_col) FROM test.test_exclude";
        analyzeFail(sql, "Column(s) [invalid_col] do not exist");
    }

    @Test
    public void testExcludeWithTableAlias() {
        String sql = "SELECT t.* EXCLUDE (email) FROM test.test_exclude t";
        analyzeSuccess(sql);
    }

    @Test
    public void testExcludeWithWhereClause() {
        String sql = "SELECT * EXCLUDE (age) FROM test.test_exclude WHERE id = 1";
        analyzeSuccess(sql);
    }

    @Test
    public void testExceptKeyword() {
        String sql = "SELECT * EXCEPT (age) FROM test.test_exclude";
        analyzeSuccess(sql);
    }

    @Test
    public void testCaseInsensitiveExclude() {
        String sql = "SELECT * EXCLUDE (NAME) FROM test.test_exclude";
        analyzeSuccess(sql);
    }

    @Test
    public void testExcludeAllColumns() {
        String sql = "SELECT * EXCLUDE (id, name, age, email) FROM test.test_exclude";
        analyzeFail(sql, "EXCLUDE clause removes all columns");
    }

    @Test
    public void testExcludeInSubquery() {
        String sql = "SELECT * FROM (SELECT * EXCLUDE (email) FROM test.test_exclude) sub";
        analyzeSuccess(sql);
    }

    @Test
    public void testExcludeInCTE() {
        String sql = "WITH cte AS (SELECT * EXCLUDE (name) FROM test.test_exclude) SELECT * FROM cte";
        analyzeSuccess(sql);
    }

    @Test
    public void testInvalidExcludePosition() {
        String sql = "SELECT id, name EXCLUDE (name) FROM test.test_exclude";
        analyzeFail(sql, "Unexpected input 'EXCLUDE'");
    }

    @Test
    public void testDuplicateExclude() {
        String sql = "SELECT * EXCLUDE (age, age) FROM test.test_exclude";
        analyzeSuccess(sql);
    }
}