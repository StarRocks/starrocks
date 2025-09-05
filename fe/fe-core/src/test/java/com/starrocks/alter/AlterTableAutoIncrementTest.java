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

package com.starrocks.alter;

import com.starrocks.common.FeConstants;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AlterTableAutoIncrementTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        AnalyzeTestUtil.init();
    }

    @Test
    public void testAlterTableAutoIncrement() throws Exception {        
        analyzeFail("ALTER TABLE test_auto_increment AUTO_INCREMENT = 0");
        analyzeFail("ALTER TABLE test_auto_increment AUTO_INCREMENT = -1");
        analyzeFail("ALTER TABLE xxxxxx AUTO_INCREMENT = 1");
        analyzeSuccess("ALTER TABLE test_auto_increment AUTO_INCREMENT = 1");
    }
}
