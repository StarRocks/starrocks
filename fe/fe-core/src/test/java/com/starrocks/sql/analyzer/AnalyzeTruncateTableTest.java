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

import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeTruncateTableTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void normalTest() {
        TruncateTableStmt stmt = (TruncateTableStmt) analyzeSuccess("TRUNCATE TABLE example_db.tbl;");
        Assert.assertEquals("tbl", stmt.getTblName());
        Assert.assertEquals("example_db", stmt.getDbName());

        stmt = (TruncateTableStmt) analyzeSuccess("TRUNCATE TABLE tbl PARTITION(p1, p2);");
        Assert.assertEquals("tbl", stmt.getTblName());
        Assert.assertEquals("test", stmt.getDbName());
        Assert.assertEquals(stmt.getTblRef().getPartitionNames().getPartitionNames().toString(), "[p1, p2]");
    }

    @Test
    public void failureTest() {
        analyzeFail("TRUNCATE TABLE tbl PARTITION();");
    }
}
