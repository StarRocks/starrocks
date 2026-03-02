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

package com.starrocks.sql.ast;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AdminShowTabletStatusStmtTest {
    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        ctx = AnalyzeTestUtil.getConnectContext();
    }

    @Test
    public void testNormal() {
        String sql = "ADMIN SHOW TABLET STATUS FROM db1.tbl1 WHERE STATUS = 'NORMAL'";
        AdminShowTabletStatusStmt stmt = (AdminShowTabletStatusStmt) AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertEquals("db1", stmt.getDbName());
        Assertions.assertEquals("tbl1", stmt.getTblName());
        Assertions.assertNull(stmt.getPartitionRef());
        Assertions.assertNotNull(stmt.getWhere());
        Assertions.assertTrue(stmt.getProperties().isEmpty());
    }

    @Test
    public void testWithPartitionAndProperties() {
        String sql = "ADMIN SHOW TABLET STATUS FROM db1.tbl1 PARTITION (p1, p2) " +
                "PROPERTIES ('max_missing_data_files_to_show'='10') WHERE STATUS = 'MISSING_DATA'";
        AdminShowTabletStatusStmt stmt = (AdminShowTabletStatusStmt) AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertEquals("db1", stmt.getDbName());
        Assertions.assertEquals("tbl1", stmt.getTblName());
        Assertions.assertNotNull(stmt.getPartitionRef());
        Assertions.assertEquals(2, stmt.getPartitionRef().getPartitionNames().size());
        Assertions.assertNotNull(stmt.getWhere());
        Assertions.assertEquals(10, stmt.getMaxMissingDataFilesToShow());
    }

    @Test
    public void testInvalidStatus() {
        String sql = "ADMIN SHOW TABLET STATUS FROM db1.tbl1 WHERE STATUS = 'UNKNOWN'";
        AnalyzeTestUtil.analyzeFail(sql, "status =|!= 'NORMAL'|'MISSING_META'|'MISSING_DATA'");
    }
}
