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
import com.starrocks.sql.ast.AlterWarehouseStmt;
import com.starrocks.sql.ast.CreateWarehouseStmt;
import com.starrocks.sql.ast.DropWarehouseStmt;
import com.starrocks.sql.ast.ResumeWarehouseStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SuspendWarehouseStmt;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class WarehouseStmtTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testCreateWarehouseParserAndAnalyzer() {
        String sql_1 = "CREATE WAREHOUSE warehouse_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof CreateWarehouseStmt);
        String sql_2 = "CREATE WAREHOUSE warehouse_2 properties(\"min_cluster\"=\"3\")";
        StatementBase stmt2 = AnalyzeTestUtil.analyzeSuccess(sql_2);
        Assert.assertEquals("CREATE WAREHOUSE 'warehouse_2' WITH PROPERTIES(\"min_cluster\"  =  \"3\")",
                stmt2.toSql());
    }

    @Test
    public void testDropWarehouseParserAndAnalyzer() {
        // test DROP WAREHOUSE warehouse_name
        String sql_1 = "DROP WAREHOUSE warehouse_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof DropWarehouseStmt);
        Assert.assertEquals("DROP WAREHOUSE 'warehouse_1'", stmt.toSql());
        String sql_2 = "DROP WAREHOUSE";
        AnalyzeTestUtil.analyzeFail(sql_2);

        // test DROP WAREHOUSE 'warehouse_name'
        String sql_3 = "DROP WAREHOUSE 'warehouse_1'";
        StatementBase stmt2 = AnalyzeTestUtil.analyzeSuccess(sql_3);
        Assert.assertTrue(stmt2 instanceof DropWarehouseStmt);
    }

    @Test
    public void testOpWarehouseParserAndAnalyzer() {
        String sql_1 = "SUSPEND WAREHOUSE warehouse_1";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(stmt instanceof SuspendWarehouseStmt);
        String sql_2 = "RESUME WAREHOUSE warehouse_1";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql_2);
        Assert.assertTrue(stmt instanceof ResumeWarehouseStmt);
        String sql_3 = "ALTER WAREHOUSE warehouse_1 ADD CLUSTER";
        stmt = AnalyzeTestUtil.analyzeSuccess(sql_3);
        Assert.assertTrue(stmt instanceof AlterWarehouseStmt);
    }
}
