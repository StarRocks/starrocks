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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class PreparedStmtTest{
    private static ConnectContext ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void test() throws Exception {
        String sql1 = "PREPARE stmt1 FROM insert into demo.prepare_stmt values (?, ?, ?, ?);";
        String sql2= "PREPARE stmt2 FROM select * from demo.prepare_stmt where k1 = ? and k2 = ?;";
        String sql3 = "PREPARE stmt3 FROM 'select * from demo.prepare_stmt';";

        PrepareStmt stmt1 = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
        PrepareStmt stmt2 = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        PrepareStmt stmt3 = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql3, ctx);
        Assert.assertNotNull(stmt1);
        Assert.assertEquals(stmt1.getParameters().size(),  4);
        Assert.assertEquals(stmt2.getParameters().size(),  2);
        Assert.assertEquals(stmt3.getParameters().size(),  0);
    }
}
