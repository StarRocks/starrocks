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
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.ShowProcesslistStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShowProcesslistStmtTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws Exception {
        testSuccess("SHOW PROCESSLIST");
        testSuccess("SHOW FULL PROCESSLIST");
    }

    private void testSuccess(String originStmt) throws Exception {
        ShowProcesslistStmt stmt = (ShowProcesslistStmt)UtFrameUtils.parseStmtWithNewParser(originStmt, connectContext);
        ShowResultSetMetaData metaData = stmt.getMetaData();
        Assert.assertNotNull(metaData);
        Assert.assertEquals(11, metaData.getColumnCount());
        Assert.assertEquals("Id", metaData.getColumn(0).getName());
        Assert.assertEquals("User", metaData.getColumn(1).getName());
        Assert.assertEquals("Host", metaData.getColumn(2).getName());
        Assert.assertEquals("Db", metaData.getColumn(3).getName());
        Assert.assertEquals("Command", metaData.getColumn(4).getName());
        Assert.assertEquals("ConnectionStartTime", metaData.getColumn(5).getName());
        Assert.assertEquals("Time", metaData.getColumn(6).getName());
        Assert.assertEquals("State", metaData.getColumn(7).getName());
        Assert.assertEquals("Info", metaData.getColumn(8).getName());
        Assert.assertEquals("IsPending", metaData.getColumn(9).getName());
    }
}
