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
import com.starrocks.sql.ast.ShowBrokerStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShowBrokerStmtTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws Exception {
        ShowBrokerStmt stmt = (ShowBrokerStmt)UtFrameUtils.parseStmtWithNewParser("show broker", connectContext);
        ShowResultSetMetaData metaData = stmt.getMetaData();
        Assert.assertNotNull(metaData);
        Assert.assertEquals(7, metaData.getColumnCount());
        Assert.assertEquals("Name", metaData.getColumn(0).getName());
        Assert.assertEquals("IP", metaData.getColumn(1).getName());
        Assert.assertEquals("Port", metaData.getColumn(2).getName());
        Assert.assertEquals("Alive", metaData.getColumn(3).getName());
        Assert.assertEquals("LastStartTime", metaData.getColumn(4).getName());
        Assert.assertEquals("LastUpdateTime", metaData.getColumn(5).getName());
        Assert.assertEquals("ErrMsg", metaData.getColumn(6).getName());
    }
}