// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
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