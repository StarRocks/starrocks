package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.ast.ShowPrivilegesStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShowPrivilegesStmtTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws Exception {
        ShowPrivilegesStmt stmt = (ShowPrivilegesStmt)
                UtFrameUtils.parseStmtWithNewParser("show privileges", connectContext);
        ShowResultSetMetaData metaData = stmt.getMetaData();
        Assert.assertNotNull(metaData);
    }
}
