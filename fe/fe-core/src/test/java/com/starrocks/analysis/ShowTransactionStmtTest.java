package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ShowTransactionStmtTest {

    private ConnectContext ctx;

    @Before
    public void setUp() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");
    }

    @Test
    public void testNormal() throws Exception {
        SlotRef slotRef = new SlotRef(null, "id");
        StringLiteral stringLiteral = new StringLiteral("123");
        IntLiteral intLiteral = new IntLiteral(123);
        BinaryPredicate equalPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, intLiteral);
        
        ShowTransactionStmt stmt = new ShowTransactionStmt("", equalPredicate);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        Assert.assertEquals("SHOW TRANSACTION WHERE `id` = 123", stmt.toString());

        stmt = new ShowTransactionStmt("testDb", equalPredicate);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        Assert.assertEquals("SHOW TRANSACTION FROM `testDb` WHERE `id` = 123", stmt.toString());
        Assert.assertEquals("testDb", stmt.getDbName());
        
        ShowResultSetMetaData metaData = stmt.getMetaData();
        Assert.assertNotNull(metaData);
        Assert.assertEquals("TransactionId", metaData.getColumn(0).getName());
        Assert.assertEquals("Label", metaData.getColumn(1).getName());
        Assert.assertEquals("Coordinator", metaData.getColumn(2).getName());
        Assert.assertEquals("TransactionStatus", metaData.getColumn(3).getName());
        Assert.assertEquals("LoadJobSourceType", metaData.getColumn(4).getName());
        Assert.assertEquals("PrepareTime", metaData.getColumn(5).getName());
        Assert.assertEquals("CommitTime", metaData.getColumn(6).getName());
        Assert.assertEquals("PublishTime", metaData.getColumn(7).getName());
        Assert.assertEquals("FinishTime", metaData.getColumn(8).getName());
        Assert.assertEquals("Reason", metaData.getColumn(9).getName());
        Assert.assertEquals("ErrorReplicasCount", metaData.getColumn(10).getName());
        Assert.assertEquals("ListenerId", metaData.getColumn(11).getName());
        Assert.assertEquals("TimeoutMs", metaData.getColumn(12).getName());
        Assert.assertEquals("ErrMsg", metaData.getColumn(13).getName());
    }

    @Test
    public void checkShowTransactionStmtRedirectStatus() {
        ShowTransactionStmt stmt = new ShowTransactionStmt("", null);
        assertEquals(stmt.getRedirectStatus(), RedirectStatus.FORWARD_NO_SYNC);
    }
}
