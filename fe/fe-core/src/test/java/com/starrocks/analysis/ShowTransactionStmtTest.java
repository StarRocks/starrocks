package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.ShowTransactionStmt;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static org.junit.Assert.assertEquals;

public class ShowTransactionStmtTest {
    
    @Before
    public void setUp() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNormal() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
        ShowTransactionStmt stmt = (ShowTransactionStmt) analyzeSuccess("SHOW TRANSACTION FROM test WHERE `id` = 123");
        Assert.assertEquals(123, stmt.getTxnId());

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
    public void testNoDb() throws UserException, AnalysisException {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase(null);
        analyzeFail("SHOW TRANSACTION", "No database selected");
    }

    @Test
    public void testNoWhere() throws UserException, AnalysisException {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
    }

    @Test
    public void testInvalidWhere() {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
        String failMessage = "Where clause should looks like: ID = $transaction_id";
        analyzeFail("SHOW TRANSACTION", "should supply condition like: ID = $transaction_id");
        analyzeFail("SHOW TRANSACTION WHERE STATE = 'RUNNING'", failMessage);
        analyzeFail("SHOW TRANSACTION WHERE STATE LIKE 'RUNNING'", failMessage);
        analyzeFail("SHOW TRANSACTION WHERE STATE != 'LOADING'", failMessage);
        analyzeFail("SHOW TRANSACTION WHERE ID = ''", failMessage);
        analyzeFail("SHOW TRANSACTION WHERE ID = '123'", failMessage);
    }

    @Test
    public void checkShowTransactionStmtRedirectStatus() {
        ShowTransactionStmt stmt = new ShowTransactionStmt("", null);
        assertEquals(stmt.getRedirectStatus(), RedirectStatus.FORWARD_NO_SYNC);
    }
}
