package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.StarRocksException;
import com.starrocks.sql.ast.ShowResultSetMetaData;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.ShowTransactionStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShowTransactionStmtTest {
    
    @BeforeEach
    public void setUp() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNormal() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase("test");
        ShowTransactionStmt stmt = (ShowTransactionStmt) analyzeSuccess("SHOW TRANSACTION FROM test WHERE `id` = 123");
        Assertions.assertEquals(123, stmt.getTxnId());

        ShowResultSetMetaData metaData = stmt.getMetaData();
        Assertions.assertNotNull(metaData);
        Assertions.assertEquals("TransactionId", metaData.getColumn(0));
        Assertions.assertEquals("Label", metaData.getColumn(1));
        Assertions.assertEquals("Coordinator", metaData.getColumn(2));
        Assertions.assertEquals("TransactionStatus", metaData.getColumn(3));
        Assertions.assertEquals("LoadJobSourceType", metaData.getColumn(4));
        Assertions.assertEquals("PrepareTime", metaData.getColumn(5));
        Assertions.assertEquals("CommitTime", metaData.getColumn(6));
        Assertions.assertEquals("PublishTime", metaData.getColumn(7));
        Assertions.assertEquals("FinishTime", metaData.getColumn(8));
        Assertions.assertEquals("Reason", metaData.getColumn(9));
        Assertions.assertEquals("ErrorReplicasCount", metaData.getColumn(10));
        Assertions.assertEquals("ListenerId", metaData.getColumn(11));
        Assertions.assertEquals("TimeoutMs", metaData.getColumn(12));
        Assertions.assertEquals("ErrMsg", metaData.getColumn(13));
    }

    @Test
    public void testNoDb() throws StarRocksException, AnalysisException {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase(null);
        analyzeFail("SHOW TRANSACTION", "No database selected");
    }

    @Test
    public void testNoWhere() throws StarRocksException, AnalysisException {
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
