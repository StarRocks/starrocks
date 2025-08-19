package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.ShowTransactionStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

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

        ShowResultSetMetaData metaData = new ShowResultMetaFactory().getMetadata(stmt);
        Assertions.assertNotNull(metaData);
        Assertions.assertEquals("TransactionId", metaData.getColumn(0).getName());
        Assertions.assertEquals("Label", metaData.getColumn(1).getName());
        Assertions.assertEquals("Coordinator", metaData.getColumn(2).getName());
        Assertions.assertEquals("TransactionStatus", metaData.getColumn(3).getName());
        Assertions.assertEquals("LoadJobSourceType", metaData.getColumn(4).getName());
        Assertions.assertEquals("PrepareTime", metaData.getColumn(5).getName());
        Assertions.assertEquals("PreparedTime", metaData.getColumn(6).getName());
        Assertions.assertEquals("CommitTime", metaData.getColumn(7).getName());
        Assertions.assertEquals("PublishTime", metaData.getColumn(8).getName());
        Assertions.assertEquals("FinishTime", metaData.getColumn(9).getName());
        Assertions.assertEquals("Reason", metaData.getColumn(10).getName());
        Assertions.assertEquals("ErrorReplicasCount", metaData.getColumn(11).getName());
        Assertions.assertEquals("ListenerId", metaData.getColumn(12).getName());
        Assertions.assertEquals("TimeoutMs", metaData.getColumn(13).getName());
        Assertions.assertEquals("PreparedTimeoutMs", metaData.getColumn(14).getName());
        Assertions.assertEquals("ErrMsg", metaData.getColumn(15).getName());
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
}
