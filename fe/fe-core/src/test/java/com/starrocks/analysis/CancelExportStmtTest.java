package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import com.starrocks.qe.ConnectContext;

public class CancelExportStmtTest {
    @Mocked
    private ConnectContext connectContext;
    @Mocked
    private com.starrocks.analysis.Analyzer analyzer;

    @Before
    public void setUp() throws AnalysisException {

    }

    @Test
    public void testNormal() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setDatabase("testDb");

        {
            CancelExportStmt stmt = new CancelExportStmt(null, null);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);

            SlotRef slotRef = new SlotRef(null, "queryid");
            StringLiteral stringLiteral = new StringLiteral("abc");

            BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);

            stmt = new CancelExportStmt("testDb", binaryPredicate);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
            Assert.assertEquals("CANCEL EXPORT FROM testDb WHERE `queryid` = \'abc\'", stmt.toString());
        }
    }
}
