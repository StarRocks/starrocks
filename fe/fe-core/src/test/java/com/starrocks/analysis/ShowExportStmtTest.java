package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import com.starrocks.qe.ConnectContext;

import java.util.List;

public class ShowExportStmtTest {
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
            ShowExportStmt stmt = new ShowExportStmt(null, null, null, null);
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);

            SlotRef slotRef = new SlotRef(null, "queryid");
            StringLiteral stringLiteral = new StringLiteral("abc");

            BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);
            List<OrderByElement> sort = null;

            stmt = new ShowExportStmt("testDb", binaryPredicate, sort, new LimitElement(10));
            com.starrocks.sql.analyzer.Analyzer.analyze(stmt, connectContext);
            Assert.assertEquals("SHOW EXPORT FROM testDb WHERE `queryid` = \'abc\' LIMIT 10",
                    stmt.toString());
        }
    }
}
