package com.starrocks.analysis;

import com.starrocks.catalog.Catalog;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AdminSetReplicaStatusStmtTest {
    private static ConnectContext connectContext;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws Exception {
        String stmt = "admin set replica status properties(\"tablet_id\" = \"10003\",\"backend_id\" = \"10001\",\"status\" = \"ok\");";
        AdminSetReplicaStatusStmt adminSetReplicaStatusStmt =
                (AdminSetReplicaStatusStmt) UtFrameUtils.parseStmtWithNewParser(stmt, connectContext);
        Catalog.getCurrentCatalog().setReplicaStatus(adminSetReplicaStatusStmt);
    }

    @Test
    public void testErrorParameters() throws Exception {
        String stmt = "admin set replica status properties(\"tablet_id\" = \"10003\",\"status\" = \"ok\");";
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Should add following properties: TABLET_ID, BACKEND_ID and STATUS");
        AdminSetReplicaStatusStmt adminSetReplicaStatusStmt =
                (AdminSetReplicaStatusStmt) UtFrameUtils.parseStmtWithNewParser(stmt, connectContext);
        Catalog.getCurrentCatalog().setReplicaStatus(adminSetReplicaStatusStmt);
    }

}
