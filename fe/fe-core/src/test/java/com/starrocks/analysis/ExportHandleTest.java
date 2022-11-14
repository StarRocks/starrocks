// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.analysis;

import com.starrocks.load.ExportMgr;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CancelExportStmt;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class ExportHandleTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Test
    public void testCancelExportHandler(@Mocked ExportMgr exportMgr, @Mocked EditLog editLog) {

        new MockUp<ConnectContext>() {
            @Mock
            GlobalStateMgr getGlobalStateMgr() {
                return globalStateMgr;
            }
        };

        new Expectations() {
            {
                globalStateMgr.getExportMgr();
                result = exportMgr;
            }
        };

        try {
            DDLStmtExecutor.execute(new CancelExportStmt("repo", null), new ConnectContext());
        } catch (Exception ex) {
            Assert.fail();
        }
    }
}
