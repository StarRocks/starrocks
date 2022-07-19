package com.starrocks.sql.analyzer;

import com.starrocks.alter.AlterTest;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.Privilege;
import com.starrocks.qe.ConnectContext;
import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzerRestoreTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        AlterTest.beforeClass();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testShowRestore() {
        String sql = "SHOW RESTORE FROM test;";
        analyzeSuccess(sql);
        analyzeSuccess("SHOW RESTORE;");
        analyzeFail("SHOW RESTORE FROM test1;");
        analyzeFail("SHOW RESTORE FROM test1;");
        new MockUp<Auth>() {
            @Mock
            public boolean checkDbPriv(ConnectContext ctx, String qualifiedDb, PrivPredicate wanted) {
                return false;
            }
        };
        analyzeFail("SHOW RESTORE FROM test;");
    }
}
