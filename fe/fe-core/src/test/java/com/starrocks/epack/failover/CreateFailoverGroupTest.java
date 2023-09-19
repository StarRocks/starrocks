// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.starrocks.common.DdlException;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.epack.policy.TestUtils.assertThrows;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class CreateFailoverGroupTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @Test
    public void testCreatePrimaryFailoverGroup() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt1 = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP test_group1 " +
                    "CATALOGS = default_catalog " +
                    "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                    "SCHEDULE = '1h'");

        DDLStmtExecutor.execute(stmt1, starRocksAssert.getCtx());

        CreatePrimaryFailoverGroupStmt stmt2 = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP test_group2 " +
                    "DATABASES = test " +
                    "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                    "SCHEDULE = '1h'");

        DDLStmtExecutor.execute(stmt2, starRocksAssert.getCtx());

        assertThrows("Failover group 'test_group2' exists", DdlException.class, () ->
                DDLStmtExecutor.execute(stmt2, starRocksAssert.getCtx()));
    }

    @Test
    public void testCreateSecondaryFailoverGroup() throws Exception {
        CreateSecondaryFailoverGroupStmt stmt = (CreateSecondaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCreateSecondaryFailoverGroup " +
                    "AS REPLICA OF '192.168.0.1:9090'");

        DDLStmtExecutor.execute(stmt, starRocksAssert.getCtx());

        assertThrows("Failover group 'testCreateSecondaryFailoverGroup' exists", DdlException.class, () ->
                DDLStmtExecutor.execute(stmt, starRocksAssert.getCtx()));
    }
}
