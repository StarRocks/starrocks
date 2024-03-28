// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.starrocks.common.DdlException;
import com.starrocks.epack.sql.ast.AlterFailoverGroupAddStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRefreshStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupRemoveStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupResumeStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSetStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupSuspendStmt;
import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.CreateSecondaryFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DescribeFailoverGroupStmt;
import com.starrocks.epack.sql.ast.DropFailoverGroupStmt;
import com.starrocks.epack.sql.ast.ShowFailoverGroupsStmt;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
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
        CreatePrimaryFailoverGroupStmt createStmt1 = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP test_group1 " +
                        "CATALOGS = default_catalog " +
                        "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        DDLStmtExecutor.execute(createStmt1, starRocksAssert.getCtx());

        AlterFailoverGroupSetStmt alterStmt1 = (AlterFailoverGroupSetStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP test_group1 SET " +
                        "CATALOGS = default_catalog " +
                        "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");
        DDLStmtExecutor.execute(alterStmt1, starRocksAssert.getCtx());

        AlterFailoverGroupAddStmt alterStmt2 = (AlterFailoverGroupAddStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP test_group1 ADD " +
                        "'az3:192.168.0.2:9090' TO MEMBERS");
        DDLStmtExecutor.execute(alterStmt2, starRocksAssert.getCtx());

        AlterFailoverGroupRemoveStmt alterStmt3 = (AlterFailoverGroupRemoveStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP test_group1 REMOVE " +
                        "'az3' FROM MEMBERS");
        DDLStmtExecutor.execute(alterStmt3, starRocksAssert.getCtx());

        ShowFailoverGroupsStmt showStmt1 = (ShowFailoverGroupsStmt) analyzeSuccess(
                "SHOW FAILOVER GROUPS");
        ShowExecutor.execute(showStmt1, starRocksAssert.getCtx());

        DescribeFailoverGroupStmt descStmt1 = (DescribeFailoverGroupStmt) analyzeSuccess(
                "DESC FAILOVER GROUP test_group1");
        ShowExecutor.execute(descStmt1, starRocksAssert.getCtx());

        CreatePrimaryFailoverGroupStmt createStmt2 = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP test_group2 " +
                        "DATABASES = test " +
                        "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        DDLStmtExecutor.execute(createStmt2, starRocksAssert.getCtx());

        assertThrows("Failover group 'test_group2' exists", DdlException.class, () ->
                DDLStmtExecutor.execute(createStmt2, starRocksAssert.getCtx()));

        DropFailoverGroupStmt dropStmt1 = (DropFailoverGroupStmt) analyzeSuccess(
                "DROP FAILOVER GROUP test_group1");
        DDLStmtExecutor.execute(dropStmt1, starRocksAssert.getCtx());

        DropFailoverGroupStmt dropStmt2 = (DropFailoverGroupStmt) analyzeSuccess(
                "DROP FAILOVER GROUP test_group2");
        DDLStmtExecutor.execute(dropStmt2, starRocksAssert.getCtx());
    }

    @Test
    public void testCreateSecondaryFailoverGroup() throws Exception {
        CreateSecondaryFailoverGroupStmt createStmt = (CreateSecondaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP testCreateSecondaryFailoverGroup " +
                        "AS REPLICA OF '192.168.0.1:9090'");

        DDLStmtExecutor.execute(createStmt, starRocksAssert.getCtx());

        assertThrows("Failover group 'testCreateSecondaryFailoverGroup' exists", DdlException.class, () ->
                DDLStmtExecutor.execute(createStmt, starRocksAssert.getCtx()));

        AlterFailoverGroupSuspendStmt alterStmt1 = (AlterFailoverGroupSuspendStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP testCreateSecondaryFailoverGroup SUSPEND");

        DDLStmtExecutor.execute(alterStmt1, starRocksAssert.getCtx());

        AlterFailoverGroupResumeStmt alterStmt2 = (AlterFailoverGroupResumeStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP testCreateSecondaryFailoverGroup RESUME");

        DDLStmtExecutor.execute(alterStmt2, starRocksAssert.getCtx());

        AlterFailoverGroupRefreshStmt alterStmt3 = (AlterFailoverGroupRefreshStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP testCreateSecondaryFailoverGroup REFRESH");

        DDLStmtExecutor.execute(alterStmt3, starRocksAssert.getCtx());

        DropFailoverGroupStmt dropStmt1 = (DropFailoverGroupStmt) analyzeSuccess(
                "DROP FAILOVER GROUP testCreateSecondaryFailoverGroup");
        DDLStmtExecutor.execute(dropStmt1, starRocksAssert.getCtx());
    }
}
