// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.epack.sql.ast.AlterFailoverGroupAddStmt;
import com.starrocks.epack.sql.ast.AlterFailoverGroupPrimaryStmt;
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
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class FailoverGroupAnalyzerTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testAnalyzeCreatePrimaryFailoverGroup() {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP test_group " +
                        "INCLUDE_TABLES = test_catalog.*.*, " +
                                "test_db.*, test_catalog.test_db.*, " +
                                "test_table, test_db.test_table, test_catalog.test_db.test_table " +
                        "EXCLUDE_TABLES = test_catalog.*.*, " +
                                "test_db.*, test_catalog.test_db.*, " +
                                "test_table, test_db.test_table, test_catalog.test_db.test_table " +
                        "MEMBERS = " +
                                "'test_member1:SELF'," +
                                "'test_member2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        Assert.assertEquals("test_group", stmt.getFailoverGroupName());

        Assert.assertNotNull(stmt.getIncludeCatalogs());
        Assert.assertEquals(1, stmt.getIncludeCatalogs().size());
        Assert.assertEquals("test_catalog", stmt.getIncludeCatalogs().get(0));

        Assert.assertNotNull(stmt.getIncludeDatabases());
        Assert.assertEquals(2, stmt.getIncludeDatabases().size());
        Assert.assertEquals("default_catalog", stmt.getIncludeDatabases().get(0).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeDatabases().get(0).getDatabase());
        Assert.assertEquals("test_catalog", stmt.getIncludeDatabases().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeDatabases().get(1).getDatabase());

        Assert.assertNotNull(stmt.getIncludeTables());
        Assert.assertEquals(3, stmt.getIncludeTables().size());
        Assert.assertEquals("default_catalog", stmt.getIncludeTables().get(0).getCatalog());
        Assert.assertEquals("test", stmt.getIncludeTables().get(0).getDb());
        Assert.assertEquals("test_table", stmt.getIncludeTables().get(0).getTbl());
        Assert.assertEquals("default_catalog", stmt.getIncludeTables().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeTables().get(1).getDb());
        Assert.assertEquals("test_table", stmt.getIncludeTables().get(1).getTbl());
        Assert.assertEquals("test_catalog", stmt.getIncludeTables().get(2).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeTables().get(2).getDb());
        Assert.assertEquals("test_table", stmt.getIncludeTables().get(2).getTbl());

        Assert.assertNotNull(stmt.getExcludeCatalogs());
        Assert.assertEquals(1, stmt.getExcludeCatalogs().size());
        Assert.assertEquals("test_catalog", stmt.getExcludeCatalogs().get(0));

        Assert.assertNotNull(stmt.getExcludeDatabases());
        Assert.assertEquals(2, stmt.getExcludeDatabases().size());
        Assert.assertEquals("default_catalog", stmt.getExcludeDatabases().get(0).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeDatabases().get(0).getDatabase());
        Assert.assertEquals("test_catalog", stmt.getExcludeDatabases().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeDatabases().get(1).getDatabase());

        Assert.assertNotNull(stmt.getExcludeTables());
        Assert.assertEquals(3, stmt.getExcludeTables().size());
        Assert.assertEquals("default_catalog", stmt.getExcludeTables().get(0).getCatalog());
        Assert.assertEquals("test", stmt.getExcludeTables().get(0).getDb());
        Assert.assertEquals("test_table", stmt.getExcludeTables().get(0).getTbl());
        Assert.assertEquals("default_catalog", stmt.getExcludeTables().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeTables().get(1).getDb());
        Assert.assertEquals("test_table", stmt.getExcludeTables().get(1).getTbl());
        Assert.assertEquals("test_catalog", stmt.getExcludeTables().get(2).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeTables().get(2).getDb());
        Assert.assertEquals("test_table", stmt.getExcludeTables().get(2).getTbl());

        Assert.assertNotNull(stmt.getMembers());
        Assert.assertEquals(2, stmt.getMembers().size());
        Assert.assertEquals("test_member1:SELF", stmt.getMembers().get(0));
        Assert.assertEquals("test_member2:192.168.0.1:9090", stmt.getMembers().get(1));

        Assert.assertNotNull(stmt.getSchedule());
        Assert.assertEquals("1h", stmt.getSchedule());

        analyzeFail(
                "CREATE FAILOVER GROUP test_group " +
                        "MEMBERS = " +
                                "'test_member1:SELF', ''" +
                        "SCHEDULE = '1h'",
                "Member is empty");
    }

    @Test
    public void testAnalyzeCreateSecondaryFailoverGroup() {
        CreateSecondaryFailoverGroupStmt stmt = (CreateSecondaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP test_group " +
                        "AS REPLICA OF '192.168.0.1:9090'");

        Assert.assertEquals("test_group", stmt.getFailoverGroupName());
        Assert.assertEquals("192.168.0.1:9090", stmt.getPrimaryMember());

        analyzeFail(
                "CREATE FAILOVER GROUP test_group " +
                        "AS REPLICA OF ''",
                "Primary member is empty");
    }

    @Test
    public void testAnalyzeDropFailoverGroup() {
        DropFailoverGroupStmt stmt1 = (DropFailoverGroupStmt) analyzeSuccess(
                "DROP FAILOVER GROUP test_group1");
        Assert.assertEquals(false, stmt1.getIfExists());
        Assert.assertEquals("test_group1", stmt1.getFailoverGroupName());

        DropFailoverGroupStmt stmt2 = (DropFailoverGroupStmt) analyzeSuccess(
                "DROP FAILOVER GROUP IF EXISTS test_group2");
        Assert.assertEquals(true, stmt2.getIfExists());
        Assert.assertEquals("test_group2", stmt2.getFailoverGroupName());

        analyzeFail(
                "DROP FAILOVER GROUP ''",
                "Failover group name is empty");
    }

    @Test
    public void testAnalyzeShowFailoverGroup() {
        analyzeSuccess("SHOW FAILOVER GROUPS");

        ShowFailoverGroupsStmt stmt1 = (ShowFailoverGroupsStmt) analyzeSuccess(
                "SHOW FAILOVER GROUPS LIKE 'test_group1'");
        Assert.assertEquals("test_group1", stmt1.getPattern());

        analyzeFail(
                "SHOW FAILOVER GROUPS LIKE ''",
                "Failover group pattern is empty");
    }

    @Test
    public void testAnalyzeDescribeFailoverGroup() {
        DescribeFailoverGroupStmt stmt1 = (DescribeFailoverGroupStmt) analyzeSuccess(
                "DESCRIBE FAILOVER GROUP test_group1");
        Assert.assertEquals("test_group1", stmt1.getFailoverGroupName());

        analyzeFail(
                "DESCRIBE FAILOVER GROUP ''",
                "Failover group name is empty");
    }

    @Test
    public void testAnalyzeAlterFailoverGroupSet() {
        AlterFailoverGroupSetStmt stmt = (AlterFailoverGroupSetStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP test_group SET " +
                        "INCLUDE_TABLES = test_catalog.*.*, " +
                                "test_db.*, test_catalog.test_db.*, " +
                                "test_table, test_db.test_table, test_catalog.test_db.test_table " +
                        "EXCLUDE_TABLES = test_catalog.*.*, " +
                                "test_db.*, test_catalog.test_db.*, " +
                                "test_table, test_db.test_table, test_catalog.test_db.test_table " +
                        "MEMBERS = " +
                                "'test_member1:SELF'," +
                                "'test_member2:192.168.0.1:9090'" +
                        "SCHEDULE = '1h'");

        Assert.assertEquals("test_group", stmt.getFailoverGroupName());

        Assert.assertNotNull(stmt.getIncludeCatalogs());
        Assert.assertEquals(1, stmt.getIncludeCatalogs().size());
        Assert.assertEquals("test_catalog", stmt.getIncludeCatalogs().get(0));

        Assert.assertNotNull(stmt.getIncludeDatabases());
        Assert.assertEquals(2, stmt.getIncludeDatabases().size());
        Assert.assertEquals("default_catalog", stmt.getIncludeDatabases().get(0).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeDatabases().get(0).getDatabase());
        Assert.assertEquals("test_catalog", stmt.getIncludeDatabases().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeDatabases().get(1).getDatabase());

        Assert.assertNotNull(stmt.getIncludeTables());
        Assert.assertEquals(3, stmt.getIncludeTables().size());
        Assert.assertEquals("default_catalog", stmt.getIncludeTables().get(0).getCatalog());
        Assert.assertEquals("test", stmt.getIncludeTables().get(0).getDb());
        Assert.assertEquals("test_table", stmt.getIncludeTables().get(0).getTbl());
        Assert.assertEquals("default_catalog", stmt.getIncludeTables().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeTables().get(1).getDb());
        Assert.assertEquals("test_table", stmt.getIncludeTables().get(1).getTbl());
        Assert.assertEquals("test_catalog", stmt.getIncludeTables().get(2).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeTables().get(2).getDb());
        Assert.assertEquals("test_table", stmt.getIncludeTables().get(2).getTbl());

        Assert.assertNotNull(stmt.getExcludeCatalogs());
        Assert.assertEquals(1, stmt.getExcludeCatalogs().size());
        Assert.assertEquals("test_catalog", stmt.getExcludeCatalogs().get(0));

        Assert.assertNotNull(stmt.getExcludeDatabases());
        Assert.assertEquals(2, stmt.getExcludeDatabases().size());
        Assert.assertEquals("default_catalog", stmt.getExcludeDatabases().get(0).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeDatabases().get(0).getDatabase());
        Assert.assertEquals("test_catalog", stmt.getExcludeDatabases().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeDatabases().get(1).getDatabase());

        Assert.assertNotNull(stmt.getExcludeTables());
        Assert.assertEquals(3, stmt.getExcludeTables().size());
        Assert.assertEquals("default_catalog", stmt.getExcludeTables().get(0).getCatalog());
        Assert.assertEquals("test", stmt.getExcludeTables().get(0).getDb());
        Assert.assertEquals("test_table", stmt.getExcludeTables().get(0).getTbl());
        Assert.assertEquals("default_catalog", stmt.getExcludeTables().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeTables().get(1).getDb());
        Assert.assertEquals("test_table", stmt.getExcludeTables().get(1).getTbl());
        Assert.assertEquals("test_catalog", stmt.getExcludeTables().get(2).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeTables().get(2).getDb());
        Assert.assertEquals("test_table", stmt.getExcludeTables().get(2).getTbl());

        Assert.assertNotNull(stmt.getMembers());
        Assert.assertEquals(2, stmt.getMembers().size());
        Assert.assertEquals("test_member1:SELF", stmt.getMembers().get(0));
        Assert.assertEquals("test_member2:192.168.0.1:9090", stmt.getMembers().get(1));

        Assert.assertNotNull(stmt.getSchedule());
        Assert.assertEquals("1h", stmt.getSchedule());

        analyzeFail(
                "ALTER FAILOVER GROUP test_group SET " +
                        "MEMBERS = " +
                                "'test_member1:SELF', ''",
                "Member is empty");

        analyzeFail(
                "ALTER FAILOVER GROUP test_group SET " +
                        "SCHEDULE = ''",
                "Schedule is empty");
    }

    @Test
    public void testAnalyzeAlterFailoverGroupAdd() {
        AlterFailoverGroupAddStmt stmt = (AlterFailoverGroupAddStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP test_group ADD " +
                                "test_catalog.*.*, " +
                                "test_db.*, test_catalog.test_db.*, " +
                                "test_table, test_db.test_table, test_catalog.test_db.test_table " +
                        "TO INCLUDE_TABLES " +
                                "test_catalog.*.*, " +
                                "test_db.*, test_catalog.test_db.*, " +
                                "test_table, test_db.test_table, test_catalog.test_db.test_table " +
                        "TO EXCLUDE_TABLES " +
                                "'test_member1:SELF', " +
                                "'test_member2:192.168.0.1:9090' " +
                        "TO MEMBERS ");

        Assert.assertEquals("test_group", stmt.getFailoverGroupName());

        Assert.assertNotNull(stmt.getIncludeCatalogs());
        Assert.assertEquals(1, stmt.getIncludeCatalogs().size());
        Assert.assertEquals("test_catalog", stmt.getIncludeCatalogs().get(0));

        Assert.assertNotNull(stmt.getIncludeDatabases());
        Assert.assertEquals(2, stmt.getIncludeDatabases().size());
        Assert.assertEquals("default_catalog", stmt.getIncludeDatabases().get(0).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeDatabases().get(0).getDatabase());
        Assert.assertEquals("test_catalog", stmt.getIncludeDatabases().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeDatabases().get(1).getDatabase());

        Assert.assertNotNull(stmt.getIncludeTables());
        Assert.assertEquals(3, stmt.getIncludeTables().size());
        Assert.assertEquals("default_catalog", stmt.getIncludeTables().get(0).getCatalog());
        Assert.assertEquals("test", stmt.getIncludeTables().get(0).getDb());
        Assert.assertEquals("test_table", stmt.getIncludeTables().get(0).getTbl());
        Assert.assertEquals("default_catalog", stmt.getIncludeTables().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeTables().get(1).getDb());
        Assert.assertEquals("test_table", stmt.getIncludeTables().get(1).getTbl());
        Assert.assertEquals("test_catalog", stmt.getIncludeTables().get(2).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeTables().get(2).getDb());
        Assert.assertEquals("test_table", stmt.getIncludeTables().get(2).getTbl());

        Assert.assertNotNull(stmt.getExcludeCatalogs());
        Assert.assertEquals(1, stmt.getExcludeCatalogs().size());
        Assert.assertEquals("test_catalog", stmt.getExcludeCatalogs().get(0));

        Assert.assertNotNull(stmt.getExcludeDatabases());
        Assert.assertEquals(2, stmt.getExcludeDatabases().size());
        Assert.assertEquals("default_catalog", stmt.getExcludeDatabases().get(0).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeDatabases().get(0).getDatabase());
        Assert.assertEquals("test_catalog", stmt.getExcludeDatabases().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeDatabases().get(1).getDatabase());

        Assert.assertNotNull(stmt.getExcludeTables());
        Assert.assertEquals(3, stmt.getExcludeTables().size());
        Assert.assertEquals("default_catalog", stmt.getExcludeTables().get(0).getCatalog());
        Assert.assertEquals("test", stmt.getExcludeTables().get(0).getDb());
        Assert.assertEquals("test_table", stmt.getExcludeTables().get(0).getTbl());
        Assert.assertEquals("default_catalog", stmt.getExcludeTables().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeTables().get(1).getDb());
        Assert.assertEquals("test_table", stmt.getExcludeTables().get(1).getTbl());
        Assert.assertEquals("test_catalog", stmt.getExcludeTables().get(2).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeTables().get(2).getDb());
        Assert.assertEquals("test_table", stmt.getExcludeTables().get(2).getTbl());

        Assert.assertNotNull(stmt.getMembers());
        Assert.assertEquals(2, stmt.getMembers().size());
        Assert.assertEquals("test_member1:SELF", stmt.getMembers().get(0));
        Assert.assertEquals("test_member2:192.168.0.1:9090", stmt.getMembers().get(1));

        analyzeFail(
                "ALTER FAILOVER GROUP test_group ADD " +
                        "'test_member1:SELF', '' TO MEMBERS",
                "Member is empty");
    }

    @Test
    public void testAnalyzeAlterFailoverGroupRemove() {
        AlterFailoverGroupRemoveStmt stmt = (AlterFailoverGroupRemoveStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP test_group REMOVE " +
                                "test_catalog.*.*, " +
                                "test_db.*, test_catalog.test_db.*, " +
                                "test_table, test_db.test_table, test_catalog.test_db.test_table " +
                        "FROM INCLUDE_TABLES " +
                                "test_catalog.*.*, " +
                                "test_db.*, test_catalog.test_db.*, " +
                                "test_table, test_db.test_table, test_catalog.test_db.test_table " +
                        "FROM EXCLUDE_TABLES " +
                                "'test_member1:SELF', " +
                                "'test_member2:192.168.0.1:9090' " +
                        "FROM MEMBERS ");

        Assert.assertEquals("test_group", stmt.getFailoverGroupName());

        Assert.assertNotNull(stmt.getIncludeCatalogs());
        Assert.assertEquals(1, stmt.getIncludeCatalogs().size());
        Assert.assertEquals("test_catalog", stmt.getIncludeCatalogs().get(0));

        Assert.assertNotNull(stmt.getIncludeDatabases());
        Assert.assertEquals(2, stmt.getIncludeDatabases().size());
        Assert.assertEquals("default_catalog", stmt.getIncludeDatabases().get(0).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeDatabases().get(0).getDatabase());
        Assert.assertEquals("test_catalog", stmt.getIncludeDatabases().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeDatabases().get(1).getDatabase());

        Assert.assertNotNull(stmt.getIncludeTables());
        Assert.assertEquals(3, stmt.getIncludeTables().size());
        Assert.assertEquals("default_catalog", stmt.getIncludeTables().get(0).getCatalog());
        Assert.assertEquals("test", stmt.getIncludeTables().get(0).getDb());
        Assert.assertEquals("test_table", stmt.getIncludeTables().get(0).getTbl());
        Assert.assertEquals("default_catalog", stmt.getIncludeTables().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeTables().get(1).getDb());
        Assert.assertEquals("test_table", stmt.getIncludeTables().get(1).getTbl());
        Assert.assertEquals("test_catalog", stmt.getIncludeTables().get(2).getCatalog());
        Assert.assertEquals("test_db", stmt.getIncludeTables().get(2).getDb());
        Assert.assertEquals("test_table", stmt.getIncludeTables().get(2).getTbl());

        Assert.assertNotNull(stmt.getExcludeCatalogs());
        Assert.assertEquals(1, stmt.getExcludeCatalogs().size());
        Assert.assertEquals("test_catalog", stmt.getExcludeCatalogs().get(0));

        Assert.assertNotNull(stmt.getExcludeDatabases());
        Assert.assertEquals(2, stmt.getExcludeDatabases().size());
        Assert.assertEquals("default_catalog", stmt.getExcludeDatabases().get(0).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeDatabases().get(0).getDatabase());
        Assert.assertEquals("test_catalog", stmt.getExcludeDatabases().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeDatabases().get(1).getDatabase());

        Assert.assertNotNull(stmt.getExcludeTables());
        Assert.assertEquals(3, stmt.getExcludeTables().size());
        Assert.assertEquals("default_catalog", stmt.getExcludeTables().get(0).getCatalog());
        Assert.assertEquals("test", stmt.getExcludeTables().get(0).getDb());
        Assert.assertEquals("test_table", stmt.getExcludeTables().get(0).getTbl());
        Assert.assertEquals("default_catalog", stmt.getExcludeTables().get(1).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeTables().get(1).getDb());
        Assert.assertEquals("test_table", stmt.getExcludeTables().get(1).getTbl());
        Assert.assertEquals("test_catalog", stmt.getExcludeTables().get(2).getCatalog());
        Assert.assertEquals("test_db", stmt.getExcludeTables().get(2).getDb());
        Assert.assertEquals("test_table", stmt.getExcludeTables().get(2).getTbl());

        Assert.assertNotNull(stmt.getMembers());
        Assert.assertEquals(2, stmt.getMembers().size());
        Assert.assertEquals("test_member1:SELF", stmt.getMembers().get(0));
        Assert.assertEquals("test_member2:192.168.0.1:9090", stmt.getMembers().get(1));

        analyzeFail(
                "ALTER FAILOVER GROUP test_group REMOVE " +
                        "'test_member1:SELF', '' FROM MEMBERS",
                "Member is empty");
    }

    @Test
    public void testAnalyzeAlterFailoverGroupRefresh() {
        AlterFailoverGroupRefreshStmt stmt1 = (AlterFailoverGroupRefreshStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP test_group1 REFRESH");
        Assert.assertEquals(false, stmt1.getIfExists());
        Assert.assertEquals("test_group1", stmt1.getFailoverGroupName());

        AlterFailoverGroupRefreshStmt stmt2 = (AlterFailoverGroupRefreshStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP IF EXISTS test_group2 REFRESH");
        Assert.assertEquals(true, stmt2.getIfExists());
        Assert.assertEquals("test_group2", stmt2.getFailoverGroupName());

        analyzeFail(
                "ALTER FAILOVER GROUP '' REFRESH",
                "Failover group name is empty");
    }

    @Test
    public void testAnalyzeAlterFailoverGroupPrimary() {
        AlterFailoverGroupPrimaryStmt stmt1 = (AlterFailoverGroupPrimaryStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP test_group1 PRIMARY");
        Assert.assertEquals(false, stmt1.getIfExists());
        Assert.assertEquals("test_group1", stmt1.getFailoverGroupName());

        AlterFailoverGroupPrimaryStmt stmt2 = (AlterFailoverGroupPrimaryStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP IF EXISTS test_group2 PRIMARY");
        Assert.assertEquals(true, stmt2.getIfExists());
        Assert.assertEquals("test_group2", stmt2.getFailoverGroupName());

        analyzeFail(
                "ALTER FAILOVER GROUP '' PRIMARY",
                "Failover group name is empty");
    }

    @Test
    public void testAnalyzeAlterFailoverGroupSuspend() {
        AlterFailoverGroupSuspendStmt stmt1 = (AlterFailoverGroupSuspendStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP test_group1 SUSPEND");
        Assert.assertEquals(false, stmt1.getIfExists());
        Assert.assertEquals("test_group1", stmt1.getFailoverGroupName());

        AlterFailoverGroupSuspendStmt stmt2 = (AlterFailoverGroupSuspendStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP IF EXISTS test_group2 SUSPEND");
        Assert.assertEquals(true, stmt2.getIfExists());
        Assert.assertEquals("test_group2", stmt2.getFailoverGroupName());

        analyzeFail(
                "ALTER FAILOVER GROUP '' SUSPEND",
                "Failover group name is empty");
    }

    @Test
    public void testAnalyzeAlterFailoverGroupResume() {
        AlterFailoverGroupResumeStmt stmt1 = (AlterFailoverGroupResumeStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP test_group1 RESUME");
        Assert.assertEquals(false, stmt1.getIfExists());
        Assert.assertEquals("test_group1", stmt1.getFailoverGroupName());

        AlterFailoverGroupResumeStmt stmt2 = (AlterFailoverGroupResumeStmt) analyzeSuccess(
                "ALTER FAILOVER GROUP IF EXISTS test_group2 RESUME");
        Assert.assertEquals(true, stmt2.getIfExists());
        Assert.assertEquals("test_group2", stmt2.getFailoverGroupName());

        analyzeFail(
                "ALTER FAILOVER GROUP '' RESUME",
                "Failover group name is empty");
    }
}
