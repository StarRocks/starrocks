// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.authentication.AuthenticationManager;
import com.starrocks.privilege.PrivilegeManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class PrivilegeCheckerV2Test {
    private static StarRocksAssert starRocksAssert;
    private static UserIdentity testUser;

    private static PrivilegeManager privilegeManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        starRocksAssert.withDatabase("db1");
        starRocksAssert.withDatabase("db2");
        starRocksAssert.withTable(createTblStmtStr);
        privilegeManager = starRocksAssert.getCtx().getGlobalStateMgr().getPrivilegeManager();
        starRocksAssert.getCtx().setRemoteIP("localhost");
        privilegeManager.initBuiltinRolesAndUsers();
        ctxToRoot();
        createUsers();
    }

    private static void ctxToTestUser() {
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setQualifiedUser(testUser.getQualifiedUser());
    }

    private static void ctxToRoot() {
        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);
        starRocksAssert.getCtx().setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
    }

    private static void createUsers() throws Exception {
        String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());

        AuthenticationManager authenticationManager =
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationManager();
        authenticationManager.createUser(createUserStmt);
        testUser = createUserStmt.getUserIdent();

        createUserSql = "CREATE USER 'test2' IDENTIFIED BY ''";
        createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        authenticationManager.createUser(createUserStmt);

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create role test_role", starRocksAssert.getCtx()), starRocksAssert.getCtx());
    }

    private static void verifyGrantRevoke(String sql, String grantSql, String revokeSql,
                                          String expectError) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        // 1. before grant: access denied
        ctxToTestUser();
        try {
            PrivilegeCheckerV2.check(statement, ctx);
            Assert.fail();
        } catch (SemanticException e) {
            System.out.println(e.getMessage() + ", sql: " + sql);
            Assert.assertTrue(e.getMessage().contains(expectError));
        }

        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql, ctx), ctx);

        ctxToTestUser();
        PrivilegeCheckerV2.check(statement, ctx);

        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(revokeSql, ctx), ctx);

        ctxToTestUser();
        try {
            PrivilegeCheckerV2.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (SemanticException e) {
            System.out.println(e.getMessage() + ", sql: " + sql);
            Assert.assertTrue(e.getMessage().contains(expectError));
        }
    }

<<<<<<< HEAD
=======
    private static void verifyMultiGrantRevoke(String sql, List<String> grantSqls, List<String> revokeSqls,
                                               String expectError) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        // 1. before grant: access denied
        ctxToTestUser();
        try {
            PrivilegeCheckerV2.check(statement, ctx);
            Assert.fail();
        } catch (SemanticException e) {
            System.out.println(e.getMessage() + ", sql: " + sql);
            Assert.assertTrue(e.getMessage().contains(expectError));
        }

        // 2. grant privileges
        ctxToRoot();
        grantSqls.forEach(grantSql -> {
            try {
                DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql, ctx), ctx);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // 3. check privileges after grant
        ctxToTestUser();
        PrivilegeCheckerV2.check(statement, ctx);

        // 4. revoke privileges
        ctxToRoot();
        revokeSqls.forEach(revokeSql -> {
            try {
                DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(revokeSql, ctx), ctx);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // 5. check privileges after revoke
        ctxToTestUser();
        try {
            PrivilegeCheckerV2.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (SemanticException e) {
            System.out.println(e.getMessage() + ", sql: " + sql);
            Assert.assertTrue(e.getMessage().contains(expectError));
        }
    }

    private static void verifyNODEAndGRANT(String sql, String expectError) throws Exception {
        ctxToRoot();
        ConnectContext ctx = starRocksAssert.getCtx();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        // user 'root' has GRANT/NODE privilege
        PrivilegeCheckerV2.check(statement, starRocksAssert.getCtx());

        try {
            ctxToTestUser();
            // user 'test' not has GRANT/NODE privilege
            PrivilegeCheckerV2.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (SemanticException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains(expectError));
        }
    }

    private static void checkOperateLoad(String sql) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();

        // check resoure privilege
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        try {
            PrivilegeCheckerV2.check(statement, ctx);
            Assert.fail();
        } catch (SemanticException e) {
            System.out.println(e.getMessage() + ", sql: " + sql);
            Assert.assertTrue(e.getMessage().contains(
                    "USAGE denied to user 'test'@'localhost' for resoure '[my_spark]'"
            ));
        }
        ctxToRoot();
        String grantResource = "grant USAGE on resource 'my_spark' to test;";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantResource, ctx), ctx);
        ctxToTestUser();

        // check table privilege
        verifyGrantRevoke(
                sql,
                "grant insert on db1.tbl1 to test",
                "revoke insert on db1.tbl1 from test",
                "INSERT command denied to user 'test'@'localhost' for table '[tbl1]'");
    }

    @Test
    public void testCatalogStatement() throws Exception {
        starRocksAssert.withCatalog("create external catalog test_ex_catalog properties (\"type\"=\"iceberg\")");
        ConnectContext ctx = starRocksAssert.getCtx();

        // Anyone can use default_catalog, but can't use other external catalog without any action on it
        ctxToTestUser();
        PrivilegeCheckerV2.check(
                UtFrameUtils.parseStmtWithNewParser("use 'catalog default_catalog'", ctx), ctx);
        try {
            PrivilegeCheckerV2.check(
                    UtFrameUtils.parseStmtWithNewParser("use 'catalog test_ex_catalog'", ctx), ctx);
        } catch (SemanticException e) {
            Assert.assertTrue(e.getMessage().contains(
                    "Access denied for user 'test' to catalog"));
        }
        verifyGrantRevoke(
                "use 'catalog test_ex_catalog'",
                "grant USAGE on catalog test_ex_catalog to test",
                "revoke USAGE on catalog test_ex_catalog from test",
                "Access denied for user 'test' to catalog");
        verifyGrantRevoke(
                "use 'catalog test_ex_catalog'",
                "grant DROP on catalog test_ex_catalog to test",
                "revoke DROP on catalog test_ex_catalog from test",
                "Access denied for user 'test' to catalog");

        // check create external catalog: CREATE_EXTERNAL_CATALOG on system object
        verifyGrantRevoke(
                "create external catalog test_ex_catalog2 properties (\"type\"=\"iceberg\")",
                "grant CREATE_EXTERNAL_CATALOG on system to test",
                "revoke CREATE_EXTERNAL_CATALOG on system from test",
                "Access denied for user 'test' to catalog");

        // check drop external catalog: DROP on catalog
        verifyGrantRevoke(
                "drop catalog test_ex_catalog",
                "grant DROP on catalog test_ex_catalog to test",
                "revoke DROP on catalog test_ex_catalog from test",
                "Access denied for user 'test' to catalog");

        // check show catalogs only show catalog where the user has any privilege on
        starRocksAssert.withCatalog("create external catalog test_ex_catalog3 properties (\"type\"=\"iceberg\")");
        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant DROP on catalog test_ex_catalog3 to test", ctx), ctx);
        ctxToTestUser();
        ShowResultSet res = new ShowExecutor(ctx,
                (ShowStmt) UtFrameUtils.parseStmtWithNewParser("SHOW catalogs", ctx)).execute();
        Assert.assertEquals(1, res.getResultRows().size());
        Assert.assertEquals("test_ex_catalog3", res.getResultRows().get(0).get(0));
    }

    @Test
    public void testResourceGroupStmt() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createRg3Sql = "create resource group rg3\n" +
                "to\n" +
                "    (query_type in ('select'), source_ip='192.168.6.1/24'),\n" +
                "    (query_type in ('select'))\n" +
                "with (\n" +
                "    'cpu_core_limit' = '1',\n" +
                "    'mem_limit' = '80%',\n" +
                "    'concurrency_limit' = '10',\n" +
                "    'type' = 'normal'\n" +
                ");";
        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(createRg3Sql, ctx), ctx);
        ctxToTestUser();

        // test no authorization on show resource groups
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser("show resource groups", ctx);
        PrivilegeCheckerV2.check(statement, ctx);

        // test drop resource group
        verifyGrantRevoke(
                "drop resource group rg3",
                "grant DROP on resource_group rg3 to test",
                "revoke DROP on resource_group rg3 from test",
                "Access denied; you need (at least one of) the DROP privilege(s) for this operation");

        // test drop resource group
        verifyGrantRevoke(
                "drop resource group rg3",
                "grant DROP on resource_group rg3 to test",
                "revoke DROP on resource_group rg3 from test",
                "Access denied; you need (at least one of) the DROP privilege(s) for this operation");

        String sql = "" +
                "ALTER RESOURCE GROUP rg3 \n" +
                "ADD \n" +
                "   (user='rg1_user5', role='rg1_role5', source_ip='192.168.4.1/16')";
        // test drop resource group
        verifyGrantRevoke(
                sql,
                "grant ALTER on resource_group rg3 to test",
                "revoke ALTER on resource_group rg3 from test",
                "Access denied; you need (at least one of) the ALTER privilege(s) for this operation");

        // test create resource group
        String createRg4Sql = "create resource group rg4\n" +
                "to\n" +
                "    (query_type in ('select'), source_ip='192.168.6.1/24'),\n" +
                "    (query_type in ('select'))\n" +
                "with (\n" +
                "    'cpu_core_limit' = '1',\n" +
                "    'mem_limit' = '80%',\n" +
                "    'concurrency_limit' = '10',\n" +
                "    'type' = 'normal'\n" +
                ");";
        verifyGrantRevoke(
                createRg4Sql,
                "grant create_resource_group on system to test",
                "revoke create_resource_group on system from test",
                "Access denied; you need (at least one of) the CREATE_RESOURCE_GROUP privilege(s) for this operation");

        // test grant/revoke on all resource_groups
        verifyGrantRevoke(
                sql,
                "grant ALTER on all resource_groups to test",
                "revoke ALTER on all resource_groups from test",
                "Access denied; you need (at least one of) the ALTER privilege(s) for this operation");
    }

    @Test
    public void testAnalyzeStatements() throws Exception {
        // check analyze table: SELECT + INSERT on table
        verifyGrantRevoke(
                "analyze table db1.tbl1",
                "grant SELECT,INSERT on db1.tbl1 to test",
                "revoke SELECT,INSERT on db1.tbl1 from test",
                "SELECT command denied to user 'test'");

        // check create analyze all: need SELECT + INSERT on all tables in all databases
        verifyMultiGrantRevoke(
                "create analyze all;",
                Arrays.asList(
                        "grant SELECT,INSERT on db1.tbl1 to test",
                        "grant SELECT,INSERT on db1.tbl2 to test",
                        "grant SELECT,INSERT on db2.tbl1 to test",
                        "grant SELECT,INSERT on db3.tbl1 to test"
                ),
                Arrays.asList(
                        "revoke SELECT,INSERT on db1.tbl1 from test",
                        "revoke SELECT,INSERT on db1.tbl2 from test",
                        "revoke SELECT,INSERT on db2.tbl1 from test",
                        "revoke SELECT,INSERT on db3.tbl1 from test"
                ),
                "SELECT command denied to user 'test'");

        // check create analyze database xxx: need SELECT + INSERT on all tables in the database
        verifyMultiGrantRevoke(
                "create analyze database db1;",
                Arrays.asList(
                        "grant SELECT,INSERT on db1.tbl1 to test",
                        "grant SELECT,INSERT on db1.tbl2 to test"
                ),
                Arrays.asList(
                        "revoke SELECT,INSERT on db1.tbl1 from test",
                        "revoke SELECT,INSERT on db1.tbl2 from test"
                ),
                "SELECT command denied to user 'test'");

        // check create analyze table xxx: need SELECT + INSERT on the table
        verifyMultiGrantRevoke(
                "create analyze table db1.tbl1;",
                Arrays.asList(
                        "grant SELECT,INSERT on db1.tbl1 to test"
                ),
                Arrays.asList(
                        "revoke SELECT,INSERT on db1.tbl1 from test"
                ),
                "SELECT command denied to user 'test'");

        // check drop stats xxx: SELECT + INSERT on table
        verifyGrantRevoke(
                "drop stats db1.tbl1",
                "grant SELECT,INSERT on db1.tbl1 to test",
                "revoke SELECT,INSERT on db1.tbl1 from test",
                "SELECT command denied to user 'test'");

        // check analyze table xxx drop histogram on xxx_col: SELECT + INSERT on table
        verifyGrantRevoke(
                "analyze table db1.tbl1 drop histogram on k1",
                "grant SELECT,INSERT on db1.tbl1 to test",
                "revoke SELECT,INSERT on db1.tbl1 from test",
                "SELECT command denied to user 'test'");
    }

    @Test
    public void testShowAnalyzeJobStatement() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant DROP on db1.tbl1 to test", ctx), ctx);
        ctxToTestUser();
        AnalyzeManager analyzeManager = GlobalStateMgr.getCurrentAnalyzeMgr();
        AnalyzeJob analyzeJob = new AnalyzeJob(-1, -1, Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.FINISH, LocalDateTime.MIN);
        List<String> showResult = ShowAnalyzeJobStmt.showAnalyzeJobs(ctx, analyzeJob);
        System.out.println(showResult);
        // can show result for analyze job with all type
        Assert.assertNotNull(showResult);

        analyzeJob.setId(2);
        analyzeManager.addAnalyzeJob(analyzeJob);
        grantRevokeSqlAsRoot("grant SELECT,INSERT on db1.tbl1 to test");
        grantRevokeSqlAsRoot("grant SELECT,INSERT on db1.tbl2 to test");
        grantRevokeSqlAsRoot("grant SELECT,INSERT on db3.tbl1 to test");
        try {
            PrivilegeCheckerV2.checkPrivilegeForKillAnalyzeStmt(ctx, analyzeJob.getId());
        } catch (SemanticException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("You need SELECT and INSERT action on db2.tbl1"));
        }
        grantRevokeSqlAsRoot("grant SELECT,INSERT on db2.tbl1 to test");
        PrivilegeCheckerV2.checkPrivilegeForKillAnalyzeStmt(ctx, analyzeJob.getId());
        grantRevokeSqlAsRoot("revoke SELECT,INSERT on db1.tbl1 from test");
        grantRevokeSqlAsRoot("revoke SELECT,INSERT on db1.tbl2 from test");
        grantRevokeSqlAsRoot("revoke SELECT,INSERT on db2.tbl1 from test");
        grantRevokeSqlAsRoot("revoke SELECT,INSERT on db3.tbl1 from test");

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db1 = globalStateMgr.getDb("db1");
        analyzeJob = new AnalyzeJob(db1.getId(), -1, Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.FINISH, LocalDateTime.MIN);
        showResult = ShowAnalyzeJobStmt.showAnalyzeJobs(ctx, analyzeJob);
        System.out.println(showResult);
        // can show result for analyze job with db.*
        Assert.assertNotNull(showResult);

        analyzeJob.setId(3);
        analyzeManager.addAnalyzeJob(analyzeJob);
        grantRevokeSqlAsRoot("grant SELECT,INSERT on db1.tbl1 to test");
        try {
            PrivilegeCheckerV2.checkPrivilegeForKillAnalyzeStmt(ctx, analyzeJob.getId());
        } catch (SemanticException e) {
            Assert.assertTrue(e.getMessage().contains("You need SELECT and INSERT action on db1.tbl2"));
        }
        grantRevokeSqlAsRoot("grant SELECT,INSERT on db1.tbl2 to test");
        PrivilegeCheckerV2.checkPrivilegeForKillAnalyzeStmt(ctx, analyzeJob.getId());
        grantRevokeSqlAsRoot("revoke SELECT,INSERT on db1.tbl1 from test");
        grantRevokeSqlAsRoot("revoke SELECT,INSERT on db1.tbl2 from test");

        Table tbl1 = db1.getTable("tbl1");
        analyzeJob = new AnalyzeJob(db1.getId(), tbl1.getId(), Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.FINISH, LocalDateTime.MIN);
        showResult = ShowAnalyzeJobStmt.showAnalyzeJobs(ctx, analyzeJob);
        System.out.println(showResult);
        // can show result for analyze job on table that user has any privilege on
        Assert.assertNotNull(showResult);
        Assert.assertEquals("tbl1", showResult.get(2));

        analyzeJob.setId(4);
        analyzeManager.addAnalyzeJob(analyzeJob);
        try {
            PrivilegeCheckerV2.checkPrivilegeForKillAnalyzeStmt(ctx, analyzeJob.getId());
        } catch (SemanticException e) {
            Assert.assertTrue(e.getMessage().contains("You need SELECT and INSERT action on db1.tbl1"));
        }
        grantRevokeSqlAsRoot("grant SELECT,INSERT on db1.tbl1 to test");
        PrivilegeCheckerV2.checkPrivilegeForKillAnalyzeStmt(ctx, analyzeJob.getId());
        grantRevokeSqlAsRoot("revoke SELECT,INSERT on db1.tbl1 from test");

        Database db2 = globalStateMgr.getDb("db2");
        tbl1 = db2.getTable("tbl1");
        analyzeJob = new AnalyzeJob(db2.getId(), tbl1.getId(), Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.FINISH, LocalDateTime.MIN);
        showResult = ShowAnalyzeJobStmt.showAnalyzeJobs(ctx, analyzeJob);
        System.out.println(showResult);
        // cannot show result for analyze job on table that user doesn't have any privileges on
        Assert.assertNull(showResult);
        grantRevokeSqlAsRoot("revoke DROP on db1.tbl1 from test");
    }

    @Test
    public void testShowAnalyzeStatusStatement() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant DROP on db1.tbl1 to test", ctx), ctx);
        ctxToTestUser();
        AnalyzeManager analyzeManager = GlobalStateMgr.getCurrentAnalyzeMgr();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db1 = globalStateMgr.getDb("db1");
        Table tbl1 = db1.getTable("tbl1");

        AnalyzeStatus analyzeStatus = new AnalyzeStatus(1, db1.getId(), tbl1.getId(),
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(),
                LocalDateTime.of(2020, 1, 1, 1, 1));
        analyzeStatus.setEndTime(LocalDateTime.of(2020, 1, 1, 1, 1));
        analyzeStatus.setStatus(StatsConstants.ScheduleStatus.FINISH);
        analyzeStatus.setReason("Test Success");
        List<String> showResult = ShowAnalyzeStatusStmt.showAnalyzeStatus(ctx, analyzeStatus);
        System.out.println(showResult);
        // can show result for analyze status on table that user has any privilege on
        Assert.assertNotNull(showResult);
        Assert.assertEquals("tbl1", showResult.get(2));

        grantRevokeSqlAsRoot("grant SELECT,INSERT on db1.tbl1 to test");
        analyzeManager.addAnalyzeStatus(analyzeStatus);
        PrivilegeCheckerV2.checkPrivilegeForKillAnalyzeStmt(ctx, analyzeStatus.getId());
        grantRevokeSqlAsRoot("revoke SELECT,INSERT on db1.tbl1 from test");

        try {
            PrivilegeCheckerV2.checkPrivilegeForKillAnalyzeStmt(ctx, analyzeStatus.getId());
        } catch (SemanticException e) {
            Assert.assertTrue(e.getMessage().contains("You need SELECT and INSERT action"));
        }

        BasicStatsMeta basicStatsMeta = new BasicStatsMeta(db1.getId(), tbl1.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1), Maps.newHashMap());
        showResult = ShowBasicStatsMetaStmt.showBasicStatsMeta(ctx, basicStatsMeta);
        System.out.println(showResult);
        // can show result for stats on table that user has any privilege on
        Assert.assertNotNull(showResult);

        HistogramStatsMeta histogramStatsMeta = new HistogramStatsMeta(db1.getId(), tbl1.getId(), "v1",
                StatsConstants.AnalyzeType.HISTOGRAM,
                LocalDateTime.of(2020, 1, 1, 1, 1),
                Maps.newHashMap());
        showResult = ShowHistogramStatsMetaStmt.showHistogramStatsMeta(ctx, histogramStatsMeta);
        System.out.println(showResult);
        // can show result for stats on table that user has any privilege on
        Assert.assertNotNull(showResult);

        Database db2 = globalStateMgr.getDb("db2");
        tbl1 = db2.getTable("tbl1");
        analyzeStatus = new AnalyzeStatus(1, db2.getId(), tbl1.getId(),
                Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(),
                LocalDateTime.of(2020, 1, 1, 1, 1));
        showResult = ShowAnalyzeStatusStmt.showAnalyzeStatus(ctx, analyzeStatus);
        System.out.println(showResult);
        // cannot show result for analyze status on table that user doesn't have any privileges on
        Assert.assertNull(showResult);

        basicStatsMeta = new BasicStatsMeta(db2.getId(), tbl1.getId(), null,
                StatsConstants.AnalyzeType.FULL,
                LocalDateTime.of(2020, 1, 1, 1, 1), Maps.newHashMap());
        showResult = ShowBasicStatsMetaStmt.showBasicStatsMeta(ctx, basicStatsMeta);
        System.out.println(showResult);
        // cannot show result for stats on table that user doesn't have any privilege on
        Assert.assertNull(showResult);

        histogramStatsMeta = new HistogramStatsMeta(db2.getId(), tbl1.getId(), "v1",
                StatsConstants.AnalyzeType.HISTOGRAM,
                LocalDateTime.of(2020, 1, 1, 1, 1),
                Maps.newHashMap());
        showResult = ShowHistogramStatsMetaStmt.showHistogramStatsMeta(ctx, histogramStatsMeta);
        System.out.println(showResult);
        // cannot show result for stats on table that user doesn't have any privilege on
        Assert.assertNull(showResult);
        grantRevokeSqlAsRoot("revoke DROP on db1.tbl1 from test");
    }

>>>>>>> 8edd06fff ([BugFix] Fix mysql client unable to change current catalog (#18266))
    @Test
    public void testTableSelectDeleteInsert() throws Exception {
        verifyGrantRevoke(
                "select * from db1.tbl1",
                "grant select on db1.tbl1 to test",
                "revoke select on db1.tbl1 from test",
                "SELECT command denied to user 'test'");
        verifyGrantRevoke(
                "insert into db1.tbl1 values ('petals', 'on', 'a', 99);",
                "grant insert on db1.tbl1 to test",
                "revoke insert on db1.tbl1 from test",
                "INSERT command denied to user 'test'");
        verifyGrantRevoke(
                "delete from db1.tbl1 where k3 = 1",
                "grant delete on db1.tbl1 to test",
                "revoke delete on db1.tbl1 from test",
                "DELETE command denied to user 'test'");
    }

    @Test
    public void testTableCreateDrop() throws Exception {
        String createTableSql = "create table db1.tbl2(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        verifyGrantRevoke(
                createTableSql,
                "grant create_table on database db1 to test",
                "revoke create_table on database db1 from test",
                "Access denied for user 'test' to database 'db1'");
        verifyGrantRevoke(
                "drop table db1.tbl1",
                "grant drop on db1.tbl1 to test",
                "revoke drop on db1.tbl1 from test",
                "DROP command denied to user 'test'");
    }

    @Test
    public void testGrantRevokePrivilege() throws Exception {
        verifyGrantRevoke(
                "grant select on db1.tbl1 to test",
                "grant select on db1.tbl1 to test with grant option",
                "revoke select on db1.tbl1 from test",
                "Access denied; you need (at least one of) the GRANT privilege(s) for this operation");
        verifyGrantRevoke(
                "revoke select on db1.tbl1 from test",
                "grant select on db1.tbl1 to test with grant option",
                "revoke select on db1.tbl1 from test with grant option",
                "Access denied; you need (at least one of) the GRANT privilege(s) for this operation");
    }

    @Test
    public void testResourceStmt() throws Exception {
        String createResourceStmt = "create external resource 'hive0' PROPERTIES(" +
                "\"type\"  =  \"hive\", \"hive.metastore.uris\"  =  \"thrift://127.0.0.1:9083\")";
        verifyGrantRevoke(
                createResourceStmt,
                "grant create_resource on system to test",
                "revoke create_resource on system from test",
                "Access denied; you need (at least one of) the CREATE_RESOURCE privilege(s) for this operation");
        starRocksAssert.withResource(createResourceStmt);

        verifyGrantRevoke(
                "alter RESOURCE hive0 SET PROPERTIES (\"hive.metastore.uris\" = \"thrift://10.10.44.91:9083\");",
                "grant alter on resource 'hive0' to test",
                "revoke alter on resource 'hive0' from test",
                "Access denied; you need (at least one of) the ALTER privilege(s) for this operation");

        verifyGrantRevoke(
                "drop resource hive0;",
                "grant drop on resource hive0 to test",
                "revoke drop on resource hive0 from test",
                "Access denied; you need (at least one of) the DROP privilege(s) for this operation");

        // on all
        verifyGrantRevoke(
                "drop resource hive0;",
                "grant drop on all resources to test",
                "revoke drop on all resources from test",
                "Access denied; you need (at least one of) the DROP privilege(s) for this operation");
    }

    @Test
    public void testViewStmt() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();

        // grant select on base table to user
        String grantBaseTableSql = "grant select on db1.tbl1 to test";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                grantBaseTableSql, ctx), ctx);

        // privilege check for create_view on database
        String createViewStmt = "create view db1.view1 as select * from db1.tbl1";
        String grantCreateViewStmt = "grant create_view on database db1 to test";
        String revokeCreateViewStmt = "revoke create_view on database db1 from test";
        verifyGrantRevoke(
                createViewStmt,
                grantCreateViewStmt,
                revokeCreateViewStmt,
                "Access denied for user 'test' to database 'db1'");

        // revoke select on base table, grant create_viw on database to user
        String revokeBaseTableSql = "revoke select on db1.tbl1 from test";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                revokeBaseTableSql, ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                grantCreateViewStmt, ctx), ctx);
        verifyGrantRevoke(
                createViewStmt,
                grantBaseTableSql,
                revokeBaseTableSql,
                "SELECT command denied to user 'test'");

        // create the view
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(createViewStmt, ctx), ctx);

        // revoke create_view on database, grant select on base table to user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                revokeCreateViewStmt, ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                grantBaseTableSql, ctx), ctx);
        String grantAlterSql = "grant alter on view db1.view1 to test";
        String revokeAlterSql = "revoke alter on view db1.view1 from test";
        String alterViewSql = "alter view db1.view1 as select k2, k3 from db1.tbl1";
        verifyGrantRevoke(
                alterViewSql,
                grantAlterSql,
                revokeAlterSql,
                "ALTER command denied to user 'test'");

        // revoke select on base table, grant alter on view to user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                revokeBaseTableSql, ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                grantAlterSql, ctx), ctx);
        verifyGrantRevoke(
                alterViewSql,
                grantBaseTableSql,
                revokeBaseTableSql,
                "SELECT command denied to user 'test'");

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                revokeAlterSql, ctx), ctx);

        // test select view
        String selectViewSql = "select * from db1.view1";
        verifyGrantRevoke(
                selectViewSql,
                grantBaseTableSql,
                revokeBaseTableSql,
                "SELECT command denied to user 'test'");
        verifyGrantRevoke(
                selectViewSql,
                "grant select on view db1.view1 to test",
                "revoke select on view db1.view1 from test",
                "SELECT command denied to user 'test'");

        // drop view
        verifyGrantRevoke(
                "drop view db1.view1",
                "grant drop on view db1.view1 to test",
                "revoke drop on view db1.view1 from test",
                "DROP command denied to user 'test'");
    }

    @Test
    public void testPluginStmts() throws Exception {
        String grantSql = "grant plugin on system to test";
        String revokeSql = "revoke plugin on system from test";
        String err = "Access denied; you need (at least one of) the PLUGIN privilege(s) for this operation";

        String sql = "INSTALL PLUGIN FROM \"/home/users/starrocks/auditdemo.zip\"";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);

        sql = "UNINSTALL PLUGIN auditdemo";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);

        sql = "SHOW PLUGINS";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);
    }

    @Test
    public void testFileStmts() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        String grantSelectTableSql = "grant select on db1.tbl1 to test";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSelectTableSql, ctx), ctx);

        // check file in system
        String createFileSql = "CREATE FILE \"client.key\" IN db1\n" +
                "PROPERTIES(\"catalog\" = \"internal\", \"url\" = \"http://test.bj.bcebos.com/kafka-key/client.key\")";
        String dropFileSql = "DROP FILE \"client.key\" FROM db1 PROPERTIES(\"catalog\" = \"internal\")";

        verifyGrantRevoke(
                createFileSql,
                "grant file on system to test",
                "revoke file on system from test",
                "Access denied; you need (at least one of) the FILE privilege(s) for this operation");
        verifyGrantRevoke(
                dropFileSql,
                "grant file on system to test",
                "revoke file on system from test",
                "Access denied; you need (at least one of) the FILE privilege(s) for this operation");

        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        String revokeSelectTableSql = "revoke select on db1.tbl1 from test";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(revokeSelectTableSql, ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant file on system to test", ctx), ctx);

        // check any action in table
        String dbDeniedError = "Access denied for user 'test' to database 'db1'";
        verifyGrantRevoke(createFileSql, grantSelectTableSql, revokeSelectTableSql, dbDeniedError);
        verifyGrantRevoke(dropFileSql, grantSelectTableSql, revokeSelectTableSql, dbDeniedError);
        verifyGrantRevoke("show file from db1", grantSelectTableSql, revokeSelectTableSql, dbDeniedError);

        // check any action in db
        String grantDropDbSql = "grant drop on database db1 to test";
        String revokeDropDbSql = "revoke drop on database db1 from test";
        verifyGrantRevoke(createFileSql, grantDropDbSql, revokeDropDbSql, dbDeniedError);
        verifyGrantRevoke(dropFileSql, grantDropDbSql, revokeDropDbSql, dbDeniedError);
        verifyGrantRevoke("show file from db1", grantDropDbSql, revokeDropDbSql, dbDeniedError);
    }

    @Test
    public void testBlackListStmts() throws Exception {
        String grantSql = "grant blacklist on system to test";
        String revokeSql = "revoke blacklist on system from test";
        String err = "Access denied; you need (at least one of) the BLACKLIST privilege(s) for this operation";

        String sql = "ADD SQLBLACKLIST \"select count\\\\(\\\\*\\\\) from .+\";";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);

        sql = "DELETE SQLBLACKLIST 0";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);

        sql = "SHOW SQLBLACKLIST";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);
    }

    @Test
    public void testRoleUserStmts() throws Exception {
        String grantSql = "grant user_admin to test";
        String revokeSql = "revoke user_admin from test";
        String err = "Access denied; you need (at least one of) the GRANT privilege(s) for this operation";
        String sql;

        sql = "grant test_role to test";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);

        sql = "revoke test_role from test";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);

        sql = "create user tesssst";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);

        sql = "drop user test";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);

        sql = "alter user test identified by 'asdf'";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);

        sql = "show roles";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);

        sql = "create role testrole2";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);

        sql = "drop role test_role";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);
    }

    @Test
    public void testShowPrivsForOther() throws Exception {
        String grantSql = "grant user_admin to test";
        String revokeSql = "revoke user_admin from test";
        String err = "Access denied; you need (at least one of) the GRANT privilege(s) for this operation";
        String sql;

        ConnectContext ctx = starRocksAssert.getCtx();

        sql = "show grants for test2";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);
        ctxToTestUser();
        PrivilegeCheckerV2.check(UtFrameUtils.parseStmtWithNewParser("show grants", ctx), ctx);

        sql = "show authentication for test2";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);
        ctxToTestUser();
        PrivilegeCheckerV2.check(UtFrameUtils.parseStmtWithNewParser("show authentication", ctx), ctx);

        sql = "SHOW PROPERTY FOR 'test2'";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);
        ctxToTestUser();
        PrivilegeCheckerV2.check(UtFrameUtils.parseStmtWithNewParser("show property", ctx), ctx);

        sql = "set property for 'test2' 'max_user_connections' = '100'";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);
        ctxToTestUser();
        PrivilegeCheckerV2.check(UtFrameUtils.parseStmtWithNewParser(
                "set property 'max_user_connections' = '100'", ctx), ctx);
    }

    @Test
    public void testExecuteAs() throws Exception {
        verifyGrantRevoke(
                "EXECUTE AS test2 WITH NO REVERT",
                "grant impersonate on user test2 to test",
                "revoke impersonate on user test2 from test",
                "Access denied; you need (at least one of) the IMPERSONATE privilege(s) for this operation");
    }

    @Test
    public void testDatabaseStmt() throws Exception {
        final String testDbName = "db_for_db_stmt_test";
        starRocksAssert.withDatabase(testDbName);
        String createTblStmtStr = "create table " + testDbName +
                ".tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) " +
                "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable(createTblStmtStr);

        List<String> statements = Lists.newArrayList();
        statements.add("use " + testDbName + ";");
        statements.add("show create database " + testDbName + ";");
        for (String stmt : statements) {
            // Test `use database` | `show create database xxx`: check any privilege on db
            verifyGrantRevoke(
                    stmt,
                    "grant DROP on database " + testDbName + " to test",
                    "revoke DROP on database " + testDbName + " from test",
                    "Access denied for user 'test' to database '" + testDbName + "'");
            verifyGrantRevoke(
                    stmt,
                    "grant CREATE_FUNCTION on database " + testDbName + " to test",
                    "revoke CREATE_FUNCTION on database " + testDbName + " from test",
                    "Access denied for user 'test' to database '" + testDbName + "'");
            verifyGrantRevoke(
                    stmt,
                    "grant ALTER on database " + testDbName + " to test",
                    "revoke ALTER on database " + testDbName + " from test",
                    "Access denied for user 'test' to database '" + testDbName + "'");
        }

        // Test `use database` : check any privilege on tables under db
        verifyGrantRevoke(
                "use " + testDbName + ";",
                "grant select on " + testDbName + ".tbl1 to test",
                "revoke select on " + testDbName + ".tbl1 from test",
                "Access denied for user 'test' to database '" + testDbName + "'");

        // Test `recover database xxx`: check DROP on db and CREATE_DATABASE on internal catalog
        // TODO(yiming): check for CREATE_DATABASE on internal catalog after catalog is added
        verifyGrantRevoke(
                "recover database " + testDbName + ";",
                "grant DROP on database " + testDbName + " to test",
                "revoke DROP on database " + testDbName + " from test",
                "Access denied for user 'test' to database '" + testDbName + "'");

        // Test `alter database xxx set...`: check ALTER on db
        verifyGrantRevoke(
                "alter database " + testDbName + " set data quota 10T;",
                "grant ALTER on database " + testDbName + " to test",
                "revoke ALTER on database " + testDbName + " from test",
                "Access denied for user 'test' to database '" + testDbName + "'");
        verifyGrantRevoke(
                "alter database " + testDbName + " set replica quota 102400;",
                "grant ALTER on database " + testDbName + " to test",
                "revoke ALTER on database " + testDbName + " from test",
                "Access denied for user 'test' to database '" + testDbName + "'");

        // Test `drop database xxx...`: check DROP on db
        verifyGrantRevoke(
                "drop database " + testDbName + ";",
                "grant DROP on database " + testDbName + " to test",
                "revoke DROP on database " + testDbName + " from test",
                "Access denied for user 'test' to database '" + testDbName + "'");
        verifyGrantRevoke(
                "drop database if exists " + testDbName + " force;",
                "grant DROP on database " + testDbName + " to test",
                "revoke DROP on database " + testDbName + " from test",
                "Access denied for user 'test' to database '" + testDbName + "'");

        // Test `alter database xxx rename xxx_new`: check ALTER on db
        verifyGrantRevoke(
                "alter database " + testDbName + " rename new_db_name;",
                "grant ALTER on database " + testDbName + " to test",
                "revoke ALTER on database " + testDbName + " from test",
                "Access denied for user 'test' to database '" + testDbName + "'");
    }
}
