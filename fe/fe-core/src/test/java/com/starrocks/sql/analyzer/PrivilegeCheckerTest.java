// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.FunctionName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.backup.BlobStorage;
import com.starrocks.backup.RemoteFile;
import com.starrocks.backup.Repository;
import com.starrocks.backup.RepositoryMgr;
import com.starrocks.backup.Status;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.KafkaUtil;
import com.starrocks.mysql.MysqlChannel;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.ShowAnalyzeJobStmt;
import com.starrocks.sql.ast.ShowAnalyzeStatusStmt;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowBasicStatsMetaStmt;
import com.starrocks.sql.ast.ShowHistogramStatsMetaStmt;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.HistogramStatsMeta;
import com.starrocks.statistic.NativeAnalyzeStatus;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.channels.SocketChannel;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

public class PrivilegeCheckerTest {
    private static StarRocksAssert starRocksAssert;
    private static UserIdentity testUser;

    private static AuthorizationMgr authorizationManager;

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        UtFrameUtils.addBroker("broker0");
        String createTblStmtStr1 = "create table db1.tbl1(event_day DATE, k1 varchar(32), " +
                "k2 varchar(32), k3 varchar(32), k4 int) "
                + "primary KEY(event_day, k1, k2, k3) " + " PARTITION BY RANGE(event_day)(\n" +
                "PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n" +
                "PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n" +
                "PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n" +
                "PARTITION p20200324 VALUES LESS THAN (\"2020-03-25\")\n" +
                ")\n" + "distributed by hash(k1) buckets 3 properties('replication_num' = '1', \n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "    \"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "    \"dynamic_partition.start\" = \"-3\",\n" +
                "    \"dynamic_partition.end\" = \"3\",\n" +
                "    \"dynamic_partition.prefix\" = \"p\",\n" +
                "    \"dynamic_partition.buckets\" = \"32\"" + ");";
        String createTblStmtStr2 = "create table db2.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                +
                "AGGREGATE KEY(k1, k2, k3, k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        String createTblStmtStr3 = "create table db1.tbl2(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                +
                "AGGREGATE KEY(k1, k2, k3, k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";

        connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("db1");
        starRocksAssert.withDatabase("db2");
        starRocksAssert.withDatabase("db3");
        starRocksAssert.withTable(createTblStmtStr1);
        starRocksAssert.withTable(createTblStmtStr2);
        starRocksAssert.withTable(createTblStmtStr3);
        createMvForTest(starRocksAssert.getCtx());

        authorizationManager = starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr();
        starRocksAssert.getCtx().setRemoteIP("localhost");
        authorizationManager.initBuiltinRolesAndUsers();
        ctxToRoot();
        createUsers();
    }

    private static void createMvForTest(ConnectContext connectContext) throws Exception {
        starRocksAssert.withTable("CREATE TABLE db3.tbl1\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values less than('2020-02-01'),\n" +
                "    PARTITION p2 values less than('2020-03-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        String sql = "create materialized view db3.mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, k2 from db3.tbl1;";
        createMaterializedView(sql, connectContext);
    }

    private static void createMaterializedView(String sql, ConnectContext connectContext) throws Exception {
        Config.enable_experimental_mv = true;
        CreateMaterializedViewStatement createMaterializedViewStatement =
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createMaterializedView(createMaterializedViewStatement);
    }

    private static void mockRepository() {
        new MockUp<RepositoryMgr>() {
            @Mock
            public Repository getRepo(String repoName) {
                Repository repository = new Repository(1, "repo", false, "", null);
                Field field1 = null;
                try {
                    field1 = repository.getClass().getDeclaredField("storage");
                } catch (NoSuchFieldException e) {
                    // ignore
                }
                if (field1 != null) {
                    field1.setAccessible(true);
                }
                BlobStorage storage = new BlobStorage("", null);
                try {
                    if (field1 != null) {
                        field1.set(repository, storage);
                    }
                } catch (IllegalAccessException e) {
                    // ignore
                }
                return repository;
            }
        };

        new MockUp<BlobStorage>() {
            @Mock
            public Status list(String remotePath, List<RemoteFile> result) {
                return Status.OK;
            }
        };
    }

    private static void mockAddBackupJob(String dbName) throws Exception {
        mockRepository();
        ctxToRoot();
        String createBackupSql = "BACKUP SNAPSHOT " + dbName + ".backup_name1 " +
                "TO example_repo " +
                "ON (tbl1) " +
                "PROPERTIES ('type' = 'full');";
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(createBackupSql,
                starRocksAssert.getCtx());
        DDLStmtExecutor.execute(statement, starRocksAssert.getCtx());
        String showBackupSql = "SHOW BACKUP FROM " + dbName + ";";
        StatementBase showExportSqlStmt = UtFrameUtils.parseStmtWithNewParser(showBackupSql, starRocksAssert.getCtx());
        ShowExecutor executor = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) showExportSqlStmt);
        ShowResultSet set = executor.execute();
        Assert.assertTrue(set.getResultRows().size() > 0);
    }

    private static void mockBroker() {
        new MockUp<BrokerMgr>() {
            @Mock
            public FsBroker getAnyBroker(String brokerName) {
                return new FsBroker();
            }

            @Mock
            public FsBroker getBroker(String brokerName, String host) throws AnalysisException {
                return new FsBroker();
            }
        };
    }

    private static void ctxToTestUser() throws PrivilegeException {
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setCurrentRoleIds(
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr().getRoleIdsByUser(testUser)
        );
        starRocksAssert.getCtx().setQualifiedUser(testUser.getQualifiedUser());
    }

    private static void ctxToRoot() throws PrivilegeException {
        starRocksAssert.getCtx().setCurrentUserIdentity(UserIdentity.ROOT);
        starRocksAssert.getCtx().setCurrentRoleIds(
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr().getRoleIdsByUser(UserIdentity.ROOT)
        );

        starRocksAssert.getCtx().setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
    }

    private static void grantOrRevoke(String sql) throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql,
                        starRocksAssert.getCtx()),
                starRocksAssert.getCtx());
    }

    private static void createUsers() throws Exception {
        String createUserSql = "CREATE USER 'test' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());

        AuthenticationMgr authenticationManager =
                starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();
        authenticationManager.createUser(createUserStmt);
        testUser = createUserStmt.getUserIdentity();

        createUserSql = "CREATE USER 'test2' IDENTIFIED BY ''";
        createUserStmt = (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        authenticationManager.createUser(createUserStmt);

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "create role test_role", starRocksAssert.getCtx()), starRocksAssert.getCtx());
    }

    private static void verifyGrantRevoke(String sql, String grantSql, String revokeSql,
                                          String expectError) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctxToRoot();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        // 1. before grant: access denied
        ctxToTestUser();
        try {
            Authorizer.check(statement, ctx);
            Assert.fail();
        } catch (AccessDeniedException e) {
            System.out.println(e.getMessage() + ", sql: " + sql);
            Assert.assertTrue(e.getMessage().contains(expectError));
        }

        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql, ctx), ctx);

        ctxToTestUser();
        Authorizer.check(statement, ctx);

        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(revokeSql, ctx), ctx);

        ctxToTestUser();
        try {
            Authorizer.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (AccessDeniedException e) {
            System.out.println(e.getMessage() + ", sql: " + sql);
            Assert.assertTrue(e.getMessage().contains(expectError));
        }
    }

    private static void verifyMultiGrantRevoke(String sql, List<String> grantSqls, List<String> revokeSqls,
                                               String expectError) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());

        // 1. before grant: access denied
        ctxToTestUser();
        try {
            Authorizer.check(statement, ctx);
            Assert.fail();
        } catch (AccessDeniedException e) {
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
        Authorizer.check(statement, ctx);

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
            Authorizer.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (AccessDeniedException e) {
            System.out.println(e.getMessage() + ", sql: " + sql);
            Assert.assertTrue(e.getMessage().contains(expectError));
        }
    }

    private static void verifyNODEAndGRANT(String sql, String expectError) throws Exception {
        ctxToRoot();
        ConnectContext ctx = starRocksAssert.getCtx();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        // user 'root' has GRANT/NODE privilege
        Authorizer.check(statement, starRocksAssert.getCtx());

        try {
            ctxToTestUser();
            // user 'test' not has GRANT/NODE privilege
            Authorizer.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (AccessDeniedException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains(expectError));
        }
    }

    private static void checkOperateLoad(String sql) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();

        // check resoure privilege
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        try {
            Authorizer.check(statement, ctx);
            Assert.fail();
        } catch (AccessDeniedException e) {
            System.out.println(e.getMessage() + ", sql: " + sql);
            Assert.assertTrue(e.getMessage().contains(
                    "USAGE denied to user 'test'@'localhost' for resource '[my_spark]'"
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
        Authorizer.check(
                UtFrameUtils.parseStmtWithNewParser("use 'catalog default_catalog'", ctx), ctx);
        try {
            Authorizer.check(
                    UtFrameUtils.parseStmtWithNewParser("use 'catalog test_ex_catalog'", ctx), ctx);
        } catch (AccessDeniedException e) {
            Assert.assertTrue(e.getMessage().contains("Access denied;"));
        }
        verifyGrantRevoke(
                "use 'catalog test_ex_catalog'",
                "grant USAGE on catalog test_ex_catalog to test",
                "revoke USAGE on catalog test_ex_catalog from test",
                "Access denied;");
        verifyGrantRevoke(
                "use 'catalog test_ex_catalog'",
                "grant DROP on catalog test_ex_catalog to test",
                "revoke DROP on catalog test_ex_catalog from test",
                "Access denied; you need (at least one of) the ANY IN CATALOG");

        // check create external catalog: CREATE EXTERNAL CATALOG on system object
        verifyGrantRevoke(
                "create external catalog test_ex_catalog2 properties (\"type\"=\"iceberg\")",
                "grant CREATE EXTERNAL CATALOG on system to test",
                "revoke CREATE EXTERNAL CATALOG on system from test",
                "Access denied; you need (at least one of) the CREATE EXTERNAL CATALOG privilege(s)");

        // check drop external catalog: DROP on catalog
        verifyGrantRevoke(
                "drop catalog test_ex_catalog",
                "grant DROP on catalog test_ex_catalog to test",
                "revoke DROP on catalog test_ex_catalog from test",
                "Access denied; you need (at least one of) the DROP privilege(s)");

        // check show catalogs only show catalog where the user has any privilege on
        starRocksAssert.withCatalog("create external catalog test_ex_catalog3 properties (\"type\"=\"iceberg\")");
        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant DROP on catalog test_ex_catalog3 to test", ctx), ctx);
        ctxToTestUser();
        ShowResultSet res = new ShowExecutor(ctx,
                (ShowStmt) UtFrameUtils.parseStmtWithNewParser("SHOW catalogs", ctx)).execute();
        System.out.println(res.getResultRows());
        Assert.assertEquals(2, res.getResultRows().size());
        Assert.assertEquals("test_ex_catalog3", res.getResultRows().get(1).get(0));
    }

    @Test
    public void testSelectCatalogTable() throws Exception {
        ctxToTestUser();
        try {
            StmtExecutor stmtExecutor = new StmtExecutor(starRocksAssert.getCtx(), "select * from hive0.tpch.region");
            stmtExecutor.execute();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Access denied;"));
        }

        ctxToRoot();
        StmtExecutor stmtExecutor = new StmtExecutor(starRocksAssert.getCtx(), "set catalog hive0");
        stmtExecutor.execute();
        grantRevokeSqlAsRoot("grant SELECT on tpch.region to test");
        ctxToTestUser();
        try {
            stmtExecutor = new StmtExecutor(starRocksAssert.getCtx(), "select * from hive0.tpch.region");
            stmtExecutor.execute();
        } catch (Exception e) {
            Assert.assertFalse(e.getMessage().contains("Access denied;"));
        }
        ctxToRoot();
        grantRevokeSqlAsRoot("revoke SELECT on tpch.region from test");
        stmtExecutor = new StmtExecutor(starRocksAssert.getCtx(), "set catalog default_catalog");
        stmtExecutor.execute();
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
        Authorizer.check(statement, ctx);

        // test drop resource group
        verifyGrantRevoke(
                "drop resource group rg3",
                "grant DROP on resource group rg3 to test",
                "revoke DROP on resource group rg3 from test",
                "Access denied; you need (at least one of) the DROP privilege(s) on RESOURCE GROUP rg3 " +
                        "for this operation");

        String sql = "" +
                "ALTER RESOURCE GROUP rg3 \n" +
                "ADD \n" +
                "   (user='rg1_user5', role='rg1_role5', source_ip='192.168.4.1/16')";
        // test drop resource group
        verifyGrantRevoke(
                sql,
                "grant ALTER on resource group rg3 to test",
                "revoke ALTER on resource group rg3 from test",
                "Access denied; you need (at least one of) the ALTER privilege(s)");

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
                "grant create resource group on system to test",
                "revoke create resource group on system from test",
                "Access denied; you need (at least one of) the CREATE RESOURCE GROUP privilege(s)");

        // test grant/revoke on all resource groups
        verifyGrantRevoke(
                sql,
                "grant ALTER on all resource groups to test",
                "revoke ALTER on all resource groups from test",
                "Access denied; you need (at least one of) the ALTER privilege(s)");
    }

    @Test
    public void testAnalyzeStatements() throws Exception {
        // check analyze table: SELECT + INSERT on table
        verifyGrantRevoke(
                "analyze table db1.tbl1",
                "grant SELECT,INSERT on db1.tbl1 to test",
                "revoke SELECT,INSERT on db1.tbl1 from test",
                "Access denied; you need (at least one of) the SELECT privilege(s) on TABLE tbl1 for this operation");

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
                "Access denied; you need (at least one of) the SELECT privilege(s)");

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
                "Access denied; you need (at least one of) the SELECT privilege(s)");

        // check create analyze table xxx: need SELECT + INSERT on the table
        verifyMultiGrantRevoke(
                "create analyze table db1.tbl1;",
                Arrays.asList(
                        "grant SELECT,INSERT on db1.tbl1 to test"
                ),
                Arrays.asList(
                        "revoke SELECT,INSERT on db1.tbl1 from test"
                ),
                "Access denied; you need (at least one of) the SELECT privilege(s)");

        // check drop stats xxx: SELECT + INSERT on table
        verifyGrantRevoke(
                "drop stats db1.tbl1",
                "grant SELECT,INSERT on db1.tbl1 to test",
                "revoke SELECT,INSERT on db1.tbl1 from test",
                "Access denied; you need (at least one of) the SELECT privilege(s)");

        // check analyze table xxx drop histogram on xxx_col: SELECT + INSERT on table
        verifyGrantRevoke(
                "analyze table db1.tbl1 drop histogram on k1",
                "grant SELECT,INSERT on db1.tbl1 to test",
                "revoke SELECT,INSERT on db1.tbl1 from test",
                "Access denied; you need (at least one of) the SELECT privilege(s)");
    }

    @Test
    public void testShowAnalyzeJobStatement() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant DROP on db1.tbl1 to test", ctx), ctx);
        ctxToTestUser();
        AnalyzeMgr analyzeManager = GlobalStateMgr.getCurrentAnalyzeMgr();
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
            new StmtExecutor(ctx, "").checkPrivilegeForKillAnalyzeStmt(ctx, analyzeJob.getId());
        } catch (AccessDeniedException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("Access denied;"));
        }
        grantRevokeSqlAsRoot("grant SELECT,INSERT on db2.tbl1 to test");
        new StmtExecutor(ctx, "").checkPrivilegeForKillAnalyzeStmt(ctx, analyzeJob.getId());
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
            new StmtExecutor(ctx, "").checkPrivilegeForKillAnalyzeStmt(ctx, analyzeJob.getId());
        } catch (AccessDeniedException e) {
            Assert.assertTrue(e.getMessage().contains("Access denied;"));
        }
        grantRevokeSqlAsRoot("grant SELECT,INSERT on db1.tbl2 to test");
        new StmtExecutor(ctx, "").checkPrivilegeForKillAnalyzeStmt(ctx, analyzeJob.getId());
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
            new StmtExecutor(ctx, "").checkPrivilegeForKillAnalyzeStmt(ctx, analyzeJob.getId());
        } catch (AccessDeniedException e) {
            Assert.assertTrue(e.getMessage().contains("Access denied;"));
        }
        grantRevokeSqlAsRoot("grant SELECT,INSERT on db1.tbl1 to test");
        new StmtExecutor(ctx, "").checkPrivilegeForKillAnalyzeStmt(ctx, analyzeJob.getId());
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
        AnalyzeMgr analyzeManager = GlobalStateMgr.getCurrentAnalyzeMgr();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db1 = globalStateMgr.getDb("db1");
        Table tbl1 = db1.getTable("tbl1");

        AnalyzeStatus analyzeStatus = new NativeAnalyzeStatus(1, db1.getId(), tbl1.getId(),
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
        new StmtExecutor(ctx, "").checkPrivilegeForKillAnalyzeStmt(ctx, analyzeStatus.getId());
        grantRevokeSqlAsRoot("revoke SELECT,INSERT on db1.tbl1 from test");

        try {
            new StmtExecutor(ctx, "").checkPrivilegeForKillAnalyzeStmt(ctx, analyzeStatus.getId());
        } catch (AccessDeniedException e) {
            Assert.assertTrue(e.getMessage().contains("Access denied;"));
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
        analyzeStatus = new NativeAnalyzeStatus(1, db2.getId(), tbl1.getId(),
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

    @Test
    public void testTableSelectDeleteInsert() throws Exception {
        verifyGrantRevoke(
                "select * from db1.tbl1",
                "grant select on db1.tbl1 to test",
                "revoke select on db1.tbl1 from test",
                "Access denied; you need (at least one of) the SELECT privilege(s) on TABLE tbl1 for this operation");
        verifyGrantRevoke(
                "insert into db1.tbl1 values ('2020-03-23', 'petals', 'on', 'a', 99);",
                "grant insert on db1.tbl1 to test",
                "revoke insert on db1.tbl1 from test",
                "Access denied; you need (at least one of) the INSERT privilege(s) on TABLE tbl1 for this operation");
        verifyGrantRevoke(
                "delete from db1.tbl1 where k3 = 1",
                "grant delete on db1.tbl1 to test",
                "revoke delete on db1.tbl1 from test",
                "Access denied; you need (at least one of) the DELETE privilege(s) on TABLE tbl1 for this operation");
        verifyGrantRevoke(
                "update db1.tbl1 set k4 = 2 where k3 = 1",
                "grant update on db1.tbl1 to test",
                "revoke update on db1.tbl1 from test",
                "Access denied; you need (at least one of) the UPDATE privilege(s) on TABLE tbl1 for this operation");
        verifyGrantRevoke(
                "select k1, k2 from db3.mv1",
                "grant select on materialized view db3.mv1 to test",
                "revoke select on materialized view db3.mv1 from test",
                "Access denied; you need (at least one of) the SELECT privilege(s) on MATERIALIZED VIEW mv1 " +
                        "for this operation");
    }

    @Test
    public void testTableCreateDrop() throws Exception {
        String createTableSql = "create table db1.tbl22(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        verifyGrantRevoke(
                createTableSql,
                "grant create table on database db1 to test",
                "revoke create table on database db1 from test",
                "Access denied; you need (at least one of) the CREATE TABLE");
        verifyGrantRevoke(
                "drop table db1.tbl1",
                "grant drop on db1.tbl1 to test",
                "revoke drop on db1.tbl1 from test",
                "Access denied; you need (at least one of) the DROP privilege(s) on TABLE tbl1 for this operation");

        // check CTAS: create table on db and SELECT on source table
        String createTableAsSql = "create table db1.ctas_t1 as select k1,k2 from db1.tbl1;";
        verifyMultiGrantRevoke(
                createTableAsSql,
                Arrays.asList(
                        "grant create table on database db1 to test",
                        "grant select on table db1.tbl1 to test"
                ),
                Arrays.asList(
                        "revoke create table on database db1 from test",
                        "revoke select on table db1.tbl1 from test"
                ),
                "Access denied; you need (at least one of) the CREATE TABLE privilege(s) on DATABASE db1 " +
                        "for this operation");
        // check CTAS: don't need 'INSERT' priv for InsertStmt created by CTAS statement
        ConnectContext ctx = starRocksAssert.getCtx();
        CreateTableAsSelectStmt createTableAsSelectStmt = (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(
                "create table db1.ctas_t2 as select k1,k2 from db1.tbl1", ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant select on TABLE db1.tbl1 to test", starRocksAssert.getCtx()), starRocksAssert.getCtx());
        Authorizer.check(createTableAsSelectStmt.getInsertStmt(), ctx);

        // check create table like: create table on db and SELECT on existed table
        String createTableLikeSql = "create table db1.like_tbl like db1.tbl1;";
        verifyMultiGrantRevoke(
                createTableLikeSql,
                Arrays.asList(
                        "grant create table on database db1 to test",
                        "grant select on table db1.tbl1 to test"
                ),
                Arrays.asList(
                        "revoke create table on database db1 from test",
                        "revoke select on table db1.tbl1 from test"
                ),
                "Access denied; you need (at least one of) the CREATE TABLE privilege(s) on DATABASE db1 " +
                        "for this operation");

        // check recover table: create table on db and DROP on dropped table
        verifyMultiGrantRevoke(
                "recover table db1.tbl1",
                Arrays.asList(
                        "grant create table on database db1 to test",
                        "grant drop on table db1.tbl1 to test"
                ),
                Arrays.asList(
                        "revoke create table on database db1 from test",
                        "revoke drop on table db1.tbl1 from test"
                ),
                "Access denied; you need (at least one of) the CREATE TABLE privilege(s) on DATABASE db1 for " +
                        "this operation");

        // check refresh external table: ALTER
        verifyGrantRevoke(
                "refresh external table db1.tbl1",
                "grant ALTER on db1.tbl1 to test",
                "revoke ALTER on db1.tbl1 from test",
                "Access denied; you need (at least one of) the ALTER privilege(s) on TABLE tbl1 for this operation");

        // check alter table: ALTER
        verifyGrantRevoke(
                "alter table db1.tbl1 drop partition p1",
                "grant ALTER on db1.tbl1 to test",
                "revoke ALTER on db1.tbl1 from test",
                "Access denied; you need (at least one of) the ALTER privilege(s) on TABLE tbl1 for this operation");

        // check cancel alter table: ALTER
        verifyGrantRevoke(
                "cancel alter table rollup from db1.tbl1 (1, 2, 3)",
                "grant ALTER on db1.tbl1 to test",
                "revoke ALTER on db1.tbl1 from test",
                "Access denied; you need (at least one of) the ALTER privilege(s) on TABLE tbl1 for this operation");

        List<String> sqls = Arrays.asList(
                "desc db1.tbl1",
                "show create table db1.tbl1",
                "show columns from db1.tbl1",
                "show partitions from db1.tbl1"
        );
        for (String sql : sqls) {
            // check describe table: any privilege
            verifyGrantRevoke(
                    sql,
                    "grant SELECT on db1.tbl1 to test",
                    "revoke SELECT on db1.tbl1 from test",
                    "Access denied; you need (at least one of) the ANY privilege(s) on TABLE tbl1");
            verifyGrantRevoke(
                    sql,
                    "grant DELETE on db1.tbl1 to test",
                    "revoke DELETE on db1.tbl1 from test",
                    "Access denied;");
        }

        // check show table status: only return table user has any privilege on
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser("show table status from db1", ctx);
        grantRevokeSqlAsRoot("grant SELECT on db1.tbl2 to test");
        ctxToTestUser();
        ShowExecutor showExecutor = new ShowExecutor(ctx, (ShowStmt) statement);
        ShowResultSet showResultSet = showExecutor.execute();
        grantRevokeSqlAsRoot("revoke SELECT on db1.tbl2 from test");
        List<List<String>> resultRows = showResultSet.getResultRows();
        System.out.println(resultRows);
        Assert.assertEquals(1, resultRows.size());
        Assert.assertEquals("tbl2", resultRows.get(0).get(0));

        // check recover partition: create table on db and DROP on dropped table
        verifyMultiGrantRevoke(
                "recover partition p1 from db1.tbl1",
                Arrays.asList(
                        "grant INSERT on table db1.tbl1 to test",
                        "grant ALTER on table db1.tbl1 to test"
                ),
                Arrays.asList(
                        "revoke INSERT on table db1.tbl1 from test",
                        "revoke ALTER on table db1.tbl1 from test"
                ),
                "Access denied;");

        // check CTAS: create table on db and SELECT on source table
        List<String> submitTaskSqls = Arrays.asList(
                "submit task as create table db1.ctas_t1 as select k1,k2 from db1.tbl1;",
                "submit task as create table ctas_t11 as select k1,k2 from tbl1;" // test unqualified name
        );
        ctx.setDatabase("db1");
        for (String submitTaskSql : submitTaskSqls) {
            verifyMultiGrantRevoke(
                    submitTaskSql,
                    Arrays.asList(
                            "grant create table on database db1 to test",
                            "grant select on table db1.tbl1 to test"
                    ),
                    Arrays.asList(
                            "revoke create table on database db1 from test",
                            "revoke select on table db1.tbl1 from test"
                    ),
                    "Access denied;");
        }

        // check drop non-existed table
        statement = UtFrameUtils.parseStmtWithNewParser(
                "drop table if exists db1.tbl_not_exist1", ctx);
        ctxToRoot();
        DDLStmtExecutor.execute(statement, ctx);
        Authorizer.check(statement, ctx);
    }

    @Test
    public void testShowDynamicPartitionTables() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser("SHOW DYNAMIC PARTITION TABLES from db1", ctx);
        grantRevokeSqlAsRoot("grant SELECT on db1.tbl1 to test");
        ctxToTestUser();
        ShowExecutor showExecutor = new ShowExecutor(ctx, (ShowStmt) statement);
        ShowResultSet showResultSet = showExecutor.execute();
        grantRevokeSqlAsRoot("revoke SELECT on db1.tbl1 from test");
        List<List<String>> resultRows = showResultSet.getResultRows();
        System.out.println(resultRows);
        Assert.assertEquals(1, resultRows.size());
        Assert.assertEquals("tbl1", resultRows.get(0).get(0));
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
                "revoke select on db1.tbl1 from test",
                "Access denied; you need (at least one of) the GRANT privilege(s) for this operation");
    }

    @Test
    public void testResourceStmt() throws Exception {
        String createResourceStmt = "create external resource 'hive0' PROPERTIES(" +
                "\"type\"  =  \"hive\", \"hive.metastore.uris\"  =  \"thrift://127.0.0.1:9083\")";
        String createResourceStmt1 = "create external resource 'hive1' PROPERTIES(" +
                "\"type\"  =  \"hive\", \"hive.metastore.uris\"  =  \"thrift://127.0.0.1:9084\")";
        verifyGrantRevoke(
                createResourceStmt,
                "grant create resource on system to test",
                "revoke create resource on system from test",
                "Access denied; you need (at least one of) the CREATE RESOURCE privilege(s)");
        starRocksAssert.withResource(createResourceStmt);
        starRocksAssert.withResource(createResourceStmt1);

        verifyGrantRevoke(
                "alter RESOURCE hive0 SET PROPERTIES (\"hive.metastore.uris\" = \"thrift://10.10.44.91:9083\");",
                "grant alter on resource 'hive0' to test",
                "revoke alter on resource 'hive0' from test",
                "Access denied; you need (at least one of) the ALTER privilege(s)");

        verifyGrantRevoke(
                "drop resource hive0;",
                "grant drop on resource hive0 to test",
                "revoke drop on resource hive0 from test",
                "Access denied; you need (at least one of) the DROP privilege(s)");

        // on all
        verifyGrantRevoke(
                "drop resource hive0;",
                "grant drop on all resources to test",
                "revoke drop on all resources from test",
                "Access denied; you need (at least one of) the DROP privilege(s)");

        // check show resources only show resource the user has any privilege on
        grantRevokeSqlAsRoot("grant alter on resource 'hive1' to test");
        ctxToTestUser();
        List<List<String>> results = GlobalStateMgr.getCurrentState().getResourceMgr().getResourcesInfo();
        grantRevokeSqlAsRoot("revoke alter on resource 'hive1' from test");
        System.out.println(results);
        Assert.assertTrue(results.size() > 0);
        Assert.assertTrue(results.stream().anyMatch(m -> m.contains("hive1")));
        Assert.assertFalse(results.stream().anyMatch(m -> m.contains("hive0")));
    }

    @Test
    public void testShowProcessList(@Mocked MysqlChannel channel,
                                    @Mocked SocketChannel socketChannel) throws Exception {
        new Expectations() {
            {
                channel.getRemoteHostPortString();
                minTimes = 0;
                result = "127.0.0.1:12345";

                channel.close();
                minTimes = 0;

                channel.getRemoteIp();
                minTimes = 0;
                result = "192.168.1.1";
            }
        };

        ConnectContext ctx1 = new ConnectContext(socketChannel);
        ctx1.setQualifiedUser("test");
        ctx1.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx1.setConnectionId(1);
        ConnectContext ctx2 = new ConnectContext(socketChannel);
        ctx2.setQualifiedUser("test2");
        ctx2.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx2.setConnectionId(2);

        // State
        Assert.assertNotNull(ctx1.getState());

        ConnectScheduler connectScheduler = new ConnectScheduler(Config.qe_max_connection);
        connectScheduler.registerConnection(ctx1);
        connectScheduler.registerConnection(ctx2);

        // Without operate privilege on system, test can only see its own process list
        ctxToTestUser();
        List<ConnectContext.ThreadInfo> results = connectScheduler.listConnection(starRocksAssert.getCtx(), "test");
        long nowMs = System.currentTimeMillis();
        for (ConnectContext.ThreadInfo threadInfo : results) {
            System.out.println(threadInfo.toRow(nowMs, true));
        }
        Assert.assertEquals(1, results.size());
        Assert.assertEquals("test", results.get(0).toRow(nowMs, true).get(1));

        // With operate privilege on system, test can only see all the process list
        grantRevokeSqlAsRoot("grant operate on system to test");
        results = connectScheduler.listConnection(starRocksAssert.getCtx(), "test");
        for (ConnectContext.ThreadInfo threadInfo : results) {
            System.out.println(threadInfo.toRow(nowMs, true));
        }
        Assert.assertEquals(2, results.size());
        Assert.assertEquals("test2", results.get(01).toRow(nowMs, true).get(1));
        grantRevokeSqlAsRoot("revoke operate on system from test");
    }

    @Test
    public void testViewStmt() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();

        // grant select on base table to user
        String grantBaseTableSql = "grant select on db1.tbl1 to test";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                grantBaseTableSql, ctx), ctx);

        // privilege check for create view on database
        String createViewStmt = "create view db1.view1 as select * from db1.tbl1";
        String grantCreateViewStmt = "grant create view on database db1 to test";
        String revokeCreateViewStmt = "revoke create view on database db1 from test";
        verifyGrantRevoke(
                createViewStmt,
                grantCreateViewStmt,
                revokeCreateViewStmt,
                "Access denied; you need (at least one of) the CREATE VIEW privilege(s) on " +
                        "DATABASE db1 for this operation");

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
                "Access denied; you need (at least one of) the SELECT privilege(s) on TABLE tbl1 for this operation");

        // create the view
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(createViewStmt, ctx), ctx);

        // revoke create view on database, grant select on base table to user
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
                "Access denied; you need (at least one of) the ALTER privilege(s) on VIEW view1 for this operation");

        // revoke select on base table, grant alter on view to user
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                revokeBaseTableSql, ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                grantAlterSql, ctx), ctx);
        verifyGrantRevoke(
                alterViewSql,
                grantBaseTableSql,
                revokeBaseTableSql,
                "Access denied;");

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                revokeAlterSql, ctx), ctx);

        // test select view
        String selectViewSql = "select * from db1.view1";

        verifyGrantRevoke(
                selectViewSql,
                "grant select on view db1.view1 to test",
                "revoke select on view db1.view1 from test",
                "Access denied;");

        // drop view
        verifyGrantRevoke(
                "drop view db1.view1",
                "grant drop on view db1.view1 to test",
                "revoke drop on view db1.view1 from test",
                "Access denied;");
    }

    @Test
    public void testPluginStmts() throws Exception {
        String grantSql = "grant plugin on system to test";
        String revokeSql = "revoke plugin on system from test";
        String err = "Access denied; you need (at least one of) the PLUGIN privilege(s) on SYSTEM for this operation";

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
                "Access denied; you need (at least one of) the FILE privilege(s) on SYSTEM for this operation");
        verifyGrantRevoke(
                dropFileSql,
                "grant file on system to test",
                "revoke file on system from test",
                "Access denied; you need (at least one of) the FILE privilege(s) on SYSTEM for this operation");

        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        String revokeSelectTableSql = "revoke select on db1.tbl1 from test";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(revokeSelectTableSql, ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant file on system to test", ctx), ctx);

        // check any action in table
        String dbDeniedError = "Access denied;";
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
        String err = "Access denied; you need (at least one of) the BLACKLIST privilege(s) on SYSTEM for this operation";

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
        String err = "Access denied; you need (at least one of) the GRANT privilege(s) on SYSTEM for this operation";
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

        sql = "alter role testrole2 set comment=\"yi shan yi shan, liang jing jing\"";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);

        sql = "drop role test_role";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);
    }

    @Test
    public void testShowPrivsForOther() throws Exception {
        String grantSql = "grant user_admin to test";
        String revokeSql = "revoke user_admin from test";
        String err = "Access denied; you need (at least one of) the GRANT privilege(s) on SYSTEM for this operation";
        String sql;

        ConnectContext ctx = starRocksAssert.getCtx();

        sql = "show grants for test2";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);
        ctxToTestUser();
        Authorizer.check(UtFrameUtils.parseStmtWithNewParser("show grants", ctx), ctx);

        sql = "show authentication for test2";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);
        sql = "show all authentication";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);
        ctxToTestUser();
        Authorizer.check(UtFrameUtils.parseStmtWithNewParser("show authentication", ctx), ctx);

        sql = "SHOW PROPERTY FOR 'test2'";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);
        ctxToTestUser();
        Authorizer.check(UtFrameUtils.parseStmtWithNewParser("show property", ctx), ctx);

        sql = "set property for 'test2' 'max_user_connections' = '100'";
        verifyGrantRevoke(sql, grantSql, revokeSql, err);
        ctxToTestUser();
        Authorizer.check(UtFrameUtils.parseStmtWithNewParser(
                "set property 'max_user_connections' = '100'", ctx), ctx);
    }

    @Test
    public void testSetGlobalVar() throws Exception {
        ctxToRoot();
        verifyGrantRevoke(
                "SET global enable_cbo = true",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied; you need (at least one of) the OPERATE privilege(s) on SYSTEM for this operation");
    }

    @Test
    public void testExecuteAs() throws Exception {
        verifyGrantRevoke(
                "EXECUTE AS test2 WITH NO REVERT",
                "grant impersonate on user test2 to test",
                "revoke impersonate on user test2 from test",
                "Access denied; you need (at least one of) the IMPERSONATE privilege(s) on " +
                        "USER test2 for this operation");
    }

    // Temporarily switch to root and grant privileges to a user, then switch to normal user context
    private void grantRevokeSqlAsRoot(String grantSql) throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql, ctx), ctx);
        ctxToTestUser();
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
                    "Access denied");
            verifyGrantRevoke(
                    stmt,
                    "grant CREATE FUNCTION on database " + testDbName + " to test",
                    "revoke CREATE FUNCTION on database " + testDbName + " from test",
                    "Access denied");
            verifyGrantRevoke(
                    stmt,
                    "grant ALTER on database " + testDbName + " to test",
                    "revoke ALTER on database " + testDbName + " from test",
                    "Access denied");
        }

        // Test `use database` : check any privilege on tables in db
        verifyGrantRevoke(
                "use " + testDbName + ";",
                "grant select on " + testDbName + ".tbl1 to test",
                "revoke select on " + testDbName + ".tbl1 from test",
                "Access denied");

        // Test `use database` : check any privilege on any function in db
        Database db1 = GlobalStateMgr.getCurrentState().getDb("db1");
        FunctionName fn = FunctionName.createFnName("db1.my_udf_json_get");
        Function function = new Function(fn, Arrays.asList(Type.STRING, Type.STRING), Type.STRING, false);
        try {
            db1.addFunction(function);
        } catch (Throwable e) {
            // ignore
        }
        verifyGrantRevoke(
                "use db1",
                "grant drop on FUNCTION db1.MY_UDF_JSON_GET(string, string) to test",
                "revoke drop on FUNCTION db1.MY_UDF_JSON_GET(string, string) from test",
                "Access denied");

        // Test `recover database xxx`: check DROP on db and CREATE DATABASE on internal catalog
        verifyMultiGrantRevoke(
                "recover database " + testDbName + ";",
                Arrays.asList(
                        "grant CREATE DATABASE on catalog default_catalog to test"),
                Arrays.asList(
                        "revoke CREATE DATABASE on catalog default_catalog from test;"
                ),
                "Access denied; you need (at least one of) the CREATE DATABASE privilege(s) ");
        grantRevokeSqlAsRoot("grant DROP on database " + testDbName + " to test");
        try {
            verifyMultiGrantRevoke(
                    "recover database " + testDbName + ";",
                    Arrays.asList(
                            "grant DROP on database " + testDbName + " to test"),
                    Arrays.asList(
                            "revoke DROP on database " + testDbName + " from test",
                            "revoke CREATE DATABASE on catalog default_catalog from test;"),
                    "Access denied");
        } catch (AccessDeniedException e) {
            Assert.assertTrue(e.getMessage().contains("Access denied"));
        } finally {
            grantRevokeSqlAsRoot("revoke DROP on database " + testDbName + " from test");
        }

        // Test `alter database xxx set...`: check ALTER on db
        verifyGrantRevoke(
                "alter database " + testDbName + " set data quota 10T;",
                "grant ALTER on database " + testDbName + " to test",
                "revoke ALTER on database " + testDbName + " from test",
                "Access denied");
        verifyGrantRevoke(
                "alter database " + testDbName + " set replica quota 102400;",
                "grant ALTER on database " + testDbName + " to test",
                "revoke ALTER on database " + testDbName + " from test",
                "Access denied");

        // Test `drop database xxx...`: check DROP on db
        verifyGrantRevoke(
                "drop database " + testDbName + ";",
                "grant DROP on database " + testDbName + " to test",
                "revoke DROP on database " + testDbName + " from test",
                "Access denied");
        verifyGrantRevoke(
                "drop database if exists " + testDbName + " force;",
                "grant DROP on database " + testDbName + " to test",
                "revoke DROP on database " + testDbName + " from test",
                "Access denied");

        // Test `alter database xxx rename xxx_new`: check ALTER on db
        verifyGrantRevoke(
                "alter database " + testDbName + " rename new_db_name;",
                "grant ALTER on database " + testDbName + " to test",
                "revoke ALTER on database " + testDbName + " from test",
                "Access denied");

        // Test `create database`: check CREATE DATABASE on catalog
        verifyGrantRevoke(
                "create database db8;",
                "grant CREATE DATABASE on catalog default_catalog to test",
                "revoke CREATE DATABASE on catalog default_catalog from test",
                "Access denied");
        verifyGrantRevoke(
                "create database if not exists db8;",
                "grant CREATE DATABASE on catalog default_catalog to test",
                "revoke CREATE DATABASE on catalog default_catalog from test",
                "Access denied");

        // check drop non-existed database
        ConnectContext ctx = starRocksAssert.getCtx();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(
                "drop database if exists db_not_exist1", ctx);
        ctxToRoot();
        DDLStmtExecutor.execute(statement, ctx);
        Authorizer.check(statement, ctx);
    }

    @Test
    public void testShowNodeStmt() throws Exception {
        verifyGrantRevoke(
                "show backends",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");

        verifyNODEAndGRANT(
                "show backends",
                "Access denied;");

        verifyGrantRevoke(
                "show frontends",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");

        verifyNODEAndGRANT(
                "show frontends",
                "Access denied;");

        verifyGrantRevoke(
                "show broker",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");

        verifyNODEAndGRANT(
                "show broker",
                "Access denied;");

        verifyGrantRevoke(
                "show compute nodes",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");

        verifyNODEAndGRANT(
                "show compute nodes",
                "Access denied;");

    }

    @Test
    public void testShowTabletStmt() throws Exception {
        verifyGrantRevoke(
                "show tablet from example_db.example_table",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");
    }

    @Test
    public void testShowTransactionStmt() throws Exception {
        ctxToTestUser();
        ConnectContext ctx = starRocksAssert.getCtx();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser("SHOW TRANSACTION FROM db WHERE ID=4005;", ctx);
        Authorizer.check(statement, starRocksAssert.getCtx());
    }

    @Test
    public void testAdminOperateStmt() throws Exception {
        // AdminSetConfigStmt
        verifyGrantRevoke(
                "admin set frontend config (\"key\" = \"value\");",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");

        // AdminSetReplicaStatusStmt
        verifyGrantRevoke(
                "ADMIN SET REPLICA STATUS PROPERTIES(\"tablet_id\" = \"10003\", " +
                        "\"backend_id\" = \"10001\", \"status\" = \"bad\");",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");

        // AdminShowConfigStmt
        verifyGrantRevoke(
                "ADMIN SHOW FRONTEND CONFIG;",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");

        // AdminShowReplicaDistributionStatement
        verifyGrantRevoke(
                "ADMIN SHOW REPLICA DISTRIBUTION FROM example_db.example_table PARTITION(p1, p2);",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");

        // AdminShowReplicaStatusStatement
        verifyGrantRevoke(
                "ADMIN SHOW REPLICA STATUS FROM example_db.example_table;",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");

        // AdminRepairTableStatement
        verifyGrantRevoke(
                "ADMIN REPAIR TABLE example_db.example_table PARTITION(p1);",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");

        // AdminCancelRepairTableStatement
        verifyGrantRevoke(
                "ADMIN CANCEL REPAIR TABLE example_db.example_table PARTITION(p1);",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");

        // AdminCheckTabletsStatement
        verifyGrantRevoke(
                "ADMIN CHECK TABLET (1, 2) PROPERTIES (\"type\" = \"CONSISTENCY\");",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");
    }

    @Test
    public void testAlterSystemStmt() throws Exception {
        // AlterSystemStmt
        verifyNODEAndGRANT("ALTER SYSTEM ADD FOLLOWER \"127.0.0.1:9010\";",
                "Access denied; you need (at least one of) the NODE privilege(s) on SYSTEM for this operation");

        // CancelAlterSystemStmt
        verifyNODEAndGRANT("CANCEL DECOMMISSION BACKEND \"127.0.0.1:9010\", \"127.0.0.1:9011\";",
                "Access denied; you need (at least one of) the NODE privilege(s) on SYSTEM for this operation");
    }

    @Test
    public void testKillStmt(@Mocked MysqlChannel channel,
                             @Mocked SocketChannel socketChannel) throws Exception {
        new Expectations() {
            {
                channel.getRemoteHostPortString();
                minTimes = 0;
                result = "127.0.0.1:12345";

                channel.close();
                minTimes = 0;

                channel.getRemoteIp();
                minTimes = 0;
                result = "192.168.1.1";
            }
        };

        new MockUp<ConnectContext>() {
            @Mock
            public void kill(boolean killConnection) {
            }
        };

        ConnectContext ctx1 = new ConnectContext(socketChannel);
        ctx1.setCurrentUserIdentity(new UserIdentity("test", "%"));
        ctx1.setQualifiedUser("test");
        ctx1.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx1.setConnectionId(1);
        ConnectContext ctx2 = new ConnectContext(socketChannel);
        ctx2.setQualifiedUser("test2");
        ctx2.setCurrentUserIdentity(new UserIdentity("test2", "%"));
        ctx2.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx2.setConnectionId(2);

        // State
        Assert.assertNotNull(ctx1.getState());

        ConnectScheduler connectScheduler = new ConnectScheduler(Config.qe_max_connection);
        connectScheduler.registerConnection(ctx1);
        connectScheduler.registerConnection(ctx2);

        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.getState().setOk();
        ConnectScheduler origConnectScheduler = ctx.getConnectScheduler();
        ctx.setConnectScheduler(connectScheduler);
        ctxToTestUser();

        // can kill self without privilege check
        StatementBase killStatement = UtFrameUtils.parseStmtWithNewParser("kill 1", ctx);
        StmtExecutor stmtExecutor = new StmtExecutor(starRocksAssert.getCtx(), killStatement);
        stmtExecutor.execute();
        killStatement = UtFrameUtils.parseStmtWithNewParser("kill query 1", ctx);
        stmtExecutor = new StmtExecutor(starRocksAssert.getCtx(), killStatement);
        stmtExecutor.execute();

        // cannot kill other user's connection/query without 'OPERATE' privilege on 'SYSTEM'
        killStatement = UtFrameUtils.parseStmtWithNewParser("kill 2", ctx);
        stmtExecutor = new StmtExecutor(starRocksAssert.getCtx(), killStatement);
        stmtExecutor.execute();
        Assert.assertTrue(ctx.getState().isError());
        System.out.println(ctx.getState().getErrorMessage());
        Assert.assertTrue(ctx.getState().getErrorMessage().contains(
                "Access denied;"));


        // can kill other user's connection/query after privilege granted
        grantRevokeSqlAsRoot("grant OPERATE on system to test");
        killStatement = UtFrameUtils.parseStmtWithNewParser("kill 2", ctx);
        stmtExecutor = new StmtExecutor(starRocksAssert.getCtx(), killStatement);
        stmtExecutor.execute();
        grantRevokeSqlAsRoot("revoke OPERATE on system from test");

        // reset state
        ctx.setConnectScheduler(origConnectScheduler);
    }

    @Test
    public void testShowProcStmt() throws Exception {
        // ShowProcStmt
        verifyGrantRevoke(
                "show proc '/backends'",
                "grant OPERATE on system to test",
                "revoke OPERATE on system from test",
                "Access denied;");
    }

    @Test
    public void testSetStmt() throws Exception {
        String sql = "SET PASSWORD FOR 'jack'@'192.%' = PASSWORD('123456');";
        String expectError = "Access denied; you need (at least one of) the GRANT privilege(s) on SYSTEM for this operation";
        verifyNODEAndGRANT(sql, expectError);

        ctxToTestUser();
        // user 'test' not has GRANT/NODE privilege
        sql = "set password = PASSWORD('123456')";
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Authorizer.check(statement, starRocksAssert.getCtx());

        sql = "set password for test = PASSWORD('123456')";
        statement = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        Authorizer.check(statement, starRocksAssert.getCtx());
    }

    @Test
    public void testRoutineLoadStmt() throws Exception {
        // CREATE ROUTINE LOAD STMT
        String createSql = "CREATE ROUTINE LOAD db1.job_name2 ON tbl1 " +
                "COLUMNS(c1) FROM KAFKA " +
                "( 'kafka_broker_list' = 'broker1:9092', 'kafka_topic' = 'my_topic', " +
                " 'kafka_partitions' = '0,1,2', 'kafka_offsets' = '0,0,0');";
        verifyGrantRevoke(
                createSql,
                "grant insert on db1.tbl1 to test",
                "revoke insert on db1.tbl1 from test",
                "Access denied; you need (at least one of) the INSERT privilege(s) on TABLE tbl1 for this operation");

        // ALTER ROUTINE LOAD STMT
        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties) {
                return Lists.newArrayList(0, 1, 2);
            }
        };
        String alterSql = "ALTER ROUTINE LOAD FOR db1.job_name2 PROPERTIES ( 'desired_concurrent_number' = '1')";
        ConnectContext ctx = starRocksAssert.getCtx();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(alterSql, starRocksAssert.getCtx());
        try {
            Authorizer.check(statement, ctx);
            Assert.fail();
        } catch (SemanticException e) {
            System.out.println(e.getMessage() + ", sql: " + alterSql);
            Assert.assertTrue(
                    e.getMessage().contains("Routine load job [job_name2] not found when checking privilege"));
        }
        ctxToRoot();
        starRocksAssert.withRoutineLoad(createSql);
        ctxToTestUser();
        verifyGrantRevoke(
                "ALTER ROUTINE LOAD FOR db1.job_name1 PROPERTIES ( 'desired_concurrent_number' = '1');",
                "grant insert on db1.tbl1 to test",
                "revoke insert on db1.tbl1 from test",
                "Access denied; you need (at least one of) the INSERT privilege(s) on TABLE tbl1 for this operation");

        // STOP ROUTINE LOAD STMT
        verifyGrantRevoke(
                "STOP ROUTINE LOAD FOR db1.job_name1;",
                "grant insert on db1.tbl1 to test",
                "revoke insert on db1.tbl1 from test",
                "Access denied; you need (at least one of) the INSERT privilege(s) on TABLE tbl1 for this operation");

        // RESUME ROUTINE LOAD STMT
        verifyGrantRevoke(
                "RESUME ROUTINE LOAD FOR db1.job_name1;",
                "grant insert on db1.tbl1 to test",
                "revoke insert on db1.tbl1 from test",
                "Access denied; you need (at least one of) the INSERT privilege(s) on TABLE tbl1 for this operation");

        // PAUSE ROUTINE LOAD STMT
        verifyGrantRevoke(
                "PAUSE ROUTINE LOAD FOR db1.job_name1;",
                "grant insert on db1.tbl1 to test",
                "revoke insert on db1.tbl1 from test",
                "Access denied; you need (at least one of) the INSERT privilege(s) on TABLE tbl1 for this operation");

        // SHOW ROUTINE LOAD stmt;
        String showRoutineLoadSql = "SHOW ROUTINE LOAD FOR db1.job_name1;";
        statement = UtFrameUtils.parseStmtWithNewParser(showRoutineLoadSql, starRocksAssert.getCtx());
        Authorizer.check(statement, ctx);

        // SHOW ROUTINE LOAD TASK FROM DB
        String showRoutineLoadTaskSql = "SHOW ROUTINE LOAD TASK FROM db1 WHERE JobName = 'job_name1';";
        statement = UtFrameUtils.parseStmtWithNewParser(showRoutineLoadTaskSql, starRocksAssert.getCtx());
        Authorizer.check(statement, ctx);
    }

    @Test
    public void testRoutineLoadShowStmt() throws Exception {
        ctxToRoot();
        String createSql = "CREATE ROUTINE LOAD db1.job_name1 ON tbl1 " +
                "COLUMNS(c1) FROM KAFKA " +
                "( 'kafka_broker_list' = 'broker1:9092', 'kafka_topic' = 'my_topic', " +
                " 'kafka_partitions' = '0,1,2', 'kafka_offsets' = '0,0,0');";
        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties) {
                return Lists.newArrayList(0, 1, 2);
            }
        };
        starRocksAssert.withRoutineLoad(createSql);

        String showRoutineLoadTaskSql = "SHOW ROUTINE LOAD TASK FROM db1 WHERE JobName = 'job_name1';";
        StatementBase statementTask =
                UtFrameUtils.parseStmtWithNewParser(showRoutineLoadTaskSql, starRocksAssert.getCtx());
        ShowExecutor executor = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) statementTask);
        ShowResultSet set = executor.execute();
        for (int i = 0; i < 30; i++) {
            set = executor.execute();
            if (set.getResultRows().size() > 0) {
                break;
            } else {
                Thread.sleep(1000);
            }
        }
        Assert.assertTrue(set.getResultRows().size() > 0);

        ctxToTestUser();
        // SHOW ROUTINE LOAD TASK
        ShowExecutor executorBeforeGrant = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) statementTask);
        set = executorBeforeGrant.execute();
        Assert.assertEquals(0, set.getResultRows().size());
        ctxToRoot();
        DDLStmtExecutor.execute(
                UtFrameUtils.parseStmtWithNewParser("grant insert on db1.tbl1 to test", starRocksAssert.getCtx()),
                starRocksAssert.getCtx());
        ctxToTestUser();
        ShowExecutor executorAfterGrant = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) statementTask);
        set = executorAfterGrant.execute();
        Assert.assertTrue(set.getResultRows().size() > 0);
        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("revoke insert on db1.tbl1 from test",
                        starRocksAssert.getCtx()),
                starRocksAssert.getCtx());
        ctxToTestUser();
    }

    @Test
    public void testLoadStmt() throws Exception {
        // LOAD STMT
        // create resource
        String createResourceStmt = "CREATE EXTERNAL RESOURCE \"my_spark\"" +
                "PROPERTIES (" +
                "\"type\" = \"spark\"," +
                "\"spark.master\" = \"yarn\", " +
                "\"spark.submit.deployMode\" = \"cluster\", " +
                "\"spark.executor.memory\" = \"1g\", " +
                "\"spark.yarn.queue\" = \"queue0\", " +
                "\"spark.hadoop.yarn.resourcemanager.address\" = \"resourcemanager_host:8032\", " +
                "\"spark.hadoop.fs.defaultFS\" = \"hdfs://namenode_host:9000\", " +
                "\"working_dir\" = \"hdfs://namenode_host:9000/tmp/starrocks\", " +
                "\"broker\" = \"broker0\", " +
                "\"broker.username\" = \"user0\", " +
                "\"broker.password\" = \"password0\"" +
                ");";
        starRocksAssert.withResource(createResourceStmt);
        // create load & check resource privilege
        String createSql = "LOAD LABEL db1.job_name1" +
                "(DATA INFILE('hdfs://test:8080/user/starrocks/data/input/example1.csv') " +
                "INTO TABLE tbl1) " +
                "WITH RESOURCE 'my_spark'" +
                "('username' = 'test_name','password' = 'pwd') " +
                "PROPERTIES ('timeout' = '3600');";
        ctxToRoot();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(createSql, starRocksAssert.getCtx());
        ctxToTestUser();
        ConnectContext ctx = starRocksAssert.getCtx();
        try {
            Authorizer.check(statement, ctx);
            Assert.fail();
        } catch (AccessDeniedException e) {
            System.out.println(e.getMessage() + ", sql: " + createSql);
            Assert.assertTrue(e.getMessage().contains(
                    "Access denied; you need (at least one of) the USAGE privilege(s) on RESOURCE my_spark for this operation"
            ));
        }
        // create load & check table privilege
        createSql = "LOAD LABEL db1.job_name1" +
                "(DATA INFILE('hdfs://test:8080/user/starrocks/data/input/example1.csv') " +
                "INTO TABLE tbl1) " +
                "WITH BROKER 'my_broker'" +
                "('username' = 'test_name','password' = 'pwd') " +
                "PROPERTIES ('timeout' = '3600');";
        verifyGrantRevoke(
                createSql,
                "grant insert on db1.tbl1 to test",
                "revoke insert on db1.tbl1 from test",
                "INSERT command denied to user 'test'@'localhost' for table '[tbl1]'");

        // create broker load
        createSql = "LOAD LABEL db1.job_name1" +
                "(DATA INFILE('hdfs://test:8080/user/starrocks/data/input/example1.csv') " +
                "INTO TABLE tbl1) " +
                "WITH RESOURCE 'my_spark'" +
                "('username' = 'test_name','password' = 'pwd') " +
                "PROPERTIES ('timeout' = '3600');";
        ctxToRoot();
        starRocksAssert.withLoad(createSql);
        ctxToTestUser();

        // ALTER LOAD STMT
        String alterLoadSql = "ALTER LOAD FOR db1.job_name1 PROPERTIES ('priority' = 'LOW');";
        checkOperateLoad(alterLoadSql);

        // CANCEL LOAD STMT
        ctxToRoot();
        String revokeResource = "revoke USAGE on resource 'my_spark' from test;";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(revokeResource, ctx), ctx);
        ctxToTestUser();
        String cancelLoadSql = "CANCEL LOAD FROM db1 WHERE LABEL = 'job_name1'";
        checkOperateLoad(cancelLoadSql);

        // SHOW LOAD STMT
        String showLoadSql = "SHOW LOAD FROM db1";
        statement = UtFrameUtils.parseStmtWithNewParser(showLoadSql, starRocksAssert.getCtx());
        Authorizer.check(statement, ctx);

        // Test cancel load and table doesn't exist
        createSql = "LOAD LABEL db1.job_name2" +
                "(DATA INFILE('hdfs://test:8080/user/starrocks/data/input/example1.csv') " +
                "INTO TABLE tbl_not_exist) " +
                "WITH RESOURCE 'my_spark'" +
                "('username' = 'test_name','password' = 'pwd') " +
                "PROPERTIES ('timeout' = '3600');";
        ctxToRoot();
        String createTblStmtStr = "create table db1.tbl_not_exist(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) " +
                "AGGREGATE KEY(k1, k2, k3, k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable(createTblStmtStr);
        starRocksAssert.withLoad(createSql);
        starRocksAssert.dropTable("db1.tbl_not_exist");
        ctxToTestUser();
        grantRevokeSqlAsRoot("grant USAGE on resource 'my_spark' to test;");
        statement = UtFrameUtils.parseStmtWithNewParser(
                "CANCEL LOAD FROM db1 WHERE LABEL = 'job_name2'", starRocksAssert.getCtx());
        Authorizer.check(statement, ctx);
        grantRevokeSqlAsRoot("revoke USAGE on resource 'my_spark' from test;");
    }

    @Test
    public void testShowExportAndCancelExportStmt() throws Exception {

        ctxToRoot();
        // prepare
        mockBroker();
        String createExportSql = "EXPORT TABLE db1.tbl1 " +
                "TO 'hdfs://hdfs_host:port/a/b/c/' " +
                "WITH BROKER 'broker0'";
        starRocksAssert.withExport(createExportSql);
        String showExportSql = "SHOW EXPORT FROM db1";
        StatementBase showExportSqlStmt = UtFrameUtils.parseStmtWithNewParser(showExportSql, starRocksAssert.getCtx());
        ShowExecutor executor = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) showExportSqlStmt);
        ShowResultSet set = executor.execute();
        for (int i = 0; i < 30; i++) {
            set = executor.execute();
            if (set.getResultRows().size() > 0) {
                break;
            } else {
                Thread.sleep(1000);
            }
        }
        Assert.assertTrue(set.getResultRows().size() > 0);

        // SHOW EXPORT STMT
        ctxToTestUser();
        showExportSqlStmt = UtFrameUtils.parseStmtWithNewParser(showExportSql, starRocksAssert.getCtx());
        ShowExecutor executorBeforeGrant = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) showExportSqlStmt);
        set = executorBeforeGrant.execute();
        Assert.assertEquals(0, set.getResultRows().size());
        DDLStmtExecutor.execute(
                UtFrameUtils.parseStmtWithNewParser("grant insert on db1.tbl1 to test", starRocksAssert.getCtx()),
                starRocksAssert.getCtx());
        ctxToTestUser();
        ShowExecutor executorAfterGrant = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) showExportSqlStmt);
        set = executorAfterGrant.execute();
        Assert.assertTrue(set.getResultRows().size() > 0);
        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser("revoke insert on db1.tbl1 from test",
                        starRocksAssert.getCtx()),
                starRocksAssert.getCtx());
        ctxToTestUser();

        // CANCEL EXPORT STMT
        String queryId = set.getResultRows().get(0).get(1);
        String cancelExportSql = "CANCEL EXPORT from db1 WHERE queryid = '" + queryId + "';";
        String expectError = "Access denied; you need (at least one of) the EXPORT privilege(s)";
        verifyGrantRevoke(
                cancelExportSql,
                "grant export on db1.tbl1 to test",
                "revoke export on db1.tbl1 from test",
                expectError);
    }

    @Test
    public void testExportStmt() throws Exception {

        mockBroker();
        String createExportSql = "EXPORT TABLE db1.tbl1 " +
                "TO 'hdfs://hdfs_host:port/a/b/c/' " +
                "WITH BROKER 'broker0'";
        String expectError = "Access denied; you need (at least one of) the EXPORT privilege(s) on TABLE tbl1 for this operation";
        verifyGrantRevoke(
                createExportSql,
                "grant export on db1.tbl1 to test",
                "revoke export on db1.tbl1 from test",
                expectError);
    }

    @Test
    public void testRepositoryStmt() throws Exception {
        mockBroker();
        String expectError = "Access denied; you need (at least one of) the REPOSITORY privilege(s) on SYSTEM for this operation";

        String createRepoSql = "CREATE REPOSITORY `oss_repo` WITH BROKER `broker0` " +
                "ON LOCATION 'oss://starRocks_backup' PROPERTIES ( " +
                "'fs.oss.accessKeyId' = 'xxx'," +
                "'fs.oss.accessKeySecret' = 'yyy'," +
                "'fs.oss.endpoint' = 'oss-cn-beijing.aliyuncs.com');";
        // CREATE REPOSITORY STMT
        verifyGrantRevoke(
                createRepoSql,
                "grant repository on system to test",
                "revoke repository on system from test",
                expectError);

        mockRepository();

        // DROP REPOSITORY STMT
        verifyGrantRevoke(
                "DROP REPOSITORY `repo_name`;",
                "grant repository on system to test",
                "revoke repository on system from test",
                expectError);

        // SHOW SNAPSHOT STMT
        verifyGrantRevoke(
                "SHOW SNAPSHOT ON oss_repo;",
                "grant repository on system to test",
                "revoke repository on system from test",
                expectError);
    }

    @Test
    public void testBackupStmt() throws Exception {
        mockRepository();
        String expectError = "Access denied; you need (at least one of) the REPOSITORY privilege(s) on SYSTEM for this operation";
        String createBackupSql = "BACKUP SNAPSHOT db1.backup_name1 " +
                "TO example_repo " +
                "ON (tbl1) " +
                "PROPERTIES ('type' = 'full');";

        // check REPOSITORY privilege
        ctxToTestUser();
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(createBackupSql,
                starRocksAssert.getCtx());
        try {
            Authorizer.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (AccessDeniedException e) {
            System.out.println(e.getMessage() + ", sql: " + createBackupSql);
            Assert.assertTrue(e.getMessage().contains(expectError));
        }

        ctxToRoot();
        grantOrRevoke("grant repository on system to test");
        // check EXPORT privilege
        ctxToTestUser();
        expectError = "Access denied; you need (at least one of) the EXPORT privilege(s) on TABLE tbl1 for this operation";
        try {
            Authorizer.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (AccessDeniedException e) {
            System.out.println(e.getMessage() + ", sql: " + createBackupSql);
            Assert.assertTrue(e.getMessage().contains(expectError));
        }

        ctxToRoot();
        grantOrRevoke("grant export on db1.tbl1 to test");
        // has all privilege
        ctxToTestUser();
        Authorizer.check(statement, starRocksAssert.getCtx());
        // revoke all privilege
        ctxToRoot();
        grantOrRevoke("revoke repository on system from test");
        grantOrRevoke("revoke export on db1.tbl1 from test");
        ctxToTestUser();
    }

    @Test
    public void testShowBackupStmtInShowExecutor() throws Exception {

        mockAddBackupJob("db1");
        ctxToTestUser();
        String showBackupSql = "SHOW BACKUP FROM db1;";
        StatementBase showExportSqlStmt = UtFrameUtils.parseStmtWithNewParser(showBackupSql, starRocksAssert.getCtx());
        ShowExecutor executor = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) showExportSqlStmt);
        ShowResultSet set = executor.execute();
        Assert.assertEquals(0, set.getResultRows().size());
        ctxToRoot();
        grantOrRevoke("grant export on db1.tbl1 to test");
        // user(test) has all privilege
        ctxToTestUser();
        executor = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) showExportSqlStmt);
        set = executor.execute();
        Assert.assertTrue(set.getResultRows().size() > 0);
        // revoke all privilege
        ctxToRoot();
        grantOrRevoke("revoke export on db1.tbl1 from test");
        ctxToTestUser();
    }

    @Test
    public void testShowBackupStmtInChecker() throws Exception {
        String expectError = "Access denied; you need (at least one of) the REPOSITORY";
        verifyGrantRevoke(
                "SHOW BACKUP FROM db1;",
                "grant repository on system to test",
                "revoke repository on system from test",
                expectError);
    }

    @Test
    public void testCancelBackupStmt() throws Exception {
        mockAddBackupJob("db2");
        ctxToRoot();
        grantOrRevoke("grant repository on system to test");
        ctxToTestUser();
        String cancelBackupSql = "CANCEL BACKUP FROM db2;";
        verifyGrantRevoke(cancelBackupSql,
                "grant export on db2.tbl1 to test",
                "revoke export on db2.tbl1 from test",
                "Access denied; you need (at least one of) the EXPORT privilege(s) on TABLE tbl1 for this operation");
        ctxToRoot();
        grantOrRevoke("revoke repository on system from test");
    }

    @Test
    public void testRestoreStmt() throws Exception {

        ctxToTestUser();
        String restoreSql = "RESTORE SNAPSHOT db1.`snapshot_1` FROM `example_repo` " +
                "ON ( `tbl1` ) " +
                "PROPERTIES ( 'backup_timestamp' = '2018-05-04-16-45-08', 'replication_num' = '1');";

        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(restoreSql, starRocksAssert.getCtx());

        ctxToTestUser();
        String expectError = "Access denied; you need (at least one of) the REPOSITORY privilege(s) on SYSTEM for this operation";
        try {
            Authorizer.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (AccessDeniedException e) {
            System.out.println(e.getMessage() + ", sql: " + restoreSql);
            Assert.assertTrue(e.getMessage().contains(expectError));
        }
        ctxToRoot();
        grantOrRevoke("grant repository on system to test");
        ctxToTestUser();
        expectError = "Access denied; you need (at least one of) the CREATE TABLE privilege(s) on DATABASE db1 " +
                "for this operation";
        try {
            Authorizer.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (AccessDeniedException e) {
            System.out.println(e.getMessage() + ", sql: " + restoreSql);
            Assert.assertTrue(e.getMessage().contains(expectError));
        }
        ctxToRoot();
        grantOrRevoke("grant create table on database db1 to test");

        verifyGrantRevoke(restoreSql,
                "grant SELECT,INSERT on db1.tbl1 to test",
                "revoke SELECT,INSERT on db1.tbl1 from test",
                "Access denied; you need (at least one of) the INSERT privilege(s)");
        // revoke
        ctxToRoot();
        grantOrRevoke("revoke repository on system from test");
        grantOrRevoke("revoke all on database db1 from test");
    }

    @Test
    public void testCreateMaterializedViewStatement() throws Exception {

        Config.enable_experimental_mv = true;
        String createSql = "create materialized view db1.mv1 " +
                "distributed by hash(k2)" +
                "refresh async START('9999-12-31') EVERY(INTERVAL 3 SECOND) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, db1.tbl1.k2 from db1.tbl1;";

        String expectError = "Access denied; you need (at least one of) the CREATE MATERIALIZED VIEW privilege(s) " +
                "on DATABASE db1 for this operation";
        verifyGrantRevoke(
                createSql,
                "grant create materialized view on DATABASE db1 to test",
                "revoke create materialized view on DATABASE db1 from test",
                expectError);
    }

    @Test
    public void testAlterMaterializedViewStatement() throws Exception {

        Config.enable_experimental_mv = true;
        String createSql = "create materialized view db1.mv1 " +
                "distributed by hash(k2)" +
                "refresh async START('9999-12-31') EVERY(INTERVAL 3 SECOND) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, db1.tbl1.k2 from db1.tbl1;";
        starRocksAssert.withMaterializedView(createSql);
        verifyGrantRevoke(
                "alter materialized view db1.mv1 rename mv2;",
                "grant alter on materialized view db1.mv1 to test",
                "revoke alter on materialized view db1.mv1 from test",
                "Access denied; you need (at least one of) the ALTER privilege(s) on " +
                        "MATERIALIZED VIEW mv1 for this operation");
        ctxToRoot();
        starRocksAssert.dropMaterializedView("db1.mv1");
        ctxToTestUser();
    }

    @Test
    public void testRefreshMaterializedViewStatement() throws Exception {

        ctxToRoot();
        Config.enable_experimental_mv = true;
        String createSql = "create materialized view db1.mv2 " +
                "distributed by hash(k2)" +
                "refresh async START('9999-12-31') EVERY(INTERVAL 3 SECOND) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, db1.tbl1.k2 from db1.tbl1;";
        starRocksAssert.withMaterializedView(createSql);
        verifyGrantRevoke(
                "REFRESH MATERIALIZED VIEW db1.mv2;",
                "grant refresh on materialized view db1.mv2 to test",
                "revoke refresh on materialized view db1.mv2 from test",
                "Access denied; you need (at least one of) the REFRESH privilege(s) on MATERIALIZED VIEW mv2 " +
                        "for this operation");
        verifyGrantRevoke(
                "CANCEL REFRESH MATERIALIZED VIEW db1.mv2;",
                "grant refresh on materialized view db1.mv2 to test",
                "revoke refresh on materialized view db1.mv2 from test",
                "Access denied; you need (at least one of) the REFRESH privilege(s) on MATERIALIZED VIEW mv2 " +
                        "for this operation");

        ctxToRoot();
        starRocksAssert.dropMaterializedView("db1.mv2");
        ctxToTestUser();
    }

    @Test
    public void testShowMaterializedViewStatement() throws Exception {
        ctxToRoot();
        Config.enable_experimental_mv = true;
        String createSql = "create materialized view db1.mv3 " +
                "distributed by hash(k2)" +
                "refresh async START('9999-12-31') EVERY(INTERVAL 3 SECOND) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, db1.tbl1.k2 from db1.tbl1;";
        starRocksAssert.withMaterializedView(createSql);
        String showBackupSql = "SHOW MATERIALIZED VIEWS FROM db1;";
        StatementBase showExportSqlStmt = UtFrameUtils.parseStmtWithNewParser(showBackupSql, starRocksAssert.getCtx());
        ShowExecutor executor = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) showExportSqlStmt);
        ShowResultSet set = executor.execute();
        Assert.assertTrue(set.getResultRows().size() > 0);
        grantOrRevoke("grant SELECT,INSERT on db1.tbl1 to test");
        ctxToTestUser();
        executor = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) showExportSqlStmt);
        set = executor.execute();
        Assert.assertEquals(0, set.getResultRows().size());
        ctxToRoot();
        grantOrRevoke("grant refresh on materialized view db1.mv3 to test");
        ctxToTestUser();
        executor = new ShowExecutor(starRocksAssert.getCtx(), (ShowStmt) showExportSqlStmt);
        set = executor.execute();
        Assert.assertTrue(set.getResultRows().size() > 0);
        ctxToRoot();
        grantOrRevoke("revoke SELECT,INSERT on db1.tbl1 from test");
        grantOrRevoke("revoke refresh on materialized view db1.mv3 from test");
        starRocksAssert.dropMaterializedView("db1.mv3");
        ctxToTestUser();
    }

    @Test
    public void testDropMaterializedViewStatement() throws Exception {

        ctxToRoot();
        Config.enable_experimental_mv = true;
        String createSql = "create materialized view db1.mv4 " +
                "distributed by hash(k2)" +
                "refresh async START('9999-12-31') EVERY(INTERVAL 3 SECOND) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, db1.tbl1.k2 from db1.tbl1;";
        starRocksAssert.withMaterializedView(createSql);

        DropMaterializedViewStmt statement = (DropMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(
                "drop materialized view db1.mv4", starRocksAssert.getCtx());

        ctxToTestUser();
        try {
            GlobalStateMgr.getCurrentState().dropMaterializedView(statement);
        } catch (AccessDeniedException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains(
                    "Access denied; you need (at least one of) the DROP privilege(s)"));
        }

        grantRevokeSqlAsRoot("grant drop on materialized view db1.mv4 to test");
        GlobalStateMgr.getCurrentState().dropMaterializedView(statement);
        GlobalStateMgr.getCurrentState().getAuthorizationMgr().removeInvalidObject();
        ctxToTestUser();
    }

    @Test
    public void testCreateFunc() throws Exception {

        new MockUp<CreateFunctionStmt>() {
            @Mock
            public void analyze(ConnectContext context) throws AnalysisException {
            }
        };

        String createSql = "CREATE FUNCTION db1.MY_UDF_JSON_GET(string, string) RETURNS string " +
                "properties ( " +
                "'symbol' = 'com.starrocks.udf.sample.UDFSplit', 'object_file' = 'test' " +
                ")";
        String expectError = "Access denied; you need (at least one of) the " +
                "CREATE FUNCTION privilege(s)";
        verifyGrantRevoke(
                createSql,
                "grant CREATE FUNCTION on DATABASE db1 to test",
                "revoke CREATE FUNCTION on DATABASE db1 from test",
                expectError);
    }

    @Test
    public void testCreateGlobalFunc() throws Exception {

        new MockUp<CreateFunctionStmt>() {
            @Mock
            public void analyze(ConnectContext context) throws AnalysisException {
            }
        };

        String createSql = "CREATE GLOBAL FUNCTION MY_UDF_JSON_GET(string, string) RETURNS string " +
                "properties ( " +
                "'symbol' = 'com.starrocks.udf.sample.UDFSplit', 'object_file' = 'test' " +
                ")";
        String expectError = "Access denied; you need (at least one of) the CREATE GLOBAL FUNCTION privilege(s) " +
                "on SYSTEM for this operation";
        verifyGrantRevoke(
                createSql,
                "grant CREATE GLOBAL FUNCTION on system to test",
                "revoke CREATE GLOBAL FUNCTION on system from test",
                expectError);
    }

    @Test
    public void testFunc() throws Exception {
        Database db1 = GlobalStateMgr.getCurrentState().getDb("db1");
        FunctionName fn = FunctionName.createFnName("db1.my_udf_json_get");
        Function function = new Function(fn, Arrays.asList(Type.STRING, Type.STRING), Type.STRING, false);
        try {
            db1.addFunction(function);
        } catch (Throwable e) {
            // ignore
        }

        verifyGrantRevoke(
                "DROP FUNCTION db1.MY_UDF_JSON_GET(string, string);",
                "grant drop on ALL FUNCTIONS in database db1 to test",
                "revoke drop on ALL FUNCTIONS in database db1 from test",
                "Access denied; you need (at least one of) the DROP privilege(s) on FUNCTION " +
                        "my_udf_json_get(VARCHAR,VARCHAR)");

        verifyGrantRevoke(
                "DROP FUNCTION db1.MY_UDF_JSON_GET(string, string);",
                "grant drop on FUNCTION db1.MY_UDF_JSON_GET(string, string) to test",
                "revoke drop on FUNCTION db1.MY_UDF_JSON_GET(string, string) from test",
                "Access denied; you need (at least one of) the DROP privilege(s) on FUNCTION " +
                        "my_udf_json_get(VARCHAR,VARCHAR) for this operation");

        // add test for drop non-existed function
        ctxToTestUser();
        try {
            UtFrameUtils.parseStmtWithNewParser("drop function db1.non_existed_fn(int,int)",
                    starRocksAssert.getCtx());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Unknown function"));
        }

        // test select from only function without table
        Config.enable_udf = true;
        ctxToTestUser();
        try {
            UtFrameUtils.parseStmtWithNewParser("select db1.MY_UDF_JSON_GET('a', 'b')",
                    starRocksAssert.getCtx());
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Access denied; you need (at least one of)"));
        }
        grantRevokeSqlAsRoot("grant USAGE on FUNCTION db1.MY_UDF_JSON_GET(string, string) to test");
        // parse success after grant usage
        UtFrameUtils.parseStmtWithNewParser("select db1.MY_UDF_JSON_GET('a', 'b')",
                starRocksAssert.getCtx());
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(
                "select db1.MY_UDF_JSON_GET(k2, k3) from db1.tbl1",
                starRocksAssert.getCtx());
        try {
            Authorizer.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains(
                    "Access denied; you need (at least one of) the SELECT privilege(s) on TABLE tbl1 for this operation"));
        }
        grantRevokeSqlAsRoot("revoke USAGE on FUNCTION db1.MY_UDF_JSON_GET(string, string) from test");
        Config.enable_udf = false;
    }

    @Test
    public void testDropGlobalFunc() throws Exception {

        FunctionName fn = FunctionName.createFnName("my_udf_json_get");
        fn.setAsGlobalFunction();
        Function function = new Function(fn, Arrays.asList(Type.STRING, Type.STRING), Type.STRING, false);
        try {
            GlobalStateMgr.getCurrentState().getGlobalFunctionMgr().replayAddFunction(function);
        } catch (Throwable e) {
            // ignore
        }

        verifyGrantRevoke(
                "drop global function my_udf_json_get (string, string);",
                "grant drop on ALL GLOBAL FUNCTIONS to test",
                "revoke drop on ALL GLOBAL FUNCTIONS from test",
                "Access denied; you need (at least one of) the DROP privilege(s) on GLOBAL FUNCTION " +
                        "my_udf_json_get(VARCHAR,VARCHAR) for this operation");

        verifyGrantRevoke(
                "drop global function my_udf_json_get (string, string);",
                "grant drop on GLOBAL FUNCTION my_udf_json_get(string,string) to test",
                "revoke drop on GLOBAL FUNCTION my_udf_json_get(string,string) from test",
                "Access denied; you need (at least one of) the DROP privilege(s) on GLOBAL FUNCTION " +
                        "my_udf_json_get(VARCHAR,VARCHAR) for this operation");

        Config.enable_udf = true;
        ctxToTestUser();
        try {
            UtFrameUtils.parseStmtWithNewParser("select my_udf_json_get('a', 'b')",
                    starRocksAssert.getCtx());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains(
                    "Access denied; you need (at least one of) the USAGE privilege(s) on GLOBAL FUNCTION " +
                            "my_udf_json_get(VARCHAR,VARCHAR)"));
        }
        Config.enable_udf = false;
    }

    @Test
    public void testShowFunc() throws Exception {

        Database db1 = GlobalStateMgr.getCurrentState().getDb("db1");
        FunctionName fn = FunctionName.createFnName("db1.my_udf_json_get");
        Function function = new Function(fn, Arrays.asList(Type.STRING, Type.STRING), Type.STRING, false);
        try {
            db1.addFunction(function);
        } catch (Throwable e) {
            // ignore
        }
        String showSql = "show full functions in db1";
        String expectError = "Access denied for user 'test' to database 'db1'";
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(showSql, starRocksAssert.getCtx());
        ctxToTestUser();
        Authorizer.check(statement, starRocksAssert.getCtx());

        ctxToRoot();
        grantOrRevoke("grant create materialized view on DATABASE db1 to test");
        expectError = "You need any privilege on any TABLE/VIEW/MV in database";
        ctxToTestUser();
        Authorizer.check(statement, starRocksAssert.getCtx());
        ctxToRoot();
        grantOrRevoke("grant select on db1.tbl1 to test");
        Authorizer.check(statement, starRocksAssert.getCtx());
        ctxToRoot();
        grantOrRevoke("revoke create materialized view on DATABASE db1 from test");
        grantOrRevoke("revoke select on db1.tbl1 from test");
    }

    @Test
    public void testShowGlobalFunc() throws Exception {
        FunctionName fn = FunctionName.createFnName("my_udf_json_get");
        fn.setAsGlobalFunction();
        Function function = new Function(fn, Arrays.asList(Type.STRING, Type.STRING), Type.STRING, false);
        try {
            GlobalStateMgr.getCurrentState().getGlobalFunctionMgr().replayAddFunction(function);
        } catch (Throwable e) {
            // ignore
        }

        String showSql = "show full global functions";
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(showSql, starRocksAssert.getCtx());
        ctxToTestUser();
        Authorizer.check(statement, starRocksAssert.getCtx());
    }

    @Test
    public void testUseGlobalFunc() throws Exception {
        FunctionName fn = FunctionName.createFnName("my_udf_json_get");
        fn.setAsGlobalFunction();
        Function function = new Function(1, fn, Arrays.asList(Type.STRING, Type.STRING), Type.STRING, false);
        try {
            GlobalStateMgr.getCurrentState().getGlobalFunctionMgr().replayAddFunction(function);
        } catch (Throwable e) {
            // ignore
        }

        ctxToTestUser();
        String selectSQL = "select my_udf_json_get('hello', 'world')";
        String selectSQL2 = "select my_udf_json_get2('hello', 'world')";
        try {
            StatementBase statement = UtFrameUtils.parseStmtWithNewParser(selectSQL, starRocksAssert.getCtx());
            Authorizer.check(statement, starRocksAssert.getCtx());
            Assert.fail();
        } catch (AccessDeniedException e) {
            System.out.println(e.getMessage() + ", sql: " + selectSQL);
            Assert.assertTrue(e.getMessage().contains("Access denied; you need (at least one of) the USAGE privilege(s) " +
                    "on GLOBAL FUNCTION my_udf_json_get(VARCHAR,VARCHAR) for this operation"));
        }

        // grant usage on global function
        ctxToRoot();
        grantOrRevoke("grant usage on global function my_udf_json_get(string,string) to test");
        ctxToTestUser();

        try {
            Config.enable_udf = true;
            StatementBase statement = UtFrameUtils.parseStmtWithNewParser(selectSQL, starRocksAssert.getCtx());
            Authorizer.check(statement, starRocksAssert.getCtx());
        } finally {
            Config.enable_udf = false;
        }

        // grant on all global functions.
        ctxToRoot();
        grantOrRevoke("revoke usage on global function my_udf_json_get(string,string) from test");
        grantOrRevoke("grant usage on all global functions to test");
        ctxToTestUser();
        try {
            Config.enable_udf = true;
            StatementBase statement = UtFrameUtils.parseStmtWithNewParser(selectSQL, starRocksAssert.getCtx());
            Authorizer.check(statement, starRocksAssert.getCtx());
        } finally {
            Config.enable_udf = false;
        }


        fn = FunctionName.createFnName("my_udf_json_get2");
        fn.setAsGlobalFunction();
        function = new Function(2, fn, Arrays.asList(Type.STRING, Type.STRING), Type.STRING, false);
        try {
            GlobalStateMgr.getCurrentState().getGlobalFunctionMgr().replayAddFunction(function);
        } catch (Throwable e) {
            // ignore
        }

        // grant usage on global function
        ctxToRoot();
        grantOrRevoke("grant usage on global function my_udf_json_get(string,string), my_udf_json_get2(string,string) to test");
        ctxToTestUser();

        try {
            Config.enable_udf = true;
            StatementBase statement = UtFrameUtils.parseStmtWithNewParser(selectSQL, starRocksAssert.getCtx());
            Authorizer.check(statement, starRocksAssert.getCtx());

            statement = UtFrameUtils.parseStmtWithNewParser(selectSQL2, starRocksAssert.getCtx());
            Authorizer.check(statement, starRocksAssert.getCtx());
        } finally {
            Config.enable_udf = false;
        }

        // grant on all global functions.
        ctxToRoot();
        grantOrRevoke("revoke usage on global function my_udf_json_get(string,string), " +
                "my_udf_json_get2(string,string) from test");
        grantOrRevoke("grant usage on all global functions to test");
        ctxToTestUser();
        try {
            Config.enable_udf = true;
            StatementBase statement = UtFrameUtils.parseStmtWithNewParser(selectSQL, starRocksAssert.getCtx());
            Authorizer.check(statement, starRocksAssert.getCtx());

            statement = UtFrameUtils.parseStmtWithNewParser(selectSQL2, starRocksAssert.getCtx());
            Authorizer.check(statement, starRocksAssert.getCtx());
        } finally {
            Config.enable_udf = false;
        }
    }

    @Test
    public void testShowAuthentication() throws com.starrocks.common.AnalysisException, DdlException, PrivilegeException {
        ctxToTestUser();
        ShowAuthenticationStmt stmt = new ShowAuthenticationStmt(testUser, false);
        ShowExecutor executor = new ShowExecutor(starRocksAssert.getCtx(), stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertEquals(4, resultSet.getMetaData().getColumnCount());
        Assert.assertEquals("UserIdentity", resultSet.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Password", resultSet.getMetaData().getColumn(1).getName());
        Assert.assertEquals("AuthPlugin", resultSet.getMetaData().getColumn(2).getName());
        Assert.assertEquals("UserForAuthPlugin", resultSet.getMetaData().getColumn(3).getName());
        Assert.assertEquals("[['test'@'%', No, MYSQL_NATIVE_PASSWORD, null]]",
                resultSet.getResultRows().toString());

        stmt = new ShowAuthenticationStmt(null, true);
        executor = new ShowExecutor(starRocksAssert.getCtx(), stmt);
        resultSet = executor.execute();
        Assert.assertEquals("[['root'@'%', No, MYSQL_NATIVE_PASSWORD, null], " +
                        "['test2'@'%', No, " +
                        "MYSQL_NATIVE_PASSWORD, null], ['test'@'%', No, MYSQL_NATIVE_PASSWORD, null]]",
                resultSet.getResultRows().toString());

        stmt = new ShowAuthenticationStmt(UserIdentity.ROOT, false);
        executor = new ShowExecutor(starRocksAssert.getCtx(), stmt);
        resultSet = executor.execute();
        Assert.assertEquals("[['root'@'%', No, MYSQL_NATIVE_PASSWORD, null]]",
                resultSet.getResultRows().toString());
    }

    @Test
    public void testGrantRevokeBuiltinRole() throws Exception {
        String sql = "create role r1";
        AuthorizationMgr manager = starRocksAssert.getCtx().getGlobalStateMgr().getAuthorizationMgr();
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        DDLStmtExecutor.execute(stmt, starRocksAssert.getCtx());

        ctxToRoot();
        Authorizer.check(UtFrameUtils.parseStmtWithNewParser(
                "grant root to role r1", starRocksAssert.getCtx()), starRocksAssert.getCtx());

        Authorizer.check(UtFrameUtils.parseStmtWithNewParser(
                "grant cluster_admin to role r1", starRocksAssert.getCtx()), starRocksAssert.getCtx());

        try {
            Authorizer.check(UtFrameUtils.parseStmtWithNewParser(
                    "revoke root from root", starRocksAssert.getCtx()), starRocksAssert.getCtx());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Can not revoke root role from root user.",
                    e.getMessage());
        }

        ctxToTestUser();
        try {
            Authorizer.check(UtFrameUtils.parseStmtWithNewParser(
                    "grant root to role r1", starRocksAssert.getCtx()), starRocksAssert.getCtx());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Can not grant root or cluster_admin role " +
                    "except root user.", e.getMessage());
        }

        try {
            Authorizer.check(UtFrameUtils.parseStmtWithNewParser(
                    "grant cluster_admin to role r1", starRocksAssert.getCtx()), starRocksAssert.getCtx());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Can not grant root or cluster_admin " +
                    "role except root user.", e.getMessage());
        }

        sql = "drop role r1";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        DDLStmtExecutor.execute(stmt, starRocksAssert.getCtx());
    }

    @Test
    public void testSetDefaultRole() throws Exception {
        String sql = "create role r1, r2";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        DDLStmtExecutor.execute(stmt, starRocksAssert.getCtx());

        sql = "create user u1";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        DDLStmtExecutor.execute(stmt, starRocksAssert.getCtx());

        sql = "create user u2";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        DDLStmtExecutor.execute(stmt, starRocksAssert.getCtx());

        ctxToRoot();
        sql = "grant r1 to u1";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        DDLStmtExecutor.execute(stmt, starRocksAssert.getCtx());
        Authorizer.check(UtFrameUtils.parseStmtWithNewParser(
                "set default role r1 to u1", starRocksAssert.getCtx()), starRocksAssert.getCtx());

        starRocksAssert.getCtx().setCurrentUserIdentity(new UserIdentity("u1", "%"));
        Authorizer.check(UtFrameUtils.parseStmtWithNewParser(
                "set default role r1 to u1", starRocksAssert.getCtx()), starRocksAssert.getCtx());

        starRocksAssert.getCtx().setCurrentUserIdentity(new UserIdentity("u2", "%"));
        try {
            Authorizer.check(UtFrameUtils.parseStmtWithNewParser(
                    "set default role r1 to u1", starRocksAssert.getCtx()), starRocksAssert.getCtx());
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(),
                    "Access denied; you need (at least one of) the GRANT privilege(s) on SYSTEM for this operation");
        }

        sql = "drop user u1";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        DDLStmtExecutor.execute(stmt, starRocksAssert.getCtx());

        sql = "drop user u2";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        DDLStmtExecutor.execute(stmt, starRocksAssert.getCtx());

        sql = "drop role r1, r2";
        stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        DDLStmtExecutor.execute(stmt, starRocksAssert.getCtx());
    }


    @Test
    public void testQueryAndDML() throws Exception {
        starRocksAssert.withTable("CREATE TABLE db1.`tprimary` (\n" +
                "  `pk` bigint NOT NULL COMMENT \"\",\n" +
                "  `v1` string NOT NULL COMMENT \"\",\n" +
                "  `v2` int NOT NULL,\n" +
                "  `v3` array<int> not null" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`pk`)\n" +
                "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant SELECT on TABLE db1.tbl1, db1.tbl2 to test", starRocksAssert.getCtx()), starRocksAssert.getCtx());

        verifyGrantRevoke(
                "select * from db1.tbl1, db1.tbl2, db2.tbl1",
                "grant SELECT on TABLE db2.tbl1 to test",
                "revoke SELECT on TABLE db2.tbl1 from test",
                "Access denied; you need (at least one of) the SELECT privilege(s) on TABLE tbl1 for this operation");

        verifyGrantRevoke(
                "select * from db1.tbl1, db1.tbl2 where exists (select * from db2.tbl1)",
                "grant SELECT on TABLE db2.tbl1 to test",
                "revoke SELECT on TABLE db2.tbl1 from test",
                "Access denied; you need (at least one of) the SELECT privilege(s) on TABLE tbl1 for this operation");

        verifyGrantRevoke(
                "with cte as (select * from db2.tbl1) select * from db1.tbl1, db1.tbl2",
                "grant SELECT on TABLE db2.tbl1 to test",
                "revoke SELECT on TABLE db2.tbl1 from test",
                "Access denied; you need (at least one of) the SELECT privilege(s) on TABLE tbl1 for this operation");

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant SELECT on TABLE db2.tbl1 to test", starRocksAssert.getCtx()), starRocksAssert.getCtx());

        verifyGrantRevoke(
                "insert into db1.tbl2 select * from db2.tbl1",
                "grant SELECT, INSERT on TABLE db1.tbl2 to test",
                "revoke INSERT on TABLE db1.tbl2 from test",
                "Access denied;");

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant INSERT on TABLE db1.tbl2 to test", starRocksAssert.getCtx()), starRocksAssert.getCtx());

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke select on TABLE db2.tbl1 from test", starRocksAssert.getCtx()), starRocksAssert.getCtx());
        verifyGrantRevoke(
                "insert into db1.tbl2 select * from db2.tbl1",
                "grant SELECT on TABLE db2.tbl1 to test",
                "revoke SELECT on TABLE db2.tbl1 from test",
                "Access denied; you need (at least one of) the SELECT privilege(s) on TABLE tbl1 for this operation");

        verifyGrantRevoke(
                "update db1.tprimary set v1 = db1.tbl1.k1 from db1.tbl1 where db1.tbl1.k2 = db1.tprimary.pk",
                "grant select, update on table db1.tprimary to test",
                "revoke update on TABLE db1.tprimary from test",
                "Access denied; you need (at least one of) the UPDATE privilege(s) on TABLE");

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant UPDATE on TABLE db1.tprimary to test", starRocksAssert.getCtx()), starRocksAssert.getCtx());
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke select on TABLE db1.tbl1 from test", starRocksAssert.getCtx()), starRocksAssert.getCtx());

        verifyGrantRevoke(
                "update db1.tprimary set v1 = db1.tbl1.k1 from db1.tbl1 where db1.tbl1.k2 = db1.tprimary.pk",
                "grant select on table db1.tbl1 to test",
                "revoke select on TABLE db1.tbl1 from test",
                "Access denied; you need (at least one of) the SELECT privilege(s) on TABLE tbl1 for this operation");

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant select on TABLE db1.tbl1 to test", starRocksAssert.getCtx()), starRocksAssert.getCtx());

        verifyGrantRevoke(
                "delete from db1.tprimary using db1.tbl1 where db1.tbl1.k2 = db1.tprimary.pk",
                "grant select, delete on table db1.tprimary to test",
                "revoke delete on TABLE db1.tprimary from test",
                "Access denied; you need (at least one of) the DELETE privilege(s) on TABLE");

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant DELETE on TABLE db1.tprimary to test", starRocksAssert.getCtx()), starRocksAssert.getCtx());
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke select on TABLE db1.tbl1 from test", starRocksAssert.getCtx()), starRocksAssert.getCtx());

        verifyGrantRevoke(
                "delete from db1.tprimary using db1.tbl1 where db1.tbl1.k2 = db1.tprimary.pk",
                "grant select on TABLE db1.tbl1 to test",
                "revoke select on TABLE db1.tbl1 from test",
                "Access denied; you need (at least one of) the SELECT privilege(s) on TABLE");

        starRocksAssert.dropTable("db1.tprimary");
    }

    @Test
    public void testShowTable() throws Exception {
        ctxToRoot();
        Config.enable_experimental_mv = true;
        String createSql = "create materialized view db1.mv5 " +
                "distributed by hash(k2)" +
                "refresh async START('9999-12-31') EVERY(INTERVAL 1000 SECOND) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") " +
                "as select k1, db1.tbl1.k2 from db1.tbl1;";
        starRocksAssert.withMaterializedView(createSql);

        String createViewSql = "create view db1.view5 as select * from db1.tbl1";
        starRocksAssert.withView(createViewSql);


        ConnectContext ctx = starRocksAssert.getCtx();
        grantRevokeSqlAsRoot("grant select on db1.tbl1 to test");
        ctxToTestUser();
        ShowResultSet res = new ShowExecutor(ctx,
                (ShowStmt) UtFrameUtils.parseStmtWithNewParser("SHOW tables from db1", ctx)).execute();
        System.out.println(res.getResultRows());
        Assert.assertEquals(1, res.getResultRows().size());
        Assert.assertEquals("tbl1", res.getResultRows().get(0).get(0));

        // can show mv if we have any privilege on it
        grantRevokeSqlAsRoot("grant alter on materialized view db1.mv5 to test");
        res = new ShowExecutor(ctx,
                (ShowStmt) UtFrameUtils.parseStmtWithNewParser("SHOW tables from db1", ctx)).execute();
        System.out.println(res.getResultRows());
        Assert.assertEquals(2, res.getResultRows().size());
        Assert.assertEquals("mv5", res.getResultRows().get(0).get(0));


        // can show view if we have any privilege on it
        grantRevokeSqlAsRoot("grant drop on view db1.view5 to test");
        res = new ShowExecutor(ctx,
                (ShowStmt) UtFrameUtils.parseStmtWithNewParser("SHOW tables from db1", ctx)).execute();
        System.out.println(res.getResultRows());
        Assert.assertEquals(3, res.getResultRows().size());
        Assert.assertEquals("view5", res.getResultRows().get(2).get(0));
        grantRevokeSqlAsRoot("revoke drop on view db1.view5 from test");
        grantRevokeSqlAsRoot("revoke alter on materialized view db1.mv5 from test");
        grantRevokeSqlAsRoot("revoke select on db1.tbl1 from test");

        ctxToRoot();
        starRocksAssert.dropMaterializedView("db1.mv5");
        starRocksAssert.dropView("db1.view5");
        ctxToTestUser();
    }

    @Test
    public void testStorageVolumeStatement() throws Exception {
        String createSql = "create storage volume local type = s3 " +
                "locations = ('s3://starrocks-cloud-data-zhangjiakou/luzijie/') comment 'comment' properties " +
                "(\"aws.s3.endpoint\"=\"endpoint\", \"aws.s3.region\"=\"us-test-2\", " +
                "\"aws.s3.use_aws_sdk_default_behavior\" = \"false\", \"aws.s3.access_key\"=\"accesskey\", " +
                "\"aws.s3.secret_key\"=\"secretkey\");";
        ConnectContext ctx = starRocksAssert.getCtx();

        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(createSql, ctx), ctx);
        ctxToTestUser();

        // test no authorization on show storage volumes
        ShowResultSet res = new ShowExecutor(ctx,
                (ShowStmt) UtFrameUtils.parseStmtWithNewParser("show storage volumes", ctx)).execute();
        Assert.assertEquals(0, res.getResultRows().size());

        // test desc storage volume
        verifyGrantRevoke(
                "desc storage volume local",
                "grant USAGE on storage volume local to test",
                "revoke USAGE on storage volume local from test",
                "Access denied; you need (at least one of) the ANY privilege(s) on STORAGE VOLUME local " +
                        "for this operation");

        // test drop storage volume
        verifyGrantRevoke(
                "drop storage volume local",
                "grant DROP on storage volume local to test",
                "revoke DROP on storage volume local from test",
                "Access denied;");

        String sql = "" +
                "ALTER STORAGE VOLUME local \n" +
                "SET \n" +
                "   (\"aws.s3.region\"=\"us-west-1\", \"aws.s3.endpoint\"=\"endpoint1\", \"enabled\"=\"true\")";
        // test drop resource group
        verifyGrantRevoke(
                sql,
                "grant ALTER on storage volume local to test",
                "revoke ALTER on storage volume local from test",
                "Access denied;");

        // test create storage volume
        String createSv1Sql = "create storage volume sv1 type = s3 " +
                "locations = ('s3://starrocks-cloud-data-zhangjiakou/luzijie/') comment 'comment' properties " +
                "(\"aws.s3.endpoint\"=\"endpoint\", \"aws.s3.region\"=\"us-test-2\", " +
                "\"aws.s3.use_aws_sdk_default_behavior\" = \"false\", \"aws.s3.access_key\"=\"accesskey\", " +
                "\"aws.s3.secret_key\"=\"secretkey\");";
        verifyGrantRevoke(
                createSv1Sql,
                "grant create storage volume on system to test",
                "revoke create storage volume on system from test",
                "Access denied;");

        // test usage of storage volume
        String grantCreateDb = "grant create database on catalog default_catalog to test";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantCreateDb, ctx), ctx);
        ctxToTestUser();
        String createDbSql = "create database testdb properties (\"storage_volume\"=\"local\")";
        verifyGrantRevoke(
                createDbSql,
                "grant USAGE on storage volume local to test",
                "revoke USAGE ON storage volume local from test",
                "Access denied;");
        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(createDbSql, ctx), ctx);
        String grantCreateTable = "grant create table on database testdb to test";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantCreateTable, ctx), ctx);
        ctxToTestUser();
        String createTableSql = "create table testdb.t2(c0 INT) duplicate key(c0) distributed " +
                "by hash(c0) buckets 1 properties(\"storage_volume\" = \"local\");";
        verifyGrantRevoke(
                createTableSql,
                "grant USAGE on storage volume local to test",
                "revoke USAGE ON storage volume local from test",
                "Access denied;");

        ctxToRoot();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke create database on catalog default_catalog from test", ctx), ctx);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "revoke create table on database testdb from test", ctx), ctx);

        ctxToTestUser();
        // test grant/revoke on all storage volumes
        verifyGrantRevoke(
                sql,
                "grant ALTER on all storage volumes to test",
                "revoke ALTER on all storage volumes from test",
                "Access denied;");
    }
}
