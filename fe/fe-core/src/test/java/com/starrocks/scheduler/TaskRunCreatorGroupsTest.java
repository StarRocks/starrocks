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

package com.starrocks.scheduler;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.authentication.FileGroupProvider;
import com.starrocks.authentication.GroupProvider;
import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.ranger.starrocks.RangerStarRocksAccessController;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A task run (SUBMIT TASK, MV refresh) acts as its creator, and the creator's groups are part of that identity:
 * roles granted TO EXTERNAL GROUP and Ranger policies written for a group must apply in the task as they do at login.
 */
public class TaskRunCreatorGroupsTest extends MVTestBase {
    private static final String PROVIDER = "task_run_group_provider";
    private static final String GROUP = "task_run_analysts";
    private static final String GROUP_ROLE = "task_run_group_role";
    private static final String OWN_ROLE = "task_run_own_role";
    private static final String NATIVE_USER = "task_run_group_user";
    private static final String EXTERNAL_USER = "task_run_external_user";
    private static final String RANGER_USER = "task_run_ranger_user";
    private static final String ICEBERG = MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME;
    private static final String INSERT = "INSERT INTO test.group_dst SELECT k, v FROM test.group_src";
    private static final AtomicInteger NEXT_TASK = new AtomicInteger();

    private static String[] savedGroupProvider;
    private final boolean savedUseCreator = Config.mv_use_creator_based_authorization;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, ICEBERG);
        starRocksAssert.withTable("CREATE TABLE test.group_src (k INT, v INT) DUPLICATE KEY(k) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withTable("CREATE TABLE test.group_dst (k INT, v INT) DUPLICATE KEY(k) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withTable("CREATE TABLE test.ranger_dst (id INT, data STRING) DUPLICATE KEY(id) " +
                "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");

        new MockUp<FileGroupProvider>() {
            @Mock
            public InputStream getPath(String groupFileUrl) {
                String line = GROUP + ":" + String.join(",", NATIVE_USER, EXTERNAL_USER, RANGER_USER);
                return new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8));
            }
        };
        GlobalStateMgr.getCurrentState().getAuthenticationMgr().replayCreateGroupProvider(PROVIDER,
                Map.of(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, FileGroupProvider.TYPE,
                        FileGroupProvider.GROUP_FILE_URL, "unused"));
        savedGroupProvider = Config.group_provider;
        Config.group_provider = new String[] {PROVIDER};

        executeAsRoot("CREATE ROLE " + GROUP_ROLE);
        executeAsRoot("GRANT SELECT ON TABLE test.group_src TO ROLE " + GROUP_ROLE);
        executeAsRoot("GRANT INSERT ON TABLE test.group_dst TO ROLE " + GROUP_ROLE);
        executeAsRoot("GRANT " + GROUP_ROLE + " TO EXTERNAL GROUP " + GROUP);
        executeAsRoot("CREATE ROLE " + OWN_ROLE);
        executeAsRoot("CREATE USER '" + NATIVE_USER + "' IDENTIFIED BY ''");
        executeAsRoot("GRANT " + OWN_ROLE + " TO USER '" + NATIVE_USER + "'");
        executeAsRoot("GRANT CREATE MATERIALIZED VIEW ON DATABASE test TO USER '" + NATIVE_USER + "'");
        executeAsRoot("CREATE USER '" + RANGER_USER + "' IDENTIFIED BY ''");
        executeAsRoot("GRANT INSERT ON TABLE test.ranger_dst TO USER '" + RANGER_USER + "'");
    }

    @AfterAll
    public static void afterClass() {
        Config.group_provider = savedGroupProvider;
        GlobalStateMgr.getCurrentState().getAuthenticationMgr().replayDropGroupProvider(PROVIDER);
    }

    @AfterEach
    public void restoreConfig() {
        Config.mv_use_creator_based_authorization = savedUseCreator;
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testSubmitTaskRunsWithCreatorGroupRoles() throws Exception {
        ConnectContext session = login(new UserIdentity(NATIVE_USER, "%"));
        Task task = submit(session, INSERT);

        ConnectContext taskContext = taskRunContext(task);
        Assertions.assertEquals(Set.of(GROUP), taskContext.getGroups());
        Assertions.assertTrue(taskContext.getCurrentRoleIds().contains(roleId(GROUP_ROLE)));
        Assertions.assertTrue(taskContext.getCurrentRoleIds().contains(roleId(OWN_ROLE)));
        Assertions.assertDoesNotThrow(() -> authorize(taskContext, task.getDefinition()));
    }

    @Test
    public void testMvRefreshRunsWithCreatorGroupRoles() throws Exception {
        ConnectContext session = login(new UserIdentity(NATIVE_USER, "%"));
        executeAs(session, "CREATE MATERIALIZED VIEW test.group_mv REFRESH DEFERRED MANUAL " +
                "PROPERTIES ('replication_num' = '1') AS SELECT k, v FROM test.group_src");
        try {
            MaterializedView mv = getMv("group_mv");
            Task task = GlobalStateMgr.getCurrentState().getTaskManager()
                    .getTask(TaskBuilder.getMvTaskName(mv.getId()));

            ConnectContext refreshContext = taskRunContext(task);
            Assertions.assertEquals(Set.of(GROUP), refreshContext.getGroups());
            // PCT checks the base-table SELECT when the refresh's INSERT OVERWRITE is planned for execution.
            Assertions.assertDoesNotThrow(() -> authorize(refreshContext, "SELECT k, v FROM test.group_src"));
        } finally {
            starRocksAssert.dropMaterializedView("group_mv");
        }
    }

    @Test
    public void testExternalUserTaskRunGetsGroupRoles() throws Exception {
        UserIdentity external = UserIdentity.createEphemeralUserIdent(EXTERNAL_USER, "%");
        ConnectContext session = login(external);
        Task task = submit(session, INSERT);

        ConnectContext taskContext = taskRunContext(task);
        Assertions.assertTrue(taskContext.getCurrentUserIdentity().isEphemeral());
        Assertions.assertEquals(Set.of(GROUP), taskContext.getGroups());
        Assertions.assertEquals(Set.of(roleId(GROUP_ROLE)), taskContext.getCurrentRoleIds());
        Assertions.assertDoesNotThrow(() -> authorize(taskContext, task.getDefinition()));
    }

    @Test
    public void testGroupResolutionFailureRunsWithoutGroups() throws Exception {
        ConnectContext session = login(new UserIdentity(NATIVE_USER, "%"));
        Task task = submit(session, INSERT);

        AtomicInteger lookups = new AtomicInteger();
        new MockUp<FileGroupProvider>() {
            @Mock
            public Set<String> getGroup(UserIdentity userIdentity, String distinguishedName) {
                lookups.incrementAndGet();
                throw new IllegalStateException("directory unavailable");
            }
        };
        ConnectContext taskContext = taskRunContext(task);

        Assertions.assertTrue(lookups.get() > 0, "the task run never asked the group provider");
        Assertions.assertTrue(taskContext.getGroups().isEmpty());
        Assertions.assertFalse(taskContext.getCurrentRoleIds().contains(roleId(GROUP_ROLE)));
        Assertions.assertTrue(taskContext.getCurrentRoleIds().contains(roleId(OWN_ROLE)));
        Exception e = Assertions.assertThrows(Exception.class, () -> authorize(taskContext, task.getDefinition()));
        Assertions.assertTrue(e.getMessage().contains("Access denied"), e.getMessage());
    }

    @Test
    public void testRangerGroupPoliciesApplyInSubmitTask() throws Exception {
        new MockUp<RangerBasePlugin>() {
            @Mock
            void init() {
            }
        };
        RangerStarRocksAccessController ranger = new PolicyOnlyRangerController();
        ranger.getRangerPlugin().setPolicies(groupRowFilterAndMask());
        AccessControlProvider provider = Authorizer.getInstance();
        AccessController previous = provider.catalogToAccessControl.put(ICEBERG, ranger);
        try {
            ConnectContext session = login(new UserIdentity(RANGER_USER, "%"));
            Task task = submit(session, "INSERT INTO test.ranger_dst SELECT id, data FROM " + ICEBERG +
                    ".unpartitioned_db.t0");

            String plan = planWithPolicies(taskRunContext(task), task.getDefinition());
            Assertions.assertTrue(plan.contains("12345"), plan);
            Assertions.assertTrue(plan.contains("xmask"), plan);
        } finally {
            if (previous == null) {
                provider.catalogToAccessControl.remove(ICEBERG);
            } else {
                provider.catalogToAccessControl.put(ICEBERG, previous);
            }
        }
    }

    /** What login leaves on a session once the group provider has answered. */
    private static ConnectContext login(UserIdentity user) throws Exception {
        ConnectContext ctx = UtFrameUtils.initCtxForNewPrivilege(user);
        ctx.setGroups(Set.of(GROUP));
        ctx.setCurrentRoleIds(user, Set.of(GROUP));
        return ctx;
    }

    private static Task submit(ConnectContext session, String sql) throws Exception {
        try (var ignored = session.bindScope()) {
            session.setDatabase("test");
            String submit = "SUBMIT TASK task_run_groups_" + NEXT_TASK.incrementAndGet() + " AS " + sql;
            SubmitTaskStmt stmt = (SubmitTaskStmt) SqlParser.parse(submit, session.getSessionVariable()).get(0);
            Analyzer.analyze(stmt, session);
            Authorizer.check(stmt, session);
            return TaskBuilder.buildTask(stmt, session);
        }
    }

    private static ConnectContext taskRunContext(Task task) {
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        try {
            return taskRun.buildTaskRunConnectContext();
        } finally {
            connectContext.setThreadLocalInfo();
        }
    }

    private static void authorize(ConnectContext ctx, String sql) throws Exception {
        try (var ignored = ctx.bindScope()) {
            StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
            Analyzer.analyze(stmt, ctx);
            Authorizer.check(stmt, ctx);
        }
    }

    /** Plans the statement the way SqlTaskRunProcessor does, with every relation open to policy rewrite. */
    private static String planWithPolicies(ConnectContext ctx, String sql) throws Exception {
        try (var ignored = ctx.bindScope()) {
            ctx.setExecutionId(UUIDUtil.toTUniqueId(UUIDUtil.genUUID()));
            StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
            new AstTraverser<Void, Void>() {
                @Override
                public Void visitRelation(Relation relation, Void context) {
                    relation.setNeedRewrittenByPolicy(true);
                    return null;
                }
            }.visit(stmt);
            return StatementPlanner.plan(stmt, ctx).getExplainString(TExplainLevel.NORMAL);
        }
    }

    private static long roleId(String role) {
        return GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdByNameAllowNull(role);
    }

    private static void executeAs(ConnectContext ctx, String sql) throws Exception {
        try (var ignored = ctx.bindScope()) {
            ctx.setDatabase("test");
            StatementBase statement = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
            Analyzer.analyze(statement, ctx);
            Authorizer.check(statement, ctx);
            DDLStmtExecutor.execute(statement, ctx);
        }
    }

    private static void executeAsRoot(String sql) throws Exception {
        executeAs(connectContext, sql);
    }

    private static ServicePolicies groupRowFilterAndMask() throws Exception {
        String json = Files.readString(Path.of(System.getenv("STARROCKS_HOME"),
                "conf/ranger/ranger-servicedef-starrocks.json"));
        RangerServiceDef serviceDef = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readValue(json, RangerServiceDef.class);

        RangerPolicy rowFilter = new RangerPolicy();
        rowFilter.setId(1L);
        rowFilter.setService("starrocks");
        rowFilter.setName("t0_row_filter");
        rowFilter.setPolicyType(RangerPolicy.POLICY_TYPE_ROWFILTER);
        rowFilter.setIsEnabled(true);
        rowFilter.setResources(t0Resources(null));
        rowFilter.setRowFilterPolicyItems(List.of(new RangerPolicy.RangerRowFilterPolicyItem(
                new RangerPolicy.RangerPolicyItemRowFilterInfo("id = 12345"),
                List.of(new RangerPolicy.RangerPolicyItemAccess("select")),
                new ArrayList<>(), List.of(GROUP), new ArrayList<>(), new ArrayList<>(), false)));

        RangerPolicy mask = new RangerPolicy();
        mask.setId(2L);
        mask.setService("starrocks");
        mask.setName("t0_data_mask");
        mask.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK);
        mask.setIsEnabled(true);
        mask.setResources(t0Resources("data"));
        mask.setDataMaskPolicyItems(List.of(new RangerPolicy.RangerDataMaskPolicyItem(
                List.of(new RangerPolicy.RangerPolicyItemAccess("select")),
                new RangerPolicy.RangerPolicyItemDataMaskInfo("CUSTOM", null, "'xmask'"),
                new ArrayList<>(), List.of(GROUP), new ArrayList<>(), new ArrayList<>(), false)));

        ServicePolicies policies = new ServicePolicies();
        policies.setServiceName("starrocks");
        policies.setServiceId(1L);
        policies.setPolicyVersion(1L);
        policies.setServiceDef(serviceDef);
        policies.setPolicies(List.of(rowFilter, mask));
        return policies;
    }

    private static Map<String, RangerPolicy.RangerPolicyResource> t0Resources(String column) {
        Map<String, RangerPolicy.RangerPolicyResource> resources = new HashMap<>();
        resources.put("catalog", new RangerPolicy.RangerPolicyResource(ICEBERG));
        resources.put("database", new RangerPolicy.RangerPolicyResource("unpartitioned_db"));
        resources.put("table", new RangerPolicy.RangerPolicyResource("t0"));
        if (column != null) {
            resources.put("column", new RangerPolicy.RangerPolicyResource(column));
        }
        return resources;
    }

    /** Real Ranger row-filter and mask evaluation; access checks are not what these tests are about. */
    private static final class PolicyOnlyRangerController extends RangerStarRocksAccessController {
        @Override
        public void checkCatalogAction(ConnectContext context, String catalogName, PrivilegeType privilegeType) {
        }

        @Override
        public void checkAnyActionOnCatalog(ConnectContext context, String catalogName) {
        }

        @Override
        public void checkDbAction(ConnectContext context, String catalogName, String db, PrivilegeType privilegeType) {
        }

        @Override
        public void checkAnyActionOnDb(ConnectContext context, String catalogName, String db) {
        }

        @Override
        public void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType) {
        }

        @Override
        public void checkAnyActionOnTable(ConnectContext context, TableName tableName) {
        }

        @Override
        public void checkColumnAction(ConnectContext context, TableName tableName, String column,
                                      PrivilegeType privilegeType) {
        }
    }
}
