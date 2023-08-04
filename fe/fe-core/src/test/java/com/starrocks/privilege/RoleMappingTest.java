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

package com.starrocks.privilege;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.LDAPGroupCacheMgr;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.common.DdlException;
import com.starrocks.epack.persist.OperationTypeEPack;
import com.starrocks.epack.persist.RoleMappingPersistInfo;
import com.starrocks.epack.privilege.AuthorizationMgrEpack;
import com.starrocks.epack.privilege.LDAPRoleMapping;
import com.starrocks.epack.sql.analyzer.RoleMappingStatementAnalyzer;
import com.starrocks.epack.sql.ast.AlterRoleMappingStatement;
import com.starrocks.epack.sql.ast.CreateRoleMappingStatement;
import com.starrocks.epack.sql.ast.CreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.DropRoleMappingStatement;
import com.starrocks.epack.sql.ast.ShowRoleMappingStatement;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DropRoleStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RoleMappingTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    private static SecurityIntegration ldap2;
    private static SecurityIntegration ldap3;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        UtFrameUtils.setUpForPersistTest();

        // init databases and tables
        starRocksAssert.withDatabase("test11");
        starRocksAssert.withTable("create table test11.test11 (id int) duplicate key (id) distributed by" +
                " hash(id) buckets 10 properties(\"replication_num\"=\"1\");");

        starRocksAssert.withDatabase("test22");
        starRocksAssert.withTable("create table test22.test11 (id int) duplicate key (id) distributed by" +
                " hash(id) buckets 10 properties(\"replication_num\"=\"1\");");

        // init 2 roles
        starRocksAssert.withRole("role11read");
        starRocksAssert.withRole("role22write");

        // init priv for roles
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant select on all tables in database test11 to role role11read",
                starRocksAssert.getCtx()), starRocksAssert.getCtx());
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "grant select on all tables in database test22 to role role22write",
                starRocksAssert.getCtx()), starRocksAssert.getCtx());

        // init security integration
        String sql = "create security integration ldap2 properties (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        CreateSecurityIntegrationStatement createSecurityIntegrationStatement =
                (CreateSecurityIntegrationStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        DDLStmtExecutor.execute(createSecurityIntegrationStatement, connectContext);
        ldap2 = GlobalStateMgr.getCurrentState().getAuthenticationMgr().getSecurityIntegration("ldap2");
        sql = "create security integration ldap3 properties (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        createSecurityIntegrationStatement =
                (CreateSecurityIntegrationStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        DDLStmtExecutor.execute(createSecurityIntegrationStatement, connectContext);
        ldap3 = GlobalStateMgr.getCurrentState().getAuthenticationMgr().getSecurityIntegration("ldap3");
    }

    private void createRoleMapping(String sql) throws Exception {
        CreateRoleMappingStatement createRoleMappingStatement =
                (CreateRoleMappingStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        RoleMappingStatementAnalyzer.analyze(createRoleMappingStatement, connectContext);
        DDLStmtExecutor.execute(createRoleMappingStatement, connectContext);
    }

    private void alterRoleMapping(String sql) throws Exception {
        AlterRoleMappingStatement alterRoleMappingStatement =
                (AlterRoleMappingStatement) UtFrameUtils.parseStmtWithNewParser(sql,
                        connectContext);
        RoleMappingStatementAnalyzer.analyze(alterRoleMappingStatement, connectContext);
        DDLStmtExecutor.execute(alterRoleMappingStatement, connectContext);
    }

    private void dropRoleMapping(String name) throws Exception {
        DropRoleMappingStatement dropRoleMappingStatement =
                (DropRoleMappingStatement) UtFrameUtils.parseStmtWithNewParser("drop role mapping " + name,
                        connectContext);
        RoleMappingStatementAnalyzer.analyze(dropRoleMappingStatement, connectContext);
        DDLStmtExecutor.execute(dropRoleMappingStatement, connectContext);
    }

    @Test
    public void testCreateRoleMappingNormal() throws Exception {
        String sql = "create role mapping rm2\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap2\",\n" +
                "\"role\" = \"role11read\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        System.out.println(sql);
        createRoleMapping(sql);
        LDAPRoleMapping ldapRoleMapping = (LDAPRoleMapping) connectContext.getGlobalStateMgr()
                .getAuthorizationMgr().getRoleMappingMetaMgr().getRoleMapping("rm2");
        Assert.assertEquals("rm2", ldapRoleMapping.getName());
        Assert.assertEquals("ldap2", ldapRoleMapping.getIntegrationName());
        Assert.assertEquals("role11read", ldapRoleMapping.getRoleName());
        Assert.assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com",
                                "cn=sr_read_only_group,ou=Group,dc=apple,dc=com")
                ),
                ldapRoleMapping.getGroupSet());

        // clean
        dropRoleMapping("rm2");
    }

    private void assertExceptionContains(String sql, String msg) {
        try {
            createRoleMapping(sql);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains(msg));
        }
    }

    @Test
    public void testCreateRoleMappingAbnormal() throws Exception {
        // missing required key: 'integration_name'
        String sql = "create role mapping rm2\n" +
                "properties (\n" +
                "\"role\" = \"role11read\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        assertExceptionContains(sql, "missing required property: integration_name");

        // missing required key: 'role'
        sql = "create role mapping rm2\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap2\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        assertExceptionContains(sql, "missing required property: role");

        // if the correlated integration is of type 'ldap', we need 'ldap_group_list' key
        sql = "create role mapping rm2\n" +
                "properties (\n" +
                "\"role\" = \"role11read\",\n" +
                "\"integration_name\" = \"ldap2\"\n" +
                ")";
        assertExceptionContains(sql, "missing required property: ldap_group_list");

        // role must exist
        sql = "create role mapping rm2\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap2\",\n" +
                "\"role\" = \"role11read1\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        assertExceptionContains(sql, "role 'role11read1' doesn't exist");

        // security integration must exist
        sql = "create role mapping rm2\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap21\",\n" +
                "\"role\" = \"role11read\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        assertExceptionContains(sql, "security integration 'ldap21' doesn't exist");
    }

    @Test
    public void testRoleMappingDdlPersist() throws Exception {
        AuthorizationMgr masterManager = new AuthorizationMgrEpack(GlobalStateMgr.getCurrentState(), null);
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(emptyImage.getDataOutputStream());

        // master create role mapping rm3, rm4
        String sql1 = "create role mapping rm3\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap2\",\n" +
                "\"role\" = \"role11read\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        String sql2 = "create role mapping rm4\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap2\",\n" +
                "\"role\" = \"role22write\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group2,ou=Group,dc=apple,dc=com;\"" +
                ")";
        String sql3 = "create role mapping rm5\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap3\",\n" +
                "\"role\" = \"role22write\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group2,ou=Group,dc=apple,dc=com;\"" +
                ")";
        System.out.println(sql2);
        CreateRoleMappingStatement createRoleMappingStatement =
                (CreateRoleMappingStatement) UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);
        masterManager.getRoleMappingMetaMgr().createRoleMapping(createRoleMappingStatement.getName(),
                createRoleMappingStatement.getPropertyMap(), false);
        createRoleMappingStatement =
                (CreateRoleMappingStatement) UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
        masterManager.getRoleMappingMetaMgr().createRoleMapping(createRoleMappingStatement.getName(),
                createRoleMappingStatement.getPropertyMap(), false);
        createRoleMappingStatement =
                (CreateRoleMappingStatement) UtFrameUtils.parseStmtWithNewParser(sql3, connectContext);
        masterManager.getRoleMappingMetaMgr().createRoleMapping(createRoleMappingStatement.getName(),
                createRoleMappingStatement.getPropertyMap(), false);
        Map<String, String> alterProps = Maps.newHashMap();
        alterProps.put("ldap_group_list",
                "cn=sr_read_only_group99,ou=Group,dc=apple,dc=com;cn=sr_read_only_group88,ou=Group,dc=apple,dc=com");
        masterManager.getRoleMappingMetaMgr().alterRoleMapping("rm5", alterProps, false);
        masterManager.getRoleMappingMetaMgr().dropRoleMapping("rm5", false);

        // make final snapshot
        UtFrameUtils.PseudoImage finalImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(finalImage.getDataOutputStream());

        new MockUp<LDAPGroupCacheMgr>() {
            @Mock
            public List<String> getBelongedGroupsByUsername(String integrationName, String username) {
                if (username.equals("ldap_user")) {
                    return Arrays.asList("cn=sr_read_only_group,ou=Group,dc=apple,dc=com",
                            "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com");
                } else {
                    return null;
                }
            }
        };

        new MockUp<AuthorizationMgr>() {
            @Mock
            public Long getRoleIdByNameAllowNull(String name) {
                if (name.equals("role11read")) {
                    return 11L;
                } else if (name.equals("role22write")) {
                    return 22L;
                }
                return -1L;
            }
        };

        new MockUp<AuthenticationMgr>() {
            @Mock
            public SecurityIntegration getSecurityIntegration(String name) {
                if (name.equals("ldap2")) {
                    return ldap2;
                } else if (name.equals("ldap3")) {
                    return ldap3;
                } else {
                    return null;
                }
            }
        };

        // check role mapping for ldap user
        Set<Long> mappedRoleIds =
                masterManager.getRoleMappingMetaMgr().getMappedRoleIdsForLdapUser("ldap2", "ldap_user");
        System.out.println(mappedRoleIds);
        Assert.assertEquals(2, mappedRoleIds.size());
        Assert.assertTrue(mappedRoleIds.contains(11L));
        Assert.assertTrue(mappedRoleIds.contains(22L));

        // test replay OP_CREATE_ROLE_MAPPING edit log
        AuthorizationMgr followerManager = new AuthorizationMgrEpack(GlobalStateMgr.getCurrentState(), null);
        followerManager.loadV2(new SRMetaBlockReader(emptyImage.getDataInputStream()));
        Assert.assertNull(followerManager.getRoleMappingMetaMgr().getRoleMapping("rm3"));
        RoleMappingPersistInfo info = (RoleMappingPersistInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_ROLE_MAPPING);
        followerManager.getRoleMappingMetaMgr().replayCreateRoleMapping(info.name, info.propertyMap);
        Assert.assertNotNull(followerManager.getRoleMappingMetaMgr().getRoleMapping("rm3"));
        info = (RoleMappingPersistInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_ROLE_MAPPING);
        followerManager.getRoleMappingMetaMgr().replayCreateRoleMapping(info.name, info.propertyMap);
        Assert.assertNotNull(followerManager.getRoleMappingMetaMgr().getRoleMapping("rm4"));
        info = (RoleMappingPersistInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_ROLE_MAPPING);
        followerManager.getRoleMappingMetaMgr().replayCreateRoleMapping(info.name, info.propertyMap);
        Assert.assertNotNull(followerManager.getRoleMappingMetaMgr().getRoleMapping("rm5"));

        // test replay OP_ALTER_ROLE_MAPPING
        info = (RoleMappingPersistInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_ALTER_ROLE_MAPPING);
        followerManager.getRoleMappingMetaMgr().replayAlterRoleMapping(info.name, info.propertyMap);
        Assert.assertTrue(followerManager.getRoleMappingMetaMgr().getGroupToRolesMap().get("ldap3")
                .get("cn=sr_read_only_group99,ou=Group,dc=apple,dc=com").contains("role22write"));
        Assert.assertTrue(followerManager.getRoleMappingMetaMgr().getGroupToRolesMap().get("ldap3")
                .get("cn=sr_read_only_group88,ou=Group,dc=apple,dc=com").contains("role22write"));

        // test replay OP_DROP_ROLE_MAPPING
        info = (RoleMappingPersistInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_DROP_ROLE_MAPPING);
        followerManager.getRoleMappingMetaMgr().replayDropRoleMapping(info.name);
        Assert.assertNull(followerManager.getRoleMappingMetaMgr().getRoleMapping("rm5"));

        // simulate restart (load from image)
        AuthorizationMgr imageManager = new AuthorizationMgrEpack(GlobalStateMgr.getCurrentState(), null);
        imageManager.loadV2(new SRMetaBlockReader(finalImage.getDataInputStream()));
        Assert.assertNotNull(imageManager.getRoleMappingMetaMgr().getRoleMapping("rm4"));
        Assert.assertNull(imageManager.getRoleMappingMetaMgr().getRoleMapping("rm5"));
    }

    @Test
    public void testDropRoleMapping() throws Exception {
        String sql = "create role mapping rm21\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap2\",\n" +
                "\"role\" = \"role11read\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        createRoleMapping(sql);
        sql = "create role mapping rm22\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap2\",\n" +
                "\"role\" = \"role11read\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com\"" +
                ")";
        createRoleMapping(sql);
        sql = "create role mapping rm23\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap3\",\n" +
                "\"role\" = \"role11read\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        createRoleMapping(sql);

        List<String> roleMappings = Arrays.asList("rm21", "rm23");
        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        for (String roleMapping : roleMappings) {
            dropRoleMapping(roleMapping);
            Assert.assertNull(authorizationManager.getRoleMappingMetaMgr().getRoleMapping(roleMapping));
        }
        Assert.assertEquals(1, authorizationManager.getRoleMappingMetaMgr().getAllRoleMappings().size());

        Map<String, Set<String>> roleToRoleMappingsMap =
                authorizationManager.getRoleMappingMetaMgr().getRoleToRoleMappingsMap();
        Assert.assertEquals(1, roleToRoleMappingsMap.size());
        Assert.assertEquals(Sets.newHashSet(Collections.singletonList("rm22")),
                roleToRoleMappingsMap.get("role11read"));

        Map<String, Map<String, List<String>>> groupToRolesMap = authorizationManager.getRoleMappingMetaMgr()
                .getGroupToRolesMap();
        Assert.assertEquals(1, groupToRolesMap.size());
        Map<String, List<String>> map = groupToRolesMap.get("ldap2");
        Assert.assertEquals(1, map.size());
        Assert.assertEquals(Collections.singletonList("role11read"),
                map.get("cn=sr_read_only_group,ou=Group,dc=apple,dc=com"));

        // clean
        dropRoleMapping("rm22");
    }

    @Test
    public void testShowRoleMapping() throws Exception {
        String sql = "create role mapping rm23\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap3\",\n" +
                "\"role\" = \"role11read\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        createRoleMapping(sql);
        sql = "create role mapping rm21\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap2\",\n" +
                "\"role\" = \"role11read\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        createRoleMapping(sql);

        ShowRoleMappingStatement showStmt = (ShowRoleMappingStatement) UtFrameUtils
                .parseStmtWithNewParser("SHOW role mappings", connectContext);
        RoleMappingStatementAnalyzer.analyze(showStmt, connectContext);
        ShowResultSet res = new ShowExecutor(connectContext, showStmt).execute();
        System.out.println(res.getResultRows());

        Assert.assertEquals(2, res.getResultRows().size());
        Assert.assertEquals("[rm23, ldap3, role11read, cn=sr_read_only_group2,ou=Group,dc=apple,dc=com;" +
                        "cn=sr_read_only_group,ou=Group,dc=apple,dc=com, 1970-01-01 07:59:59]",
                res.getResultRows().get(1).toString());

        // clean
        dropRoleMapping("rm23");
        dropRoleMapping("rm21");
    }

    @Test
    public void testAlterRoleMapping() throws Exception {
        String sql = "create role mapping rm23\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap3\",\n" +
                "\"role\" = \"role11read\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        createRoleMapping(sql);

        // test alter non-existed role mapping
        sql = "alter role mapping rm23_non_exist set (\n" +
                "\"integration_name\" = \"ldap3\",\n" +
                "\"role\" = \"role22write\"\n" +
                ")";
        try {
            alterRoleMapping(sql);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("role mapping 'rm23_non_exist' not found"));
        }

        // test change to non-existed role
        sql = "alter role mapping rm23 set (\n" +
                "\"integration_name\" = \"ldap3\",\n" +
                "\"role\" = \"role22write_non_exist\"\n" +
                ")";
        try {
            alterRoleMapping(sql);
        } catch (SemanticException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("role 'role22write_non_exist' doesn't exist"));
        }

        // test change to non-existed integration
        sql = "alter role mapping rm23 set (\n" +
                "\"integration_name\" = \"ldap3_non_exist\",\n" +
                "\"role\" = \"role22write\"\n" +
                ")";
        try {
            alterRoleMapping(sql);
        } catch (DdlException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("security integration 'ldap3_non_exist' doesn't exist"));
        }

        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();

        // test only change role and check group list
        sql = "alter role mapping rm23 set (\n" +
                "\"integration_name\" = \"ldap3\",\n" +
                "\"role\" = \"role22write\"\n" +
                ")";
        System.out.println(sql);
        alterRoleMapping(sql);
        Map<String, Set<String>> roleToRoleMappingsMap =
                authorizationManager.getRoleMappingMetaMgr().getRoleToRoleMappingsMap();
        Assert.assertEquals(1, roleToRoleMappingsMap.size());
        Assert.assertEquals(Sets.newHashSet(Collections.singletonList("rm23")),
                roleToRoleMappingsMap.get("role22write"));
        Map<String, Map<String, List<String>>> groupToRolesMap = authorizationManager.getRoleMappingMetaMgr()
                .getGroupToRolesMap();
        Assert.assertEquals(1, groupToRolesMap.size());
        Map<String, List<String>> map = groupToRolesMap.get("ldap3");
        Assert.assertEquals(2, map.size());
        Assert.assertEquals(Collections.singletonList("role22write"),
                map.get("cn=sr_read_only_group,ou=Group,dc=apple,dc=com"));

        // test change group list
        sql = "alter role mapping rm23 set (\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group2,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group23,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        alterRoleMapping(sql);
        roleToRoleMappingsMap =
                authorizationManager.getRoleMappingMetaMgr().getRoleToRoleMappingsMap();
        Assert.assertEquals(1, roleToRoleMappingsMap.size());
        Assert.assertEquals(Sets.newHashSet(Collections.singletonList("rm23")),
                roleToRoleMappingsMap.get("role22write"));
        groupToRolesMap = authorizationManager.getRoleMappingMetaMgr()
                .getGroupToRolesMap();
        Assert.assertEquals(1, groupToRolesMap.size());
        map = groupToRolesMap.get("ldap3");
        Assert.assertEquals(2, map.size());
        Assert.assertEquals(Collections.singletonList("role22write"),
                map.get("cn=sr_read_only_group2,ou=Group,dc=apple,dc=com"));
        Assert.assertEquals(Collections.singletonList("role22write"),
                map.get("cn=sr_read_only_group23,ou=Group,dc=apple,dc=com"));

        // test change security integration
        sql = "alter role mapping rm23 set (\n" +
                "\"integration_name\" = \"ldap2\"\n" +
                ")";
        alterRoleMapping(sql);
        roleToRoleMappingsMap =
                authorizationManager.getRoleMappingMetaMgr().getRoleToRoleMappingsMap();
        Assert.assertEquals(1, roleToRoleMappingsMap.size());
        Assert.assertEquals(Sets.newHashSet(Collections.singletonList("rm23")),
                roleToRoleMappingsMap.get("role22write"));
        groupToRolesMap = authorizationManager.getRoleMappingMetaMgr()
                .getGroupToRolesMap();
        Assert.assertEquals(1, groupToRolesMap.size());
        map = groupToRolesMap.get("ldap2");
        Assert.assertEquals(2, map.size());

        // clean
        dropRoleMapping("rm23");
    }

    @Test
    public void testDropRoleForbidden() throws Exception {
        String sql = "create role mapping rm21\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap2\",\n" +
                "\"role\" = \"role11read\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        createRoleMapping(sql);

        try {
            DropRoleStmt stmt =
                    (DropRoleStmt) UtFrameUtils.parseStmtWithNewParser("drop role role11read", connectContext);
            DDLStmtExecutor.execute(stmt, connectContext);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains(
                    "cannot drop role 'role11read' because role mappings '[rm21]' are using this role"));
        }

        // clean
        dropRoleMapping("rm21");
    }
}
