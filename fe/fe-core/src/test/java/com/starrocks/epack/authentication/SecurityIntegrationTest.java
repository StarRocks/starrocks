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

package com.starrocks.epack.authentication;

import com.google.common.collect.Lists;
import com.starrocks.authentication.AuthenticationException;
import com.starrocks.authentication.AuthenticationHandler;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.epack.authorization.RoleMappingMetaMgr;
import com.starrocks.epack.persist.OperationTypeEPack;
import com.starrocks.epack.qe.ShowExecutorVisitorEPack;
import com.starrocks.epack.sql.analyzer.RoleMappingStatementAnalyzer;
import com.starrocks.epack.sql.ast.CreateRoleMappingStatement;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.privilege.AuthPlugin;
import com.starrocks.persist.SecurityIntegrationPersistInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SecurityIntegrationStatementAnalyzer;
import com.starrocks.sql.ast.integration.AlterSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.CreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.DropSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowSecurityIntegrationStatement;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityIntegrationTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.enable_create_ldap_security_integration = true;
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        UtFrameUtils.setUpForPersistTest();
    }

    private void createSecurityIntegration(String sql) throws Exception {
        CreateSecurityIntegrationStatement createSecurityIntegrationStatement =
                (CreateSecurityIntegrationStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        SecurityIntegrationStatementAnalyzer.analyze(createSecurityIntegrationStatement, connectContext);
        DDLStmtExecutor.execute(createSecurityIntegrationStatement, connectContext);
    }

    private void alterSecurityIntegration(String sql) throws Exception {
        AlterSecurityIntegrationStatement alterSecurityIntegrationStatement =
                (AlterSecurityIntegrationStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        SecurityIntegrationStatementAnalyzer.analyze(alterSecurityIntegrationStatement, connectContext);
        DDLStmtExecutor.execute(alterSecurityIntegrationStatement, connectContext);
    }

    private void dropSecurityIntegration(String name) throws Exception {
        String sql = "drop security integration " + name;
        DropSecurityIntegrationStatement dropSecurityIntegrationStatement =
                (DropSecurityIntegrationStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        SecurityIntegrationStatementAnalyzer.analyze(dropSecurityIntegrationStatement, connectContext);
        DDLStmtExecutor.execute(dropSecurityIntegrationStatement, connectContext);
    }

    @Test
    public void testCreateSecurityIntegrationNormal() throws Exception {
        AuthenticationMgrEPack authenticationMgrEPack =
                (AuthenticationMgrEPack) GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String sql = "create security integration ldap1 properties (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        createSecurityIntegration(sql);
        LDAPSecurityIntegration ldap1 = (LDAPSecurityIntegration)
                authenticationMgrEPack.getSecurityIntegration("ldap1");
        Assert.assertEquals("ldap", ldap1.getType());
        Assert.assertEquals("memberUid", ldap1.getLdapUserGroupMatchAttr());
        Assert.assertEquals("uid", ldap1.getLdapUserSearchAttr());
        Assert.assertEquals("uid=admin", ldap1.getLdapBindRootDn());
        Assert.assertEquals("aaa", ldap1.getLdapBindRootPwd());
        Assert.assertEquals("dc=apple, dc=com", ldap1.getLdapBindBaseDn());
        Assert.assertEquals(1500, ldap1.getLdapCacheRefreshInterval());
        authenticationMgrEPack.dropSecurityIntegration("ldap1", false);

        sql = "create security integration ldap1 properties (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"regex:CN=.*\\\\(([^)]+)\\\\)\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        createSecurityIntegration(sql);
        ldap1 = (LDAPSecurityIntegration)
                authenticationMgrEPack.getSecurityIntegration("ldap1");
        System.out.println("regex:CN=.*\\(([^)]+)\\)");
        System.out.println(ldap1.getLdapUserGroupMatchAttr());
        Assert.assertEquals("regex:CN=.*\\(([^)]+)\\)", ldap1.getLdapUserGroupMatchAttr());
        authenticationMgrEPack.dropSecurityIntegration("ldap1", false);
    }

    private void assertExceptionContains(String sql, String msg) {
        try {
            createSecurityIntegration(sql);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(msg));
        }
    }

    @Test
    public void testCreateSecurityIntegrationAbnormal() throws Exception {
        // missing type
        String sql = "create security integration ldap1 properties (" +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        assertExceptionContains(sql, "missing required property: type");

        // missing root dn
        sql = "create security integration ldap1 properties (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        assertExceptionContains(sql, "missing required property: ldap_bind_root_dn");

        // unsupported type
        sql = "create security integration ldap1 properties (" +
                "\"type\" = \"oracle\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        assertExceptionContains(sql, "unsupported security integration type 'oracle'");

        // already exists
        sql = "create security integration ldap2 properties (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        createSecurityIntegration(sql);
        assertExceptionContains(sql, "security integration 'ldap2' already exists");

        // invalid ldap_group_match_use_member_uid
        sql = "create security integration ldap221 properties (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_group_match_use_member_uid\" = \"invalid\"" +
                ")";
        assertExceptionContains(sql, "invalid 'ldap_group_match_use_member_uid' property value");

        // invalid ldap_user_group_match_attr with regex format
        sql = "create security integration ldap222 properties (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"regex:\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_group_match_use_member_uid\" = \"false\"" +
                ")";
        assertExceptionContains(sql, "invalid 'ldap_user_group_match_attr' property value");
    }

    @Test
    public void testRetrieveMemberName() {
        // non-regex format
        String result = LDAPGroupCacheMgr.retrieveMemberNameFromDn(
                "memberUid=Harry,OU=Group,DC=example,DC=com", "memberUid");
        Assert.assertEquals("Harry", result);
        result = LDAPGroupCacheMgr.retrieveMemberNameFromDn(
                "uid=Hermione,OU=Group,DC=example,DC=com", "uid");
        Assert.assertEquals("Hermione", result);
        result = LDAPGroupCacheMgr.retrieveMemberNameFromDn(
                "uid=Ron,OU=Group,DC=example,DC=com", "name");
        Assert.assertNull(result);
        result = LDAPGroupCacheMgr.retrieveMemberNameFromDn("uid=Ron\\, xxx,OU=Group,DC=example,DC=com", "uid");
        Assert.assertEquals("Ron\\, xxx", result);

        // regex format
        result = LDAPGroupCacheMgr.retrieveMemberNameFromDn(
                "CN=Albus Percival Wulfric Brian Dumbledore (apwbd),OU=Group,DC=example,DC=com",
                "regex:CN=.*\\(([^)]+)\\)");
        Assert.assertEquals("apwbd", result);
        result = LDAPGroupCacheMgr.retrieveMemberNameFromDn(
                "CN=Albus Percival Wulfric Brian Dumbledore - apwbd,OU=Group,DC=example,DC=com",
                "regex:CN=.* - (.*)");
        Assert.assertEquals("apwbd", result);
    }

    @Test
    public void testSecurityIntegrationDdlPersist() throws Exception {
        AuthenticationMgrEPack masterManager = new AuthenticationMgrEPack();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(emptyImage.getImageWriter());

        // master create security integration ldap3
        String sql = "create security integration ldap3 properties (" +
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
        masterManager.createSecurityIntegration(createSecurityIntegrationStatement.getName(),
                createSecurityIntegrationStatement.getPropertyMap(), false);
        sql = "create security integration ldap4 properties (" +
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
        masterManager.createSecurityIntegration(createSecurityIntegrationStatement.getName(),
                createSecurityIntegrationStatement.getPropertyMap(), false);
        masterManager.dropSecurityIntegration("ldap4", false);
        sql = "alter security integration ldap3 set (" +
                "\"ldap_bind_root_pwd\" = \"bbb\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"3000\"" +
                ")";

        new MockUp<GlobalStateMgr>() {
            @Mock
            public AuthenticationMgr getAuthenticationMgr() {
                return masterManager;
            }
        };

        AlterSecurityIntegrationStatement alterSecurityIntegrationStatement =
                (AlterSecurityIntegrationStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        masterManager.alterSecurityIntegration(alterSecurityIntegrationStatement.getName(),
                alterSecurityIntegrationStatement.getProperties(), false);

        // make final snapshot
        UtFrameUtils.PseudoImage finalImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(finalImage.getImageWriter());

        // test replay OP_CREATE_SECURITY_INTEGRATION edit log
        AuthenticationMgrEPack followerManager = new AuthenticationMgrEPack();
        followerManager.loadV2(emptyImage.getMetaBlockReader());
        Assert.assertNull(followerManager.getSecurityIntegration("ldap3"));
        SecurityIntegrationPersistInfo info = (SecurityIntegrationPersistInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_SECURITY_INTEGRATION);
        followerManager.replayCreateSecurityIntegration(info.name, info.propertyMap);
        Assert.assertNotNull(followerManager.getSecurityIntegration("ldap3"));

        // test replay OP_DROP_SECURITY_INTEGRATION edit log
        info = (SecurityIntegrationPersistInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_CREATE_SECURITY_INTEGRATION);
        followerManager.replayCreateSecurityIntegration(info.name, info.propertyMap);
        Assert.assertNotNull(followerManager.getSecurityIntegration("ldap4"));
        info = (SecurityIntegrationPersistInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_DROP_SECURITY_INTEGRATION);
        followerManager.replayDropSecurityIntegration(info.name);
        Assert.assertNull(followerManager.getSecurityIntegration("ldap4"));

        // test replay OP_ALTER_SECURITY_INTEGRATION edit log
        info = (SecurityIntegrationPersistInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationTypeEPack.OP_ALTER_SECURITY_INTEGRATION);
        followerManager.replayAlterSecurityIntegration(info.name, info.propertyMap);
        Assert.assertEquals(3000,
                ((LDAPSecurityIntegration) followerManager.getSecurityIntegration("ldap3"))
                        .getLdapCacheRefreshInterval());

        // simulate restart (load from image)
        AuthenticationMgrEPack imageManager = new AuthenticationMgrEPack();
        imageManager.loadV2(finalImage.getMetaBlockReader());
        Assert.assertNotNull(imageManager.getSecurityIntegration("ldap3"));
        Assert.assertNull(imageManager.getSecurityIntegration("ldap4"));
        Assert.assertEquals("bbb",
                ((LDAPSecurityIntegration) imageManager.getSecurityIntegration("ldap3"))
                        .getLdapBindRootPwd());

        new MockUp<LDAPAuthProviderForExternal>() {
            @Mock
            public void authenticate(ConnectContext context, UserIdentity userIdentity, byte[] authResponse)
                    throws AuthenticationException {
            }
        };

        new MockUp<RoleMappingMetaMgr>() {
            @Mock
            public Set<Long> getMappedRoleIdsForLdapUser(String integrationName, String username) {
                return new HashSet<>(Arrays.asList(11L, 22L));
            }
        };

        // check authentication with auth chain
        System.out.println(Arrays.asList(Config.authentication_chain));
        Config.authentication_chain = new String[] {"ldap3", "native"};
        System.out.println(Arrays.asList(Config.authentication_chain));
        byte[] seed = "petals on a wet black bough".getBytes(StandardCharsets.UTF_8);
        byte[] scramble = MysqlPassword.scramble(seed, "abc");
        connectContext.setAuthDataSalt(seed);
        connectContext.setAuthPlugin(AuthPlugin.Client.MYSQL_CLEAR_PASSWORD.toString());
        UserIdentity userIdentity =
                AuthenticationHandler.authenticate(connectContext, "ldap_external_user", "192.168.0.1", scramble);

        System.out.println(userIdentity);
        Assert.assertEquals("'ldap_external_user'@'192.168.0.1'", userIdentity.toString());
        Assert.assertTrue(userIdentity.isEphemeral());
    }

    private void createRoleMapping(String sql) throws Exception {
        CreateRoleMappingStatement createRoleMappingStatement =
                (CreateRoleMappingStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        RoleMappingStatementAnalyzer.analyze(createRoleMappingStatement, connectContext);
        DDLStmtExecutor.execute(createRoleMappingStatement, connectContext);
    }

    @Test
    public void testAlterSecurityIntegration() throws Exception {
        AuthenticationMgrEPack authenticationMgrEPack =
                (AuthenticationMgrEPack) GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        String sql = "create security integration ldap1foralter properties (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        createSecurityIntegration(sql);

        // test not found
        sql = "alter security integration ldap1foralter_none_exist set (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        try {
            alterSecurityIntegration(sql);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("security integration 'ldap1foralter_none_exist' not found"));
        }

        // test alter type not allowed
        sql = "alter security integration ldap1foralter set (" +
                "\"type\" = \"ldap1\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        try {
            alterSecurityIntegration(sql);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("'type' property cannot be changed"));
        }

        // test alter normal
        LDAPSecurityIntegration integrationForAlterOld = (LDAPSecurityIntegration)
                authenticationMgrEPack.getSecurityIntegration("ldap1foralter");
        Assert.assertEquals("memberUid", integrationForAlterOld.getLdapUserGroupMatchAttr());
        sql = "alter security integration ldap1foralter set (" +
                "\"ldap_user_group_match_attr\" = \"uidOfNames\"," +
                "\"ldap_bind_root_pwd\" = \"bbb\"" +
                ")";
        alterSecurityIntegration(sql);
        LDAPSecurityIntegration integrationForAlterNew = (LDAPSecurityIntegration)
                authenticationMgrEPack.getSecurityIntegration("ldap1foralter");
        Assert.assertEquals("uidOfNames", integrationForAlterNew.getLdapUserGroupMatchAttr());
        Assert.assertEquals("bbb", integrationForAlterNew.getLdapBindRootPwd());

        // clean
        dropSecurityIntegration("ldap1foralter");
    }

    @Test
    public void testDropSecurityIntegration() throws Exception {
        String sql = "create security integration ldap1fordrop properties (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        createSecurityIntegration(sql);
        sql = "create role mapping rm2\n" +
                "properties (\n" +
                "\"integration_name\" = \"ldap1fordrop\",\n" +
                "\"role\" = \"role11read\",\n" +
                "\"ldap_group_list\" = \"cn=sr_read_only_group,ou=Group,dc=apple,dc=com;" +
                "cn=sr_read_only_group2,ou=Group,dc=apple,dc=com\"\n" +
                ")";
        starRocksAssert.withRole("role11read");
        createRoleMapping(sql);
        try {
            dropSecurityIntegration("ldap1fordrop");
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("[rm2] role mappings are currently associated with"));
        }
    }

    @Test
    public void testShowSecurityIntegration() throws Exception {
        String sql = "create security integration ldap1forshow properties (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        createSecurityIntegration(sql);
        sql = "create security integration ldap2forshow properties (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        createSecurityIntegration(sql);
        ShowSecurityIntegrationStatement showStmt = (ShowSecurityIntegrationStatement) UtFrameUtils
                .parseStmtWithNewParser("SHOW security integrations", connectContext);
        SecurityIntegrationStatementAnalyzer.analyze(showStmt, connectContext);
        ShowResultSet res = new ShowExecutorVisitorEPack().visitShowSecurityIntegrationStatement(showStmt, connectContext);
        System.out.println(res.getResultRows());
        Assert.assertEquals("[ldap1forshow, ldap, \\N]", res.getResultRows().get(0).toString());
        Assert.assertEquals("[ldap2forshow, ldap, \\N]", res.getResultRows().get(1).toString());
    }

    @Test
    public void testShowCreateSecurityIntegration() throws Exception {
        String sql = "create security integration ldap1forshowcreate properties (" +
                "\"type\" = \"ldap\"," +
                "\"ldap_user_group_match_attr\" = \"memberUid\"," +
                "\"ldap_user_search_attr\" = \"uid\"," +
                "\"ldap_bind_root_dn\" = \"uid=admin\"," +
                "\"ldap_bind_root_pwd\" = \"aaa\"," +
                "\"ldap_bind_base_dn\" = \"dc=apple, dc=com\"," +
                "\"ldap_cache_refresh_interval\" = \"1500\"" +
                ")";
        createSecurityIntegration(sql);
        ShowCreateSecurityIntegrationStatement showStmt = (ShowCreateSecurityIntegrationStatement) UtFrameUtils
                .parseStmtWithNewParser("SHOW create security integration ldap1forshowcreate", connectContext);
        SecurityIntegrationStatementAnalyzer.analyze(showStmt, connectContext);
        ShowResultSet res = new ShowExecutorVisitorEPack().visitShowCreateSecurityIntegrationStatement(showStmt, connectContext);
        System.out.println(res.getResultRows());
        Assert.assertTrue(res.getResultRows().get(0).get(1).contains("\"ldap_user_group_match_attr\" = \"memberUid\""));
        // test the show create result can actually work
        dropSecurityIntegration("ldap1forshowcreate");
        System.out.println(res.getResultRows().get(0).get(1));
        createSecurityIntegration(res.getResultRows().get(0).get(1));
        // clean
        dropSecurityIntegration("ldap1forshowcreate");
    }

    @Test
    public void testGetFinalMemberAttrId() throws Exception {
        Attributes attributes = new BasicAttributes();
        attributes.put(new BasicAttribute("member;range=0-1499", "aaa"));
        attributes.put(new BasicAttribute("member;range=0-1499", "bbb"));
        attributes.put(new BasicAttribute("member;range=0-1499", "ccc"));
        attributes.put(new BasicAttribute("member;range=0-1499", "ddd"));
        attributes.put(new BasicAttribute("member;range=0-1499", "eee"));
        Assert.assertEquals("member", LDAPGroupCacheMgr.getMemberAttrId(attributes, "member"));
        Assert.assertEquals("member;range=0-1499", LDAPGroupCacheMgr.getMemberAttrId(attributes,
                "regex:member;range=(\\d+)-(\\d+)"));
    }

    @Test
    public void testGetMemberNamesFromGroupOfNamesException() throws Exception {
        String groupName = "cn=group_rd1,ou=Group,dc=example,dc=com";
        InitialDirContext initialDirContext = mock(InitialDirContext.class);
        mockGroupAttribute(initialDirContext, groupName, "member;range=0-1499", "groupOfNames",
                Lists.newArrayList("uid=zhangsan,ou=people,dc=example,dc=com",
                        "uid=lisi,ou=people,dc=example,dc=com", "uid=wangwu,ou=people,dc=example,dc=com",
                        "uid=maliu,ou=people,dc=example,dc=com", "uid=xiaoming,ou=people,dc=example,dc=com",
                        "cn=sub_group_rd1,ou=Group,dc=example,dc=com"));
        mockGroupAttribute(initialDirContext, "cn=sub_group_rd1,ou=Group,dc=example,dc=com",
                "member;range=0-1499", "groupOfNames",
                Lists.newArrayList("uid=xiaohong,ou=people,dc=example,dc=com"));

        // wrong regex
        Set<String> names =  LDAPGroupCacheMgr.getMemberNamesFromGroupOfNames(initialDirContext, groupName,
                "regex:aaa", "uid");
        Assert.assertEquals(0, names.size());
        names =  LDAPGroupCacheMgr.getMemberNamesFromGroupOfNames(initialDirContext, groupName,
                "members", "uid");
        Assert.assertEquals(0, names.size());
    }

    @Test
    public void testGetMemberNamesFromADGroupException() throws Exception {
        String groupName = "cn=group_rd1,ou=Group,dc=example,dc=com";
        InitialDirContext initialDirContext = mock(InitialDirContext.class);
        mockGroupAttribute(initialDirContext, groupName, "member;range=0-1499", "group",
                Lists.newArrayList("uid=zhangsan,ou=people,dc=example,dc=com",
                        "uid=lisi,ou=people,dc=example,dc=com", "uid=wangwu,ou=people,dc=example,dc=com",
                        "uid=maliu,ou=people,dc=example,dc=com", "uid=xiaoming,ou=people,dc=example,dc=com",
                        "cn=sub_group_rd1,ou=Group,dc=example,dc=com"));
        mockGroupAttribute(initialDirContext, "cn=sub_group_rd1,ou=Group,dc=example,dc=com",
                "member;range=0-1499", "group",
                Lists.newArrayList("uid=xiaohong,ou=people,dc=example,dc=com"));

        // wrong regex
        Set<String> names =  LDAPGroupCacheMgr.getMemberNamesFromADGroup(initialDirContext, groupName,
                "regex:aaa", "uid", false);
        Assert.assertEquals(0, names.size());
        names =  LDAPGroupCacheMgr.getMemberNamesFromADGroup(initialDirContext, groupName,
                "members", "uid", false);
        Assert.assertEquals(0, names.size());
    }

    @Test
    public void testGetMemberNamesFromGroupOfNames() throws Exception {
        // 1. test getNames by regex:member;range=(\d+)-(\d+)
        String groupName = "cn=group_rd1,ou=Group,dc=example,dc=com";
        InitialDirContext initialDirContext = mock(InitialDirContext.class);
        mockGroupAttribute(initialDirContext, groupName, "member;range=0-1499", "groupOfNames",
                Lists.newArrayList("uid=zhangsan,ou=people,dc=example,dc=com",
                        "uid=lisi,ou=people,dc=example,dc=com", "uid=wangwu,ou=people,dc=example,dc=com",
                        "uid=maliu,ou=people,dc=example,dc=com", "uid=xiaoming,ou=people,dc=example,dc=com",
                        "cn=sub_group_rd1,ou=Group,dc=example,dc=com"));
        mockGroupAttribute(initialDirContext, "cn=sub_group_rd1,ou=Group,dc=example,dc=com",
                "member;range=0-1499", "groupOfNames",
                Lists.newArrayList("uid=xiaohong,ou=people,dc=example,dc=com"));

        Set<String> names = LDAPGroupCacheMgr.getMemberNamesFromGroupOfNames(initialDirContext, groupName,
                "regex:member;range=(\\d+)-(\\d+)", "uid");
        Assert.assertEquals(6, names.size());
        Assert.assertTrue(names.contains("zhangsan"));
        Assert.assertTrue(names.contains("lisi"));
        Assert.assertTrue(names.contains("wangwu"));
        Assert.assertTrue(names.contains("maliu"));
        Assert.assertTrue(names.contains("xiaoming"));
        Assert.assertTrue(names.contains("xiaohong"));

        // 2. test getNames by member
        String groupName2 = "cn=group_rd2,ou=Group,dc=example,dc=com";
        mockGroupAttribute(initialDirContext, groupName2, "member", "groupOfNames",
                Lists.newArrayList("uid=zhangsan,ou=people,dc=example,dc=com",
                        "uid=lisi,ou=people,dc=example,dc=com", "uid=wangwu,ou=people,dc=example,dc=com",
                        "uid=maliu,ou=people,dc=example,dc=com", "uid=xiaoming,ou=people,dc=example,dc=com",
                        "cn=sub_group_rd2,ou=Group,dc=example,dc=com"));
        mockGroupAttribute(initialDirContext, "cn=sub_group_rd2,ou=Group,dc=example,dc=com",
                "member", "groupOfNames",
                Lists.newArrayList("uid=xiaohong,ou=people,dc=example,dc=com"));

        names = LDAPGroupCacheMgr.getMemberNamesFromGroupOfNames(initialDirContext, groupName2, "member", "uid");
        Assert.assertEquals(6, names.size());
        Assert.assertTrue(names.contains("zhangsan"));
        Assert.assertTrue(names.contains("lisi"));
        Assert.assertTrue(names.contains("wangwu"));
        Assert.assertTrue(names.contains("maliu"));
        Assert.assertTrue(names.contains("xiaoming"));
        Assert.assertTrue(names.contains("xiaohong"));
    }

    @Test
    public void testGetMemberNamesFromADGroup() throws Exception {
        // 1. test getNames by regex:member;range=(\d+)-(\d+)
        String groupName = "cn=group_rd1,ou=Group,dc=example,dc=com";
        InitialDirContext initialDirContext = mock(InitialDirContext.class);
        mockGroupAttribute(initialDirContext, groupName, "member;range=0-1499", "group",
                Lists.newArrayList("uid=zhangsan,ou=people,dc=example,dc=com",
                        "uid=lisi,ou=people,dc=example,dc=com", "uid=wangwu,ou=people,dc=example,dc=com",
                        "uid=maliu,ou=people,dc=example,dc=com", "uid=xiaoming,ou=people,dc=example,dc=com",
                        "cn=sub_group_rd1,ou=Group,dc=example,dc=com"));
        mockGroupAttribute(initialDirContext, "cn=sub_group_rd1,ou=Group,dc=example,dc=com",
                "member;range=0-1499", "group",
                Lists.newArrayList("uid=xiaohong,ou=people,dc=example,dc=com"));

        Set<String> names = LDAPGroupCacheMgr.getMemberNamesFromADGroup(initialDirContext, groupName,
                "regex:member;range=(\\d+)-(\\d+)", "uid", false);
        Assert.assertEquals(6, names.size());
        Assert.assertTrue(names.contains("zhangsan"));
        Assert.assertTrue(names.contains("lisi"));
        Assert.assertTrue(names.contains("wangwu"));
        Assert.assertTrue(names.contains("maliu"));
        Assert.assertTrue(names.contains("xiaoming"));
        Assert.assertTrue(names.contains("xiaohong"));

        // 2. test getNames by member
        String groupName2 = "cn=group_rd2,ou=Group,dc=example,dc=com";
        mockGroupAttribute(initialDirContext, groupName2, "member", "group",
                Lists.newArrayList("uid=zhangsan,ou=people,dc=example,dc=com",
                        "uid=lisi,ou=people,dc=example,dc=com", "uid=wangwu,ou=people,dc=example,dc=com",
                        "uid=maliu,ou=people,dc=example,dc=com", "uid=xiaoming,ou=people,dc=example,dc=com",
                        "cn=sub_group_rd2,ou=Group,dc=example,dc=com"));
        mockGroupAttribute(initialDirContext, "cn=sub_group_rd2,ou=Group,dc=example,dc=com",
                "member", "group",
                Lists.newArrayList("uid=xiaohong,ou=people,dc=example,dc=com"));

        names = LDAPGroupCacheMgr.getMemberNamesFromADGroup(initialDirContext, groupName2, "member", "uid", false);
        Assert.assertEquals(6, names.size());
        Assert.assertTrue(names.contains("zhangsan"));
        Assert.assertTrue(names.contains("lisi"));
        Assert.assertTrue(names.contains("wangwu"));
        Assert.assertTrue(names.contains("maliu"));
        Assert.assertTrue(names.contains("xiaoming"));
        Assert.assertTrue(names.contains("xiaohong"));

        // 3. test getNames by memberUid
        String groupName3 = "cn=group_rd3,ou=Group,dc=example,dc=com";
        mockGroupAttribute(initialDirContext, groupName3, "member", "group",
                Lists.newArrayList("uid=zhangsan,ou=people,dc=example,dc=com",
                        "uid=lisi,ou=people,dc=example,dc=com", "uid=wangwu,ou=people,dc=example,dc=com",
                        "uid=maliu,ou=people,dc=example,dc=com", "uid=xiaoming,ou=people,dc=example,dc=com",
                        "cn=sub_group_rd2,ou=Group,dc=example,dc=com"));
        mockGroupAttribute(initialDirContext, "cn=sub_group_rd3,ou=Group,dc=example,dc=com",
                "memberUid", "group",
                Lists.newArrayList("xiaohong"));

        names = LDAPGroupCacheMgr.getMemberNamesFromADGroup(initialDirContext, groupName3, "member", "uid", true);
        Assert.assertEquals(6, names.size());
        Assert.assertTrue(names.contains("zhangsan"));
        Assert.assertTrue(names.contains("lisi"));
        Assert.assertTrue(names.contains("wangwu"));
        Assert.assertTrue(names.contains("maliu"));
        Assert.assertTrue(names.contains("xiaoming"));
        Assert.assertTrue(names.contains("xiaohong"));
    }

    private void mockGroupAttribute(DirContext context, String groupDN,
                                    String memberId, String groupType, List<String> members) throws Exception {
        BasicAttribute attribute = new BasicAttribute(memberId);
        for (String member : members) {
            attribute.add(member);
        }
        BasicAttributes basicAttributes = new BasicAttributes();
        basicAttributes.put(attribute);
        basicAttributes.put("objectClass", groupType);

        when(context.getAttributes(groupDN)).thenReturn(basicAttributes);
    }
}
