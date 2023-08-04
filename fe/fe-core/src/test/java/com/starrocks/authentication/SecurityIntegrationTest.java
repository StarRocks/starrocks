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


package com.starrocks.authentication;

import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.epack.persist.OperationTypeEPack;
import com.starrocks.epack.persist.SecurityIntegrationPersistInfo;
import com.starrocks.epack.privilege.AuthenticationMgrEPack;
import com.starrocks.epack.privilege.RoleMappingMetaMgr;
import com.starrocks.epack.sql.analyzer.RoleMappingStatementAnalyzer;
import com.starrocks.epack.sql.analyzer.SecurityIntegrationStatementAnalyzer;
import com.starrocks.epack.sql.ast.AlterSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.CreateRoleMappingStatement;
import com.starrocks.epack.sql.ast.CreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.DropSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ShowCreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ShowSecurityIntegrationStatement;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
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
import java.util.Set;

public class SecurityIntegrationTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
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
                GlobalStateMgr.getCurrentState().getAuthenticationMgr().getSecurityIntegration("ldap1");
        Assert.assertEquals("ldap", ldap1.getType());
        Assert.assertEquals("memberUid", ldap1.getLdapUserGroupMatchAttr());
        Assert.assertEquals("uid", ldap1.getLdapUserSearchAttr());
        Assert.assertEquals("uid=admin", ldap1.getLdapBindRootDn());
        Assert.assertEquals("aaa", ldap1.getLdapBindRootPwd());
        Assert.assertEquals("dc=apple, dc=com", ldap1.getLdapBindBaseDn());
        Assert.assertEquals(1500, ldap1.getLdapCacheRefreshInterval());
        GlobalStateMgr.getCurrentState().getAuthenticationMgr().dropSecurityIntegration("ldap1", false);
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
    }

    @Test
    public void testSecurityIntegrationDdlPersist() throws Exception {
        AuthenticationMgr masterManager = new AuthenticationMgrEPack();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.saveV2(emptyImage.getDataOutputStream());

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
        masterManager.saveV2(finalImage.getDataOutputStream());

        // test replay OP_CREATE_SECURITY_INTEGRATION edit log
        AuthenticationMgr followerManager = new AuthenticationMgrEPack();
        followerManager.loadV2(new SRMetaBlockReader(emptyImage.getDataInputStream()));
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
        AuthenticationMgr imageManager = new AuthenticationMgrEPack();
        imageManager.loadV2(new SRMetaBlockReader(finalImage.getDataInputStream()));
        Assert.assertNotNull(imageManager.getSecurityIntegration("ldap3"));
        Assert.assertNull(imageManager.getSecurityIntegration("ldap4"));
        Assert.assertEquals("bbb",
                ((LDAPSecurityIntegration) imageManager.getSecurityIntegration("ldap3"))
                        .getLdapBindRootPwd());

        new MockUp<LDAPAuthProviderForExternal>() {
            @Mock
            public void authenticate(String user, String host, byte[] password, byte[] randomString,
                                     UserAuthenticationInfo authenticationInfo) throws AuthenticationException {
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
        UserIdentity userIdentity =
                imageManager.checkPassword("ldap_external_user", "192.168.0.1", scramble, seed);
        System.out.println(userIdentity);
        Assert.assertEquals("'ldap_external_user'@'ldap3'", userIdentity.toString());
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
                GlobalStateMgr.getCurrentState().getAuthenticationMgr()
                        .getSecurityIntegration("ldap1foralter");
        Assert.assertEquals("memberUid", integrationForAlterOld.getLdapUserGroupMatchAttr());
        sql = "alter security integration ldap1foralter set (" +
                "\"ldap_user_group_match_attr\" = \"uidOfNames\"," +
                "\"ldap_bind_root_pwd\" = \"bbb\"" +
                ")";
        alterSecurityIntegration(sql);
        LDAPSecurityIntegration integrationForAlterNew = (LDAPSecurityIntegration)
                GlobalStateMgr.getCurrentState().getAuthenticationMgr()
                        .getSecurityIntegration("ldap1foralter");
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
        ShowResultSet res = new ShowExecutor(connectContext, showStmt).execute();
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
        ShowResultSet res = new ShowExecutor(connectContext, showStmt).execute();
        System.out.println(res.getResultRows());
        Assert.assertTrue(res.getResultRows().get(0).get(1).contains("\"ldap_user_group_match_attr\" = \"memberUid\""));
        // test the show create result can actually work
        dropSecurityIntegration("ldap1forshowcreate");
        System.out.println(res.getResultRows().get(0).get(1));
        createSecurityIntegration(res.getResultRows().get(0).get(1));
        // clean
        dropSecurityIntegration("ldap1forshowcreate");
    }
}
