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
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.SecurityIntegrationInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class SecurityIntegrationTest {
    private static ConnectContext connectContext;
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.setUpForPersistTest();
    }

    private void createSecurityIntegration(String sql) throws Exception {
        CreateSecurityIntegrationStatement createSecurityIntegrationStatement =
                (CreateSecurityIntegrationStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        DDLStmtExecutor.execute(createSecurityIntegrationStatement, connectContext);
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
    public void testCreateSecurityIntegrationPersist() throws Exception {
        AuthenticationMgr masterManager = new AuthenticationMgr();
        UtFrameUtils.PseudoJournalReplayer.resetFollowerJournalQueue();
        UtFrameUtils.PseudoImage emptyImage = new UtFrameUtils.PseudoImage();
        masterManager.save(emptyImage.getDataOutputStream());

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
                createSecurityIntegrationStatement.getPropertyMap());

        // make final snapshot
        UtFrameUtils.PseudoImage finalImage = new UtFrameUtils.PseudoImage();
        masterManager.save(finalImage.getDataOutputStream());

        // test replay OP_CREATE_SECURITY_INTEGRATION edit log
        AuthenticationMgr followerManager = AuthenticationMgr.load(emptyImage.getDataInputStream());
        Assert.assertNull(followerManager.getSecurityIntegration("ldap3"));
        SecurityIntegrationInfo info = (SecurityIntegrationInfo)
                UtFrameUtils.PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_SECURITY_INTEGRATION);
        followerManager.replayCreateSecurityIntegration(info.name, info.propertyMap);
        Assert.assertNotNull(followerManager.getSecurityIntegration("ldap3"));

        // simulate restart (load from image)
        AuthenticationMgr imageManager = AuthenticationMgr.load(finalImage.getDataInputStream());
        Assert.assertNotNull(imageManager.getSecurityIntegration("ldap3"));

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


}
