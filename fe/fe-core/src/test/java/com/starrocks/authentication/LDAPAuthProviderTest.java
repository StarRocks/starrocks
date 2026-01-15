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

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.UserAuthOption;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.parser.NodePosition;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

class LDAPAuthProviderTest {

    @BeforeAll
    public static void setUp() throws Exception {
        new MockUp<LDAPAuthProvider>() {
            @Mock
            private void checkPassword(String dn, String password) throws Exception {
                // mock: always success
            }

            @Mock
            private String findUserDNByRoot(String user) throws Exception {
                return "uid=test,ou=People,dc=starrocks,dc=com";
            }
        };

        // Mock EditLog
        EditLog editLog = spy(new EditLog(null));
        doNothing().when(editLog).logEdit(anyShort(), any());
        GlobalStateMgr.getCurrentState().setEditLog(editLog);

        AuthenticationMgr authenticationMgr = new AuthenticationMgr();
        GlobalStateMgr.getCurrentState().setAuthenticationMgr(authenticationMgr);

        authenticationMgr.createUser(new CreateUserStmt(
                new UserRef("ldap_user", "%", false, NodePosition.ZERO),
                true,
                new UserAuthOption("AUTHENTICATION_LDAP_SIMPLE",
                        "uid=ldap_user,ou=company,dc=example,dc=com",
                        false, NodePosition.ZERO),
                List.of(), Map.of(), NodePosition.ZERO));

        Map<String, String> properties = new HashMap<>();
        properties.put(GroupProvider.GROUP_PROVIDER_PROPERTY_TYPE_KEY, "file");
        properties.put(FileGroupProvider.GROUP_FILE_URL, "file_group");

        String groupName = "file_group_provider";
        authenticationMgr.replayCreateGroupProvider(groupName, properties);
        Config.group_provider = new String[] {groupName};
    }

    @AfterAll
    public static void teardown() throws Exception {
    }

    @Test
    void testAuthenticateSetsDNWhenLdapUserDNProvided() throws Exception {

        String providedDN = "cn=test,ou=People,dc=starrocks,dc=com";
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                "cn=admin,dc=starrocks,dc=com", "secret",
                "ou=People,dc=starrocks,dc=com", "uid",
                providedDN);

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("ldap_user", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(authCtx, user, authResponse);
        Assertions.assertEquals(providedDN, authCtx.getDistinguishedName());
    }

    @Test
    void testAuthenticateSetsDNWhenFindByRoot() throws Exception {
        String discoveredDN = "uid=test,ou=People,dc=starrocks,dc=com";
        LDAPAuthProvider provider = new LDAPAuthProvider(
                "localhost", 389, false,
                null, null,
                "cn=admin,dc=starrocks,dc=com", "secret",
                "ou=People,dc=starrocks,dc=com", "uid",
                /* ldapUserDN */ null
        );

        AccessControlContext authCtx = new AccessControlContext();
        UserIdentity user = UserIdentity.createEphemeralUserIdent("ldap_user", "%");
        byte[] authResponse = "password\0".getBytes(StandardCharsets.UTF_8);

        provider.authenticate(authCtx, user, authResponse);
        Assertions.assertEquals(discoveredDN, authCtx.getDistinguishedName());
    }
}
