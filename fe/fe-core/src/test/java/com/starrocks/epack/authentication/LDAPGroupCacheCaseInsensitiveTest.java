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

import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authorization.AuthorizationMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * The enterprise {@code type='ldap'} security integration keeps its own group cache, keyed by the
 * member name exactly as the directory returned it. A login that spells the name differently used to
 * miss the cache, which this path reports as "Cannot map any role ids" and turns into a refused
 * login.
 */
public class LDAPGroupCacheCaseInsensitiveTest {
    private static final String INTEGRATION = "ldap_ee";

    private AuthenticationMgrEPack authenticationMgr;
    private LDAPGroupCacheMgr groupCacheMgr;

    @BeforeEach
    public void setUp() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY,
                LDAPSecurityIntegration.SECURITY_INTEGRATION_TYPE_LDAP);
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_LDAP_SERVER_HOST, "localhost");
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_LDAP_SERVER_PORT, "389");
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_ROOT_DN_KEY, "cn=admin,dc=example,dc=com");
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_ROOT_PWD_KEY, "secret");
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_BASE_DN_KEY, "dc=example,dc=com");

        authenticationMgr = new AuthenticationMgrEPack();
        authenticationMgr.replayCreateSecurityIntegration(INTEGRATION, properties);

        groupCacheMgr = new LDAPGroupCacheMgr(authenticationMgr, mock(AuthorizationMgr.class));
        // The directory spells the member name 'Allen'; that is what the refresh puts in the cache.
        groupCacheMgr.setMemberToGroups(INTEGRATION,
                new HashMap<>(Map.of("Allen", List.of("cn=engineering,ou=groups,dc=example,dc=com"))));
    }

    /**
     * Test case: the client typed a different casing than the directory holds.
     * Test point: with the group switch on the member still resolves to its groups, so role mapping
     *             has something to work with.
     */
    @Test
    public void testLookupIgnoresCase() {
        for (String typed : new String[] {"Allen", "allen", "ALLEN", "aLLeN"}) {
            Assertions.assertEquals(List.of("cn=engineering,ou=groups,dc=example,dc=com"),
                    groupCacheMgr.getBelongedGroupsByUsername(INTEGRATION, typed),
                    "'" + typed + "' names the same directory account as 'Allen'");
        }
    }

    /**
     * Test case: the directory holds two members whose names differ only in case.
     * Test point: they are two people, and the map gives no stable way to say which was meant, so
     *             the lookup reports nothing rather than handing out whichever came first.
     */
    @Test
    public void testAmbiguousMemberReportsNoGroups() {
        groupCacheMgr.setMemberToGroups(INTEGRATION, new HashMap<>(Map.of(
                "Allen", List.of("cn=engineering,ou=groups,dc=example,dc=com"),
                "allen", List.of("cn=finance,ou=groups,dc=example,dc=com"))));

        Assertions.assertNull(groupCacheMgr.getBelongedGroupsByUsername(INTEGRATION, "ALLEN"));
        // An exact spelling is still unambiguous and keeps working.
        Assertions.assertEquals(List.of("cn=engineering,ou=groups,dc=example,dc=com"),
                groupCacheMgr.getBelongedGroupsByUsername(INTEGRATION, "Allen"));
    }

    /**
     * Test case: a member the cache has never heard of, and an unknown security integration.
     * Test point: both still report nothing rather than throwing, whatever the switch says.
     */
    @Test
    public void testUnknownMemberAndIntegration() {
        Assertions.assertNull(groupCacheMgr.getBelongedGroupsByUsername(INTEGRATION, "nobody"));
        Assertions.assertNull(groupCacheMgr.getBelongedGroupsByUsername("no_such_integration", "Allen"));
    }
}
