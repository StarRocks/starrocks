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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;

/**
 * How the two supported authentication paths read `memberOf`, and what they request from the
 * directory while doing so.
 * <p>
 * The directory itself is replaced by overriding the three methods that talk to it, which lets the
 * test observe exactly which attributes were asked for and whether the service account fallback was
 * used. What it cannot show is what the directory receives on the wire - in particular that
 * `setReturningAttributes(new String[0])` really is translated into "no attribute" rather than into
 * an empty list, which RFC 4511 reads as "all attributes". That assertion needs an in-process
 * directory server and is tracked separately.
 */
class LDAPMemberOfAuthProviderTest {
    private static final byte[] PASSWORD = "password\0".getBytes(StandardCharsets.UTF_8);
    private static final String USER_DN = "uid=alice,ou=People,dc=starrocks,dc=com";

    private static Attributes memberOf(String attrId, String... dns) {
        BasicAttributes attributes = new BasicAttributes(true);
        BasicAttribute attribute = new BasicAttribute(attrId);
        for (String dn : dns) {
            attribute.add(dn);
        }
        attributes.put(attribute);
        return attributes;
    }

    /**
     * Records what was asked of the directory and answers with canned attributes.
     */
    private static class RecordingProvider extends LDAPAuthProvider {
        private final Attributes userReadableAttributes;
        private final Attributes serviceAccountAttributes;
        private final boolean failUserAttributeRead;

        // What the directory was asked for, one entry per request.
        private final List<String[]> searchRequestedAttributes = new ArrayList<>();
        private final List<String[]> bindRequestedAttributes = new ArrayList<>();
        private int serviceAccountProbes = 0;
        private int binds = 0;

        RecordingProvider(String bindDnPattern, String userDn, LdapGroupSource groupSource, String memberOfAttr,
                          String bindRootDn, String bindRootPwd,
                          Attributes userReadableAttributes, Attributes serviceAccountAttributes,
                          boolean failUserAttributeRead) {
            super("localhost", 389, false, null, null,
                    bindRootDn, bindRootPwd, "ou=People,dc=starrocks,dc=com", "uid",
                    userDn, bindDnPattern, groupSource, memberOfAttr);
            this.userReadableAttributes = userReadableAttributes;
            this.serviceAccountAttributes = serviceAccountAttributes;
            this.failUserAttributeRead = failUserAttributeRead;
        }

        @Override
        protected void checkPassword(String dn, String password) {
            binds++;
        }

        @Override
        protected LdapUserEntry checkPasswordAndReadAttributes(String dn, String password, String[] requestedAttributes) {
            binds++;
            bindRequestedAttributes.add(requestedAttributes);
            if (failUserAttributeRead) {
                // Mirrors the real method: the bind succeeded, only the attribute read did not.
                return new LdapUserEntry(dn, null);
            }
            return new LdapUserEntry(dn, userReadableAttributes);
        }

        @Override
        protected String findUserDNByRoot(String user) {
            searchRequestedAttributes.add(new String[0]);
            return USER_DN;
        }

        @Override
        protected LdapUserEntry findUserEntryByRoot(String user, String[] requestedAttributes) {
            searchRequestedAttributes.add(requestedAttributes);
            return new LdapUserEntry(USER_DN, failUserAttributeRead ? null : userReadableAttributes);
        }

        @Override
        protected Attributes readAttributesAsServiceAccount(String dn, String[] requestedAttributes) throws NamingException {
            serviceAccountProbes++;
            if (serviceAccountAttributes == null) {
                throw new NamingException("insufficient access rights");
            }
            return serviceAccountAttributes;
        }
    }

    private static AccessControlContext authenticate(LDAPAuthProvider provider) throws AuthenticationException {
        AccessControlContext context = new AccessControlContext();
        provider.authenticate(context, UserIdentity.createEphemeralUserIdent("alice", "%"), PASSWORD);
        return context;
    }

    @Test
    void testSearchAndBindReadsMemberOfWithinTheExistingSearch() throws Exception {
        RecordingProvider provider = new RecordingProvider(null, null, LdapGroupSource.MEMBEROF, "memberOf",
                "cn=svc,dc=starrocks,dc=com", "secret",
                memberOf("memberOf", "CN=SR Analysts,OU=Groups,DC=starrocks,DC=com",
                        "CN=Data Platform,OU=Groups,DC=starrocks,DC=com"), null, false);

        AccessControlContext context = authenticate(provider);

        Assertions.assertEquals(Set.of("SR Analysts", "Data Platform"), context.getMemberOfGroups());
        Assertions.assertEquals(USER_DN, context.getDistinguishedName());
        // One search, and it is the search that resolves the DN anyway - the attribute rides along.
        Assertions.assertEquals(1, provider.searchRequestedAttributes.size());
        Assertions.assertArrayEquals(new String[] {"memberOf"}, provider.searchRequestedAttributes.get(0));
        // Same number of binds as without the feature, and no service account probe.
        Assertions.assertEquals(1, provider.binds);
        Assertions.assertEquals(0, provider.serviceAccountProbes);
    }

    @Test
    void testDefaultGroupSourceRequestsNoAttributeAtAll() throws Exception {
        RecordingProvider provider = new RecordingProvider(null, null, LdapGroupSource.GROUP_PROVIDER, "memberOf",
                "cn=svc,dc=starrocks,dc=com", "secret",
                memberOf("memberOf", "CN=SR Analysts,OU=Groups,DC=starrocks,DC=com"), null, false);

        AccessControlContext context = authenticate(provider);

        // Nothing is read and nothing is resolved: the disabled feature costs nothing.
        Assertions.assertTrue(context.getMemberOfGroups().isEmpty());
        Assertions.assertEquals(1, provider.searchRequestedAttributes.size());
        Assertions.assertEquals(0, provider.searchRequestedAttributes.get(0).length,
                "the default path must not ask the directory for any attribute");
        Assertions.assertEquals(0, provider.serviceAccountProbes);
        Assertions.assertTrue(provider.isGroupProviderUsed());
    }

    @Test
    void testDirectBindReadsMemberOfOnTheUserConnection() throws Exception {
        RecordingProvider provider = new RecordingProvider(
                "uid=${USER},ou=People,dc=starrocks,dc=com", null, LdapGroupSource.BOTH, "memberOf",
                "cn=svc,dc=starrocks,dc=com", "secret",
                memberOf("memberOf", "CN=SR Analysts,OU=Groups,DC=starrocks,DC=com"), null, false);

        AccessControlContext context = authenticate(provider);

        Assertions.assertEquals(Set.of("SR Analysts"), context.getMemberOfGroups());
        Assertions.assertEquals("uid=alice,ou=People,dc=starrocks,dc=com", context.getDistinguishedName());
        // Exactly one bind, on which the attribute was read - no extra connection, no extra bind.
        Assertions.assertEquals(1, provider.binds);
        Assertions.assertEquals(1, provider.bindRequestedAttributes.size());
        Assertions.assertArrayEquals(new String[] {"memberOf"}, provider.bindRequestedAttributes.get(0));
        // The user read its own groups, so the fallback probe must not fire: the whole cost argument
        // for the probe rests on it being rare.
        Assertions.assertEquals(0, provider.serviceAccountProbes);
        // `both` still consults the group providers.
        Assertions.assertTrue(provider.isGroupProviderUsed());
    }

    @Test
    void testDirectBindProbesWithServiceAccountWhenTheUserReadsNothing() throws Exception {
        RecordingProvider provider = new RecordingProvider(
                "uid=${USER},ou=People,dc=starrocks,dc=com", null, LdapGroupSource.MEMBEROF, "memberOf",
                "cn=svc,dc=starrocks,dc=com", "secret",
                new BasicAttributes(true),
                memberOf("memberOf", "CN=SR Analysts,OU=Groups,DC=starrocks,DC=com"), false);

        AccessControlContext context = authenticate(provider);

        Assertions.assertEquals(Set.of("SR Analysts"), context.getMemberOfGroups());
        Assertions.assertEquals(1, provider.serviceAccountProbes);
        Assertions.assertFalse(provider.isGroupProviderUsed());
    }

    @Test
    void testDirectBindSkipsTheProbeWithoutAServiceAccount() throws Exception {
        RecordingProvider provider = new RecordingProvider(
                "uid=${USER},ou=People,dc=starrocks,dc=com", null, LdapGroupSource.MEMBEROF, "memberOf",
                /* bindRootDn */ "", /* bindRootPwd */ "",
                new BasicAttributes(true), null, false);

        AccessControlContext context = authenticate(provider);

        // No service account to fall back to: empty group set, and the login still succeeds.
        Assertions.assertTrue(context.getMemberOfGroups().isEmpty());
        Assertions.assertEquals(0, provider.serviceAccountProbes);
        Assertions.assertEquals("uid=alice,ou=People,dc=starrocks,dc=com", context.getDistinguishedName());
    }

    @Test
    void testLoginSucceedsWhenNeitherReaderCanSeeTheAttribute() throws Exception {
        RecordingProvider provider = new RecordingProvider(
                "uid=${USER},ou=People,dc=starrocks,dc=com", null, LdapGroupSource.MEMBEROF, "memberOf",
                "cn=svc,dc=starrocks,dc=com", "secret",
                null, /* service account also fails */ null, /* failUserAttributeRead */ true);

        AccessControlContext context = authenticate(provider);

        Assertions.assertTrue(context.getMemberOfGroups().isEmpty());
        Assertions.assertEquals(1, provider.serviceAccountProbes);
        Assertions.assertEquals("uid=alice,ou=People,dc=starrocks,dc=com", context.getDistinguishedName());
    }

    @Test
    void testUserWithoutAnyGroupIsNotAnError() throws Exception {
        RecordingProvider provider = new RecordingProvider(null, null, LdapGroupSource.MEMBEROF, "memberOf",
                /* no service account, so no probe on the search path anyway */ "", "",
                new BasicAttributes(true), null, false);

        AccessControlContext context = authenticate(provider);

        Assertions.assertTrue(context.getMemberOfGroups().isEmpty());
        Assertions.assertEquals(USER_DN, context.getDistinguishedName());
    }

    @Test
    void testSearchPathNeverProbes() throws Exception {
        // The reader on the search-and-bind path already is the service account, so there is nothing
        // to fall back to and no probe must be attempted even when nothing came back.
        RecordingProvider provider = new RecordingProvider(null, null, LdapGroupSource.MEMBEROF, "memberOf",
                "cn=svc,dc=starrocks,dc=com", "secret",
                new BasicAttributes(true), memberOf("memberOf", "CN=Never,DC=x"), false);

        AccessControlContext context = authenticate(provider);

        Assertions.assertEquals(0, provider.serviceAccountProbes);
        Assertions.assertTrue(context.getMemberOfGroups().isEmpty());
    }

    @Test
    void testLegacyPerUserDnFormDoesNotReadMemberOf() throws Exception {
        // `CREATE USER ... AS '<dn>'` is the legacy form and is excluded on purpose, even with
        // group_source = memberof.
        RecordingProvider provider = new RecordingProvider(null, USER_DN, LdapGroupSource.MEMBEROF, "memberOf",
                "cn=svc,dc=starrocks,dc=com", "secret",
                memberOf("memberOf", "CN=SR Analysts,OU=Groups,DC=starrocks,DC=com"), null, false);

        AccessControlContext context = authenticate(provider);

        Assertions.assertTrue(context.getMemberOfGroups().isEmpty());
        Assertions.assertTrue(provider.searchRequestedAttributes.isEmpty());
        Assertions.assertTrue(provider.bindRequestedAttributes.isEmpty());
        Assertions.assertEquals(0, provider.serviceAccountProbes);
        Assertions.assertEquals(USER_DN, context.getDistinguishedName());
    }

    @Test
    void testCustomMemberOfAttributeIsRequested() throws Exception {
        RecordingProvider provider = new RecordingProvider(null, null, LdapGroupSource.MEMBEROF, "isMemberOf",
                "cn=svc,dc=starrocks,dc=com", "secret",
                memberOf("isMemberOf", "CN=SR Analysts,OU=Groups,DC=starrocks,DC=com"), null, false);

        AccessControlContext context = authenticate(provider);

        Assertions.assertEquals(Set.of("SR Analysts"), context.getMemberOfGroups());
        Assertions.assertEquals(List.of("isMemberOf"), Arrays.asList(provider.searchRequestedAttributes.get(0)));
    }

    @Test
    void testLegacyConstructorKeepsTheOldBehaviour() {
        LDAPAuthProvider provider = new LDAPAuthProvider("localhost", 389, false, null, null,
                "cn=svc,dc=starrocks,dc=com", "secret", "ou=People,dc=starrocks,dc=com", "uid", null, null);
        Assertions.assertEquals(LdapGroupSource.GROUP_PROVIDER, provider.getGroupSource());
        Assertions.assertTrue(provider.isGroupProviderUsed());
        Assertions.assertEquals("memberOf", provider.getMemberOfAttr());
    }
}
