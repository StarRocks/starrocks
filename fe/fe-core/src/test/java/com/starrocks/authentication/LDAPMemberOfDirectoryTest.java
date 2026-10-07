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
import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import com.unboundid.ldap.listener.interceptor.InMemoryInterceptedSearchEntry;
import com.unboundid.ldap.listener.interceptor.InMemoryInterceptedSearchRequest;
import com.unboundid.ldap.listener.interceptor.InMemoryInterceptedSimpleBindRequest;
import com.unboundid.ldap.listener.interceptor.InMemoryOperationInterceptor;
import com.unboundid.ldap.sdk.Entry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

/**
 * What our code actually puts on the wire, and what a real directory hands back.
 * <p>
 * These are the assertions that cannot be made against a mock: a mock returns the attributes we
 * invented ourselves, and it cannot show which attributes the search asked the server for. Two of
 * the acceptance criteria are exactly about that:
 * <ul>
 *     <li>with the feature off, the user search must ask the directory for <b>no attribute at all</b>
 *     - `SearchControls.setReturningAttributes(new String[0])` only means that if the JNDI provider
 *     translates the empty array into the "no attributes" OID `1.1`; an empty attribute list on the
 *     wire means "all attributes" per RFC 4511, which would be the opposite of what we want;</li>
 *     <li>the service account fallback must not fire when the user could read its own attribute -
 *     the whole cost argument for that fallback rests on it being rare, and a mock cannot tell us
 *     whether a second connection was opened.</li>
 * </ul>
 */
class LDAPMemberOfDirectoryTest {
    private static final String BASE_DN = "dc=starrocks,dc=com";
    private static final String PEOPLE_DN = "ou=People," + BASE_DN;
    private static final String SERVICE_DN = "cn=svc," + BASE_DN;
    private static final String SERVICE_PWD = "svc-secret";
    private static final String USER_PWD = "user-secret";
    private static final String ALICE_DN = "uid=alice," + PEOPLE_DN;

    /**
     * The attribute list the Sun JNDI provider sends when asked for no attribute at all. RFC 4511
     * calls it the "no attributes" OID; an empty list on the wire would mean "all attributes".
     */
    private static final String NO_ATTRIBUTES_OID = "1.1";

    private InMemoryDirectoryServer directory;
    private Recorder recorder;
    private ConnectionLog connectionLog;

    /**
     * Counts the CONNECT records of the server's access log, which is an independent, server-side
     * count of how many connections the code under test opened.
     * <p>
     * It cannot tell whether they were closed again: this version of the in-memory server logs
     * CONNECT but never DISCONNECT or UNBIND (verified while writing this test). Whether connections
     * are actually released is asserted at system level instead - see the S-4 case of
     * `test_sr/auth/ldap_group_memberof`, which counts them on a real slapd.
     */
    private static class ConnectionLog extends Handler {
        private final AtomicInteger connects = new AtomicInteger();

        @Override
        public void publish(LogRecord record) {
            String message = record.getMessage();
            if (message != null && message.contains("CONNECT ")) {
                connects.incrementAndGet();
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }

    /**
     * Records what the directory was asked for, and can hide an attribute from everyone except the
     * service account, which is how a directory that forbids users from reading their own group
     * membership behaves. The in-memory server does not evaluate ACIs, so this has to be simulated.
     */
    private static class Recorder extends InMemoryOperationInterceptor {
        private final List<List<String>> searchRequestedAttributes =
                Collections.synchronizedList(new ArrayList<>());
        private final List<String> bindDNs = Collections.synchronizedList(new ArrayList<>());
        private final Set<Long> connectionIds = Collections.synchronizedSet(new HashSet<>());
        private final Map<Long, String> boundDnByConnection = new ConcurrentHashMap<>();

        private volatile String hiddenAttribute = null;

        @Override
        public void processSimpleBindRequest(InMemoryInterceptedSimpleBindRequest request) {
            String dn = request.getRequest().getBindDN();
            bindDNs.add(dn);
            connectionIds.add(request.getConnectionID());
            boundDnByConnection.put(request.getConnectionID(), dn == null ? "" : dn);
        }

        @Override
        public void processSearchRequest(InMemoryInterceptedSearchRequest request) {
            searchRequestedAttributes.add(new ArrayList<>(request.getRequest().getAttributeList()));
            connectionIds.add(request.getConnectionID());
        }

        @Override
        public void processSearchEntry(InMemoryInterceptedSearchEntry entry) {
            String hidden = hiddenAttribute;
            if (hidden == null) {
                return;
            }
            String boundDn = boundDnByConnection.get(entry.getConnectionID());
            if (SERVICE_DN.equalsIgnoreCase(boundDn)) {
                return;
            }
            Entry stripped = entry.getSearchEntry().duplicate();
            stripped.removeAttribute(hidden);
            entry.setSearchEntry(stripped);
        }

        private int bindsAs(String dn) {
            int count = 0;
            synchronized (bindDNs) {
                for (String bound : bindDNs) {
                    if (dn.equalsIgnoreCase(bound)) {
                        count++;
                    }
                }
            }
            return count;
        }

        private void reset() {
            searchRequestedAttributes.clear();
            bindDNs.clear();
            connectionIds.clear();
            boundDnByConnection.clear();
        }
    }

    @BeforeEach
    public void startDirectory() throws Exception {
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig(BASE_DN);
        // `memberOf` is an operational, NO-USER-MODIFICATION attribute in the standard schema, so it
        // cannot be written into an entry while the schema is enforced. Turning the schema off is
        // what lets the fixture look like a directory that maintains the reverse link itself.
        config.setSchema(null);
        config.addAdditionalBindCredentials(SERVICE_DN, SERVICE_PWD);
        config.setListenerConfigs(InMemoryListenerConfig.createLDAPConfig("test", 0));
        recorder = new Recorder();
        config.addInMemoryOperationInterceptor(recorder);
        connectionLog = new ConnectionLog();
        config.setAccessLogHandler(connectionLog);

        directory = new InMemoryDirectoryServer(config);
        directory.startListening();

        directory.add("dn: " + BASE_DN, "objectClass: top", "objectClass: domain", "dc: starrocks");
        directory.add("dn: " + PEOPLE_DN, "objectClass: top", "objectClass: organizationalUnit", "ou: People");
        directory.add("dn: ou=Groups," + BASE_DN, "objectClass: top", "objectClass: organizationalUnit",
                "ou: Groups");
        directory.add("dn: cn=SR Analysts,ou=Groups," + BASE_DN, "objectClass: top",
                "objectClass: groupOfNames", "cn: SR Analysts", "member: " + ALICE_DN);
        directory.add("dn: cn=Data Platform,ou=Groups," + BASE_DN, "objectClass: top",
                "objectClass: groupOfNames", "cn: Data Platform", "member: " + ALICE_DN);
        directory.add("dn: " + ALICE_DN, "objectClass: top", "objectClass: person",
                "objectClass: organizationalPerson", "objectClass: inetOrgPerson",
                "uid: alice", "cn: alice", "sn: Anderson", "userPassword: " + USER_PWD,
                "memberOf: CN=SR Analysts,OU=Groups," + BASE_DN.toUpperCase(),
                "memberOf: CN=Data Platform,OU=Groups," + BASE_DN.toUpperCase());
        // bob belongs to no group at all, so the attribute simply does not exist on his entry.
        directory.add("dn: uid=bob," + PEOPLE_DN, "objectClass: top", "objectClass: person",
                "objectClass: organizationalPerson", "objectClass: inetOrgPerson",
                "uid: bob", "cn: bob", "sn: Brown", "userPassword: " + USER_PWD);
        directory.add("dn: uid=carol," + PEOPLE_DN, "objectClass: top", "objectClass: person",
                "objectClass: organizationalPerson", "objectClass: inetOrgPerson",
                "uid: carol", "cn: carol", "sn: Clark", "userPassword: " + USER_PWD,
                "isMemberOf: CN=SR Analysts,OU=Groups," + BASE_DN.toUpperCase());

        // Only operations issued by the code under test should be counted.
        recorder.reset();
    }

    @AfterEach
    public void stopDirectory() {
        if (directory != null) {
            directory.shutDown(true);
        }
    }

    /**
     * @param bindDnPattern non-null to use direct bind, null for search-and-bind
     * @param perUserDN     non-null for the legacy `CREATE USER ... AS '<dn>'` form
     */
    private LDAPAuthProvider provider(String bindDnPattern, String perUserDN, LdapGroupSource groupSource,
                                      String memberOfAttr, boolean withServiceAccount) {
        return new LDAPAuthProvider("localhost", directory.getListenPort(), false, null, null,
                withServiceAccount ? SERVICE_DN : "", withServiceAccount ? SERVICE_PWD : "",
                BASE_DN, "uid", perUserDN, bindDnPattern, groupSource, memberOfAttr);
    }

    private AccessControlContext authenticate(LDAPAuthProvider provider, String user) throws Exception {
        AccessControlContext context = new AccessControlContext();
        provider.authenticate(context, UserIdentity.createEphemeralUserIdent(user, "%"),
                (USER_PWD + "\0").getBytes(StandardCharsets.UTF_8));
        return context;
    }

    private List<String> onlySearchAttributes() {
        Assertions.assertEquals(1, recorder.searchRequestedAttributes.size(),
                "expected exactly one search, got " + recorder.searchRequestedAttributes);
        return recorder.searchRequestedAttributes.get(0);
    }

    @Test
    void testSearchAndBindCarriesMemberOfInTheSearchItAlreadyMakes() throws Exception {
        AccessControlContext context =
                authenticate(provider(null, null, LdapGroupSource.MEMBEROF, "memberOf", true), "alice");

        Assertions.assertEquals(Set.of("SR Analysts", "Data Platform"), context.getMemberOfGroups());
        Assertions.assertEquals(ALICE_DN, context.getDistinguishedName());
        // The directory was asked for exactly the one attribute we need, on the search that resolves
        // the DN anyway.
        Assertions.assertEquals(List.of("memberOf"), onlySearchAttributes());
        // Service account bind + user bind, exactly as without the feature.
        Assertions.assertEquals(1, recorder.bindsAs(SERVICE_DN));
        Assertions.assertEquals(1, recorder.bindsAs(ALICE_DN));
    }

    /**
     * AC-6b, asserted where it actually matters: on the request the server received.
     */
    @Test
    void testDisabledGroupSourceAsksTheDirectoryForNoAttribute() throws Exception {
        AccessControlContext context =
                authenticate(provider(null, null, LdapGroupSource.GROUP_PROVIDER, "memberOf", true), "alice");

        Assertions.assertTrue(context.getMemberOfGroups().isEmpty());
        Assertions.assertEquals(ALICE_DN, context.getDistinguishedName());
        // `1.1` is the "no attributes" OID. An empty list here would mean "return everything", which
        // is what the code did before and what this assertion exists to prevent from coming back.
        Assertions.assertEquals(List.of(NO_ATTRIBUTES_OID), onlySearchAttributes());
    }

    /**
     * AC-6 for the direct-bind path: a bind response cannot carry attributes, so one read request is
     * added - but no extra connection and no extra bind.
     */
    @Test
    void testDirectBindAddsOneReadRequestAndNothingElse() throws Exception {
        String pattern = "uid=${USER}," + PEOPLE_DN;

        AccessControlContext context =
                authenticate(provider(pattern, null, LdapGroupSource.MEMBEROF, "memberOf", true), "alice");

        Assertions.assertEquals(Set.of("SR Analysts", "Data Platform"), context.getMemberOfGroups());
        Assertions.assertEquals(List.of("memberOf"), onlySearchAttributes());
        // One bind, as the user itself, and only one connection: the attribute was read on the
        // connection the bind had already opened.
        Assertions.assertEquals(List.of(ALICE_DN), recorder.bindDNs);
        Assertions.assertEquals(1, recorder.connectionIds.size());

        // Same login with the feature off: the one bind stays, the read request disappears.
        recorder.reset();
        AccessControlContext off =
                authenticate(provider(pattern, null, LdapGroupSource.GROUP_PROVIDER, "memberOf", true), "alice");
        Assertions.assertTrue(off.getMemberOfGroups().isEmpty());
        Assertions.assertEquals(List.of(ALICE_DN), recorder.bindDNs);
        Assertions.assertTrue(recorder.searchRequestedAttributes.isEmpty(),
                "the disabled feature must not read anything, got " + recorder.searchRequestedAttributes);
    }

    /**
     * AC-12, forward: the directory forbids the user from reading its own group membership, and the
     * service account fallback recovers the groups.
     */
    @Test
    void testDirectBindFallsBackToTheServiceAccount() throws Exception {
        recorder.hiddenAttribute = "memberOf";

        AccessControlContext context = authenticate(
                provider("uid=${USER}," + PEOPLE_DN, null, LdapGroupSource.MEMBEROF, "memberOf", true), "alice");

        Assertions.assertEquals(Set.of("SR Analysts", "Data Platform"), context.getMemberOfGroups());
        Assertions.assertEquals(1, recorder.bindsAs(SERVICE_DN), "the fallback probe must have bound as the service account");
        Assertions.assertEquals(1, recorder.bindsAs(ALICE_DN));
    }

    /**
     * AC-12, reverse - the half that a functional test would still pass without: when the user can
     * read its own attribute, the fallback must not fire at all.
     */
    @Test
    void testNoProbeWhenTheUserCouldReadItsOwnGroups() throws Exception {
        AccessControlContext context = authenticate(
                provider("uid=${USER}," + PEOPLE_DN, null, LdapGroupSource.MEMBEROF, "memberOf", true), "alice");

        Assertions.assertEquals(Set.of("SR Analysts", "Data Platform"), context.getMemberOfGroups());
        Assertions.assertEquals(0, recorder.bindsAs(SERVICE_DN),
                "a login that already resolved its groups must not open a service account connection");
        Assertions.assertEquals(1, recorder.connectionIds.size());
    }

    @Test
    void testDirectBindWithoutServiceAccountSkipsTheProbeAndStillLogsIn() throws Exception {
        recorder.hiddenAttribute = "memberOf";

        AccessControlContext context = authenticate(
                provider("uid=${USER}," + PEOPLE_DN, null, LdapGroupSource.MEMBEROF, "memberOf", false), "alice");

        Assertions.assertTrue(context.getMemberOfGroups().isEmpty());
        Assertions.assertEquals(List.of(ALICE_DN), recorder.bindDNs);
        Assertions.assertEquals("uid=alice," + PEOPLE_DN, context.getDistinguishedName());
    }

    @Test
    void testUserWithoutAnyGroupLogsInWithAnEmptyGroupSet() throws Exception {
        AccessControlContext context =
                authenticate(provider(null, null, LdapGroupSource.MEMBEROF, "memberOf", true), "bob");

        Assertions.assertTrue(context.getMemberOfGroups().isEmpty());
        Assertions.assertEquals("uid=bob," + PEOPLE_DN, context.getDistinguishedName());
        // Nothing to fall back to on this path: the reader already was the service account.
        Assertions.assertEquals(1, recorder.bindsAs(SERVICE_DN));
    }

    @Test
    void testCustomAttributeName() throws Exception {
        AccessControlContext context =
                authenticate(provider(null, null, LdapGroupSource.MEMBEROF, "isMemberOf", true), "carol");

        Assertions.assertEquals(Set.of("SR Analysts"), context.getMemberOfGroups());
        Assertions.assertEquals(List.of("isMemberOf"), onlySearchAttributes());
    }

    /**
     * The attribute name is matched case-insensitively by JNDI, so a user who configured `memberof`
     * still gets the values of the `memberOf` the directory returns. This confirms the assumption
     * rather than relying on it.
     */
    @Test
    void testConfiguredAttributeNameCaseDoesNotMatter() throws Exception {
        AccessControlContext context =
                authenticate(provider(null, null, LdapGroupSource.MEMBEROF, "memberof", true), "alice");

        Assertions.assertEquals(Set.of("SR Analysts", "Data Platform"), context.getMemberOfGroups());
        Assertions.assertEquals(List.of("memberof"), onlySearchAttributes());
    }

    /**
     * The legacy `AS '<dn>'` form is excluded on purpose: it binds and nothing else, even with
     * group_source = memberof.
     */
    @Test
    void testLegacyPerUserDnFormTouchesNoAttribute() throws Exception {
        AccessControlContext context =
                authenticate(provider(null, ALICE_DN, LdapGroupSource.MEMBEROF, "memberOf", true), "alice");

        Assertions.assertTrue(context.getMemberOfGroups().isEmpty());
        Assertions.assertTrue(recorder.searchRequestedAttributes.isEmpty());
        Assertions.assertEquals(List.of(ALICE_DN), recorder.bindDNs);
        Assertions.assertEquals(ALICE_DN, context.getDistinguishedName());
    }

    /**
     * Group names keep the case the directory returned them in, because they are handed to Ranger,
     * which matches case-sensitively.
     */
    @Test
    void testGroupNamesKeepTheDirectoryCase() throws Exception {
        AccessControlContext context =
                authenticate(provider(null, null, LdapGroupSource.BOTH, "memberOf", true), "alice");

        Assertions.assertTrue(context.getMemberOfGroups().contains("SR Analysts"));
        Assertions.assertFalse(context.getMemberOfGroups().contains("sr analysts"));
    }

    /**
     * Repeated logins must not drift upwards in connection count: reading `memberOf` rides on the
     * connections authentication opens anyway, so the per-login cost has to stay flat.
     */
    @Test
    void testRepeatedLoginsOpenExactlyTwoConnectionsEach() throws Exception {
        LDAPAuthProvider provider = provider(null, null, LdapGroupSource.MEMBEROF, "memberOf", true);
        for (int i = 0; i < 10; i++) {
            Assertions.assertEquals(Set.of("SR Analysts", "Data Platform"),
                    authenticate(provider, "alice").getMemberOfGroups());
        }
        // Two per login on this path: the service account search and the user bind. Counted twice
        // over, once through the operation interceptor and once through the server's access log.
        Assertions.assertEquals(20, recorder.connectionIds.size());
        Assertions.assertEquals(20, connectionLog.connects.get());
        // ... and exactly one search per login, i.e. no repeated reads and no fallback probe.
        Assertions.assertEquals(10, recorder.searchRequestedAttributes.size());
    }
}
