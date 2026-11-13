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
import com.starrocks.common.ConfigBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.naming.CommunicationException;
import javax.naming.Context;
import javax.naming.InterruptedNamingException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;
import javax.net.ssl.SSLHandshakeException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LDAPAuthProviderRetryTest {
    private static final String USER_DN = "uid=alice,ou=People,dc=test,dc=com";
    private int connectTimeout;
    private int readTimeout;
    private int maxRetries;
    private int retryInterval;

    @BeforeEach
    void setUp() {
        connectTimeout = Config.authentication_ldap_simple_conn_timeout_ms;
        readTimeout = Config.authentication_ldap_simple_conn_read_timeout_ms;
        maxRetries = Config.authentication_ldap_simple_max_retries;
        retryInterval = Config.authentication_ldap_simple_retry_interval_ms;
        Config.authentication_ldap_simple_conn_timeout_ms = 1234;
        Config.authentication_ldap_simple_conn_read_timeout_ms = 2345;
        Config.authentication_ldap_simple_max_retries = 1;
        Config.authentication_ldap_simple_retry_interval_ms = 1;
    }

    @AfterEach
    void tearDown() {
        Config.authentication_ldap_simple_conn_timeout_ms = connectTimeout;
        Config.authentication_ldap_simple_conn_read_timeout_ms = readTimeout;
        Config.authentication_ldap_simple_max_retries = maxRetries;
        Config.authentication_ldap_simple_retry_interval_ms = retryInterval;
        Thread.interrupted();
    }

    @FunctionalInterface
    private interface ContextFactory {
        DirContext create(Hashtable<String, String> env) throws NamingException;
    }

    private LDAPAuthProvider provider(ContextFactory factory, String userDN, String patterns) {
        return new LDAPAuthProvider("localhost", 389, false, null, null, "cn=admin,dc=test,dc=com", "admin-password",
                "ou=People,dc=test,dc=com", "uid", userDN, patterns) {
            @Override
            DirContext createDirContext(Hashtable<String, String> env) throws NamingException {
                return factory.create(env);
            }
        };
    }

    @Test
    void testRejectedUserCredentialsAreBoundOnlyOnce() {
        AtomicInteger calls = new AtomicInteger();
        javax.naming.AuthenticationException rejected = new javax.naming.AuthenticationException("invalid credentials");
        LDAPAuthProvider provider = provider(env -> {
            calls.incrementAndGet();
            throw rejected;
        }, USER_DN, null);
        assertSame(rejected, assertThrows(javax.naming.AuthenticationException.class,
                () -> provider.checkPassword(USER_DN, "password")));
        assertEquals(1, calls.get());
    }

    @Test
    void testRejectedRootCredentialsAreBoundOnlyOnce() {
        AtomicInteger calls = new AtomicInteger();
        javax.naming.AuthenticationException rejected = new javax.naming.AuthenticationException("invalid credentials");
        LDAPAuthProvider provider = provider(env -> {
            calls.incrementAndGet();
            throw rejected;
        }, null, null);
        assertSame(rejected, assertThrows(javax.naming.AuthenticationException.class,
                () -> provider.findUserDNByRoot("alice")));
        assertEquals(1, calls.get());
    }

    @Test
    void testUserBindRecoversAndClosesContext() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        DirContext context = mock(DirContext.class);
        LDAPAuthProvider provider = provider(env -> {
            if (calls.incrementAndGet() == 1) {
                throw new CommunicationException();
            }
            return context;
        }, USER_DN, null);
        provider.checkPassword(USER_DN, "password");
        assertEquals(2, calls.get());
        verify(context).close();
    }

    @Test
    void testTimeoutChangesApplyToSubsequentOperation() throws Exception {
        List<Hashtable<String, String>> environments = new ArrayList<>();
        LDAPAuthProvider provider = provider(env -> {
            environments.add(new Hashtable<>(env));
            return mock(DirContext.class);
        }, USER_DN, null);
        provider.checkPassword(USER_DN, "password");
        ConfigBase.setConfigField(Config.class.getField("authentication_ldap_simple_conn_timeout_ms"), "3456");
        ConfigBase.setConfigField(Config.class.getField("authentication_ldap_simple_conn_read_timeout_ms"), "4567");
        provider.checkPassword(USER_DN, "password");
        assertEquals("1234", environments.get(0).get("com.sun.jndi.ldap.connect.timeout"));
        assertEquals("2345", environments.get(0).get("com.sun.jndi.ldap.read.timeout"));
        assertEquals("3456", environments.get(1).get("com.sun.jndi.ldap.connect.timeout"));
        assertEquals("4567", environments.get(1).get("com.sun.jndi.ldap.read.timeout"));
    }

    @Test
    void testSettingsAreStableWithinRetryLoop() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        LDAPAuthProvider provider = provider(env -> {
            assertEquals("1234", env.get("com.sun.jndi.ldap.connect.timeout"));
            if (calls.incrementAndGet() == 1) {
                Config.authentication_ldap_simple_conn_timeout_ms = 5678;
                Config.authentication_ldap_simple_max_retries = 0;
                throw new CommunicationException();
            }
            return mock(DirContext.class);
        }, USER_DN, null);
        provider.checkPassword(USER_DN, "password");
        assertEquals(2, calls.get());
    }

    @Test
    void testConfigurationRejectsInvalidValuesWithoutChangingCurrentValue() throws Exception {
        String[] names = {"authentication_ldap_simple_conn_timeout_ms", "authentication_ldap_simple_conn_read_timeout_ms",
                "authentication_ldap_simple_max_retries", "authentication_ldap_simple_retry_interval_ms"};
        String[][] invalid = { {"0", "300001"}, {"0", "300001"}, {"-1", "6"}, {"0", "10001"}};
        for (int i = 0; i < names.length; i++) {
            var field = Config.class.getField(names[i]);
            Object previous = field.get(null);
            for (String value : invalid[i]) {
                assertThrows(Exception.class, () -> ConfigBase.setConfigField(field, value));
                assertEquals(previous, field.get(null));
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testRootSearchClosesFailedResourcesBeforeRetry() throws Exception {
        DirContext failedContext = mock(DirContext.class);
        NamingEnumeration<SearchResult> failedResults = mock(NamingEnumeration.class);
        when(failedContext.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(failedResults);
        when(failedResults.hasMore()).thenThrow(new CommunicationException());
        DirContext successfulContext = successfulSearchContext();
        AtomicInteger calls = new AtomicInteger();
        LDAPAuthProvider provider = provider(env -> {
            if (calls.incrementAndGet() == 1) {
                return failedContext;
            }
            verify(failedResults).close();
            verify(failedContext).close();
            return successfulContext;
        }, null, null);
        assertEquals(USER_DN, provider.findUserDNByRoot("alice"));
        assertEquals(2, calls.get());
        verify(successfulContext).close();
    }

    @Test
    void testMissingSearchUserDoesNotRetry() throws Exception {
        DirContext context = mock(DirContext.class);
        @SuppressWarnings("unchecked")
        NamingEnumeration<SearchResult> results = mock(NamingEnumeration.class);
        when(context.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(results);
        when(results.hasMore()).thenReturn(false);
        AtomicInteger calls = new AtomicInteger();
        LDAPAuthProvider provider = provider(env -> {
            calls.incrementAndGet();
            return context;
        }, null, null);
        assertThrows(AuthenticationException.class, () -> provider.findUserDNByRoot("alice"));
        assertEquals(1, calls.get());
        verify(results).close();
        verify(context).close();
    }

    @Test
    void testDuplicatePatternDoesNotRepeatRejectedBind() throws Exception {
        List<String> boundDNs = new ArrayList<>();
        DirContext context = mock(DirContext.class);
        LDAPAuthProvider provider = provider(env -> {
            String dn = env.get(Context.SECURITY_PRINCIPAL);
            boundDNs.add(dn);
            if (dn.contains("ou=Other")) {
                throw new javax.naming.AuthenticationException();
            }
            return context;
        }, null, "uid=${USER},ou=Other,dc=test,dc=com;uid=${USER},ou=Other,dc=test,dc=com;" +
                "uid=${USER},ou=People,dc=test,dc=com");
        assertEquals(USER_DN, provider.checkPasswordByDnPattern("alice", "password", new String[0]).dn());
        assertEquals(List.of("uid=alice,ou=Other,dc=test,dc=com", USER_DN), boundDNs);
        verify(context).close();
    }

    @Test
    void testPatternDoesNotFallbackOnTlsFailure() {
        CommunicationException failure = new CommunicationException();
        failure.setRootCause(new SSLHandshakeException("certificate rejected"));
        AtomicInteger calls = new AtomicInteger();
        LDAPAuthProvider provider = provider(env -> {
            calls.incrementAndGet();
            throw failure;
        }, null, "uid=${USER},ou=Other,dc=test,dc=com;uid=${USER},ou=People,dc=test,dc=com");
        assertSame(failure, assertThrows(CommunicationException.class,
                () -> provider.checkPasswordByDnPattern("alice", "password", new String[0])));
        assertEquals(1, calls.get());
    }

    @Test
    void testPatternInterruptionPreservesCauseAndDoesNotFallback() {
        InterruptedNamingException interrupted = new InterruptedNamingException();
        AtomicInteger calls = new AtomicInteger();
        LDAPAuthProvider provider = provider(env -> {
            calls.incrementAndGet();
            throw interrupted;
        }, null, "uid=${USER},ou=Other,dc=test,dc=com;uid=${USER},ou=People,dc=test,dc=com");
        AuthenticationException failure = assertThrows(AuthenticationException.class,
                () -> provider.authenticate(new AccessControlContext(), UserIdentity.createEphemeralUserIdent("alice", "%"),
                        AuthCredentials.ofPassword("alice", "password", AuthChannel.MYSQL, "%")));
        assertSame(interrupted, failure.getCause());
        assertTrue(failure.isTransientFailure());
        assertEquals(1, calls.get());
        assertTrue(Thread.currentThread().isInterrupted());
    }

    @Test
    void testTransportFailureMustNotEnterRejectedCredentialCache() {
        Config.authentication_ldap_simple_max_retries = 0;
        CommunicationException unreachable = new CommunicationException();
        LDAPAuthProvider provider = provider(env -> {
            throw unreachable;
        }, USER_DN, null);
        AuthenticationException failure = assertThrows(AuthenticationException.class,
                () -> provider.authenticate(new AccessControlContext(), UserIdentity.createEphemeralUserIdent("alice", "%"),
                        AuthCredentials.ofPassword("alice", "password", AuthChannel.MYSQL, "%")));
        assertSame(unreachable, failure.getCause());
        assertTrue(failure.isTransientFailure());
    }

    @Test
    void testRejectedCredentialsRemainNonTransient() {
        javax.naming.AuthenticationException rejected = new javax.naming.AuthenticationException("invalid credentials");
        LDAPAuthProvider provider = provider(env -> {
            throw rejected;
        }, USER_DN, null);
        AuthenticationException failure = assertThrows(AuthenticationException.class,
                () -> provider.authenticate(new AccessControlContext(), UserIdentity.createEphemeralUserIdent("alice", "%"),
                        AuthCredentials.ofPassword("alice", "wrong-password", AuthChannel.MYSQL, "%")));
        assertSame(rejected, failure.getCause());
        assertFalse(failure.isTransientFailure());
    }

    @Test
    void testAttributeReadFailureDoesNotRepeatSuccessfulUserBind() throws Exception {
        DirContext context = mock(DirContext.class);
        when(context.getAttributes(any(LdapName.class), any(String[].class))).thenThrow(new CommunicationException());
        AtomicInteger calls = new AtomicInteger();
        LDAPAuthProvider provider = provider(env -> {
            calls.incrementAndGet();
            return context;
        }, USER_DN, null);
        LdapUserEntry entry = provider.checkPasswordAndReadAttributes(USER_DN, "password", new String[] {"memberOf"});
        assertEquals(USER_DN, entry.dn());
        assertNull(entry.attributes());
        assertEquals(1, calls.get());
        verify(context).close();
    }

    @Test
    void testServiceAccountReadRetriesAndClosesFailedContext() throws Exception {
        DirContext failedContext = mock(DirContext.class);
        when(failedContext.getAttributes(any(LdapName.class), any(String[].class)))
                .thenThrow(new CommunicationException());
        DirContext successfulContext = mock(DirContext.class);
        BasicAttributes attributes = new BasicAttributes("memberOf", "cn=engineers,dc=test,dc=com");
        when(successfulContext.getAttributes(any(LdapName.class), any(String[].class))).thenReturn(attributes);
        AtomicInteger calls = new AtomicInteger();
        LDAPAuthProvider provider = provider(env -> {
            if (calls.incrementAndGet() == 1) {
                return failedContext;
            }
            verify(failedContext).close();
            return successfulContext;
        }, null, null);
        assertSame(attributes, provider.readAttributesAsServiceAccount(USER_DN, new String[] {"memberOf"}));
        assertEquals(2, calls.get());
        verify(successfulContext).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testRootSearchPreservesRequestedAttributes() throws Exception {
        DirContext context = mock(DirContext.class);
        NamingEnumeration<SearchResult> results = mock(NamingEnumeration.class);
        BasicAttributes attributes = new BasicAttributes("uid", "Alice");
        SearchResult result = new SearchResult("uid=alice", null, attributes);
        result.setNameInNamespace(USER_DN);
        String[] requested = {"uid", "memberOf"};
        when(context.search(anyString(), anyString(), any(SearchControls.class))).thenAnswer(invocation -> {
            SearchControls controls = invocation.getArgument(2);
            assertArrayEquals(requested, controls.getReturningAttributes());
            return results;
        });
        when(results.hasMore()).thenReturn(true, false);
        when(results.next()).thenReturn(result);
        LDAPAuthProvider provider = provider(env -> context, null, null);
        LdapUserEntry entry = provider.findUserEntryByRoot("alice", requested);
        assertEquals(USER_DN, entry.dn());
        assertSame(attributes, entry.attributes());
        verify(results).close();
        verify(context).close();
    }

    @SuppressWarnings("unchecked")
    private DirContext successfulSearchContext() throws NamingException {
        DirContext context = mock(DirContext.class);
        NamingEnumeration<SearchResult> results = mock(NamingEnumeration.class);
        SearchResult result = new SearchResult("uid=alice", null, null);
        result.setNameInNamespace(USER_DN);
        when(context.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(results);
        when(results.hasMore()).thenReturn(true, false);
        when(results.next()).thenReturn(result);
        return context;
    }
}
