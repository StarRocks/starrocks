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
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.naming.NamingException;

/**
 * Lifecycle tests for ALTER GROUP PROVIDER: the two-phase activation described in the feature design.
 *
 * <p>An LDAPGroupProvider is the only implementation with runtime state (a periodic refresh schedule and an
 * in-memory group cache), so it is the one that makes the ordering inside
 * {@link AuthenticationMgr#alterGroupProvider} observable. There is no in-memory LDAP server available to the
 * FE test tree, so the directory layer is faked with a MockUp: {@code prepareForActivation()} fills the new
 * instance's cache the way a real synchronous lookup would, and {@code init()} does not start a schedule.
 * What is under test is the manager's orchestration, which is where a lost-groups window would come from:
 * warm up and validate the new instance, then swap it into the map and only then destroy the old one.
 */
public class AlterGroupProviderLifecycleTest {

    private static final String PROVIDER_NAME = "alter_lifecycle_provider";
    private static final String USER = "alice";
    private static final Set<String> OLD_GROUPS = Set.of("old_group");
    private static final Set<String> NEW_GROUPS = Set.of("new_group");

    private AuthenticationMgr authenticationMgr;
    private UserIdentity user;

    @BeforeAll
    public static void setUpPersistJournal() throws Exception {
        // The swap runs inside the EditLog WAL applier, so the tests need a journal that actually completes.
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterAll
    public static void tearDownPersistJournal() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        user = UserIdentity.createEphemeralUserIdent(USER, "%");
    }

    @AfterEach
    public void tearDown() {
        GroupProvider provider = authenticationMgr.getGroupProvider(PROVIDER_NAME);
        if (provider != null) {
            authenticationMgr.replayDropGroupProvider(PROVIDER_NAME);
        }
    }

    /**
     * Test case: the new provider's cache is already warm when the ALTER returns
     * Test point: prepareForActivation() runs before the swap, so the first group lookup after ALTER sees the
     *             new directory content instead of an empty set (an empty set would silently strip every role
     *             granted TO EXTERNAL GROUP).
     */
    @Test
    public void testWarmCacheIsVisibleImmediatelyAfterAlter() throws Exception {
        mockDirectory(new AtomicBoolean(false), new ArrayList<>(), new ArrayList<>());
        createLiveProviderWithOldGroups();

        Assertions.assertEquals(OLD_GROUPS, lookupGroups(), "Sanity check: the old provider serves old_group");

        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("ldap_group_dn", "cn=new_group,dc=example,dc=com");
        authenticationMgr.alterGroupProvider(PROVIDER_NAME, alterProps);

        Assertions.assertEquals(NEW_GROUPS, lookupGroups(),
                "The very first lookup after ALTER must already see the new groups");
    }

    /**
     * Test case: the old provider is destroyed only after the new one is serving
     * Test point: the WAL applier puts the new instance into the map before destroying the old one, so at the
     *             moment the old instance goes away a lookup already resolves against the warm new instance.
     *             This is the deterministic observation point for the "no window" property: reversing the two
     *             steps, or skipping the warm-up, both turn the recorded set empty.
     */
    @Test
    public void testOldProviderIsDestroyedOnlyAfterTheSwap() throws Exception {
        List<Set<String>> groupsSeenWhenOldWasDestroyed = new ArrayList<>();
        List<GroupProvider> destroyed = new ArrayList<>();
        mockDirectory(new AtomicBoolean(false), destroyed, groupsSeenWhenOldWasDestroyed);
        GroupProvider before = createLiveProviderWithOldGroups();

        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("ldap_group_dn", "cn=new_group,dc=example,dc=com");
        authenticationMgr.alterGroupProvider(PROVIDER_NAME, alterProps);

        Assertions.assertEquals(1, destroyed.size(),
                "Exactly the old provider should be destroyed, got " + destroyed.size());
        Assertions.assertSame(before, destroyed.get(0), "The destroyed instance must be the old one");
        Assertions.assertEquals(List.of(NEW_GROUPS), groupsSeenWhenOldWasDestroyed,
                "When the old provider was destroyed, lookups must already resolve to the new groups");

        GroupProvider after = authenticationMgr.getGroupProvider(PROVIDER_NAME);
        Assertions.assertNotSame(before, after, "The map must hold the new instance");
    }

    /**
     * Test case: destroy() on a provider that was never initialised
     * Test point: the ALTER failure path destroys a provider that never ran init() (edit-log write failure),
     *             so destroy() must tolerate a null schedule. Without this, the NPE would mask the real error.
     */
    @Test
    public void testDestroyToleratesProviderThatWasNeverInitialized() {
        LDAPGroupProvider provider = new LDAPGroupProvider(PROVIDER_NAME, ldapProperties());
        Assertions.assertDoesNotThrow(provider::destroy,
                "destroy() must be safe on a provider whose init() never ran");
    }

    /**
     * Test case: replaying an ALTER whose init() fails on the follower
     * Test point: replay does a cold init() only; if that fails the follower keeps the old instance in the map
     *             rather than leaving the name unserved. A missing entry would make every lookup on that
     *             follower return no groups.
     */
    @Test
    public void testReplayAlterKeepsOldProviderWhenInitFails() throws Exception {
        AtomicBoolean initShouldFail = new AtomicBoolean(false);
        mockDirectory(initShouldFail, new ArrayList<>(), new ArrayList<>());
        GroupProvider before = createLiveProviderWithOldGroups();

        initShouldFail.set(true);
        // The journal record carries the provider's complete property map, not the delta.
        Map<String, String> replayedProps = new HashMap<>(ldapProperties());
        replayedProps.put("ldap_group_dn", "cn=new_group,dc=example,dc=com");
        authenticationMgr.replayAlterGroupProvider(PROVIDER_NAME, replayedProps);

        Assertions.assertSame(before, authenticationMgr.getGroupProvider(PROVIDER_NAME),
                "A replay whose init() failed must keep the old provider in place");
        Assertions.assertEquals(OLD_GROUPS, lookupGroups(), "The old provider must still serve lookups");
    }

    /**
     * Test case: lookups running concurrently with an ALTER never observe an empty group set
     * Test point: supplementary probe for the same property as
     *             {@link #testOldProviderIsDestroyedOnlyAfterTheSwap}, covering the instants that the destroy()
     *             hook cannot observe. It cannot fail for a correct implementation, and catches a reintroduced
     *             remove-then-put window probabilistically rather than deterministically.
     */
    @Test
    public void testConcurrentLookupsNeverSeeEmptyGroupsDuringAlter() throws Exception {
        mockDirectory(new AtomicBoolean(false), new ArrayList<>(), new ArrayList<>());
        createLiveProviderWithOldGroups();

        AtomicBoolean stop = new AtomicBoolean(false);
        List<Set<String>> emptyObservations = new ArrayList<>();
        // A throwable in the reader would otherwise only reach the default handler's stderr: the thread
        // would die, record nothing, and the assertion below would pass on an empty list - i.e. the probe
        // would report success precisely when lookups are broken.
        List<Throwable> readerFailures = new ArrayList<>();
        Thread reader = new Thread(() -> {
            try {
                while (!stop.get()) {
                    Set<String> groups = lookupGroups();
                    if (groups.isEmpty()) {
                        synchronized (emptyObservations) {
                            emptyObservations.add(groups);
                        }
                    }
                }
            } catch (Throwable t) {
                synchronized (readerFailures) {
                    readerFailures.add(t);
                }
            }
        });
        reader.start();

        try {
            for (int i = 0; i < 20; i++) {
                Map<String, String> alterProps = new HashMap<>();
                alterProps.put("ldap_group_dn", "cn=new_group_" + i + ",dc=example,dc=com");
                authenticationMgr.alterGroupProvider(PROVIDER_NAME, alterProps);
            }
        } finally {
            stop.set(true);
            reader.join();
        }

        synchronized (readerFailures) {
            Assertions.assertTrue(readerFailures.isEmpty(),
                    "Group lookups must not throw while ALTER swaps the provider, got: " + readerFailures);
        }
        synchronized (emptyObservations) {
            Assertions.assertTrue(emptyObservations.isEmpty(),
                    "Group lookups must never return an empty set while ALTER swaps the provider, saw "
                            + emptyObservations.size() + " empty results");
        }
    }

    /**
     * Test case: two ALTERs of the same provider overlapping in their validation phase
     * Test point: ALTER is read-merge-write and its validation phase runs outside the DDL lock, so two
     *             statements really can merge onto the same snapshot. The one that gets to the swap second
     *             must notice that the provider is no longer the instance it merged onto and redo the merge;
     *             publishing its stale map would drop the other delta from this FE's memory while the journal
     *             kept both, leaving the followers - and this FE after a restart - with a different provider.
     *
     * <p>The interleaving is forced with latches rather than a sleep race, so the case either exercises the
     * overlap or fails: a timing-based version degrades into a sequential no-op on a loaded runner.
     */
    @Test
    public void testConcurrentAltersDoNotLoseADelta() throws Exception {
        CountDownLatch firstIsValidating = new CountDownLatch(1);
        CountDownLatch firstMayFinishValidating = new CountDownLatch(1);
        AtomicInteger validations = new AtomicInteger();
        mockDirectoryPausingFirstValidation(firstIsValidating, firstMayFinishValidating, validations);
        createLiveProviderWithOldGroups();

        List<Throwable> failures = new ArrayList<>();
        Thread first = alterInBackground("ldap_user_search_attr", "sAMAccountName", failures);
        first.start();
        Assertions.assertTrue(firstIsValidating.await(30, TimeUnit.SECONDS),
                "The first ALTER should have reached its validation phase");

        // The first ALTER is parked in prepareForActivation() with the DDL lock released, so this one reads
        // the same properties it did and runs to completion, swapping in a provider the first one has never
        // seen.
        Thread second = alterInBackground("ldap_group_identifier_attr", "sn", failures);
        second.start();
        second.join(30000);
        Assertions.assertFalse(second.isAlive(), "The second ALTER should not be blocked by the first one");

        firstMayFinishValidating.countDown();
        first.join(30000);
        Assertions.assertFalse(first.isAlive(), "The first ALTER should finish once its validation returns");

        Assertions.assertTrue(failures.isEmpty(), "Both ALTERs should succeed, got: " + failures);
        GroupProvider after = authenticationMgr.getGroupProvider(PROVIDER_NAME);
        Assertions.assertEquals("sAMAccountName", after.getProperties().get("ldap_user_search_attr"),
                "The first delta must survive the concurrent ALTER");
        Assertions.assertEquals("sn", after.getProperties().get("ldap_group_identifier_attr"),
                "The second delta must survive the concurrent ALTER");
        Assertions.assertEquals(3, validations.get(),
                "The first ALTER should have validated twice: once on the stale base, once after redoing the merge");
    }

    /**
     * Test case: an ALTER whose base keeps changing under it
     * Test point: the retry is bounded, so a provider that is rewritten by someone else on every attempt ends
     *             with an error the operator can act on instead of a statement that never returns.
     */
    @Test
    public void testConcurrentAlterGivesUpAfterTheRetryBound() throws Exception {
        mockDirectory(new AtomicBoolean(false), new ArrayList<>(), new ArrayList<>());
        createLiveProviderWithOldGroups();

        // Replaces the provider instance from inside the validation phase, exactly as a competing statement
        // would, so every attempt finds a base that is no longer current.
        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void prepareForActivation(Invocation invocation) {
                LDAPGroupProvider provider = invocation.getInvokedInstance();
                provider.setUserToGroupCache(Map.of(USER, NEW_GROUPS));
                authenticationMgr.replayAlterGroupProvider(PROVIDER_NAME, ldapProperties());
            }
        };

        Map<String, String> alterProps = new HashMap<>();
        alterProps.put("ldap_user_search_attr", "sAMAccountName");
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> authenticationMgr.alterGroupProvider(PROVIDER_NAME, alterProps),
                "An ALTER whose base changes on every attempt must fail rather than retry forever");
        Assertions.assertTrue(exception.getMessage().contains("being modified concurrently"),
                "The error should say what happened: " + exception.getMessage());
    }

    /**
     * Test case: a property the provider type does not define
     * Test point: every getter reads the property map with an exact get(), so an unknown key would sit next
     *             to the real one and the statement would report success while changing nothing. A key that
     *             differs from a real one only in case is normalized instead - that is a spelling a user can
     *             plausibly get wrong, and refusing it would help nobody.
     */
    @Test
    public void testAlterRejectsUnknownPropertyAndNormalizesKeyCase() throws Exception {
        mockDirectory(new AtomicBoolean(false), new ArrayList<>(), new ArrayList<>());
        createLiveProviderWithOldGroups();

        Map<String, String> unknown = new HashMap<>();
        unknown.put("ldap_bind_root_pw", "rotated");
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> authenticationMgr.alterGroupProvider(PROVIDER_NAME, unknown),
                "A typo'd property must fail the statement, not be stored next to the real one");
        Assertions.assertTrue(exception.getMessage().contains("unknown property 'ldap_bind_root_pw'"),
                "The error should name the property: " + exception.getMessage());

        Map<String, String> caseVariant = new HashMap<>();
        caseVariant.put("LDAP_BIND_ROOT_PWD", "rotated");
        authenticationMgr.alterGroupProvider(PROVIDER_NAME, caseVariant);

        GroupProvider after = authenticationMgr.getGroupProvider(PROVIDER_NAME);
        Assertions.assertEquals("rotated", after.getProperties().get("ldap_bind_root_pwd"),
                "A key that differs only in case must update the property it names");
        Assertions.assertNull(after.getProperties().get("LDAP_BIND_ROOT_PWD"),
                "The map must not end up with two spellings of the same property");
    }

    /**
     * Test case: a refresh that cannot reach the directory
     * Test point: unlike the tests above, this one runs the real init()/prepareForActivation()/refreshGroups()
     *             and fakes only the directory access itself, because the property under test lives in those
     *             methods: refreshGroups() publishes its result only on success. Publishing unconditionally
     *             would blank the cache on the first failed refresh - including the delay-0 refresh that
     *             init() starts right after ALTER warmed it - and every role granted TO EXTERNAL GROUP would
     *             stop applying until a refresh succeeded again.
     */
    @Test
    public void testFailedRefreshKeepsTheCacheItAlreadyHas() throws Exception {
        AtomicBoolean fetchShouldFail = new AtomicBoolean(false);
        mockDirectoryFetch(fetchShouldFail, new AtomicInteger());

        LDAPGroupProvider provider = new LDAPGroupProvider(PROVIDER_NAME, ldapProperties());
        provider.prepareForActivation();
        Assertions.assertEquals(NEW_GROUPS, provider.getGroup(user, null),
                "Sanity check: prepareForActivation() loads the cache synchronously");

        fetchShouldFail.set(true);
        provider.refreshGroups();

        Assertions.assertEquals(NEW_GROUPS, provider.getGroup(user, null),
                "A refresh that failed must leave the previous cache in place, not publish an empty one");
    }

    /**
     * Test case: the schedule started right after a warm-up
     * Test point: prepareForActivation() has just walked the whole directory, so the periodic refresh must
     *             not walk it again immediately. Runs the real init(), so the initial delay is what decides
     *             the outcome.
     */
    @Test
    public void testWarmedProviderDoesNotWalkTheDirectoryAgainImmediately() throws Exception {
        AtomicInteger fetchCount = new AtomicInteger();
        mockDirectoryFetch(new AtomicBoolean(false), fetchCount);

        LDAPGroupProvider warm = new LDAPGroupProvider(PROVIDER_NAME, ldapProperties());
        try {
            warm.prepareForActivation();
            warm.init();
            Assertions.assertEquals(1, fetchCount.get(),
                    "Only the synchronous warm-up should have read the directory so far");
            Thread.sleep(500);
            Assertions.assertEquals(1, fetchCount.get(),
                    "The schedule of an already warm provider must not repeat the walk it just did");
        } finally {
            warm.destroy();
        }

        // ... while a cold provider (CREATE, replay, restart) does need that first refresh now.
        fetchCount.set(0);
        LDAPGroupProvider cold = new LDAPGroupProvider(PROVIDER_NAME, ldapProperties());
        try {
            cold.init();
            long deadline = System.currentTimeMillis() + 10000;
            while (fetchCount.get() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            Assertions.assertEquals(1, fetchCount.get(),
                    "A provider whose cache is cold must refresh as soon as it is initialised");
        } finally {
            cold.destroy();
        }
    }

    /**
     * Test case: replaying an ALTER on a follower
     * Test point: replay cannot run prepareForActivation() (it blocks on network I/O), so the instance it
     *             publishes starts with an empty cache. It inherits the outgoing instance's cache instead,
     *             otherwise every lookup on that node resolves no groups until the first background refresh
     *             completes - and forever if that node cannot reach the directory.
     */
    @Test
    public void testReplayInheritsTheCacheOfTheProviderItReplaces() throws Exception {
        mockDirectory(new AtomicBoolean(false), new ArrayList<>(), new ArrayList<>());
        createLiveProviderWithOldGroups();

        Map<String, String> replayedProps = new HashMap<>(ldapProperties());
        replayedProps.put("ldap_group_dn", "cn=new_group,dc=example,dc=com");
        authenticationMgr.replayAlterGroupProvider(PROVIDER_NAME, replayedProps);

        Assertions.assertNotNull(authenticationMgr.getGroupProvider(PROVIDER_NAME), "Replay must publish the new instance");
        Assertions.assertEquals(OLD_GROUPS, lookupGroups(),
                "Until its first refresh lands, the replayed provider must serve what the old one resolved");
    }

    /**
     * Fakes only the directory access of {@link LDAPGroupProvider#fetchGroupsInto}, leaving init(),
     * prepareForActivation() and refreshGroups() to run their real code. Counts the reads so a test can tell
     * how many directory walks a lifecycle actually costs.
     */
    private void mockDirectoryFetch(AtomicBoolean fetchShouldFail, AtomicInteger fetchCount) {
        new MockUp<LDAPGroupProvider>() {
            @Mock
            public boolean fetchGroupsInto(Map<String, Set<String>> groups) throws NamingException {
                fetchCount.incrementAndGet();
                if (fetchShouldFail.get()) {
                    throw new NamingException("simulated directory failure");
                }
                groups.put(USER, NEW_GROUPS);
                return true;
            }
        };
    }

    private Thread alterInBackground(String key, String value, List<Throwable> failures) {
        return new Thread(() -> {
            try {
                Map<String, String> alterProps = new HashMap<>();
                alterProps.put(key, value);
                authenticationMgr.alterGroupProvider(PROVIDER_NAME, alterProps);
            } catch (Throwable t) {
                synchronized (failures) {
                    failures.add(t);
                }
            }
        });
    }

    /**
     * Same fake directory as {@link #mockDirectory}, except that the very first validation parks until it is
     * released. That is what makes the overlap deterministic: the first ALTER is guaranteed to still be in
     * its validation phase, with the DDL lock released, while the second one runs start to finish.
     */
    private void mockDirectoryPausingFirstValidation(CountDownLatch isValidating,
                                                     CountDownLatch mayFinish,
                                                     AtomicInteger validations) {
        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() {
            }

            @Mock
            public void checkProperty() {
            }

            @Mock
            public void refreshGroups() {
            }

            @Mock
            public void prepareForActivation(Invocation invocation) throws InterruptedException {
                if (validations.incrementAndGet() == 1) {
                    isValidating.countDown();
                    Assertions.assertTrue(mayFinish.await(30, TimeUnit.SECONDS),
                            "The paused validation should have been released by the test");
                }
                LDAPGroupProvider provider = invocation.getInvokedInstance();
                provider.setUserToGroupCache(Map.of(USER, NEW_GROUPS));
            }
        };
    }

    /**
     * Fakes the directory layer of every LDAPGroupProvider created during a test:
     * - init() starts no schedule (and optionally fails, to exercise the replay failure path),
     * - checkProperty() accepts whatever the test passes in,
     * - prepareForActivation() fills the instance's cache the way a real synchronous lookup would,
     * - destroy() records the instance and what a lookup resolves to at that exact moment.
     */
    private void mockDirectory(AtomicBoolean initShouldFail,
                               List<GroupProvider> destroyed,
                               List<Set<String>> groupsSeenWhenDestroyed) {
        new MockUp<LDAPGroupProvider>() {
            @Mock
            public void init() throws DdlException {
                if (initShouldFail.get()) {
                    throw new DdlException("simulated init failure");
                }
            }

            @Mock
            public void checkProperty() {
            }

            @Mock
            public void refreshGroups() {
            }

            @Mock
            public void prepareForActivation(Invocation invocation) {
                LDAPGroupProvider provider = invocation.getInvokedInstance();
                provider.setUserToGroupCache(Map.of(USER, NEW_GROUPS));
            }

            @Mock
            public void destroy(Invocation invocation) {
                destroyed.add(invocation.getInvokedInstance());
                groupsSeenWhenDestroyed.add(lookupGroups());
                invocation.proceed();
            }
        };
    }

    /**
     * Puts a provider serving OLD_GROUPS into the map through the replay path, which does a cold init() only
     * and therefore needs no synchronous directory access.
     */
    private GroupProvider createLiveProviderWithOldGroups() {
        authenticationMgr.replayCreateGroupProvider(PROVIDER_NAME, ldapProperties());
        LDAPGroupProvider provider = (LDAPGroupProvider) authenticationMgr.getGroupProvider(PROVIDER_NAME);
        Assertions.assertNotNull(provider, "The provider under test should be in the map");
        provider.setUserToGroupCache(Map.of(USER, OLD_GROUPS));
        return provider;
    }

    /** Resolves groups through the entry point the login path uses. */
    private Set<String> lookupGroups() {
        return AuthenticationHandler.resolveGroupsFromProviders(user, null, List.of(PROVIDER_NAME));
    }

    private Map<String, String> ldapProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "ldap");
        properties.put("ldap_conn_url", "ldap://127.0.0.1:1");
        properties.put("ldap_bind_root_dn", "cn=admin,dc=example,dc=com");
        properties.put("ldap_bind_root_pwd", "secret");
        properties.put("ldap_bind_base_dn", "dc=example,dc=com");
        properties.put("ldap_group_dn", "cn=old_group,dc=example,dc=com");
        properties.put("ldap_user_search_attr", "uid");
        return properties;
    }
}
