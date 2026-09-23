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

package com.starrocks.connector.paimon;

import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolations;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The paimon per-call door: that it covers the whole catalog surface, that it fires below the
 * cache, and that it stays silent for the methods which answer locally.
 */
public class GuardedPaimonCatalogTest {
    private static final long INTERNAL_DB_ID = 40001L;
    private static final String CATALOG = "paimon_catalog";

    /**
     * The methods that answer from configuration or do nothing at this layer. Repeated here rather
     * than read from the production class on purpose: this list is the claim under review, and a
     * test that read it from the code it checks would agree with any future edit to it.
     */
    private static final Set<String> LOCAL_METHODS = Set.of(
            "options", "catalogLoader", "caseSensitive", "supportsListObjectsPaged",
            "supportsListByPattern", "supportsListTableByType", "supportsVersionManagement",
            "invalidateTable");

    private String savedMode;

    @BeforeEach
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        savedMode = Config.lock_blocking_call_validation_mode;
        LockInvariantViolations.clearViolations();
        LockHoldDepth.reset();
    }

    @AfterEach
    public void tearDown() {
        Config.lock_blocking_call_validation_mode = savedMode;
        LockInvariantViolations.clearViolations();
        LockHoldDepth.reset();
    }

    /**
     * The methods that carry no door because {@link Catalog} gives them a local default -- see the
     * class comment for why deciding per implementation was rejected. Listed here, like
     * {@link #LOCAL_METHODS}, because this is the claim under review.
     */
    private static final Set<String> CAPABILITY_METHODS = Set.of(
            "getView", "dropView", "createView", "renameView", "alterView",
            "listViews", "listViewsPaged", "listViewDetailsPaged", "listViewsPagedGlobally",
            "listTablesPagedGlobally", "listFunctionsPaged", "listFunctionsPagedGlobally",
            "listFunctionDetailsPaged",
            "repairCatalog", "repairDatabase", "repairTable", "registerTable");

    /**
     * The completeness claim, measured rather than asserted: every method of {@link Catalog} is
     * called on a guarded catalog while a lock is held, and what reported is compared against what
     * was supposed to.
     * <p>
     * Two failures this catches. A method missing from this class would be inherited from
     * {@link DelegateCatalog} -- delegating correctly, reaching the metastore, with nothing to
     * report it -- which is how a paimon upgrade could quietly add an ungated path. And a method
     * that reports when it should not, or the reverse, is a door in the wrong group; both lists
     * above are the claim, so a change to either has to be a deliberate edit here.
     */
    @Test
    public void testEveryMethodIsOverriddenAndTheDoorsAreExactlyWhereClaimed() throws Exception {
        Config.lock_blocking_call_validation_mode = "warn";
        Catalog guarded = new GuardedPaimonCatalog(CATALOG, noopCatalog());
        List<String> missing = new ArrayList<>();
        Set<String> reported = new HashSet<>();
        Set<String> silent = new HashSet<>();

        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            for (Method method : Catalog.class.getMethods()) {
                if (Modifier.isStatic(method.getModifiers()) || "close".equals(method.getName())) {
                    // close() is teardown, inherited from AutoCloseable and left to DelegateCatalog
                    continue;
                }
                Method own;
                try {
                    own = GuardedPaimonCatalog.class.getDeclaredMethod(method.getName(), method.getParameterTypes());
                } catch (NoSuchMethodException e) {
                    missing.add(method.toString());
                    continue;
                }
                long before = LockInvariantViolations.totalViolations();
                own.invoke(guarded, defaultArgsOf(method));
                if (LockInvariantViolations.totalViolations() > before) {
                    reported.add(method.getName());
                } else {
                    silent.add(method.getName());
                }
            }
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }

        Collections.sort(missing);
        Assertions.assertTrue(missing.isEmpty(),
                "paimon's Catalog has methods with no door in GuardedPaimonCatalog. Either override them "
                        + "with a goingRemote() call, or -- if the new method answers locally -- add it to "
                        + "LOCAL_METHODS or CAPABILITY_METHODS here and to the class comment. Missing: " + missing);

        Set<String> expectedSilent = new HashSet<>(LOCAL_METHODS);
        expectedSilent.addAll(CAPABILITY_METHODS);
        Assertions.assertEquals(expectedSilent, silent,
                "the methods without a door are not the ones this test claims. Reported: " + reported);
        Assertions.assertFalse(reported.isEmpty());
    }

    /** Null for anything with an identity, the zero value for a primitive: the wrapper reads none of them. */
    private static Object[] defaultArgsOf(Method method) {
        Class<?>[] types = method.getParameterTypes();
        Object[] args = new Object[types.length];
        for (int i = 0; i < types.length; i++) {
            args[i] = types[i].isPrimitive() ? defaultValueOf(types[i]) : null;
        }
        return args;
    }

    @Test
    public void testAMissedLookupUnderALockIsReported() {
        Config.lock_blocking_call_validation_mode = "warn";
        Catalog guarded = new GuardedPaimonCatalog(CATALOG, noopCatalog());

        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            guarded.listDatabases();
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }

        Assertions.assertEquals(1, LockInvariantViolations.totalViolations());
        String site = LockInvariantViolations.violationsBySite().keySet().iterator().next();
        Assertions.assertTrue(site.contains(GuardedPaimonCatalogTest.class.getName()),
                "the report should name the caller, but named: " + site);
    }

    /** The catalog is named, because which paimon catalog is slow is the first thing to know. */
    @Test
    public void testTheReportNamesTheCatalog() {
        Config.lock_blocking_call_validation_mode = "error";
        Catalog guarded = new GuardedPaimonCatalog(CATALOG, noopCatalog());

        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                    () -> guarded.getTable(Identifier.create("db", "tbl")));
            Assertions.assertTrue(e.getMessage().contains(CATALOG), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("paimon"), e.getMessage());
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
    }

    /**
     * The other half of the claim. These answer from configuration, so a report would describe a
     * wait that does not happen -- and a door that cries wolf on {@code options()} would be read
     * as noise and then ignored where it matters.
     */
    @Test
    public void testTheLocalMethodsHaveNoDoor() {
        Config.lock_blocking_call_validation_mode = "error";
        Catalog guarded = new GuardedPaimonCatalog(CATALOG, noopCatalog());

        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            guarded.options();
            guarded.caseSensitive();
            guarded.catalogLoader();
            guarded.supportsListObjectsPaged();
            guarded.supportsListByPattern();
            guarded.supportsListTableByType();
            guarded.supportsVersionManagement();
            guarded.invalidateTable(Identifier.create("db", "tbl"));
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
        Assertions.assertEquals(8, LOCAL_METHODS.size(), "update this test when the local set changes");
    }

    /**
     * The deliberate blind spot, pinned so it stays deliberate. {@link Catalog} gives the view,
     * repair and global-paging methods a local default, so whether they go remote depends on the
     * catalog underneath; guarding them would report waits that never happen on every catalog that
     * does not implement the capability, so they carry no door at all.
     *
     * <p>If this ever needs closing, the fix is not to guard them here but to decide per
     * implementation -- and then this test has to say so instead.
     */
    @Test
    public void testTheCapabilityMethodsAreADeliberateBlindSpot() {
        Config.lock_blocking_call_validation_mode = "error";
        Catalog guarded = new GuardedPaimonCatalog(CATALOG, noopCatalog());

        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            // The four the FE actually calls, through PaimonMetadata.
            guarded.getView(Identifier.create("db", "v"));
            guarded.listViews("db");
            guarded.createView(Identifier.create("db", "v"), null, true);
            guarded.dropView(Identifier.create("db", "v"), true);
        } catch (Catalog.ViewNotExistException | Catalog.ViewAlreadyExistException
                | Catalog.DatabaseNotExistException e) {
            Assertions.fail("the stub answers everything, so none of these should throw: " + e);
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations(),
                "these carry no door on purpose; if one was added, update the class comment and "
                        + "BlockingCallValidator's gap list in the same change");
    }

    @Test
    public void testNoLockMeansNoReport() {
        Config.lock_blocking_call_validation_mode = "error";
        Catalog guarded = new GuardedPaimonCatalog(CATALOG, noopCatalog());
        guarded.listDatabases();
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    /**
     * What {@code PrivilegedCatalog.tryToCreate} relies on, and the reason this wrapper extends
     * {@link DelegateCatalog} instead of being a {@link Proxy}: the chain has to stay unwrappable
     * down to the catalog paimon built, or privileges are dropped without a word.
     */
    @Test
    public void testTheChainStaysUnwrappable() {
        Catalog root = noopCatalog();
        Catalog guarded = new GuardedPaimonCatalog(CATALOG, root);
        Assertions.assertSame(root, DelegateCatalog.rootCatalog(guarded));
    }

    /**
     * A catalog that answers everything with a default value. A {@link Proxy} is the right tool
     * <em>here</em> -- it is the thing being delegated to, not the thing in the chain that paimon
     * unwraps.
     */
    private static Catalog noopCatalog() {
        return (Catalog) Proxy.newProxyInstance(
                GuardedPaimonCatalogTest.class.getClassLoader(),
                new Class<?>[] {Catalog.class},
                (proxy, method, args) -> defaultValueOf(method.getReturnType()));
    }

    private static Object defaultValueOf(Class<?> type) {
        if (!type.isPrimitive()) {
            return List.class.isAssignableFrom(type) ? Collections.emptyList() : null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == void.class) {
            return null;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == double.class) {
            return 0d;
        }
        return 0;
    }
}
