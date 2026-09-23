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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.TableLoadPurpose;
import com.starrocks.qe.ConnectContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The scope is what makes "for this query and no other" mean something, so what is pinned here is its
 * boundary: when it exists, when it ends, and what does not survive it.
 *
 * The eviction-driven alternative this replaced is why the last test matters - state that a cache could
 * drop at any moment cannot be the thing an authorization decision rests on.
 */
public class LakeFormationQueryScopeTest {

    private static final LakeFormationTableIdentity IDENTITY =
            new LakeFormationTableIdentity("lf", null, "us-west-2", "db", "t");

    private ConnectContext context;

    private ConnectContext contextFor(String user) {
        ConnectContext ctx = contextOnThisThread();
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(user, "%"));
        ctx.setThreadLocalInfo();
        return ctx;
    }

    private ConnectContext contextOnThisThread() {
        context = new ConnectContext();
        context.setQueryId(UUID.randomUUID());
        context.setThreadLocalInfo();
        return context;
    }

    /** Shaped the way StmtExecutor hands one to the planner: a user, and an execution id of its own. */
    private ConnectContext statementFor(String user) {
        ConnectContext ctx = contextFor(user);
        ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
        return ctx;
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testNoScopeExistsOutsidePlanning() {
        assertTrue(LakeFormationQueryScope.current().isEmpty());
    }

    @Test
    public void testScopeEndsWithTheStatement() {
        try (LakeFormationQueryScope.Scope ignored = LakeFormationQueryScope.open(contextOnThisThread())) {
            assertTrue(LakeFormationQueryScope.current().isPresent());
        }
        assertTrue(LakeFormationQueryScope.current().isEmpty(),
                "nothing may read what an attempt decided once the attempt is over");
    }

    /**
     * An INSERT plans its own query, and MV refresh plans inside another plan. Those inner calls have to
     * join the statement's attempt rather than start a second one, or one statement would end up with two
     * sets of credentials and two authorization decisions.
     */
    @Test
    public void testNestedPlanningJoinsTheEnclosingAttemptAndDoesNotEndIt() {
        try (LakeFormationQueryScope.Scope outer = LakeFormationQueryScope.open(contextOnThisThread())) {
            String outerAttempt = LakeFormationQueryScope.current().orElseThrow().attemptId();

            try (LakeFormationQueryScope.Scope inner = LakeFormationQueryScope.open(context)) {
                assertEquals(outerAttempt, LakeFormationQueryScope.current().orElseThrow().attemptId());
            }

            assertTrue(LakeFormationQueryScope.current().isPresent(),
                    "an inner planner call must not end the statement's attempt");
            assertEquals(outerAttempt, LakeFormationQueryScope.current().orElseThrow().attemptId());
        }
    }

    @Test
    public void testEachStatementGetsItsOwnAttemptId() {
        String first;
        try (LakeFormationQueryScope.Scope ignored = LakeFormationQueryScope.open(contextOnThisThread())) {
            first = LakeFormationQueryScope.current().orElseThrow().attemptId();
        }
        try (LakeFormationQueryScope.Scope ignored = LakeFormationQueryScope.open(contextOnThisThread())) {
            assertNotEquals(first, LakeFormationQueryScope.current().orElseThrow().attemptId(),
                    "a new statement must not be able to reuse the previous attempt's authorizations");
        }
    }

    /**
     * Hive and Glue compare database and table names case insensitively. Without normalizing the key,
     * "FROM DB.T JOIN db.t" would authorize twice, vend twice, and produce two Table objects for one table.
     */
    @Test
    public void testTwoSpellingsOfOneTableResolveOnce() {
        try (LakeFormationQueryScope.Scope ignored = LakeFormationQueryScope.open(contextOnThisThread())) {
            LakeFormationQueryScope scope = LakeFormationQueryScope.current().orElseThrow();
            AtomicInteger resolutions = new AtomicInteger();

            LakeFormationTableResolution first = scope.resolve(IDENTITY, TableLoadPurpose.DATA_ACCESS,
                    key -> countingUnregistered(resolutions));
            LakeFormationTableResolution second = scope.resolve(
                    new LakeFormationTableIdentity("lf", null, "us-west-2", "DB", "T"),
                    TableLoadPurpose.DATA_ACCESS, key -> countingUnregistered(resolutions));

            assertEquals(1, resolutions.get());
            assertSame(first, second);
        }
    }

    /**
     * A metadata-only authorization is not a data-access one, so the two cannot share an entry - otherwise
     * a DESC would satisfy a later SELECT, which is the in-place upgrade the design forbids.
     */
    @Test
    public void testPurposeIsPartOfTheKey() {
        try (LakeFormationQueryScope.Scope ignored = LakeFormationQueryScope.open(contextOnThisThread())) {
            LakeFormationQueryScope scope = LakeFormationQueryScope.current().orElseThrow();
            AtomicInteger resolutions = new AtomicInteger();

            scope.resolve(IDENTITY, TableLoadPurpose.METADATA_ONLY, key -> countingUnregistered(resolutions));
            scope.resolve(IDENTITY, TableLoadPurpose.DATA_ACCESS, key -> countingUnregistered(resolutions));

            assertEquals(2, resolutions.get(),
                    "data access must be resolved on its own rather than inheriting a metadata answer");
        }
    }

    /**
     * A refusal has to keep being a refusal. Letting the second call try again would mean one statement
     * could get two different answers out of Lake Formation, with the later one winning for no better
     * reason than being later.
     */
    @Test
    public void testAFailedResolutionIsNotRetriedWithinTheAttempt() {
        try (LakeFormationQueryScope.Scope ignored = LakeFormationQueryScope.open(contextOnThisThread())) {
            LakeFormationQueryScope scope = LakeFormationQueryScope.current().orElseThrow();
            AtomicInteger resolutions = new AtomicInteger();

            scope.resolve(IDENTITY, TableLoadPurpose.DATA_ACCESS, key -> {
                resolutions.incrementAndGet();
                return LakeFormationTableResolution.failed(
                        new LakeFormationTableAccessException("refused"));
            });
            scope.resolve(IDENTITY, TableLoadPurpose.DATA_ACCESS, key -> {
                resolutions.incrementAndGet();
                return LakeFormationTableResolution.unregistered();
            });

            assertEquals(1, resolutions.get(), "a refusal must not get a second chance");
            // The remembered outcome is still the refusal, not the second resolver's answer.
            assertThrows(LakeFormationTableAccessException.class,
                    () -> scope.find(IDENTITY, TableLoadPurpose.DATA_ACCESS).rethrowIfFailed());
        }
    }

    /**
     * A remembered refusal is rethrown as the same kind of refusal, carrying the same reason.
     *
     * <p>This is what lets a refusal survive the paths that catch broadly - materialized view refresh
     * retries on any Throwable, and the statistics layer turns a swallowed exception into "unknown" -
     * because those decide what to do by type and by message. A rethrow that widened the type or lost the
     * reason would turn a refusal into a silently degraded plan, which is the failure this whole design is
     * arranged to avoid. The wrapping itself is deliberate: each caller gets its own stack trace instead of
     * one pointing at whichever path happened to fail first.
     */
    @Test
    public void testARememberedRefusalIsRethrownAsTheSameRefusal() {
        LakeFormationTableAccessException original =
                new LakeFormationTableAccessException("cannot be read outside a planning attempt");
        LakeFormationTableResolution failed = LakeFormationTableResolution.failed(original);

        LakeFormationTableAccessException rethrown =
                assertThrows(LakeFormationTableAccessException.class, failed::rethrowIfFailed);

        assertEquals(original.getMessage(), rethrown.getMessage(), "the reason has to survive");
        assertNotSame(original, rethrown, "a fresh exception, so the trace points at this caller");
        assertSame(original, rethrown.getCause(), "with the first failure kept as the cause");
    }

    /** And a resolution that did not fail rethrows nothing, so the happy path stays quiet. */
    @Test
    public void testASuccessfulResolutionRethrowsNothing() {
        assertDoesNotThrow(() -> LakeFormationTableResolution.unregistered().rethrowIfFailed());
    }

    /**
     * Two statements planned at the same time each keep their own answers.
     *
     * <p>The attempt is a thread local, so this holds by construction - but nothing pinned it, and the whole
     * per-attempt design rests on it: if two statements could see each other's resolutions, one principal's
     * authorization could satisfy another's table, which is the failure this package exists to prevent. An
     * exact resolution count is what makes the test able to fail; "nothing threw" would pass even if the
     * attempts had been shared.
     */
    @Test
    public void testConcurrentAttemptsDoNotShareWhatTheyResolved() throws Exception {
        int planners = 8;
        CountDownLatch startLine = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(planners);
        AtomicInteger resolutions = new AtomicInteger();
        Set<String> attemptIds = ConcurrentHashMap.newKeySet();
        List<Throwable> thrown = new CopyOnWriteArrayList<>();
        ExecutorService planning = Executors.newFixedThreadPool(planners);

        try {
            for (int i = 0; i < planners; i++) {
                planning.execute(() -> {
                    try {
                        ConnectContext own = new ConnectContext();
                        own.setQueryId(UUID.randomUUID());
                        own.setThreadLocalInfo();
                        // A shared start line rather than a sleep: the attempts have to be open at the
                        // same time for this to be about isolation at all.
                        startLine.await();
                        try (LakeFormationQueryScope.Scope ignored = LakeFormationQueryScope.open(own)) {
                            LakeFormationQueryScope scope = LakeFormationQueryScope.current().orElseThrow();
                            attemptIds.add(scope.attemptId());
                            // Twice, so a leaked resolution from another attempt would show up as a
                            // resolution this attempt never had to make.
                            scope.resolve(IDENTITY, TableLoadPurpose.DATA_ACCESS,
                                    key -> countingUnregistered(resolutions));
                            scope.resolve(IDENTITY, TableLoadPurpose.DATA_ACCESS,
                                    key -> countingUnregistered(resolutions));
                        } finally {
                            ConnectContext.remove();
                        }
                    } catch (Throwable t) {
                        thrown.add(t);
                    } finally {
                        finished.countDown();
                    }
                });
            }
            startLine.countDown();
            assertTrue(finished.await(30, TimeUnit.SECONDS), "the planning threads did not finish");
        } finally {
            planning.shutdownNow();
        }

        assertTrue(thrown.isEmpty(), () -> "a planning thread failed: " + thrown);
        assertEquals(planners, resolutions.get(),
                "each attempt resolves the table once for itself - no more, and no fewer by inheriting");
        assertEquals(planners, attemptIds.size(), "and no two concurrent attempts share an id");
    }

    private static LakeFormationTableResolution countingUnregistered(AtomicInteger counter) {
        counter.incrementAndGet();
        return LakeFormationTableResolution.unregistered();
    }

    /**
     * A statement that failed between open and close leaves the scope on the thread. The next statement on
     * that pooled thread must not inherit it: the resolutions it holds were filtered for another user.
     */
    @Test
    public void testAScopeLeftBehindByAnotherUserIsNotJoined() {
        ConnectContext alice = contextFor("alice");
        ConnectContext bob = contextFor("bob");

        // Opened and never closed, the way a planning failure leaves it.
        LakeFormationQueryScope.Scope leakedHandle = LakeFormationQueryScope.open(alice);
        LakeFormationQueryScope leaked = LakeFormationQueryScope.current().orElseThrow();

        try (LakeFormationQueryScope.Scope ignored = LakeFormationQueryScope.open(bob)) {
            LakeFormationQueryScope mine = LakeFormationQueryScope.current().orElseThrow();
            assertNotSame(leaked, mine, "bob must not plan inside alice's scope");
            assertNotEquals(leaked.attemptId(), mine.attemptId());
        }
        leakedHandle.close();
    }

    /**
     * One connection runs its statements one after another, on one thread, under one user - so the user
     * alone cannot tell an enclosing attempt from a scope the previous statement failed to close. Joining
     * one would let a statement read authorizations decided before whatever was revoked in between, and
     * would do so for every later statement on that connection, because joining never ends it either.
     */
    @Test
    public void testAScopeLeftBehindByAnEarlierStatementIsNotJoined() {
        LakeFormationQueryScope.Scope leakedHandle = LakeFormationQueryScope.open(statementFor("alice"));
        LakeFormationQueryScope leaked = LakeFormationQueryScope.current().orElseThrow();

        try (LakeFormationQueryScope.Scope ignored = LakeFormationQueryScope.open(statementFor("alice"))) {
            assertNotSame(leaked, LakeFormationQueryScope.current().orElseThrow(),
                    "a new statement must not inherit what the previous one authorized");
        }
        leakedHandle.close();
    }

    /** The same statement's nested planning still joins, which is what makes one statement one attempt. */
    @Test
    public void testNestedPlanningForTheSameStatementStillJoins() {
        ConnectContext alice = statementFor("alice");
        try (LakeFormationQueryScope.Scope outer = LakeFormationQueryScope.open(alice)) {
            LakeFormationQueryScope enclosing = LakeFormationQueryScope.current().orElseThrow();
            try (LakeFormationQueryScope.Scope inner = LakeFormationQueryScope.open(alice)) {
                assertSame(enclosing, LakeFormationQueryScope.current().orElseThrow());
            }
            assertSame(enclosing, LakeFormationQueryScope.current().orElseThrow(),
                    "the inner call must not end the enclosing attempt");
        }
    }

    /**
     * An inner call that plans under a different context installs its own scope over the outer one. It has
     * to put the outer one back, or the rest of the outer statement plans with no scope at all and every
     * Lake Formation table left in it is refused for not being inside a planning attempt.
     */
    @Test
    public void testAnInnerAttemptRestoresTheOuterOneItDisplaced() {
        try (LakeFormationQueryScope.Scope outer = LakeFormationQueryScope.open(statementFor("alice"))) {
            LakeFormationQueryScope enclosing = LakeFormationQueryScope.current().orElseThrow();

            try (LakeFormationQueryScope.Scope inner = LakeFormationQueryScope.open(statementFor("bob"))) {
                assertNotSame(enclosing, LakeFormationQueryScope.current().orElseThrow());
            }

            assertSame(enclosing, LakeFormationQueryScope.current().orElseThrow(),
                    "the outer statement must still have its attempt after an inner one finishes");
        }
        assertTrue(LakeFormationQueryScope.current().isEmpty());
    }
}
