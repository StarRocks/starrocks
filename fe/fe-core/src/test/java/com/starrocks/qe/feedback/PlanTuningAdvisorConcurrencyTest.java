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

package com.starrocks.qe.feedback;

import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.feedback.analyzer.PlanTuningAnalyzer;
import com.starrocks.qe.feedback.guide.StreamingAggTuningGuide;
import com.starrocks.qe.feedback.skeleton.ScanNode;
import com.starrocks.qe.feedback.skeleton.SkeletonBuilder;
import com.starrocks.qe.feedback.skeleton.SkeletonNode;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.plan.DistributedEnvPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.opentest4j.AssertionFailedError;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Tests of PlanTuningAdvisor with real plans: the single-thread semantics that must be preserved, the equals contract of
 * the cache key, and concurrent workloads of queries that have one plan shape (same hash) but different range literals and
 * IN lists, which used to leave zombie mappings in the Caffeine cache and make putTuningGuides spin forever.
 */
class PlanTuningAdvisorConcurrencyTest extends DistributedEnvPlanTestBase {

    private static final String[] MODES = {"AIR", "MAIL", "SHIP", "RAIL", "TRUCK", "FOB", "REG AIR"};
    private static final String[] FLAGS = {"'A', 'R'", "'A', 'N'", "'R', 'N'", "'A', 'R', 'N'"};

    @BeforeAll
    public static void beforeClass() throws Exception {
        // the async profiler is of no use here, and it has crashed the forked test JVM on start up
        Config.proc_profile_cpu_enable = false;
        Config.proc_profile_mem_enable = false;
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    /**
     * A query with a range predicate on l_partkey plus two IN lists. Every shape number gives different IN lists, which are
     * residual predicates, so they are different patterns, but all of them have the same plan shape and hash code.
     */
    static String sql(int shape, long lo, long hi) {
        int flag = shape % FLAGS.length;
        int maskIdx = shape / FLAGS.length;
        // enumerate the subsets of MODES that have at least 2 elements (a 1-element IN becomes "=")
        int seen = 0;
        int mask = 0;
        for (int m = 1; m < (1 << MODES.length); m++) {
            if (Integer.bitCount(m) >= 2) {
                if (seen == maskIdx) {
                    mask = m;
                    break;
                }
                seen++;
            }
        }
        Assertions.assertNotEquals(0, mask, "shape out of range: " + shape);
        List<String> modes = new ArrayList<>();
        for (int i = 0; i < MODES.length; i++) {
            if ((mask & (1 << i)) != 0) {
                modes.add("'" + MODES[i] + "'");
            }
        }
        return "select l_shipmode, count(*) from lineitem where l_partkey >= " + lo + " and l_partkey <= " + hi
                + " and l_shipmode in (" + String.join(", ", modes) + ") and l_returnflag in (" + FLAGS[flag] + ")"
                + " group by l_shipmode";
    }

    /** A planned query. The physical plan is kept so that a fresh skeleton and guides can be built for every put. */
    static final class Planned {
        final String sql;
        final OptExpression root;
        final Map<Integer, NodeExecStats> stats;

        Planned(String sql, OptExpression root, Map<Integer, NodeExecStats> stats) {
            this.sql = sql;
            this.root = root;
            this.stats = stats;
        }

        SkeletonNode skeleton() {
            return new SkeletonBuilder(stats).buildSkeleton(root).first;
        }

        /** The same steps as StmtExecutor.analyzePlanWithExecStats: a skeleton from the plan and stats, then the analyzers. */
        Pair<SkeletonNode, OperatorTuningGuides> skeletonAndGuides() {
            Pair<SkeletonNode, Map<Integer, SkeletonNode>> pair = new SkeletonBuilder(stats).buildSkeleton(root);
            OperatorTuningGuides guides = new OperatorTuningGuides(UUID.randomUUID(), 50);
            PlanTuningAnalyzer.getInstance().analyzePlan(root, pair.second, guides);
            return Pair.create(pair.first, guides);
        }
    }

    private static void collectAggStats(OptExpression expr, Map<Integer, NodeExecStats> stats) {
        if (expr.getOp() instanceof PhysicalHashAggregateOperator agg) {
            int id = agg.getPlanNodeId();
            if (agg.getType().isAnyGlobal()) {
                stats.put(id, new NodeExecStats(id, 500000, 7, 0, 0, 0));
            } else {
                stats.put(id, new NodeExecStats(id, 3000000000L, 2000000000L, 0, 0, 0));
            }
        }
        for (OptExpression in : expr.getInputs()) {
            collectAggStats(in, stats);
        }
    }

    private Planned plan(String sql) throws Exception {
        ExecPlan execPlan = getExecPlan(sql);
        OptExpression root = execPlan.getPhysicalPlan();
        Map<Integer, NodeExecStats> stats = Maps.newHashMap();
        collectAggStats(root, stats);
        return new Planned(sql, root, stats);
    }

    private Planned plan(int shape, long lo, long hi) throws Exception {
        return plan(sql(shape, lo, hi));
    }

    /** Non-empty guides on operator opId. Guides are equal when they are on the same operator. */
    private static OperatorTuningGuides guides(int opId) {
        return guides(opId, UUID.randomUUID());
    }

    private static OperatorTuningGuides guides(int opId, UUID queryId) {
        OperatorTuningGuides guides = new OperatorTuningGuides(queryId, 50);
        guides.addTuningGuide(opId, opId, new StreamingAggTuningGuide(null));
        return guides;
    }

    private static OperatorTuningGuides put(PlanTuningAdvisor advisor, Planned p, OperatorTuningGuides guides) {
        advisor.putTuningGuides(p.sql, p.skeleton(), guides);
        return guides;
    }

    private static OperatorTuningGuides get(PlanTuningAdvisor advisor, Planned p) {
        return advisor.getTuningGuides(p.sql, p.skeleton());
    }

    // ---------------------------------------------------------------------------------------------------------------
    // single thread semantics, the behaviour that must not change
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void testEnclosedProbeHitsAndOthersMiss() throws Exception {
        PlanTuningAdvisor advisor = new PlanTuningAdvisor(8);
        Planned a = plan(0, 1000, 2000);
        // empty guides are ignored
        advisor.putTuningGuides(a.sql, a.skeleton(), new OperatorTuningGuides(UUID.randomUUID(), 50));
        Assertions.assertEquals(0, advisor.getAdvisorSize());

        OperatorTuningGuides guides = put(advisor, a, guides(1));
        Assertions.assertEquals(1, advisor.getAdvisorSize());
        Assertions.assertSame(guides, get(advisor, a));
        Assertions.assertSame(guides, get(advisor, plan(0, 1200, 1800)), "an enclosed probe hits");
        Assertions.assertNull(get(advisor, plan(0, 2500, 2800)), "a disjoint probe misses");
        Assertions.assertNull(get(advisor, plan(0, 1500, 3000)), "a partially overlapping probe misses");
        // the probe and the stored key are equal, but coverage only goes one way
        Assertions.assertNull(get(advisor, plan(0, 500, 5000)), "a wider probe misses");
        // other IN lists never match
        Assertions.assertNull(get(advisor, plan(1, 1200, 1800)));
        advisor.checkInvariants();
    }

    @Test
    void testMergeOfEqualGuides() throws Exception {
        PlanTuningAdvisor advisor = new PlanTuningAdvisor(8);
        Planned a = plan(0, 1000, 2000);
        Planned b = plan(0, 2500, 2800);
        OperatorTuningGuides guidesA = put(advisor, a, guides(1));
        UUID idA = guidesA.getOriginalQueryId();
        Assertions.assertNull(get(advisor, b));

        // same shape, equal guides: merged into the existing entry
        OperatorTuningGuides guidesB = put(advisor, b, guides(1));
        Assertions.assertEquals(1, advisor.getAdvisorSize());
        Assertions.assertSame(guidesA, get(advisor, b), "a probe in the merged range hits");
        // the merged ranges are the union: disjoint ranges stay separate, an overlapping one is joined
        Assertions.assertSame(guidesA, get(advisor, plan(0, 1000, 2000)));
        Assertions.assertSame(guidesA, get(advisor, plan(0, 2600, 2700)));
        Assertions.assertNull(get(advisor, plan(0, 2100, 2400)), "the gap between the ranges is not covered");
        Assertions.assertNull(get(advisor, plan(0, 1000, 2800)));
        put(advisor, plan(0, 1800, 2300), guides(1));
        Assertions.assertEquals(1, advisor.getAdvisorSize());
        Assertions.assertSame(guidesA, get(advisor, plan(0, 1000, 2300)));
        Assertions.assertNull(get(advisor, plan(0, 900, 2300)));
        Assertions.assertNull(get(advisor, plan(0, 1000, 2400)));

        // the entry keeps the first query id and sql
        List<List<String>> rows = advisor.getShowResult();
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(8, rows.get(0).size());
        Assertions.assertEquals(idA.toString(), rows.get(0).get(0));
        Assertions.assertEquals(a.sql, rows.get(0).get(1));
        Assertions.assertNotEquals(guidesB.getOriginalQueryId().toString(), rows.get(0).get(0));
        advisor.checkInvariants();
    }

    @Test
    void testDifferentGuidesAreDroppedAndDifferentInListIsAnotherEntry() throws Exception {
        PlanTuningAdvisor advisor = new PlanTuningAdvisor(8);
        OperatorTuningGuides guidesA = put(advisor, plan(0, 1000, 2000), guides(1));
        // same shape, different guides: dropped, and the range is not merged
        put(advisor, plan(0, 2500, 2800), guides(2));
        Assertions.assertEquals(1, advisor.getAdvisorSize());
        Assertions.assertNull(get(advisor, plan(0, 2500, 2800)));
        Assertions.assertSame(guidesA, get(advisor, plan(0, 1000, 2000)));

        // another IN list is another pattern
        OperatorTuningGuides guidesC = put(advisor, plan(1, 1000, 2000), guides(1));
        Assertions.assertEquals(2, advisor.getAdvisorSize());
        Assertions.assertSame(guidesC, get(advisor, plan(1, 1200, 1800)));
        Assertions.assertSame(guidesA, get(advisor, plan(0, 1200, 1800)));
        advisor.checkInvariants();
    }

    @Test
    void testBlacklist() throws Exception {
        PlanTuningAdvisor advisor = new PlanTuningAdvisor(8);
        Planned a = plan(0, 1000, 2000);
        OperatorTuningGuides guidesA = put(advisor, a, guides(1));

        // an unknown id changes nothing
        advisor.deleteTuningGuides(UUID.randomUUID(), true);
        Assertions.assertEquals(1, advisor.getAdvisorSize());
        Assertions.assertEquals(0, advisor.blacklistSize());

        // without the blacklist the pattern can be added again
        advisor.deleteTuningGuides(guidesA.getOriginalQueryId(), false);
        Assertions.assertEquals(0, advisor.getAdvisorSize());
        Assertions.assertEquals(0, advisor.blacklistSize());
        OperatorTuningGuides again = put(advisor, a, guides(1));
        Assertions.assertEquals(1, advisor.getAdvisorSize());

        // with the blacklist it is ignored, and so is every pattern it encloses
        advisor.deleteTuningGuides(again.getOriginalQueryId(), true);
        Assertions.assertEquals(0, advisor.getAdvisorSize());
        Assertions.assertEquals(1, advisor.blacklistSize());
        put(advisor, a, guides(1));
        put(advisor, plan(0, 1200, 1800), guides(1));
        Assertions.assertEquals(0, advisor.getAdvisorSize());
        // a wider pattern is accepted, and blacklisting it adds a second blacklist entry
        OperatorTuningGuides wider = put(advisor, plan(0, 500, 5000), guides(1));
        Assertions.assertEquals(1, advisor.getAdvisorSize());
        advisor.deleteTuningGuides(wider.getOriginalQueryId(), true);
        Assertions.assertEquals(2, advisor.blacklistSize());
        // another IN list is not blacklisted
        put(advisor, plan(1, 1000, 2000), guides(1));
        Assertions.assertEquals(1, advisor.getAdvisorSize());
        advisor.checkInvariants();

        // TRUNCATE PLAN ADVISOR clears the blacklist too
        advisor.clearAllAdvisor();
        Assertions.assertEquals(0, advisor.getAdvisorSize());
        Assertions.assertEquals(0, advisor.blacklistSize());
        Assertions.assertTrue(advisor.getShowResult().isEmpty());
        put(advisor, a, guides(1));
        Assertions.assertEquals(1, advisor.getAdvisorSize());
        advisor.checkInvariants();
    }

    /**
     * Guides of two entries can come from one query (a query with two plan shapes), and deleting the query must remove
     * every one of its entries and, with the blacklist, blacklist every one of the patterns.
     */
    @Test
    void testDeleteRemovesEveryEntryOfOneQuery() throws Exception {
        Planned a = plan(0, 1000, 2000);
        Planned b = plan(1, 1000, 2000);
        UUID queryId = UUID.randomUUID();

        PlanTuningAdvisor advisor = new PlanTuningAdvisor(8);
        put(advisor, a, guides(1, queryId));
        put(advisor, b, guides(1, queryId));
        Assertions.assertEquals(2, advisor.getAdvisorSize());
        advisor.deleteTuningGuides(queryId, false);
        Assertions.assertEquals(0, advisor.getAdvisorSize());
        Assertions.assertEquals(0, advisor.blacklistSize());
        advisor.checkInvariants();

        put(advisor, a, guides(1, queryId));
        put(advisor, b, guides(1, queryId));
        advisor.deleteTuningGuides(queryId, true);
        Assertions.assertEquals(0, advisor.getAdvisorSize());
        Assertions.assertEquals(2, advisor.blacklistSize(), "both patterns are blacklisted");
        Assertions.assertNull(get(advisor, a));
        put(advisor, b, guides(1));
        Assertions.assertEquals(0, advisor.getAdvisorSize(), "a blacklisted pattern is ignored");
        advisor.checkInvariants();
    }

    private static PlanTuningCacheKey key(Planned p) {
        return new PlanTuningCacheKey(p.sql, p.skeleton());
    }

    /** Widens the scan ranges of target by the ones of source, as putTuningGuides does for equal guides. */
    private static void merge(PlanTuningCacheKey target, PlanTuningCacheKey source) {
        for (int i = 0; i < target.getSkeletonNodes().size(); i++) {
            if (target.getSkeletonNodes().get(i) instanceof ScanNode scanNode) {
                scanNode.mergeColumnRangePredicate((ScanNode) source.getSkeletonNodes().get(i));
            }
        }
    }

    /** Asserts that equals is reflexive, symmetric, transitive and consistent with hashCode, and returns its matrix. */
    private static boolean[][] assertEqualsContract(List<PlanTuningCacheKey> keys) {
        int n = keys.size();
        boolean[][] equal = new boolean[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                equal[i][j] = keys.get(i).equals(keys.get(j));
            }
        }
        for (int i = 0; i < n; i++) {
            Assertions.assertTrue(equal[i][i], "reflexive: " + i);
            for (int j = 0; j < n; j++) {
                Assertions.assertEquals(equal[i][j], equal[j][i], "symmetric: " + i + ", " + j);
                if (!equal[i][j]) {
                    continue;
                }
                Assertions.assertEquals(keys.get(i).hashCode(), keys.get(j).hashCode(), "hashCode: " + i + ", " + j);
                for (int k = 0; k < n; k++) {
                    if (equal[j][k]) {
                        Assertions.assertTrue(equal[i][k], "transitive: " + i + ", " + j + ", " + k);
                    }
                }
            }
        }
        return equal;
    }

    /**
     * The cache key is a Caffeine key, so equals must be an equivalence relation consistent with hashCode, and neither
     * may change when the ranges of a stored key are widened in place. An asymmetric equals made Caffeine leave dead
     * mappings behind on eviction, and later puts spun forever on them.
     */
    @Test
    void testCacheKeyEqualsContract() throws Exception {
        long[] lows = {1000, 1200, 500, 2500};
        long[] highs = {2000, 1800, 5000, 2800};
        List<PlanTuningCacheKey> keys = new ArrayList<>();
        for (int shape = 0; shape < 3; shape++) {
            for (int r = 0; r < lows.length; r++) {
                keys.add(key(plan(shape, lows[r], highs[r])));
            }
        }
        boolean[][] equal = assertEqualsContract(keys);
        // keys of one shape are equal whatever their ranges, keys of other IN lists are not
        for (int i = 0; i < keys.size(); i++) {
            for (int j = 0; j < keys.size(); j++) {
                Assertions.assertEquals(i / lows.length == j / lows.length, equal[i][j], "keys " + i + " and " + j);
            }
        }

        // a merge widens the ranges of a stored key in place, which must not change its equality or hash code
        int[] hashCodes = keys.stream().mapToInt(PlanTuningCacheKey::hashCode).toArray();
        for (int shape = 0; shape < 3; shape++) {
            int base = shape * lows.length;
            merge(keys.get(base), keys.get(base + 2));
            merge(keys.get(base + 1), keys.get(base + 3));
        }
        Assertions.assertArrayEquals(hashCodes, keys.stream().mapToInt(PlanTuningCacheKey::hashCode).toArray());
        Assertions.assertArrayEquals(equal, assertEqualsContract(keys));
        // the widened key covers the range merged into it, but still not the other way around
        Assertions.assertTrue(keys.get(2).isCoveredBy(keys.get(0)));
        Assertions.assertFalse(keys.get(0).isCoveredBy(keys.get(3)));
    }

    @Test
    void testShowResultAndCapacity() throws Exception {
        PlanTuningAdvisor advisor = new PlanTuningAdvisor(2);
        List<Planned> planned = new ArrayList<>();
        List<OperatorTuningGuides> all = new ArrayList<>();
        for (int shape = 0; shape < 3; shape++) {
            planned.add(plan(shape, 1000, 2000));
            all.add(put(advisor, planned.get(shape), guides(1)));
        }
        // the cache evicts down to its capacity, which entry goes is up to the Caffeine policy
        advisor.checkInvariants();
        Assertions.assertEquals(2, advisor.getAdvisorSize());
        Set<String> liveIds = new HashSet<>();
        for (int shape = 0; shape < 3; shape++) {
            OperatorTuningGuides found = get(advisor, planned.get(shape));
            if (found != null) {
                Assertions.assertSame(all.get(shape), found, "a shape finds only its own guides");
                liveIds.add(found.getOriginalQueryId().toString());
            }
        }
        Assertions.assertEquals(2, liveIds.size());
        List<List<String>> rows = advisor.getShowResult();
        Assertions.assertEquals(2, rows.size());
        Set<String> rowIds = new HashSet<>();
        for (List<String> row : rows) {
            Assertions.assertEquals(8, row.size());
            rowIds.add(row.get(0));
        }
        Assertions.assertEquals(liveIds, rowIds);
        advisor.checkInvariants();
    }

    @Test
    void testOptimizedQueryRecords() {
        PlanTuningAdvisor advisor = new PlanTuningAdvisor(8);
        OperatorTuningGuides guides = guides(1);
        UUID first = UUID.randomUUID();
        advisor.addOptimizedQueryRecord(first, new OperatorTuningGuides.OptimizedRecord(guides, List.of()));
        Assertions.assertSame(guides, advisor.getOperatorTuningGuides(first));
        Assertions.assertNotNull(advisor.getOptimizedRecord(first));
        Assertions.assertNull(advisor.getOptimizedRecord(null));
        advisor.removeOptimizedQueryRecord(first);
        Assertions.assertNull(advisor.getOptimizedRecord(first));
        Assertions.assertNull(advisor.getOperatorTuningGuides(first));
        Assertions.assertNull(advisor.getOperatorTuningGuides(null));
        advisor.removeOptimizedQueryRecord(null);

        // records of queries planned outside StmtExecutor.execute are never removed, they must not pile up
        for (int i = 0; i < 12_000; i++) {
            advisor.addOptimizedQueryRecord(UUID.randomUUID(), new OperatorTuningGuides.OptimizedRecord(guides, List.of()));
        }
        long records = advisor.optimizedQueryRecordCount();
        Assertions.assertTrue(records > 0 && records <= 10_000, "records: " + records);

        // deleting the guides of a query drops its record, TRUNCATE drops all of them
        UUID id = UUID.randomUUID();
        advisor.addOptimizedQueryRecord(id, new OperatorTuningGuides.OptimizedRecord(guides, List.of()));
        advisor.deleteTuningGuides(id, false);
        Assertions.assertNull(advisor.getOptimizedRecord(id));
        UUID other = UUID.randomUUID();
        advisor.addOptimizedQueryRecord(other, new OperatorTuningGuides.OptimizedRecord(guides, List.of()));
        advisor.clearAllAdvisor();
        Assertions.assertNull(advisor.getOptimizedRecord(other));
        Assertions.assertEquals(0, advisor.optimizedQueryRecordCount());
    }

    // ---------------------------------------------------------------------------------------------------------------
    // concurrency
    // ---------------------------------------------------------------------------------------------------------------

    /** What a runWorkload did, summed over all the threads. */
    private static final class WorkloadResult {
        final int puts;
        final int gets;
        final int hits;

        WorkloadResult(int puts, int gets, int hits) {
            this.puts = puts;
            this.gets = gets;
            this.hits = hits;
        }
    }

    /**
     * Every thread does putTuningGuides for 67% of its iterations, getTuningGuides for the rest, and deleteTuningGuides
     * with the blacklist of a recently put query for deletePercent of them, all with fresh skeletons and guides built from
     * the pre-planned queries.
     */
    private static WorkloadResult runWorkload(PlanTuningAdvisor advisor, List<Planned> pool, int threads, int iterations,
                                              long seed, int deletePercent) {
        ConcurrentLinkedQueue<UUID> recent = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        AtomicInteger puts = new AtomicInteger();
        AtomicInteger gets = new AtomicInteger();
        AtomicInteger hits = new AtomicInteger();
        AtomicInteger done = new AtomicInteger();
        // filled by the timed body in another thread, and read on a timeout
        List<Thread> workers = new CopyOnWriteArrayList<>();
        runWithTimeout(workers, () -> {
            for (int t = 0; t < threads; t++) {
                final int idx = t;
                Thread worker = new Thread(() -> {
                    Random rnd = new Random(seed + idx);
                    for (int n = 0; n < iterations; n++, done.incrementAndGet()) {
                        try {
                            Planned p = pool.get(rnd.nextInt(pool.size()));
                            int dice = rnd.nextInt(100);
                            if (dice < 67) {
                                Pair<SkeletonNode, OperatorTuningGuides> pair = p.skeletonAndGuides();
                                recent.add(pair.second.getOriginalQueryId());
                                advisor.putTuningGuides(p.sql, pair.first, pair.second);
                                puts.incrementAndGet();
                            } else if (dice < 100 - deletePercent) {
                                gets.incrementAndGet();
                                if (advisor.getTuningGuides(p.sql, p.skeleton()) != null) {
                                    hits.incrementAndGet();
                                }
                            } else {
                                UUID id = recent.poll();
                                if (id != null) {
                                    advisor.deleteTuningGuides(id, true);
                                }
                            }
                        } catch (Throwable e) {
                            errors.add(e);
                        }
                    }
                }, "plan-advisor-worker-" + idx);
                worker.setDaemon(true);
                workers.add(worker);
                worker.start();
            }
            for (Thread worker : workers) {
                worker.join();
            }
        }, () -> done.get() + " of " + threads * iterations + " operations done");
        Assertions.assertTrue(errors.isEmpty(), "worker failed: " + errors.peek());
        Assertions.assertTrue(puts.get() > 0);
        return new WorkloadResult(puts.get(), gets.get(), hits.get());
    }

    /**
     * Runs body with a 60 seconds timeout. On a timeout the failure tells how far the workers got and where each live worker
     * is, which tells a spinning worker from a slow one.
     */
    private static void runWithTimeout(List<Thread> workers, Executable body, Supplier<String> progress) {
        try {
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(60), body);
        } catch (AssertionFailedError e) {
            StringBuilder sb = new StringBuilder(e.getMessage()).append(", ").append(progress.get());
            for (Thread worker : workers) {
                if (worker.isAlive()) {
                    sb.append('\n').append(worker.getName()).append(' ').append(worker.getState());
                    for (StackTraceElement frame : worker.getStackTrace()) {
                        sb.append("\n    at ").append(frame);
                    }
                }
            }
            throw new AssertionFailedError(sb.toString(), e);
        }
    }

    /**
     * One IN list, many range literals, so every query is of one shape: the concurrent puts must merge into one entry, and
     * that entry must serve every probe inside the union of the ranges that were put. There are no deletes: a delete
     * empties the entry, and the workload would spend its time building it up again.
     */
    @Test
    void testConcurrentSameShape() throws Exception {
        Random rnd = new Random(7);
        List<Planned> pool = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            long lo = rnd.nextInt(1000);
            pool.add(plan(0, lo, lo + rnd.nextInt(300)));
        }
        PlanTuningAdvisor advisor = new PlanTuningAdvisor(4);
        WorkloadResult result = runWorkload(advisor, pool, 24, 1000, 100, 0);
        advisor.checkInvariants();
        // one shape is one entry, however the puts interleave
        Assertions.assertEquals(1, advisor.getAdvisorSize());

        // The first puts create the entry and widen it until it covers the 40 ranges, which takes about 40 * ln(40) = 150
        // puts, so a few hundred of the 24000 operations. From then on every get hits (4 of 7982 gets missed in a run), so
        // the floor of 95% leaves a wide margin. An entry that is not widened covers only the range of its first put, and a
        // blacklisted shape never hits, so either of them would be far below the floor.
        Assertions.assertTrue(result.gets > 5000, "gets: " + result.gets);
        Assertions.assertTrue(result.hits >= 0.95 * result.gets, "hits " + result.hits + " of " + result.gets + " gets");

        // The ranges were merged: every pool query was put, so each of them is inside the union now.
        for (Planned p : pool) {
            Assertions.assertNotNull(get(advisor, p), "a probe in the merged range hits: " + p.sql);
        }
        Assertions.assertNull(get(advisor, plan(0, 5000, 6000)), "a probe outside the union misses");
        Assertions.assertNull(get(advisor, plan(0, 0, 5000)), "a probe wider than the union misses");
        Assertions.assertEquals(1, advisor.getAdvisorSize());
        Assertions.assertEquals(1, advisor.getShowResult().size());
        advisor.checkInvariants();
    }

    /**
     * More shapes (distinct IN lists, one hash code) than the capacity: the cache is full and evicts all the time. All the
     * keys share one ConcurrentHashMap bin, so every lookup walks the bin and the puts are serialized on its lock, and the
     * workload is kept small for that. This is the setup in which an asymmetric equals made an eviction remove the wrong
     * mapping and leave a dead one behind, on which every later put of an equal key spun forever.
     */
    @Test
    void testConcurrentEvictionChurn() throws Exception {
        int capacity = 300;
        Random rnd = new Random(11);
        List<Planned> pool = new ArrayList<>();
        for (int shape = 0; shape < capacity + 100; shape++) {
            long lo = rnd.nextInt(1000);
            pool.add(plan(shape, lo, lo + rnd.nextInt(300)));
        }
        PlanTuningAdvisor advisor = new PlanTuningAdvisor(capacity);

        // 400 shapes into 300 places: the cache evicts down to its capacity, and every shape left finds its own guides
        List<OperatorTuningGuides> first = new ArrayList<>();
        for (Planned p : pool) {
            first.add(put(advisor, p, guides(1)));
        }
        advisor.checkInvariants();
        Assertions.assertEquals(capacity, advisor.getAdvisorSize());
        int found = 0;
        for (int i = 0; i < pool.size(); i++) {
            OperatorTuningGuides guides = get(advisor, pool.get(i));
            if (guides != null) {
                Assertions.assertSame(first.get(i), guides, "a shape finds only its own guides");
                found++;
            }
        }
        Assertions.assertEquals(capacity, found);

        runWorkload(advisor, pool, 16, 250, 200, 3);
        advisor.checkInvariants();
        Assertions.assertTrue(advisor.getAdvisorSize() <= capacity);
        Assertions.assertTrue(advisor.getAdvisorSize() > 0);
        Assertions.assertEquals((int) advisor.getAdvisorSize(), advisor.getShowResult().size());
    }

    /**
     * A barrier that releases all the threads within a few nanoseconds. A CyclicBarrier parks the threads and wakes them tens
     * of microseconds apart, which is long enough to hide the race that testSimultaneousPutsOfOneShapeMakeOneEntry looks
     * for: it took hundreds of rounds to show instead of one to a few.
     */
    private static final class SpinBarrier {
        private final int parties;
        private final AtomicInteger arrived = new AtomicInteger();
        private volatile int generation;

        SpinBarrier(int parties) {
            this.parties = parties;
        }

        void await() throws InterruptedException {
            int entered = generation;
            if (arrived.incrementAndGet() == parties) {
                arrived.set(0);
                generation = entered + 1;
                return;
            }
            for (int spins = 0; generation == entered; spins++) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                if (spins < 10_000) {
                    Thread.onSpinWait();
                } else {
                    Thread.yield();
                }
            }
        }
    }

    /**
     * The first-insert race: several threads put queries of one shape into an empty advisor at the same instant, round after
     * round. Finding the match and adding or merging the entry must be one atomic step, so every round has to end with
     * exactly one entry. If they are separate steps, two threads both find no entry and both add one. The other workloads
     * cannot see that, because only the first put of a shape can lose this race.
     */
    @Test
    void testSimultaneousPutsOfOneShapeMakeOneEntry() throws Exception {
        int threads = Math.max(4, Math.min(16, Runtime.getRuntime().availableProcessors()));
        int rounds = 2000;
        Random rnd = new Random(3);
        List<Planned> pool = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            long lo = rnd.nextInt(1000);
            pool.add(plan(0, lo, lo + rnd.nextInt(300)));
        }

        AtomicReference<PlanTuningAdvisor> current = new AtomicReference<>(new PlanTuningAdvisor(8));
        AtomicReference<String> failure = new AtomicReference<>();
        AtomicInteger roundsDone = new AtomicInteger();
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        SpinBarrier start = new SpinBarrier(threads);
        SpinBarrier end = new SpinBarrier(threads);
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int idx = t;
            Planned p = pool.get(t);
            Thread worker = new Thread(() -> {
                try {
                    for (int r = 0; r < rounds; r++) {
                        Pair<SkeletonNode, OperatorTuningGuides> pair = p.skeletonAndGuides();
                        start.await();
                        if (failure.get() != null) {
                            // thread 0 set it before it arrived at this barrier, so all the threads stop in the same round
                            return;
                        }
                        current.get().putTuningGuides(p.sql, pair.first, pair.second);
                        end.await();
                        if (idx == 0) {
                            // all the puts of the round are done: thread 0 checks them and sets up the next round
                            PlanTuningAdvisor advisor = current.get();
                            if (advisor.getAdvisorSize() != 1) {
                                failure.set("round " + r + ": " + advisor.getAdvisorSize() + " entries for one shape, "
                                        + "the match and the add of putTuningGuides are not atomic");
                            }
                            advisor.checkInvariants();
                            current.set(new PlanTuningAdvisor(8));
                            roundsDone.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    // the test timed out or another worker failed
                } catch (Throwable e) {
                    errors.add(e);
                    workers.forEach(Thread::interrupt);
                }
            }, "plan-advisor-round-worker-" + idx);
            worker.setDaemon(true);
            workers.add(worker);
        }
        try {
            runWithTimeout(workers, () -> {
                for (Thread worker : workers) {
                    worker.start();
                }
                for (Thread worker : workers) {
                    worker.join();
                }
            }, () -> roundsDone.get() + " of " + rounds + " rounds done");
        } finally {
            workers.forEach(Thread::interrupt);
        }
        Assertions.assertTrue(errors.isEmpty(), "worker failed: " + errors.peek());
        Assertions.assertNull(failure.get(), failure.get());
    }
}
