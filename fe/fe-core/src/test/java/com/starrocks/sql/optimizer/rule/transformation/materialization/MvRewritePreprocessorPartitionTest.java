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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.common.PCellNone;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.optimizer.MvRewritePreprocessor;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.rule.mv.MaterializedViewWrapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MvRewritePreprocessorPartitionTest {
    private static final AtomicLong IDS = new AtomicLong(10000);

    private static MvPlanContext planContext() {
        return new MvPlanContext(OptExpression.create(new LogicalTreeAnchorOperator()), List.of(),
                new ColumnRefFactory(), true, false, "");
    }

    private static class PartitionChangingMV extends MaterializedView {
        private final CountDownLatch copying = new CountDownLatch(1);
        private final CountDownLatch writeAttempted = new CountDownLatch(1);
        private final AtomicBoolean pauseCopy = new AtomicBoolean();

        PartitionChangingMV() {
            super(IDS.incrementAndGet(), IDS.incrementAndGet(),
                    "partition_changing_mv", List.of(), KeysType.DUP_KEYS, new RangePartitionInfo(List.of()),
                    new RandomDistributionInfo(1), new MvRefreshScheme());
            nameToPartition = new TreeMap<>(String.CASE_INSENSITIVE_ORDER) {
                @Override
                public Set<String> keySet() {
                    Set<String> names = super.keySet();
                    return new AbstractSet<>() {
                        @Override
                        public int size() {
                            return names.size();
                        }

                        @Override
                        public Iterator<String> iterator() {
                            Iterator<String> iterator = names.iterator();
                            return new Iterator<>() {
                                @Override
                                public boolean hasNext() {
                                    return iterator.hasNext();
                                }

                                @Override
                                public String next() {
                                    String name = iterator.next();
                                    if (pauseCopy.compareAndSet(true, false)) {
                                        copying.countDown();
                                        await(writeAttempted);
                                    }
                                    return name;
                                }
                            };
                        }
                    };
                }
            };
            nameToPartition.put("p1", null);
            nameToPartition.put("p2", null);
        }

        void addPartitionName() {
            nameToPartition.put("p3", null);
        }

        void clearPartitionNames() {
            nameToPartition.clear();
        }

        void makeUnpartitioned() {
            partitionInfo = new SinglePartitionInfo();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS), "Timed out coordinating partition mutation");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Test
    public void testPartitionMutationDuringPreparation() throws Exception {
        PartitionChangingMV mv = new PartitionChangingMV();
        mv.pauseCopy.set(true);
        long unrelatedTableId = IDS.incrementAndGet();
        AtomicBoolean wroteDuringCopy = new AtomicBoolean();
        CompletableFuture<Void> writerResult = new CompletableFuture<>();
        Thread writer = new Thread(() -> {
            try {
                await(mv.copying);
                Locker locker = new Locker();
                // The snapshot must not block partition changes on other tables in the same database.
                boolean unrelatedAcquired = locker.tryLockTableWithIntensiveDbLock(mv.getDbId(), unrelatedTableId,
                        LockType.WRITE, 100, TimeUnit.MILLISECONDS);
                Assertions.assertTrue(unrelatedAcquired);
                locker.unLockTableWithIntensiveDbLock(mv.getDbId(), unrelatedTableId, LockType.WRITE);
                boolean acquired = locker.tryLockTableWithIntensiveDbLock(mv.getDbId(), mv.getId(), LockType.WRITE,
                        100, TimeUnit.MILLISECONDS);
                wroteDuringCopy.set(acquired);
                if (acquired) {
                    try {
                        mv.addPartitionName();
                    } finally {
                        locker.unLockTableWithIntensiveDbLock(mv.getDbId(), mv.getId(), LockType.WRITE);
                    }
                }
                mv.writeAttempted.countDown();
                if (!acquired) {
                    try (AutoCloseableLock ignored = new AutoCloseableLock(mv.getDbId(), mv.getId(), LockType.WRITE)) {
                        mv.addPartitionName();
                    }
                }
                writerResult.complete(null);
            } catch (Throwable t) {
                writerResult.completeExceptionally(t);
            } finally {
                mv.writeAttempted.countDown();
            }
        }, "mv-partition-writer");
        writer.start();
        try {
            Assertions.assertTrue(MvRewritePreprocessor.checkMvPartitionNamesToRefresh(
                    null, mv, PCellSortedSet.of(), planContext()));
        } finally {
            mv.copying.countDown();
            writerResult.get(10, TimeUnit.SECONDS);
            writer.join(TimeUnit.SECONDS.toMillis(10));
        }
        Assertions.assertFalse(wroteDuringCopy.get(), "Partition mutation must wait for the partition-name snapshot");
        Assertions.assertEquals(Set.of("p1", "p2", "p3"), mv.getPartitionNames());
        Assertions.assertTrue(MvRewritePreprocessor.checkMvPartitionNamesToRefresh(
                null, mv, PCellSortedSet.of(), planContext()));
    }

    @Test
    public void testPartitionCheckLockTimeout() throws Exception {
        PartitionChangingMV mv = new PartitionChangingMV();
        try (AutoCloseableLock ignored = new AutoCloseableLock(mv.getDbId(), mv.getId(), LockType.WRITE)) {
            CompletableFuture<Boolean> result = CompletableFuture.supplyAsync(() -> {
                ConnectContext traceContext = new ConnectContext();
                traceContext.getSessionVariable().setTraceLogMode("command");
                Tracers.register(traceContext);
                Tracers.init(Tracers.Mode.LOGS, Tracers.Module.MV, false, false);
                try {
                    boolean valid = MvRewritePreprocessor.checkMvPartitionNamesToRefresh(
                            null, mv, PCellSortedSet.of(), planContext());
                    Assertions.assertTrue(Tracers.printLogs().contains("skip this mv candidate"));
                    return valid;
                } finally {
                    Tracers.close();
                }
            });
            Assertions.assertFalse(result.get(10, TimeUnit.SECONDS));
        }
        Assertions.assertTrue(MvRewritePreprocessor.checkMvPartitionNamesToRefresh(
                null, mv, PCellSortedSet.of(), planContext()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testConcurrentPreparationLockTimeoutUsesQueryTracer(boolean enableTrace) throws Exception {
        PartitionChangingMV mv = new PartitionChangingMV();
        ConnectContext traceContext = new ConnectContext();
        traceContext.getSessionVariable().setTraceLogMode("command");
        Tracers.register(traceContext);
        Tracers.init(Tracers.Mode.LOGS, enableTrace ? Tracers.Module.MV : Tracers.Module.NONE, false, false);
        Tracers queryTracers = Tracers.get();
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        OptimizerContext optimizerContext = OptimizerFactory.mockContext(traceContext, columnRefFactory);
        MvRewritePreprocessor preprocessor = new MvRewritePreprocessor(traceContext, columnRefFactory,
                optimizerContext, new ColumnRefSet());
        Executor executor = Deencapsulation.getField(MvRewritePreprocessor.class, Executor.class);
        try (AutoCloseableLock ignored = new AutoCloseableLock(mv.getDbId(), mv.getId(), LockType.WRITE)) {
            CompletableFuture<Void> result = CompletableFuture.runAsync(() -> {
                Assertions.assertFalse(Tracers.isSetTraceModule(Tracers.Module.MV));
                Deencapsulation.invoke(preprocessor, "prepareMV", queryTracers, Set.of(),
                        MaterializedViewWrapper.create(mv, 0, planContext()), MvUpdateInfo.noRefresh(mv), 100L);
                Assertions.assertEquals("", Tracers.printLogs(), "The worker must not own the query trace");
            }, executor);
            result.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(optimizerContext.getCandidateMvs().isEmpty());
            String logs = Tracers.printLogs();
            if (enableTrace) {
                Assertions.assertTrue(logs.contains("Failed to lock mv partition_changing_mv"), logs);
                Assertions.assertTrue(logs.contains("skip this mv candidate"), logs);
            } else {
                Assertions.assertFalse(logs.contains("Failed to lock mv"), logs);
            }
        } finally {
            Tracers.close();
        }
        Assertions.assertTrue(MvRewritePreprocessor.checkMvPartitionNamesToRefresh(
                null, mv, PCellSortedSet.of(), planContext()));
    }

    @Test
    public void testPartitionFreshnessAndInvalidPlan() {
        PartitionChangingMV mv = new PartitionChangingMV();
        PCellSortedSet stale = PCellSortedSet.of();
        stale.add("p1", new PCellNone());
        Assertions.assertTrue(MvRewritePreprocessor.checkMvPartitionNamesToRefresh(null, mv, stale, planContext()));
        stale.add("p2", new PCellNone());
        Assertions.assertFalse(MvRewritePreprocessor.checkMvPartitionNamesToRefresh(null, mv, stale, planContext()));
        mv.clearPartitionNames();
        Assertions.assertTrue(MvRewritePreprocessor.checkMvPartitionNamesToRefresh(null, mv, stale, planContext()));
        mv.makeUnpartitioned();
        Assertions.assertFalse(MvRewritePreprocessor.checkMvPartitionNamesToRefresh(null, mv, stale, planContext()));
        Assertions.assertTrue(MvRewritePreprocessor.checkMvPartitionNamesToRefresh(
                null, mv, PCellSortedSet.of(), planContext()));
        Assertions.assertFalse(MvRewritePreprocessor.checkMvPartitionNamesToRefresh(null, mv, stale, null));
        Assertions.assertFalse(MvRewritePreprocessor.checkMvPartitionNamesToRefresh(
                null, mv, stale, new MvPlanContext(false, "invalid")));
    }
}
