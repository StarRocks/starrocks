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

package com.starrocks.utframe;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Records, per entry point, how often a mocked call was made and whether any of those calls ran while the
 * calling thread held an FE metadata lock ({@link LockHoldDepth#isUnderLock()}).
 *
 * <p>The lock samples are OR-accumulated: one statement reaches the same entry point along several paths, and
 * keeping only the last sample would let a later lock-free call hide an earlier locked one.
 *
 * <p>A probe made by {@link #onCurrentThread()} ignores calls from every other thread. Creating an MV hands its
 * definition to the mv-plan-cache executor, which resolves the same tables at an arbitrary moment on a thread
 * whose lock state says nothing about the statement under test.
 */
public final class LockProbe {

    private static final class Sample {
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicBoolean underLock = new AtomicBoolean();
    }

    /** The only thread whose calls are recorded, or null to record calls from any thread. */
    private final Thread owner;
    private final Map<String, Sample> samples = new ConcurrentHashMap<>();

    private LockProbe(Thread owner) {
        this.owner = owner;
    }

    /** Records calls made on the calling thread only. */
    public static LockProbe onCurrentThread() {
        return new LockProbe(Thread.currentThread());
    }

    /** Records calls made on any thread, for a call the code under test makes from a thread of its own. */
    public static LockProbe onAnyThread() {
        return new LockProbe(null);
    }

    /** Whether a call made on the current thread is recorded. */
    public boolean isProbedThread() {
        return owner == null || Thread.currentThread() == owner;
    }

    /** Counts a call to {@code key} and ORs in whether the current thread holds an FE metadata lock. */
    public void record(String key) {
        if (!isProbedThread()) {
            return;
        }
        Sample sample = samples.computeIfAbsent(key, k -> new Sample());
        sample.calls.incrementAndGet();
        if (LockHoldDepth.isUnderLock()) {
            sample.underLock.set(true);
        }
    }

    public int calls(String key) {
        Sample sample = samples.get(key);
        return sample == null ? 0 : sample.calls.get();
    }

    public boolean everUnderLock(String key) {
        Sample sample = samples.get(key);
        return sample != null && sample.underLock.get();
    }

    /** The entry points recorded so far. */
    public Set<String> keys() {
        return samples.keySet();
    }

    public boolean isEmpty() {
        return samples.isEmpty();
    }

    /** Whether any recorded entry point was ever reached under a lock. */
    public boolean anyUnderLock() {
        return samples.values().stream().anyMatch(sample -> sample.underLock.get());
    }

    public void reset() {
        samples.clear();
    }

    /** Asserts that {@code key} was reached, and never with an FE metadata lock held. */
    public void assertReachedOutsideTheLock(String key, String message) {
        Assertions.assertTrue(calls(key) > 0,
                key + " was never reached, so the lock check proves nothing: " + message);
        Assertions.assertFalse(everUnderLock(key),
                key + " ran while an FE metadata lock was held: " + message);
    }

    /** Asserts that something was recorded, and that no recorded entry point ran with an FE metadata lock held. */
    public void assertNothingUnderTheLock(String message) {
        // Without this the check below passes whenever the probe never fired.
        Assertions.assertFalse(isEmpty(), "the probe recorded no call, so it proves nothing: " + message);
        samples.forEach((key, sample) -> Assertions.assertFalse(sample.underLock.get(),
                "an FE metadata lock was held at " + key + ": " + message + "; samples: " + this));
    }

    /**
     * Mounts the probe on {@link MetadataMgr#getTable(ConnectContext, String, String, String)} for every catalog
     * but the internal one, as {@code getTable:<catalog>.<table>}.
     */
    public void probeExternalGetTable() {
        LockProbe probe = this;
        new MockUp<MetadataMgr>() {
            @Mock
            public Table getTable(Invocation invocation, ConnectContext context, String catalogName, String dbName,
                                  String tblName) {
                if (!CatalogMgr.isInternalCatalog(catalogName)) {
                    probe.record("getTable:" + catalogName + "." + tblName);
                }
                return invocation.proceed(context, catalogName, dbName, tblName);
            }
        };
    }

    /**
     * Mounts the probe on the connector calls planning makes for a scan, as {@code getTableStatistics:<table>},
     * {@code listPartitionNames:<table>} and {@code getRemoteFiles:<table>}. Mounted on MetadataMgr, the one door
     * all of them go through, so the probe does not depend on which connector the statement uses.
     */
    public void probeScanMetadata() {
        LockProbe probe = this;
        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatistics(Invocation invocation, OptimizerContext session, String catalogName,
                                                 Table table, Map<ColumnRefOperator, Column> columns,
                                                 List<PartitionKey> partitionKeys, ScalarOperator predicate,
                                                 long limit, TvrVersionRange versionRange) {
                probe.record("getTableStatistics:" + table.getName());
                return invocation.proceed(session, catalogName, table, columns, partitionKeys, predicate, limit,
                        versionRange);
            }

            @Mock
            public List<String> listPartitionNames(Invocation invocation, String catalogName, String dbName,
                                                   String tableName, ConnectorMetadataRequestContext requestContext) {
                probe.record("listPartitionNames:" + tableName);
                return invocation.proceed(catalogName, dbName, tableName, requestContext);
            }

            @Mock
            public List<RemoteFileInfo> getRemoteFiles(Invocation invocation, Table table,
                                                       GetRemoteFilesParams params) {
                probe.record("getRemoteFiles:" + table.getName());
                return invocation.proceed(table, params);
            }
        };
    }

    @Override
    public String toString() {
        Map<String, String> sorted = new TreeMap<>();
        samples.forEach((key, sample) -> sorted.put(key,
                sample.calls.get() + (sample.underLock.get() ? " calls, under lock" : " calls")));
        return sorted.toString();
    }
}
