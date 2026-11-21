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

package com.starrocks.sql.optimizer;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.MetadataMgr;
import mockit.Delegate;
import mockit.Expectations;
import mockit.FullVerifications;
import mockit.Injectable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class MvBaseTableMetadataPrefetcherTest {

    @Test
    void testPrefetchBaseTablesDoesNothingWhenDisabled(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession();
        session.getSessionVariable().setEnableParallelPrefetchMvBaseTableMetadata(false);

        MvBaseTableMetadataPrefetcher.prefetchBaseTables(List.of(external("t1")), session, metadataMgr,
                Long.MAX_VALUE);

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchBaseTablesDoesNothingWithoutQueryId(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession();
        session.setQueryId(null);

        MvBaseTableMetadataPrefetcher.prefetchBaseTables(List.of(external("t1")), session, metadataMgr,
                Long.MAX_VALUE);

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchBaseTablesDoesNothingWithoutBudget(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession();

        MvBaseTableMetadataPrefetcher.prefetchBaseTables(List.of(external("t1")), session, metadataMgr, 0);

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchBaseTablesDeduplicatesAndSkipsInternalCatalog(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession();
        BaseTableInfo t1 = external("t1");
        BaseTableInfo t2 = new BaseTableInfo("other", "other_db", "t2", "t2-id");
        new Expectations() {
            {
                metadataMgr.getTable(session, t1);
                result = Optional.empty();
                times = 1;
                metadataMgr.getTable(session, t2);
                result = Optional.empty();
                times = 1;
            }
        };

        MvBaseTableMetadataPrefetcher.prefetchBaseTables(List.of(t1,
                // the same table referenced by another mv
                external("t1"), t2,
                new BaseTableInfo(1L, "db", "internal_table", 2L)), session, metadataMgr, Long.MAX_VALUE);

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchBaseTablesBindsTheQueryContext(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession();
        AtomicBoolean checked = new AtomicBoolean();
        new Expectations() {
            {
                metadataMgr.getTable(session, (BaseTableInfo) any);
                result = new Delegate<Optional<Table>>() {
                    Optional<Table> getTable(ConnectContext context, BaseTableInfo baseTableInfo) {
                        // the query's context, so the lookup uses the query-level connector metadata of its id
                        Assertions.assertSame(session, ConnectContext.get());
                        // the validity check reads the iceberg cache only, so does the prefetch
                        Assertions.assertTrue(context.isOnlyReadIcebergCache());
                        checked.set(true);
                        return Optional.empty();
                    }
                };
                times = 1;
            }
        };

        Assertions.assertFalse(session.isOnlyReadIcebergCache());
        MvBaseTableMetadataPrefetcher.prefetchBaseTables(List.of(external("t1")), session, metadataMgr,
                Long.MAX_VALUE);

        Assertions.assertTrue(checked.get());
        // restored once the prefetch returns
        Assertions.assertFalse(session.isOnlyReadIcebergCache());
        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchBaseTablesIgnoresFailures(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession();
        BaseTableInfo failing = external("failing");
        BaseTableInfo t1 = external("t1");
        new Expectations() {
            {
                metadataMgr.getTable(session, failing);
                result = new RuntimeException("connector failure");
                times = 1;
                metadataMgr.getTable(session, t1);
                result = Optional.empty();
                times = 1;
            }
        };

        // best effort: a failed lookup is retried on demand by the validity check, the prefetch does not throw
        MvBaseTableMetadataPrefetcher.prefetchBaseTables(List.of(failing, t1), session, metadataMgr,
                Long.MAX_VALUE);

        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchBaseTablesLoadsConcurrently(@Injectable MetadataMgr metadataMgr) {
        ConnectContext session = newSession();
        session.getSessionVariable().setPrepareMetadataPoolSize(4);
        CountDownLatch allStarted = new CountDownLatch(4);
        AtomicBoolean overlapped = new AtomicBoolean(true);
        new Expectations() {
            {
                metadataMgr.getTable(session, (BaseTableInfo) any);
                result = new Delegate<Optional<Table>>() {
                    Optional<Table> getTable(ConnectContext context, BaseTableInfo baseTableInfo)
                            throws InterruptedException {
                        allStarted.countDown();
                        // only completes if all four lookups are running at the same time
                        if (!allStarted.await(5, TimeUnit.SECONDS)) {
                            overlapped.set(false);
                        }
                        return Optional.empty();
                    }
                };
                times = 4;
            }
        };

        MvBaseTableMetadataPrefetcher.prefetchBaseTables(
                IntStream.range(0, 4).mapToObj(i -> external("t" + i)).collect(Collectors.toList()),
                session, metadataMgr, Long.MAX_VALUE);

        Assertions.assertTrue(overlapped.get(), "base table lookups must run concurrently");
        new FullVerifications(metadataMgr) {
        };
    }

    @Test
    void testPrefetchBaseTablesStopsWaitingAfterTimeout(@Injectable MetadataMgr metadataMgr) throws Exception {
        ConnectContext session = newSession();
        BaseTableInfo slow = external("slow");
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch slowFinished = new CountDownLatch(1);
        new Expectations() {
            {
                metadataMgr.getTable(session, slow);
                result = new Delegate<Optional<Table>>() {
                    Optional<Table> getTable(ConnectContext context, BaseTableInfo baseTableInfo)
                            throws InterruptedException {
                        try {
                            // the slow lookup is not interrupted on timeout, it only finishes once released
                            Assertions.assertTrue(release.await(30, TimeUnit.SECONDS));
                            return Optional.empty();
                        } finally {
                            slowFinished.countDown();
                        }
                    }
                };
                times = 1;
            }
        };

        long start = System.nanoTime();
        try {
            MvBaseTableMetadataPrefetcher.prefetchBaseTables(List.of(slow), session, metadataMgr, 100);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            Assertions.assertTrue(elapsedMs < 10_000, "prefetch must return after the timeout, took " + elapsedMs);
            Assertions.assertEquals(1, slowFinished.getCount(), "the slow lookup must still be running");
        } finally {
            release.countDown();
        }
        Assertions.assertTrue(slowFinished.await(30, TimeUnit.SECONDS));
        new FullVerifications(metadataMgr) {
        };
    }

    private static BaseTableInfo external(String table) {
        return new BaseTableInfo("external", "db", table, table + "-id");
    }

    private static ConnectContext newSession() {
        ConnectContext session = new ConnectContext();
        session.setQueryId(UUID.randomUUID());
        session.getSessionVariable().setEnableParallelPrefetchMvBaseTableMetadata(true);
        session.getSessionVariable().setPrepareMetadataPoolSize(2);
        return session;
    }
}
