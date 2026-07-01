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

package com.starrocks.statistic.columns;

import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.thrift.TResultBatch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

class ExternalPredicateColumnsStorageTest extends PlanTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        StatisticsMetaManager m = new StatisticsMetaManager();
        m.createStatisticsTablesForTest();
    }

    @Test
    public void testLifecycle() {
        TableKeeper keeper = ExternalPredicateColumnsStorage.createKeeper();
        keeper.run();
        Assertions.assertNotNull(starRocksAssert.getTable(ExternalPredicateColumnsStorage.DATABASE_NAME,
                ExternalPredicateColumnsStorage.TABLE_NAME));

        SimpleExecutor executor = Mockito.mock(SimpleExecutor.class);
        ExternalPredicateColumnsStorage storage = new ExternalPredicateColumnsStorage(executor);

        // persist() before finishRestore() is a no-op
        Assertions.assertFalse(storage.isRestored());
        storage.persist();
        Mockito.verify(executor, Mockito.never()).executeDML(Mockito.anyString());

        // finishRestore() with custom time marks as restored
        storage.finishRestore(LocalDateTime.parse("2024-01-01T00:00:00"));
        Assertions.assertTrue(storage.isRestored());
    }

    @Test
    public void testGetColumnsEmptyForUnknownUUID() {
        ExternalPredicateColumnsStorage storage = new ExternalPredicateColumnsStorage(
                Mockito.mock(SimpleExecutor.class));
        Assertions.assertTrue(storage.getColumns("unknown-uuid").isEmpty());
    }

    @Test
    public void testRecordNullOrEmptyUUIDIsNoOp() {
        ExternalPredicateColumnsStorage storage = new ExternalPredicateColumnsStorage(
                Mockito.mock(SimpleExecutor.class));
        storage.record(null, "col", "predicate");
        storage.record("", "col", "predicate");
        Assertions.assertTrue(storage.getColumns("").isEmpty());
    }

    @Test
    public void testQueryGlobalStateExecutorThrows() {
        SimpleExecutor executor = Mockito.mock(SimpleExecutor.class);
        Mockito.when(executor.executeDQL(Mockito.anyString())).thenThrow(new RuntimeException("timeout"));
        ExternalPredicateColumnsStorage storage = new ExternalPredicateColumnsStorage(executor);

        Set<String> result = storage.queryGlobalState("some-uuid");
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testQueryGlobalStateReturnsRows() {
        SimpleExecutor executor = Mockito.mock(SimpleExecutor.class);
        String row1 = "{\"data\": [\"col_a\"]}";
        String row2 = "{\"data\": [\"col_b\"]}";
        TResultBatch batch = new TResultBatch();
        batch.setRows(List.of(ByteBuffer.wrap(row1.getBytes()), ByteBuffer.wrap(row2.getBytes())));
        Mockito.when(executor.executeDQL(Mockito.anyString())).thenReturn(List.of(batch));

        ExternalPredicateColumnsStorage storage = new ExternalPredicateColumnsStorage(executor);
        Set<String> result = storage.queryGlobalState("table-uuid-001");

        Assertions.assertEquals(Set.of("col_a", "col_b"), result);
    }

    @Test
    public void testQueryGlobalStateSkipsMalformedRow() {
        SimpleExecutor executor = Mockito.mock(SimpleExecutor.class);
        String validRow = "{\"data\": [\"col_ok\"]}";
        String badRow = "{invalid json}";
        TResultBatch batch = new TResultBatch();
        batch.setRows(List.of(ByteBuffer.wrap(validRow.getBytes()), ByteBuffer.wrap(badRow.getBytes())));
        Mockito.when(executor.executeDQL(Mockito.anyString())).thenReturn(List.of(batch));

        ExternalPredicateColumnsStorage storage = new ExternalPredicateColumnsStorage(executor);
        Set<String> result = storage.queryGlobalState("any-uuid");

        Assertions.assertEquals(Set.of("col_ok"), result);
    }

    @Test
    public void testPersistFlushesNewEntries() {
        SimpleExecutor executor = Mockito.mock(SimpleExecutor.class);
        ExternalPredicateColumnsStorage storage = new ExternalPredicateColumnsStorage(executor);
        // set lastPersist to the past so any recorded entry is dirty
        storage.finishRestore(LocalDateTime.parse("2024-01-01T00:00:00"));

        String uuid = "table-uuid-persist";
        storage.record(uuid, "col1", "predicate");
        storage.record(uuid, "col2", "join");

        storage.persist();

        Mockito.verify(executor, Mockito.times(1)).executeDML(Mockito.argThat(
                sql -> sql.startsWith("INSERT INTO _statistics_.external_predicate_columns")));
    }

    @Test
    public void testRestoreExecutorThrows() {
        SimpleExecutor executor = Mockito.mock(SimpleExecutor.class);
        Mockito.when(executor.executeDQL(Mockito.anyString())).thenThrow(new RuntimeException("network error"));
        ExternalPredicateColumnsStorage storage = new ExternalPredicateColumnsStorage(executor);

        storage.restore(); // must not propagate the exception
        Assertions.assertTrue(storage.getColumns("any-uuid").isEmpty());
    }

    @Test
    public void testRestoreLoadsPersistedEntries() {
        SimpleExecutor executor = Mockito.mock(SimpleExecutor.class);
        String uuid = "table-uuid-restore";
        LocalDateTime lastUsed = LocalDateTime.parse("2024-06-01T12:00:00");
        String row = String.format("{\"data\": [\"%s\", \"col_r\", \"%s\"]}",
                uuid, DateUtils.formatDateTimeUnix(lastUsed));
        TResultBatch batch = new TResultBatch();
        batch.setRows(List.of(ByteBuffer.wrap(row.getBytes())));
        Mockito.when(executor.executeDQL(Mockito.anyString())).thenReturn(List.of(batch));

        ExternalPredicateColumnsStorage storage = new ExternalPredicateColumnsStorage(executor);
        storage.restore();

        Assertions.assertEquals(Set.of("col_r"), storage.getColumns(uuid));
    }

    @Test
    public void testRestoreSkipsMalformedRow() {
        SimpleExecutor executor = Mockito.mock(SimpleExecutor.class);
        String uuid = "table-uuid-malformed";
        LocalDateTime lastUsed = LocalDateTime.parse("2024-06-01T12:00:00");
        String validRow = String.format("{\"data\": [\"%s\", \"col_good\", \"%s\"]}",
                uuid, DateUtils.formatDateTimeUnix(lastUsed));
        String badRow = "{bad json}";
        TResultBatch batch = new TResultBatch();
        batch.setRows(List.of(ByteBuffer.wrap(validRow.getBytes()), ByteBuffer.wrap(badRow.getBytes())));
        Mockito.when(executor.executeDQL(Mockito.anyString())).thenReturn(List.of(batch));

        ExternalPredicateColumnsStorage storage = new ExternalPredicateColumnsStorage(executor);
        storage.restore();

        Assertions.assertEquals(Set.of("col_good"), storage.getColumns(uuid));
    }

    @Test
    public void testVacuumExecutorThrowsStillEvictsInMemory() {
        SimpleExecutor executor = Mockito.mock(SimpleExecutor.class);
        Mockito.doThrow(new RuntimeException("delete failed")).when(executor).executeDML(Mockito.anyString());
        ExternalPredicateColumnsStorage storage = new ExternalPredicateColumnsStorage(executor);

        String uuid = "uuid-vac";
        storage.record(uuid, "col1", "predicate");
        // vacuum with future cutoff should evict even if DML throws
        storage.vacuum(LocalDateTime.parse("2099-01-01T00:00:00"));

        Assertions.assertTrue(storage.getColumns(uuid).isEmpty());
    }

    @Test
    public void testVacuumEvictsOldEntriesFromMemory() {
        SimpleExecutor executor = Mockito.mock(SimpleExecutor.class);
        ExternalPredicateColumnsStorage storage = new ExternalPredicateColumnsStorage(executor);

        String uuid = "uuid-evict";
        storage.record(uuid, "col_old", "predicate");

        storage.vacuum(LocalDateTime.parse("2099-01-01T00:00:00"));

        Assertions.assertTrue(storage.getColumns(uuid).isEmpty());
    }
}
