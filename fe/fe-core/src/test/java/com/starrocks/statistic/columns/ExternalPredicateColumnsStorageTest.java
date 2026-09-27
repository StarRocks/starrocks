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
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.thrift.TResultBatch;
import org.apache.logging.log4j.util.Strings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

class ExternalPredicateColumnsStorageTest extends PlanTestBase {

    private static int feName;

    @BeforeAll
    public static void beforeAll() throws Exception {
        StatisticsMetaManager m = new StatisticsMetaManager();
        m.createStatisticsTablesForTest();
        feName = GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf().getFid();
    }

    @Test
    public void lifecycle() {
        // create table
        TableKeeper keeper = ExternalPredicateColumnsStorage.createKeeper();
        keeper.run();
        Assertions.assertNotNull(starRocksAssert.getTable(ExternalPredicateColumnsStorage.DATABASE_NAME,
                ExternalPredicateColumnsStorage.TABLE_NAME));
        ConnectContext.ScopeGuard guard = connectContext.bindScope();

        SimpleExecutor repo = Mockito.mock(SimpleExecutor.class);
        ExternalPredicateColumnsStorage instance = new ExternalPredicateColumnsStorage(repo);

        // restore
        LocalDateTime lastPersist = LocalDateTime.parse("2024-11-10T01:00:00");
        Assertions.assertFalse(instance.isRestored());
        instance.restore();
        instance.finishRestore();
        instance.finishRestore(lastPersist);
        Assertions.assertTrue(instance.isRestored());
        Mockito.verify(repo).executeDQL(
                "SELECT fe_id, table_uuid, column_name, catalog_name, db_name, table_name, usage, last_used, created "
                        + "FROM _statistics_.external_predicate_columns WHERE true  AND fe_id = '" + feName + "'");

        // persist
        ExternalColumnUsage usage1 = new ExternalColumnUsage("hash1", "iceberg_catalog", "db1", "t1", "c1",
                ColumnUsage.UseCase.PREDICATE);
        usage1.setCreated(LocalDateTime.parse("2024-11-20T01:02:03"));
        usage1.setLastUsed(LocalDateTime.parse("2024-11-20T01:02:03"));
        instance.persist(List.of(usage1));

        Mockito.verify(repo).executeDML(String.format(
                "INSERT INTO _statistics_.external_predicate_columns(fe_id, table_uuid, column_name_hash, "
                        + "catalog_name, db_name, table_name, column_name, usage, last_used ) VALUES "
                        + "('%s', 'hash1', '%s', 'iceberg_catalog', 'db1', 't1', 'c1', 'predicate', "
                        + "'2024-11-20 01:02:03')",
                feName, usage1.getColumnNameHash()));

        // query
        instance.queryGlobalState("hash1", EnumSet.noneOf(ColumnUsage.UseCase.class));
        Mockito.verify(repo).executeDQL(
                "SELECT fe_id, table_uuid, column_name, catalog_name, db_name, table_name, usage, last_used, created "
                        + "FROM _statistics_.external_predicate_columns WHERE true  AND `table_uuid` = 'hash1'");

        // vacuum
        instance.vacuum(lastPersist);
        Mockito.verify(repo).executeDML("DELETE FROM _statistics_.external_predicate_columns " +
                "WHERE fe_id=" + Strings.quote(Integer.toString(feName)) +
                " AND last_used < '2024-11-10 01:00:00'");

        guard.close();
    }

    @Test
    public void testPersistEscapesSpecialCharacters() {
        // catalog/db/table/column names originate from external catalog metadata and must not be
        // trusted; a quote or backslash in one of them must not break out of the SQL string literal.
        TableKeeper keeper = ExternalPredicateColumnsStorage.createKeeper();
        keeper.run();
        ConnectContext.ScopeGuard guard = connectContext.bindScope();

        SimpleExecutor repo = Mockito.mock(SimpleExecutor.class);
        ExternalPredicateColumnsStorage instance = new ExternalPredicateColumnsStorage(repo);
        instance.restore();
        instance.finishRestore(LocalDateTime.parse("2024-11-10T01:00:00"));

        ExternalColumnUsage usage = new ExternalColumnUsage("hash1", "cat'alog", "db\\name", "t1", "c'1",
                ColumnUsage.UseCase.PREDICATE);
        usage.setLastUsed(LocalDateTime.parse("2024-11-20T01:02:03"));
        instance.persist(List.of(usage));

        Mockito.verify(repo).executeDML(String.format(
                "INSERT INTO _statistics_.external_predicate_columns(fe_id, table_uuid, column_name_hash, "
                        + "catalog_name, db_name, table_name, column_name, usage, last_used ) VALUES "
                        + "('%s', 'hash1', '%s', 'cat''alog', 'db\\\\name', 't1', 'c''1', 'predicate', "
                        + "'2024-11-20 01:02:03')",
                feName, usage.getColumnNameHash()));

        guard.close();
    }

    @Test
    public void testSerialization() {
        ExternalColumnUsage usage1 = new ExternalColumnUsage("hash1", "iceberg_catalog", "db1", "t1", "c1",
                ColumnUsage.UseCase.PREDICATE);
        ExternalColumnUsage usage2 = new ExternalColumnUsage("hash2", "iceberg_catalog", "db1", "t1", "c2",
                EnumSet.of(ColumnUsage.UseCase.PREDICATE, ColumnUsage.UseCase.JOIN));

        String row1 = String.format("{\"data\": ['%s','%s','%s','%s','%s','%s','%s','%s','%s']}",
                feName, "hash1", "c1", "iceberg_catalog", "db1", "t1", usage1.getUseCaseString(),
                DateUtils.formatDateTimeUnix(usage1.getLastUsed()), DateUtils.formatDateTimeUnix(usage1.getCreated()));
        String row2 = String.format("{\"data\": ['%s','%s','%s','%s','%s','%s','%s','%s','%s']}",
                feName, "hash2", "c2", "iceberg_catalog", "db1", "t1", usage2.getUseCaseString(),
                DateUtils.formatDateTimeUnix(usage2.getLastUsed()), DateUtils.formatDateTimeUnix(usage2.getCreated()));

        TResultBatch batch = new TResultBatch();
        batch.setRows(List.of(ByteBuffer.wrap(row1.getBytes()), ByteBuffer.wrap(row2.getBytes())));
        List<ExternalColumnUsage> result = ExternalPredicateColumnsStorage.resultToColumnUsage(List.of(batch));

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(usage1, result.get(0));
        Assertions.assertEquals(usage2, result.get(1));

        // corrupted json is skipped, valid rows around it still parse
        String corruptedJson = "{\"data\": [invalid json}";
        TResultBatch batchWithCorrupted = new TResultBatch();
        batchWithCorrupted.setRows(List.of(ByteBuffer.wrap(row1.getBytes()), ByteBuffer.wrap(corruptedJson.getBytes()),
                ByteBuffer.wrap(row2.getBytes())));
        List<ExternalColumnUsage> resultWithCorrupted =
                ExternalPredicateColumnsStorage.resultToColumnUsage(List.of(batchWithCorrupted));
        Assertions.assertEquals(2, resultWithCorrupted.size());
        Assertions.assertEquals(usage1, resultWithCorrupted.get(0));
        Assertions.assertEquals(usage2, resultWithCorrupted.get(1));
    }

    @Test
    public void testDeduplicateColumnUsages() {
        List<ExternalColumnUsage> duplicated = new ArrayList<>();

        ExternalColumnUsage usage1 = new ExternalColumnUsage("hash1", "iceberg_catalog", "db1", "t1", "c1",
                EnumSet.of(ColumnUsage.UseCase.PREDICATE, ColumnUsage.UseCase.DISTINCT));
        usage1.setCreated(LocalDateTime.parse("2025-01-01T01:02:03"));
        usage1.setLastUsed(LocalDateTime.parse("2025-01-02T01:02:03"));
        duplicated.add(usage1);

        ExternalColumnUsage usage2 = new ExternalColumnUsage("hash1", "iceberg_catalog", "db1", "t1", "c1",
                EnumSet.of(ColumnUsage.UseCase.PREDICATE, ColumnUsage.UseCase.JOIN));
        usage2.setCreated(LocalDateTime.parse("2025-01-01T01:02:03"));
        usage2.setLastUsed(LocalDateTime.parse("2025-01-02T02:02:03"));
        duplicated.add(usage2);

        ExternalColumnUsage usage3 = new ExternalColumnUsage("hash1", "iceberg_catalog", "db1", "t1", "c2",
                EnumSet.of(ColumnUsage.UseCase.GROUP_BY, ColumnUsage.UseCase.JOIN));
        usage3.setCreated(LocalDateTime.parse("2025-01-01T01:02:03"));
        usage3.setLastUsed(LocalDateTime.parse("2025-01-02T03:02:03"));
        duplicated.add(usage3);

        List<ExternalColumnUsage> result =
                ExternalPredicateColumnsStorage.getInstance().deduplicateColumnUsages(duplicated);

        Assertions.assertEquals(2, result.size());
        ExternalColumnUsage merged = result.stream().filter(x -> x.getColumnName().equals("c1")).findFirst().get();
        Assertions.assertEquals(EnumSet.of(ColumnUsage.UseCase.PREDICATE, ColumnUsage.UseCase.JOIN,
                ColumnUsage.UseCase.DISTINCT), merged.getUseCases());
        Assertions.assertEquals(LocalDateTime.parse("2025-01-02T02:02:03"), merged.getLastUsed());
    }
}
