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

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.DateUtils;
import com.starrocks.load.pipe.filelist.RepoExecutor;
import com.starrocks.qe.ConnectContext;
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

class PredicateColumnsStorageTest extends PlanTestBase {

    private static String feName;

    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeAll();
        StatisticsMetaManager m = new StatisticsMetaManager();
        m.createStatisticsTablesForTest();
        feName = GlobalStateMgr.getCurrentState().getNodeMgr().getNodeName();
    }

    @Test
    public void lifecycle() {
        // create table
        TableKeeper keeper = PredicateColumnsStorage.createKeeper();
        keeper.run();
        Assertions.assertNotNull(starRocksAssert.getTable(PredicateColumnsStorage.DATABASE_NAME,
                PredicateColumnsStorage.TABLE_NAME));
        ConnectContext.ScopeGuard guard = connectContext.bindScope();

        RepoExecutor repo = Mockito.mock(RepoExecutor.class);
        PredicateColumnsStorage instance = new PredicateColumnsStorage(repo);

        // restore
        LocalDateTime lastPersist = LocalDateTime.parse("2024-11-10T01:00:00");
        Assertions.assertFalse(instance.isRestored());
        instance.restore();
        instance.finishRestore();
        instance.finishRestore(lastPersist);
        Assertions.assertTrue(instance.isRestored());
        Mockito.verify(repo).executeDQL("SELECT fe_id, db_id,  table_id,  column_id, usage, last_used, created " +
                "FROM _statistics_.predicate_columns WHERE true  AND fe_id = '" + feName + "'"
        );

        // persist
        Database db = starRocksAssert.getDb("test");
        Table t0 = starRocksAssert.getTable("test", "t0");
        Column v1 = t0.getColumn("v1");
        ColumnFullId columnFullId = ColumnFullId.create(db, t0, v1);
        ColumnUsage usage1 =
                new ColumnUsage(columnFullId, TableName.fromString("t0"), ColumnUsage.UseCase.PREDICATE);
        usage1.setCreated(LocalDateTime.parse("2024-11-20T01:02:03"));
        usage1.setLastUsed(LocalDateTime.parse("2024-11-20T01:02:03"));
        List<ColumnUsage> usage = List.of(usage1);
        instance.persist(usage);

        Mockito.verify(repo)
                .executeDML(String.format("INSERT INTO _statistics_.predicate_columns(fe_id, db_id, table_id, " +
                                "column_id, usage, last_used ) " +
                                "VALUES ('%s', %d, %d, 0, 'predicate', '2024-11-20 01:02:03')", feName, db.getId(),
                        t0.getId()));

        // query
        instance.queryGlobalState(TableName.fromString("default_catalog.test.t0"),
                EnumSet.noneOf(ColumnUsage.UseCase.class));
        Mockito.verify(repo).executeDQL("SELECT fe_id, db_id,  table_id,  column_id, usage, last_used, created " +
                "FROM _statistics_.predicate_columns WHERE true  AND fe_id = '" + feName + "'");

        // TODO: vacuum
        instance.vacuum(lastPersist);
        Mockito.verify(repo).executeDML("DELETE FROM _statistics_.predicate_columns " +
                "WHERE fe_id=" + Strings.quote(feName) +
                " AND last_used < '2024-11-10 01:00:00'");

        guard.close();
    }

    @Test
    public void testSerialization() {
        Database db = starRocksAssert.getDb("test");
        Table t1 = starRocksAssert.getTable("test", "t1");
        Column v4 = t1.getColumn("v4");
        Column v5 = t1.getColumn("v5");
        ColumnUsage usage1 = new ColumnUsage(ColumnFullId.create(db, t1, v4),
                TableName.fromString("default_catalog.test.t1"),
                ColumnUsage.UseCase.PREDICATE);
        ColumnUsage usage2 = new ColumnUsage(ColumnFullId.create(db, t1, v5),
                TableName.fromString("default_catalog.test.t1"),
                EnumSet.of(ColumnUsage.UseCase.PREDICATE, ColumnUsage.UseCase.JOIN));

        String row1 = String.format("{\"data\": ['%s',%d,%d,%d,'%s','%s','%s']}",
                feName,
                usage1.getColumnFullId().getDbId(),
                usage1.getColumnFullId().getTableId(),
                usage1.getColumnFullId().getColumnUniqueId(),
                usage1.getUseCaseString(),
                DateUtils.formatDateTimeUnix(usage1.getLastUsed()),
                DateUtils.formatDateTimeUnix(usage1.getCreated())
        );
        String row2 = String.format("{\"data\": ['%s',%d,%d,%d,'%s','%s','%s']}",
                feName,
                usage2.getColumnFullId().getDbId(),
                usage2.getColumnFullId().getTableId(),
                usage2.getColumnFullId().getColumnUniqueId(),
                usage2.getUseCaseString(),
                DateUtils.formatDateTimeUnix(usage2.getLastUsed()),
                DateUtils.formatDateTimeUnix(usage2.getCreated())
        );

        TResultBatch batch = new TResultBatch();
        batch.setRows(List.of(ByteBuffer.wrap(row1.getBytes()), ByteBuffer.wrap(row2.getBytes())));
        List<ColumnUsage> result = PredicateColumnsStorage.resultToColumnUsage(List.of(batch));

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(usage1, result.get(0));
        Assertions.assertEquals(usage2, result.get(1));
    }

    @Test
    public void testDuplicateColumnUsages() {
        Database db = starRocksAssert.getDb("test");
        Table t1 = starRocksAssert.getTable("test", "t1");
        Column v4 = t1.getColumn("v4");
        Column v5 = t1.getColumn("v5");

        List<ColumnUsage> duplicatedColumnUsages = new ArrayList<>();
        ColumnUsage usage1 = new ColumnUsage(ColumnFullId.create(db, t1, v4),
                TableName.fromString("default_catalog.test.t1"),
                EnumSet.of(ColumnUsage.UseCase.PREDICATE, ColumnUsage.UseCase.DISTINCT));
        usage1.setCreated(LocalDateTime.parse("2025-01-01T01:02:03"));
        usage1.setLastUsed(LocalDateTime.parse("2025-01-02T01:02:03"));
        duplicatedColumnUsages.add(usage1);

        ColumnUsage usage2 = new ColumnUsage(ColumnFullId.create(db, t1, v4),
                TableName.fromString("default_catalog.test.t1"),
                EnumSet.of(ColumnUsage.UseCase.PREDICATE, ColumnUsage.UseCase.JOIN));
        usage2.setCreated(LocalDateTime.parse("2025-01-01T01:02:03"));
        usage2.setLastUsed(LocalDateTime.parse("2025-01-02T02:02:03"));
        duplicatedColumnUsages.add(usage2);

        ColumnUsage usage3 = new ColumnUsage(ColumnFullId.create(db, t1, v5),
                TableName.fromString("default_catalog.test.t1"),
                EnumSet.of(ColumnUsage.UseCase.GROUP_BY, ColumnUsage.UseCase.JOIN));
        usage3.setCreated(LocalDateTime.parse("2025-01-01T01:02:03"));
        usage3.setLastUsed(LocalDateTime.parse("2025-01-02T03:02:03"));
        duplicatedColumnUsages.add(usage3);

        List<ColumnUsage> result = PredicateColumnsStorage.getInstance().deduplicateColumnUsages(duplicatedColumnUsages);

        Assertions.assertEquals(2, result.size());
        ColumnUsage r1 = result.get(1);
        Assertions.assertEquals(EnumSet.of(ColumnUsage.UseCase.PREDICATE, ColumnUsage.UseCase.JOIN,
                ColumnUsage.UseCase.DISTINCT), r1.getUseCases());
        Assertions.assertEquals(LocalDateTime.parse("2025-01-02T02:02:03"), r1.getLastUsed());

    }
}