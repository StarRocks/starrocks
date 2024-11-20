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

package com.starrocks.statistic.predicate_columns;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.ColumnId;
import com.starrocks.load.pipe.filelist.RepoExecutor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.thrift.TResultBatch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
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
        Mockito.verify(repo).executeDQL("SELECT fe_id, table_catalog, table_database, " +
                "table_name, column_name, usage, last_used , created FROM _statistics_.predicate_columns WHERE fe_id " +
                "= '" + feName + "'");

        // persist
        ColumnUsage usage1 =
                new ColumnUsage(ColumnId.create("v1"), TableName.fromString("t0"), ColumnUsage.UseCase.PREDICATE);
        usage1.setCreated(LocalDateTime.parse("2024-11-20T01:02:03"));
        usage1.setLastUsed(LocalDateTime.parse("2024-11-20T01:02:03"));
        List<ColumnUsage> usage = List.of(usage1);
        instance.persist(usage);

        Mockito.verify(repo).executeDML("INSERT INTO " +
                "_statistics_.predicate_columns(fe_id, table_catalog, table_database, table_name, column_name, " +
                "usage, last_used ) VALUES ('" + feName +
                "', 'default_catalog', 'test', 't0', " +
                "'v1', 'predicate', ''2024-11-20 01:02:03'')");

        // query
        instance.queryGlobalState(TableName.fromString("default_catalog.test.t0"));
        Mockito.verify(repo).executeDQL("SELECT fe_id, table_catalog, table_database, table_name, column_name, " +
                "usage, last_used , created FROM _statistics_.predicate_columns WHERE  WHERE true " +
                "AND table_catalog = 'default_catalog' AND table_database = 'test' AND table_name = 't0'");
        instance.queryGlobalState(new TableName("test", "t0"));
        Mockito.verify(repo).executeDQL("SELECT fe_id, table_catalog, table_database, table_name, column_name, " +
                "usage, last_used , created FROM _statistics_.predicate_columns WHERE  WHERE true " +
                "AND table_database = 'test' AND table_name = 't0'");

        // TODO: vacuum

        guard.close();
    }

    @Test
    public void testSerialization() {
        ColumnUsage usage1 = new ColumnUsage(ColumnId.create("c1"),
                TableName.fromString("default_catalog.d1.t1"),
                ColumnUsage.UseCase.PREDICATE);
        ColumnUsage usage2 = new ColumnUsage(ColumnId.create("c2"),
                TableName.fromString("default_catalog.d1.t1"),
                EnumSet.of(ColumnUsage.UseCase.PREDICATE, ColumnUsage.UseCase.JOIN));

        String row1 = String.format("{\"data\":[%s, %s]}",
                (usage1.toJson()),
                (usage2.toJson())
        );

        TResultBatch batch = new TResultBatch();
        batch.setRows(List.of(ByteBuffer.wrap(row1.getBytes())));
        List<ColumnUsage> result = PredicateColumnsStorage.resultToColumnUsage(List.of(batch));

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(usage1, result.get(0));
        Assertions.assertEquals(usage2, result.get(1));
    }

}