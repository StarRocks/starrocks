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

package com.starrocks.catalog;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.common.collect.ImmutableMap;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.statistics.CacheDictManager;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class CacheDictManagerTest {
    @Test
    public void test() {
        CacheDictManager manager = new CacheDictManager();
        new Expectations(manager) {
            {
                manager.getGlobalDict(anyLong, ColumnId.create("val"));
                result = Optional.empty();
            }
        };
        manager.getGlobalDict(1, ColumnId.create("val"));
    }

    /**
     * The dict lookup finds the scanned table's database on the table itself, which the analyzer records on every
     * table it resolves, including the base table of a view in another database; the databases the statement names
     * are only the fallback for a table without one.
     */
    @Test
    public void testHasGlobalDictFindsTheDatabaseOfTheScannedTable() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        try {
            ConnectContext context = UtFrameUtils.createDefaultCtx();
            context.setThreadLocalInfo();
            Database db = new Database(30001L, "dict_db");
            GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(db);
            Column c1 = new Column("c1", VarcharType.VARCHAR);
            OlapTable table = new OlapTable(30002L, "dict_t", List.of(c1), KeysType.DUP_KEYS,
                    new SinglePartitionInfo(), new RandomDistributionInfo(1));
            db.registerTableUnlocked(table);

            CacheDictManager manager = new CacheDictManager();
            ColumnIdentifier column = new ColumnIdentifier(table.getId(), c1.getColumnId());
            ColumnDict dict = new ColumnDict(
                    ImmutableMap.of(ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)), 1), Long.MAX_VALUE);
            AsyncLoadingCache<ColumnIdentifier, Optional<ColumnDict>> cache =
                    Deencapsulation.getField(manager, "dictStatistics");
            cache.put(column, CompletableFuture.completedFuture(Optional.of(dict)));

            // No database recorded on the table, and none named by the statement: nothing to find it in.
            context.setCurrentSqlDbIds(Set.of());
            Assertions.assertFalse(manager.hasGlobalDict(table, c1.getColumnId(), 0));
            // Named by the statement: the fallback finds it.
            context.setCurrentSqlDbIds(Set.of(db.getId()));
            Assertions.assertTrue(manager.hasGlobalDict(table, c1.getColumnId(), 0));
            // Recorded on the table: found whatever the statement names.
            context.setCurrentSqlDbIds(Set.of());
            table.maySetDatabaseId(db.getId());
            Assertions.assertTrue(manager.hasGlobalDict(table, c1.getColumnId(), 0));
        } finally {
            UtFrameUtils.tearDownForPersisTest();
        }
    }
}
