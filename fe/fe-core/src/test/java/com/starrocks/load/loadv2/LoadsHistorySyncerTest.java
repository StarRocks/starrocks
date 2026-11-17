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

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.system.information.LoadsSystemTable;
import com.starrocks.common.Pair;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class LoadsHistorySyncerTest {

    private static List<String> viewColumnNames() {
        return LoadsSystemTable.columns().stream()
                .map(column -> column.getName().toLowerCase())
                .collect(Collectors.toList());
    }

    private static List<String> historyColumnNames() {
        return LoadsHistorySyncer.HISTORY_COLUMNS.stream().map(column -> column.first).collect(Collectors.toList());
    }

    private static OlapTable table(long id, List<Column> columns) {
        return new OlapTable(id, "loads_history", columns, KeysType.DUP_KEYS, new SinglePartitionInfo(),
                new HashDistributionInfo(3, Lists.newArrayList(columns.get(1))));
    }

    @Test
    public void testHistoryColumnsMirrorLoadsView() {
        Assertions.assertEquals(viewColumnNames(), historyColumnNames());
        Assertions.assertTrue(historyColumnNames().containsAll(
                List.of("txn_id", "load_id", "table_id", "parent_id", "statement", "error_code", "visible_time",
                        "sink_bytes", "client_ip", "be_ids", "commit_throttle_ms", "cpu_ns", "peak_mem_bytes")));
    }

    @Test
    public void testCreateTableDeclaresEveryColumn() {
        String createSql = LoadsHistorySyncer.createKeeper().getCreateTableSql();
        for (Pair<String, String> column : LoadsHistorySyncer.HISTORY_COLUMNS) {
            Assertions.assertTrue(createSql.contains("`" + column.first + "` " + column.second), column.first);
        }
        Assertions.assertTrue(createSql.contains("`load_finish_time` datetime not null"));
        Assertions.assertTrue(createSql.contains("`runtime_details` json"));
        Assertions.assertTrue(createSql.contains("`label` varchar(2048)"));
        Assertions.assertTrue(createSql.contains("PARTITION BY date_trunc('DAY', load_finish_time)"));
    }

    @Test
    public void testSyncSqlNamesColumnsOnBothSides() {
        String sql = LoadsHistorySyncer.SQLBuilder.buildSyncSql();
        String columnList = LoadsHistorySyncer.HISTORY_COLUMNS.stream()
                .map(column -> "`" + column.first + "`")
                .collect(Collectors.joining(", "));
        Assertions.assertTrue(sql.startsWith("INSERT INTO _statistics_.loads_history (" + columnList + ") "
                + "SELECT " + columnList + " FROM information_schema.loads WHERE load_finish_time IS NOT NULL"), sql);
        Assertions.assertFalse(sql.contains("SELECT *"));
    }

    @Test
    public void testKeeperAddsOnlyMissingColumns() {
        Column id = new Column("id", IntegerType.BIGINT);
        Column label = new Column("label", TypeFactory.createVarcharType(2048));
        Column txnId = new Column("txn_id", IntegerType.BIGINT);
        OlapTable oldTable = table(1L, Lists.newArrayList(id, label, txnId));

        List<String> missing = TableKeeper.missingColumnDefinitions(oldTable, LoadsHistorySyncer.HISTORY_COLUMNS);
        Assertions.assertEquals(LoadsHistorySyncer.HISTORY_COLUMNS.size() - 3, missing.size());
        Assertions.assertTrue(missing.stream().noneMatch(def -> def.startsWith("`id`")
                || def.startsWith("`label`") || def.startsWith("`txn_id`")));
        Assertions.assertTrue(missing.contains("`load_id` varchar(2048)"));
        Assertions.assertTrue(missing.contains("`visible_time` datetime"));
        Assertions.assertEquals("`peak_mem_bytes` bigint", missing.get(missing.size() - 1));

        String alterSql = TableKeeper.alterTableAddColumns("_statistics_", "loads_history", missing);
        Assertions.assertTrue(alterSql.startsWith("ALTER TABLE _statistics_.loads_history ADD COLUMN ("), alterSql);
        Assertions.assertTrue(alterSql.endsWith("`peak_mem_bytes` bigint)"), alterSql);

        // Column order in the existing table does not matter once every column is present.
        List<Column> shuffled = Lists.newArrayList();
        for (Pair<String, String> column : Lists.reverse(LoadsHistorySyncer.HISTORY_COLUMNS)) {
            shuffled.add(new Column(column.first, IntegerType.BIGINT));
        }
        Assertions.assertTrue(
                TableKeeper.missingColumnDefinitions(table(2L, shuffled), LoadsHistorySyncer.HISTORY_COLUMNS).isEmpty());
    }
}
