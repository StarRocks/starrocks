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

package com.starrocks.connector.lance;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.LanceTable;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.sql.optimizer.statistics.Statistics;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LanceMetadataStatisticsTest {
    @Test
    public void testDefaultRowCountAndUnknownRequestedColumns() {
        Fixture fixture = new Fixture();
        when(fixture.storage.getColumnStatistics(fixture.table, List.of("id")))
                .thenReturn(List.of(ColumnStatistic.unknown()));
        Statistics stats = fixture.statistics(Map.of(fixture.ref, fixture.column));
        Assertions.assertEquals(Config.default_statistics_output_row_count, stats.getOutputRowCount());
        Assertions.assertEquals(Statistics.StatsSource.NONE, stats.getStatsSource());
        Assertions.assertEquals(1, stats.getColumnStatistics().size());
        Assertions.assertTrue(stats.getColumnStatistic(fixture.ref).isUnknown());
    }

    @Test
    public void testCachedColumnStatisticsAndUnfilteredRowCount() {
        Fixture fixture = new Fixture();
        ColumnStatistic columnStats = new ColumnStatistic(0, 100, 0.1, 4, 50);
        when(fixture.storage.getColumnStatistics(fixture.table, List.of("id")))
                .thenReturn(List.of(columnStats));
        // Neither a false predicate nor LIMIT should be applied twice by metadata and the optimizer.
        Statistics stats = fixture.metadata.getTableStatistics(fixture.optimizer, fixture.table,
                Map.of(fixture.ref, fixture.column), List.of(), ConstantOperator.createBoolean(false), 1, null);
        Assertions.assertEquals(Config.default_statistics_output_row_count, stats.getOutputRowCount());
        Assertions.assertEquals(columnStats, stats.getColumnStatistic(fixture.ref));
    }

    @Test
    public void testEmptyProjectionStillReturnsRowCount() {
        Fixture fixture = new Fixture();
        when(fixture.storage.getColumnStatistics(fixture.table, List.of())).thenReturn(List.of());
        when(fixture.storage.getHistogramStatistics(fixture.table, List.of())).thenReturn(Map.of());
        Statistics stats = fixture.statistics(Map.of());
        Assertions.assertEquals(Config.default_statistics_output_row_count, stats.getOutputRowCount());
        Assertions.assertTrue(stats.getColumnStatistics().isEmpty());
    }

    private static class Fixture {
        private final LanceMetadata metadata = new LanceMetadata("lance_stats", Map.of(
                "table.rows.uri", "file:///tmp/rows.lance", "table.rows.schema", "id:int32,value:string"));
        private final LanceTable table = (LanceTable) metadata.getTable(null, "default", "rows");
        private final Column column = table.getColumn("id");
        private final ColumnRefFactory factory = new ColumnRefFactory();
        private final ColumnRefOperator ref = factory.create("id", column.getType(), true);
        private final OptimizerContext optimizer = OptimizerFactory.mockContext(factory);
        private final StatisticStorage storage = mock(StatisticStorage.class);

        private Fixture() {
            GlobalStateMgr state = mock(GlobalStateMgr.class);
            when(state.getStatisticStorage()).thenReturn(storage);
            when(storage.getHistogramStatistics(table, List.of("id"))).thenReturn(Map.of());
            new MockUp<GlobalStateMgr>() {
                @Mock
                public GlobalStateMgr getCurrentState() {
                    return state;
                }
            };
        }

        private Statistics statistics(Map<ColumnRefOperator, Column> columns) {
            return metadata.getTableStatistics(optimizer, table, columns, List.of(), null, -1, null);
        }
    }
}
