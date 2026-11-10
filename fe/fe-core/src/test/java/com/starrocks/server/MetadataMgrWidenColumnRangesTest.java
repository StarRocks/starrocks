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

package com.starrocks.server;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetadataMgrWidenColumnRangesTest {
    private final ColumnRefOperator ts = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
    private final ColumnRefOperator id = new ColumnRefOperator(2, IntegerType.INT, "id", true);
    private final ColumnRefOperator v = new ColumnRefOperator(3, IntegerType.INT, "v", true);
    private final Map<ColumnRefOperator, Column> columns = Map.of(
            ts, new Column("TS", DateType.DATETIME), id, new Column("id", IntegerType.INT),
            v, new Column("v", IntegerType.INT));

    private static ColumnStatistic stat(double min, double max) {
        return ColumnStatistic.builder().setMinValue(min).setMaxValue(max).setNullsFraction(0.1)
                .setDistinctValuesCount(7).setAverageRowSize(8).build();
    }

    @Test
    public void testWidensOnlyAndKeepsTheRest() {
        Statistics statistics = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(ts, stat(100, 200))
                .addColumnStatistic(id, stat(0, 50))
                .addColumnStatistic(v, ColumnStatistic.unknown())
                .setStatsSource(Statistics.StatsSource.ANALYZE).build();
        Statistics widened = MetadataMgr.widenColumnRanges(statistics, columns, Map.of(
                "ts", Pair.create(150.0, 300.0),   // newer partitions: max widens, min never narrows
                "id", Pair.create(10.0, 20.0),     // inside ANALYZE range: unchanged
                "v", Pair.create(0.0, 1.0)));      // no ANALYZE statistics: left unknown

        ColumnStatistic tsStat = widened.getColumnStatistic(ts);
        assertEquals(100, tsStat.getMinValue());
        assertEquals(300, tsStat.getMaxValue());
        assertEquals(7, tsStat.getDistinctValuesCount());
        assertEquals(0.1, tsStat.getNullsFraction());
        assertEquals(50, widened.getColumnStatistic(id).getMaxValue());
        assertTrue(widened.getColumnStatistic(v).isUnknown());
        assertEquals(1000, widened.getOutputRowCount());
        assertEquals(Statistics.StatsSource.ANALYZE, widened.getStatsSource());
    }

    @Test
    public void testNothingToWidenReturnsSameStatistics() {
        Statistics statistics = Statistics.builder().setOutputRowCount(10)
                .addColumnStatistic(id, stat(0, 50)).build();
        assertSame(statistics, MetadataMgr.widenColumnRanges(statistics, columns, Map.of()));
        assertSame(statistics, MetadataMgr.widenColumnRanges(statistics, columns, Map.of("id", Pair.create(1.0, 2.0))));
    }

    // End to end through MetadataMgr.getTableStatistics: the ANALYZE branch (skipped in unit tests by default) asks the
    // connector for the partition source ranges of the queried version and returns the widened statistics.
    @Test
    public void testAnalyzeBranchWidensWithConnectorRanges() {
        MetadataMgr metadataMgr = spy(new MetadataMgr(mock(LocalMetastore.class), mock(TemporaryTableMgr.class),
                null, null));
        Table table = mock(Table.class);
        Statistics analyzed = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(ts, stat(100, 200))
                .setStatsSource(Statistics.StatsSource.ANALYZE).build();
        doReturn(analyzed).when(metadataMgr).getTableStatisticsFromInternalStatistics(any(), any());
        ConnectorMetadata connector = mock(ConnectorMetadata.class);
        TvrTableSnapshot version = TvrTableSnapshot.of(7L);
        when(connector.getPartitionSourceColumnRanges(table, version)).thenReturn(Map.of("ts", Pair.create(50.0, 300.0)));
        doReturn(Optional.of(connector)).when(metadataMgr).getOptionalMetadata("iceberg_catalog");
        OptimizerContext session = mock(OptimizerContext.class);

        boolean runningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = false;
        Statistics statistics;
        try {
            statistics = metadataMgr.getTableStatistics(session, "iceberg_catalog", table, Map.of(ts, columns.get(ts)),
                    List.of(), null, -1, version);
        } finally {
            FeConstants.runningUnitTest = runningUnitTest;
        }
        verify(session).setObtainedFromInternalStatistics(true);
        assertEquals(50, statistics.getColumnStatistic(ts).getMinValue());
        assertEquals(300, statistics.getColumnStatistic(ts).getMaxValue());
        assertEquals(1000, statistics.getOutputRowCount());
        assertEquals(Statistics.StatsSource.ANALYZE, statistics.getStatsSource());
    }
}
