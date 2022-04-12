// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg.cost;

import com.starrocks.catalog.Column;
import com.starrocks.external.iceberg.IcebergTableKey;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class IcebergMetaCacheTest {

    @Test
    public void testTableCacheTakeEffect(@Mocked IcebergTableStatisticCalculator calculator,
                                         @Mocked Table table,
                                         @Mocked IcebergFileStats fileStats) throws ExecutionException {
        new MockUp<IcebergMetaCache>() {
            @Mock
            protected IcebergTableStatisticCalculator getCalculator(Table table) {
                return calculator;
            }

            @Mock
            protected IcebergFileStats getIcebergFileStats(Table table,
                                                           IcebergTableStatisticCalculator calculator) {
                return fileStats;
            }

            @Mock
            protected Table getTable(IcebergTableKey key) {
                return table;
            }
        };

        new Expectations() {
            {
                calculator.makeTableStatistics((IcebergFileStats) any, (List<Types.NestedField>) any, (Map<ColumnRefOperator, Column>) any);
                result = Statistics.builder().build();
            }
        };

        Executor executor = Executors.newFixedThreadPool(1);
        IcebergMetaCache icebergMetaCache = new IcebergMetaCache(executor);
        IcebergTableKey key = new IcebergTableKey("testRes", "testDb", "testTbl");
        icebergMetaCache.getTableStatistics(key, new HashMap<>());
        Assert.assertNotNull(icebergMetaCache.tableStatsCache.get(key));
    }
}
