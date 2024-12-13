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

package com.starrocks.connector.delta;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DeltaLakeFileStatsTest {
    private StructType schema;

    private static void checkColumnStatisticsEqual(ColumnStatistic left, ColumnStatistic right) {
        Assert.assertEquals(left.toString(), right.toString());
        Assert.assertEquals(left.getMinString(), right.getMinString());
        Assert.assertEquals(left.getMaxString(), right.getMaxString());
    }

    @Before
    public void setUp() throws Exception {
        schema = new StructType()
                .add("c_int", IntegerType.INTEGER)
                .add("c_char", StringType.STRING)
                .add("c_string", StringType.STRING);
    }

    @Test
    public void testGetMinMaxString() {
        Map<String, Object> minValues = new HashMap<>() {
            {
                put("c_char", "char_111");
                put("c_string", "string_111");
            }
        };

        Map<String, Object> maxValues = new HashMap<>() {
            {
                put("c_char", "char_999");
                put("c_string", "string_999");
            }
        };

        Map<String, Object> nullCounts = new HashMap<>() {
            {
                put("c_char", 0d);
                put("c_string", 0d);
            }
        };

        Set<String> nonPartitionPrimitiveColumns = new HashSet<>() {
            {
                add("c_int");
                add("c_char");
                add("c_string");
            }
        };

        DeltaLakeAddFileStatsSerDe stat = new DeltaLakeAddFileStatsSerDe(10, minValues, maxValues, nullCounts);
        DeltaLakeFileStats stats = new DeltaLakeFileStats(schema, nonPartitionPrimitiveColumns,
                null, stat, null, 10, 4096);

        ColumnStatistic columnStatistic = stats.fillColumnStats(new Column("c_char", Type.CHAR));
        ColumnStatistic checkStatistic = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                0, 16, 1, null, ColumnStatistic.StatisticType.UNKNOWN);
        checkStatistic.setMinString("char_111");
        checkStatistic.setMaxString("char_999");
        checkColumnStatisticsEqual(checkStatistic, columnStatistic);

        columnStatistic = stats.fillColumnStats(new Column("c_string", Type.STRING));
        checkStatistic = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                0, 16, 1, null, ColumnStatistic.StatisticType.UNKNOWN);
        checkStatistic.setMinString("string_111");
        checkStatistic.setMaxString("string_999");
        checkColumnStatisticsEqual(checkStatistic, columnStatistic);
    }

    @Test
    public void testUpdateStringStats() {
        Map<String, Object> minValues1 = new HashMap<>() {
            {
                put("c_string", "string_111");
            }
        };
        Map<String, Object> minValues2 = new HashMap<>() {
            {
                put("c_string", "string_000");
            }
        };
        Map<String, Object> minValues3 = new HashMap<>() {
            {
                put("c_string", "string_222");
            }
        };

        Map<String, Object> maxValues1 = new HashMap<>() {
            {
                put("c_string", "string_555");
            }
        };
        Map<String, Object> maxValues2 = new HashMap<>() {
            {
                put("c_string", "string_444");
            }
        };
        Map<String, Object> maxValues3 = new HashMap<>() {
            {
                put("c_string", "string_666");
            }
        };

        Map<String, Object> nullCounts1 = new HashMap<>() {
            {
                put("c_string", 0d);
            }
        };
        Map<String, Object> nullCounts2 = new HashMap<>() {
            {
                put("c_string", 0d);
            }
        };
        Map<String, Object> nullCounts3 = new HashMap<>() {
            {
                put("c_string", 0d);
            }
        };

        Set<String> nonPartitionPrimitiveColumns = new HashSet<>() {
            {
                add("c_string");
            }
        };

        DeltaLakeAddFileStatsSerDe stat1 = new DeltaLakeAddFileStatsSerDe(10, minValues1, maxValues1, nullCounts1);
        DeltaLakeFileStats stats = new DeltaLakeFileStats(schema, nonPartitionPrimitiveColumns, null,
                stat1, null, 10, 4096);

        DeltaLakeAddFileStatsSerDe stat2 = new DeltaLakeAddFileStatsSerDe(10, minValues2, maxValues2, nullCounts2);
        stats.update(stat2, null, 10, 4096);

        DeltaLakeAddFileStatsSerDe stat3 = new DeltaLakeAddFileStatsSerDe(10, minValues3, maxValues3, nullCounts3);
        stats.update(stat3, null, 10, 4096);

        ColumnStatistic checkStatistic = new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                0, 16, 1, null, ColumnStatistic.StatisticType.UNKNOWN);
        checkStatistic.setMinString("string_000");
        checkStatistic.setMaxString("string_666");
        checkColumnStatisticsEqual(checkStatistic, stats.fillColumnStats(new Column("c_string", Type.STRING)));
    }
}
