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
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.types.StructType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class DeltaStatisticProviderTest {
    @Test
    public void testUpdateAndDiscardFileStats(@Mocked DeltaLakeTable table, @Mocked Metadata metadata,
                                              @Mocked FileScanTask firstFile, @Mocked FileScanTask secondFile) {
        StructType schema = new StructType().add("id", io.delta.kernel.types.IntegerType.INTEGER);
        PredicateSearchKey key = PredicateSearchKey.of("db", "table", GetRemoteFilesParams.newBuilder().build());
        DeltaLakeAddFileStatsSerDe firstStats = new DeltaLakeAddFileStatsSerDe(3,
                Map.of("id", 1d), Map.of("id", 3d), Map.of("id", 0d));
        DeltaLakeAddFileStatsSerDe secondStats = new DeltaLakeAddFileStatsSerDe(5,
                Map.of("id", 4d), Map.of("id", 8d), Map.of("id", 0d));
        new Expectations() {
            {
                table.getDeltaMetadata();
                result = metadata;

                metadata.getSchema();
                result = schema;

                firstFile.getPartitionValues();
                result = Map.of();
                firstFile.getRecords();
                result = 3L;
                firstFile.getFileSize();
                result = 100L;

                secondFile.getPartitionValues();
                result = Map.of();
                secondFile.getRecords();
                result = 5L;
                secondFile.getFileSize();
                result = 200L;
            }
        };

        DeltaStatisticProvider provider = new DeltaStatisticProvider();
        provider.updateFileStats(table, key, firstFile, firstStats, Set.of("id"), Set.of());
        provider.updateFileStats(table, key, secondFile, secondStats, Set.of("id"), Set.of());

        ColumnRefOperator idRef = new ColumnRefOperator(1, IntegerType.INT, "id", true);
        Statistics statistics = provider.getTableStatistics(schema, key,
                Map.of(idRef, new Column("id", IntegerType.INT)));
        Assertions.assertEquals(8, statistics.getOutputRowCount(), 0.001);
        Assertions.assertEquals(1, statistics.getColumnStatistics().size());

        provider.discardFileStats(key);
        Assertions.assertEquals(1, provider.getCardinalityStats(schema, key, Map.of()).getOutputRowCount(), 0.001);
    }
}
