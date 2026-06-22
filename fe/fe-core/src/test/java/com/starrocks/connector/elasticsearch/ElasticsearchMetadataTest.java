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

package com.starrocks.connector.elasticsearch;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.EsTable;
import com.starrocks.common.Config;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchMetadataTest {

    @Test
    public void testGetTable(@Mocked EsRestClient client) {
        ElasticsearchMetadata metadata = new ElasticsearchMetadata(client, new HashMap<>(), "catalog");
        Assertions.assertNull(metadata.getTable(new ConnectContext(), "default_db", "not_exist_index"));
        Assertions.assertNull(metadata.getTable(new ConnectContext(), "aaaa", "tbl"));
    }

    @Test
    public void testGetTableStatistics(@Mocked EsRestClient client, @Mocked EsTable esTable) {
        new Expectations() {
            {
                esTable.getIndexName();
                result = "my_index";
                client.getRowCount("my_index");
                result = 5_000_000L;
            }
        };

        ElasticsearchMetadata metadata = new ElasticsearchMetadata(client, new HashMap<>(), "catalog");
        ColumnRefOperator colRef = new ColumnRefOperator(0, IntegerType.INT, "col", true);
        Map<ColumnRefOperator, Column> columns = new HashMap<>();
        columns.put(colRef, new Column("col", IntegerType.INT));
        Statistics stats = metadata.getTableStatistics(
                null, esTable, columns, Collections.emptyList(), null, -1, TvrTableSnapshot.empty());
        Assertions.assertEquals(5_000_000D, stats.getOutputRowCount(), 0.01);
        Assertions.assertTrue(stats.getColumnStatistics().containsKey(colRef));
    }

    @Test
    public void testGetTableStatisticsCacheHit(@Mocked EsRestClient client, @Mocked EsTable esTable) {
        new Expectations() {
            {
                esTable.getIndexName();
                result = "my_index";
                client.getRowCount("my_index");
                result = 1_000L;
            }
        };

        ElasticsearchMetadata metadata = new ElasticsearchMetadata(client, new HashMap<>(), "catalog");
        Map<ColumnRefOperator, Column> columns = Collections.emptyMap();
        metadata.getTableStatistics(null, esTable, columns, Collections.emptyList(), null, -1, TvrTableSnapshot.empty());
        metadata.getTableStatistics(null, esTable, columns, Collections.emptyList(), null, -1, TvrTableSnapshot.empty());

        // getRowCount should be called only once due to cache
        new Verifications() {
            {
                client.getRowCount(anyString);
                times = 1;
            }
        };
    }

    @Test
    public void testGetTableStatisticsFallback(@Mocked EsRestClient client, @Mocked EsTable esTable) {
        new Expectations() {
            {
                esTable.getIndexName();
                result = "my_index";
                client.getRowCount("my_index");
                result = -1L;
            }
        };

        ElasticsearchMetadata metadata = new ElasticsearchMetadata(client, new HashMap<>(), "catalog");
        Statistics stats = metadata.getTableStatistics(
                null, esTable, Collections.emptyMap(), Collections.emptyList(), null, -1, TvrTableSnapshot.empty());
        Assertions.assertEquals((double) Config.default_statistics_output_row_count,
                stats.getOutputRowCount(), 0.01);
    }
}
