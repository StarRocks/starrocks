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

package com.starrocks.statistic;

import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class StatisticSQLBuilderTest {

    @Test
    void buildQueryExternalFullStatisticsSQLFiltersHashedPartitionsOnly() {
        String sql = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL(
                "d6cfa1ed-0000-0000-0000-000000000000",
                List.of("col1"),
                List.<Type>of(IntegerType.INT));

        // Only rows whose partition_name has been migrated to the hashed format should be aggregated,
        // otherwise pre-migration raw-partition-name rows for the same table_uuid/column_name would be
        // double counted alongside their re-collected, hash-keyed counterparts.
        Assertions.assertTrue(sql.contains("partition_name like \"" + StatsConstants.EXTERNAL_PARTITION_NAME_HASH_PREFIX + "%\""),
                "expected predicate to filter partition_name by the hash prefix, got: " + sql);
    }
}
