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

import com.google.common.collect.ImmutableList;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

// external_column_statistics / external_histogram_statistics store table_uuid hashed
// (StatisticUtils.hashTableUuidForPkStorage) to stay within BE's primary_key_limit_size.
// These tests confirm every query/delete builder that filters by table_uuid matches both
// the hashed and the raw value, so historical rows written before hashing was introduced
// stay visible until they naturally age out.
class StatisticSQLBuilderTest {

    private static final String TABLE_UUID =
            "iceberg.udp_abx_etl_db1_datawarehouse.tenant.account_buying_group.d6cfa1ed-0000-0000-0000-000000000000";

    @Test
    void buildQueryExternalFullStatisticsSQLMatchesHashedAndRawUuid() {
        String hashed = StatisticUtils.hashTableUuidForPkStorage(TABLE_UUID);
        String sql = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL(
                TABLE_UUID, ImmutableList.of("col1"), ImmutableList.of(IntegerType.BIGINT));
        Assertions.assertTrue(sql.contains("table_uuid in (\"" + hashed + "\", \"" + TABLE_UUID + "\")"),
                "query predicate must match both hashed and raw table_uuid: " + sql);
    }

    @Test
    void buildDropExternalStatSQLByUuidMatchesHashedAndRawUuid() {
        String hashed = StatisticUtils.hashTableUuidForPkStorage(TABLE_UUID);
        String sql = StatisticSQLBuilder.buildDropExternalStatSQL(TABLE_UUID);
        Assertions.assertTrue(sql.contains("table_uuid in ('" + hashed + "', '" + TABLE_UUID + "')"),
                "delete predicate must match both hashed and raw table_uuid: " + sql);
    }

    @Test
    void buildQueryConnectorHistogramStatisticsSQLMatchesHashedAndRawUuid() {
        String hashed = StatisticUtils.hashTableUuidForPkStorage(TABLE_UUID);
        List<String> columnNames = ImmutableList.of("col1");
        String sql = StatisticSQLBuilder.buildQueryConnectorHistogramStatisticsSQL(TABLE_UUID, columnNames);
        Assertions.assertTrue(sql.contains("table_uuid in ('" + hashed + "', '" + TABLE_UUID + "')"),
                "query predicate must match both hashed and raw table_uuid: " + sql);
    }

    @Test
    void buildDropExternalHistogramSQLMatchesHashedAndRawUuid() {
        String hashed = StatisticUtils.hashTableUuidForPkStorage(TABLE_UUID);
        String sql = StatisticSQLBuilder.buildDropExternalHistogramSQL(TABLE_UUID, ImmutableList.of("col1"));
        Assertions.assertTrue(sql.contains("table_uuid in ('" + hashed + "', '" + TABLE_UUID + "')"),
                "delete predicate must match both hashed and raw table_uuid: " + sql);
    }
}
