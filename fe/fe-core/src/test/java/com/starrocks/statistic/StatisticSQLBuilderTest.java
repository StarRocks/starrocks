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

    @Test
    void buildQueryExternalFullStatisticsSQLGroupsByColumnNameOnly() {
        // Regression test: grouping by table_uuid too would split a table's rows into two groups
        // whenever both the hashed and raw representations are present, silently dropping one
        // group's aggregated data downstream (see the collect-vs-read consistency discussion).
        String sql = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL(
                TABLE_UUID, ImmutableList.of("col1"), ImmutableList.of(IntegerType.BIGINT));
        Assertions.assertTrue(sql.endsWith("GROUP BY column_name"), "must group by column_name only: " + sql);
        Assertions.assertFalse(sql.contains("GROUP BY table_uuid"), "must not group by table_uuid: " + sql);
    }

    @Test
    void buildDropExternalStatSQLForPartitionsOnlyMatchesRawUuid() {
        String hashed = StatisticUtils.hashTableUuidForPkStorage(TABLE_UUID);
        String sql = StatisticSQLBuilder.buildDropExternalStatSQLForPartitions(
                TABLE_UUID, ImmutableList.of("p1", "p2"), ImmutableList.of("col1", "col2"));
        Assertions.assertTrue(sql.contains("TABLE_UUID = '" + TABLE_UUID + "'"),
                "cleanup delete must target only the raw uuid: " + sql);
        Assertions.assertFalse(sql.contains(hashed),
                "cleanup delete must never also match the hashed uuid (it holds the fresh data): " + sql);
        Assertions.assertTrue(sql.contains("PARTITION_NAME IN ('p1', 'p2')"), sql);
        Assertions.assertTrue(sql.contains("COLUMN_NAME IN ('col1', 'col2')"), sql);
    }

    @Test
    void buildDropExternalHistogramSQLForRawUuidOnlyMatchesRawUuid() {
        String hashed = StatisticUtils.hashTableUuidForPkStorage(TABLE_UUID);
        String sql = StatisticSQLBuilder.buildDropExternalHistogramSQLForRawUuid(TABLE_UUID, ImmutableList.of("col1"));
        Assertions.assertTrue(sql.contains("table_uuid = '" + TABLE_UUID + "'"),
                "cleanup delete must target only the raw uuid: " + sql);
        Assertions.assertFalse(sql.contains(hashed),
                "cleanup delete must never also match the hashed uuid (it holds the fresh data): " + sql);
    }

    @Test
    void buildQueryExternalFullStatisticsSQLDedupsByLatestUpdateTime() {
        // Correctness must not depend on the write-side cleanup delete succeeding: if both a
        // raw-keyed and hashed-keyed row are briefly alive for the same partition/column, the
        // query must keep only the freshest one (by update_time) before aggregating, not double-
        // count both.
        String sql = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL(
                TABLE_UUID, ImmutableList.of("col1"), ImmutableList.of(IntegerType.BIGINT));
        Assertions.assertTrue(sql.contains(
                "row_number() over ( partition by partition_name, column_name order by update_time desc) as rn"), sql);
        Assertions.assertTrue(sql.contains(") dedup_t WHERE rn = 1 GROUP BY column_name"), sql);
    }

    @Test
    void buildQueryConnectorHistogramStatisticsSQLDedupsByLatestUpdateTime() {
        String sql = StatisticSQLBuilder.buildQueryConnectorHistogramStatisticsSQL(TABLE_UUID, ImmutableList.of("col1"));
        Assertions.assertTrue(sql.contains(
                "row_number() over ( partition by column_name order by update_time desc) as rn"), sql);
        Assertions.assertTrue(sql.contains(") dedup_t WHERE rn = 1"), sql);
    }

    @Test
    void tableUUIDPredicatesEscapeQuotesAndBackslashes() {
        // table_uuid is derived from catalog/db/table names (Table.getUUID()), so it must be
        // escaped like any other untrusted value before being embedded into a SQL string literal.
        // StarRocks decodes backslash escapes inside string literals, so doubling quotes alone
        // (the old StringEscapeUtils.escapeSql behavior) is not sufficient - see SqlUtils.escapeSqlString.
        String doubleQuoteTrickyUUID = "iceberg.db.o\"brien\\table.uuid"; // contains " and \
        String doubleQuoted = StatisticSQLBuilder.buildQueryExternalFullStatisticsSQL(
                doubleQuoteTrickyUUID, ImmutableList.of("col1"), ImmutableList.of(IntegerType.BIGINT));
        Assertions.assertTrue(doubleQuoted.contains("iceberg.db.o\\\"brien\\\\table.uuid\""), doubleQuoted);

        String singleQuoteTrickyUUID = "iceberg.db.o'brien\\table.uuid"; // contains ' and \
        String singleQuoted = StatisticSQLBuilder.buildDropExternalStatSQL(singleQuoteTrickyUUID);
        Assertions.assertTrue(singleQuoted.contains("iceberg.db.o''brien\\\\table.uuid'"), singleQuoted);
    }

    @Test
    void cleanupDeletesEscapeRawUuidAndNames() {
        String trickyUUID = "iceberg.db.o'brien\\table.uuid";
        String sql = StatisticSQLBuilder.buildDropExternalStatSQLForPartitions(
                trickyUUID, ImmutableList.of("p'1"), ImmutableList.of("c\\1"));
        Assertions.assertTrue(sql.contains("TABLE_UUID = 'iceberg.db.o''brien\\\\table.uuid'"), sql);
        Assertions.assertTrue(sql.contains("PARTITION_NAME IN ('p''1')"), sql);
        Assertions.assertTrue(sql.contains("COLUMN_NAME IN ('c\\\\1')"), sql);
    }
}
