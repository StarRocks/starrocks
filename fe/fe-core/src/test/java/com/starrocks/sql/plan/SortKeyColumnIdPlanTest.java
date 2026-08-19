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

package com.starrocks.sql.plan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * BE knows a column by its storage-side id, not by its name: SlotDescriptor#toThrift sends
 * Column#getColumnId as the slot's col_name, and TColumn#column_name - what the tablet schema is
 * built from - is the id as well. A rename changes Column#getName and deliberately leaves that id
 * alone, so anything FE sends BE that names a column by getName() stops matching afterwards.
 */
public class SortKeyColumnIdPlanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("create table sort_key_rename(k1 int, k2 int, v1 int) " +
                "duplicate key(k1, k2) distributed by hash(v1) buckets 1 " +
                "properties('replication_num'='1')");
        starRocksAssert.withTable("create table part_key_rename(dt date not null, v1 int) " +
                "duplicate key(dt) partition by range(dt) (" +
                "  partition p1 values [('2026-01-01'), ('2026-01-02'))," +
                "  partition p2 values [('2026-01-02'), ('2026-01-03'))) " +
                "distributed by hash(v1) buckets 1 properties('replication_num'='1')");
    }

    /**
     * sort_key_column_names feeds ChunkPredicateBuilder::build_scan_keys, which looks each name up in
     * column_value_ranges - a map keyed by the slot's col_name, i.e. the column id - and stops at the
     * first miss. Naming the sort key by the current column name leaves conditional_key_columns at 0
     * for a renamed table, so no short-key range is built and the scan reads the whole block instead
     * of the key range. The rows returned are the same; they are just read the hard way.
     */
    @Test
    public void testSortKeyColumnNamesAreColumnIds() throws Exception {
        starRocksAssert.ddl("alter table sort_key_rename rename column k1 to k1_new");
        try {
            String thrift = getThriftPlan("select v1 from sort_key_rename where k1_new > 1 and k2 = 1");
            Assertions.assertTrue(thrift.contains("column_name:k1,"),
                    "the tablet schema BE builds names this column by its id: " + scanNodeOf(thrift));
            Assertions.assertTrue(thrift.contains("sort_key_column_names:[k1, k2]"),
                    "sort key names must be column ids, or BE cannot match them against the slots: "
                            + scanNodeOf(thrift));
        } finally {
            starRocksAssert.ddl("alter table sort_key_rename rename column k1_new to k1");
        }
    }

    /**
     * That same list is also compared inside FE, in assignOrderByHints, against the column behind a
     * TopN filter's probe slot: when it is the leading key column the scan is told to read in the
     * TopN's direction. Both sides have to speak the same language - changing only the payload would
     * leave a name being compared against an id, and a descending TopN on a renamed key column would
     * silently lose its hint (output_asc_hint falls back to its default true).
     */
    @Test
    public void testOrderByHintSurvivesColumnRename() throws Exception {
        assertContains(getThriftPlan("select v1 from sort_key_rename order by k1 desc limit 5"),
                "output_asc_hint:false");
        starRocksAssert.ddl("alter table sort_key_rename rename column k1 to k1_new");
        try {
            assertContains(getThriftPlan("select v1 from sort_key_rename order by k1_new desc limit 5"),
                    "output_asc_hint:false");
        } finally {
            starRocksAssert.ddl("alter table sort_key_rename rename column k1_new to k1");
        }
    }

    /**
     * assignOrderByHints checks the same probe column twice: against the sort key, and against the
     * table's leading partition column, which decides whether BE may schedule the partitions in the
     * TopN's direction (partition_order_hint). The second check compares against a Column of its own,
     * so it has to be moved to ids together with the first one - leaving it on names would mean an id
     * being compared against a name, and a renamed partition column would silently lose that hint.
     */
    @Test
    public void testPartitionOrderHintSurvivesColumnRename() throws Exception {
        assertContains(getThriftPlan("select v1 from part_key_rename order by dt desc limit 5"),
                "partition_order_hint:false");
        starRocksAssert.ddl("alter table part_key_rename rename column dt to dt_new");
        try {
            assertContains(getThriftPlan("select v1 from part_key_rename order by dt_new desc limit 5"),
                    "partition_order_hint:false");
        } finally {
            starRocksAssert.ddl("alter table part_key_rename rename column dt_new to dt");
        }
    }

    private static String scanNodeOf(String thrift) {
        int i = thrift.indexOf("olap_scan_node:");
        return i < 0 ? thrift : thrift.substring(i, Math.min(i + 600, thrift.length()));
    }
}
