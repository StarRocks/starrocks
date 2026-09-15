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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TInternalScanRange;
import com.starrocks.thrift.TKeyRange;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * A scan range carries the value range of each partition column it was cut by, and BE's dynamic
 * partition pruning evaluates the partition conjuncts against those values to drop scan ranges the
 * predicate cannot match. It finds the column those values belong to in a map keyed by the slot's
 * col_name - the storage-side column id - so the range has to name the column by its id as well.
 */
public class PartitionKeyRangeColumnIdTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test_pkr").useDatabase("test_pkr")
                .withTable("create table pkr(dt date not null, v int) duplicate key(dt)" +
                        " partition by range(dt) (" +
                        "  partition p1 values [('2026-01-01'), ('2026-01-02'))," +
                        "  partition p2 values [('2026-01-02'), ('2026-01-03')))" +
                        " distributed by hash(v) buckets 1 properties('replication_num' = '1');");
    }

    @Test
    public void testPartitionKeyRangeNamesTheColumnById() throws Exception {
        // dayofmonth() is not something FE can prune partitions by, so both partitions survive into
        // the plan and BE gets a chance to drop their scan ranges - the case this naming decides.
        String sql = "select v from test_pkr.pkr where dayofmonth(%s) = 1";
        Assertions.assertEquals(List.of("dt", "dt"), rangeColumnNames(String.format(sql, "dt")));

        starRocksAssert.ddl("alter table pkr rename column dt to dt_new");
        try {
            Assertions.assertEquals(List.of("dt", "dt"), rangeColumnNames(String.format(sql, "dt_new")),
                    "a partition column range must name its column by the id, which the rename left "
                            + "alone, or BE's column_name_to_slot lookup misses it and prunes nothing");
        } finally {
            starRocksAssert.ddl("alter table pkr rename column dt_new to dt");
        }
    }

    private static List<String> rangeColumnNames(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        Pair<String, ExecPlan> plan = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        List<String> names = Lists.newArrayList();
        for (ScanNode scanNode : plan.second.getScanNodes()) {
            for (TScanRangeLocations locations : scanNode.getScanRangeLocations(0)) {
                TInternalScanRange range = locations.getScan_range().getInternal_scan_range();
                if (range == null || range.getPartition_column_ranges() == null) {
                    continue;
                }
                for (TKeyRange keyRange : range.getPartition_column_ranges()) {
                    names.add(keyRange.getColumn_name());
                }
            }
        }
        Assertions.assertFalse(names.isEmpty(), "the plan must carry partition column ranges at all");
        return names;
    }
}
