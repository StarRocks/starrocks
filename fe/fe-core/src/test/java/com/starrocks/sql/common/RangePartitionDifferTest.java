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

package com.starrocks.sql.common;

import com.google.common.collect.Maps;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RangePartitionDifferTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE range_differ_base (k1 date, v1 int)\n" +
                "PARTITION BY RANGE(k1) (\n" +
                "    PARTITION p1 VALUES LESS THAN ('2020-02-01'),\n" +
                "    PARTITION p2 VALUES LESS THAN ('2020-03-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(v1) BUCKETS 1\n" +
                "PROPERTIES('replication_num' = '1');");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW range_differ_mv\n" +
                "PARTITION BY k1 DISTRIBUTED BY HASH(k1) BUCKETS 1 REFRESH DEFERRED MANUAL\n" +
                "AS SELECT k1, sum(v1) AS v1 FROM range_differ_base GROUP BY k1");
    }

    @Test
    public void testPartitionDiffOfInactivatedMvNamesTheMv() {
        MaterializedView mv = getMv("range_differ_mv");
        RangePartitionDiffer differ =
                new RangePartitionDiffer(mv, MVTimelinessArbiter.QueryRewriteParams.ofRefresh(), null);
        mv.setInactiveAndReason("inactivated by the test");
        try {
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> differ.computePartitionDiff(null, Maps.newHashMap()));
            Assertions.assertTrue(e.getMessage().contains("materialized view range_differ_mv"), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("inactive reason: inactivated by the test"),
                    e.getMessage());
        } finally {
            mv.setActive();
        }
    }
}
