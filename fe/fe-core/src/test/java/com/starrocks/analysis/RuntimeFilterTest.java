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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/SelectStmtTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.common.FeConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RuntimeFilterTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp()
            throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        String createTblStmtStr = "CREATE TABLE `duplicate_par_tbl` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` datetime NULL COMMENT \"\",\n" +
                "  `k3` char(20) NULL COMMENT \"\",\n" +
                "  `k4` varchar(20) NULL COMMENT \"\",\n" +
                "  `k5` boolean NULL COMMENT \"\",\n" +
                "  `k6` tinyint(4) NULL COMMENT \"\",\n" +
                "  `k7` smallint(6) NULL COMMENT \"\",\n" +
                "  `k8` int(11) NULL COMMENT \"\",\n" +
                "  `k9` bigint(20) NULL COMMENT \"\",\n" +
                "  `k10` largeint(40) NULL COMMENT \"\",\n" +
                "  `k11` float NULL COMMENT \"\",\n" +
                "  `k12` double NULL COMMENT \"\",\n" +
                "  `k13` decimal128(27, 9) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(PARTITION p202006 VALUES [(\"0000-01-01\"), (\"2020-07-01\")),\n" +
                "PARTITION p202007 VALUES [(\"2020-07-01\"), (\"2020-08-01\")),\n" +
                "PARTITION p202008 VALUES [(\"2020-08-01\"), (\"2020-09-01\")))\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 3 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"fast_schema_evolution\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                "); ";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr);
        FeConstants.runningUnitTest = true;
    }

    @Test
    void testRuntimeFilterPushDown()
            throws Exception {
        String sql =
                "with tbl1 as (select * from duplicate_par_tbl partition(p202006) where k3 in('beijing','')),  \n" +
                        "tbl2 as (select * from duplicate_par_tbl partition(p202007) where k5 is null),  \n" +
                        "tbl3 as (select * from duplicate_par_tbl partition(p202008) where k13 > 60),  \n" +
                        "tbl4 as (select * from duplicate_par_tbl where k6 in(''))  \n" +
                        "select distinct k1 from(select t.k1\n" +
                        "from tbl4 d left join[shuffle] \n" +
                        "(select a.*, b.k13 as bk13, c.k13 as ck13 \n" +
                        "from tbl1 a right join[shuffle] tbl2 b on a.k13 = b.k13 \n" +
                        "right join[shuffle] tbl3 c on a.k13 = c.k13 and c.k5 = 1) t on " +
                        "t.k13 = d.k13 and t.bk13 = d.k13 and t.ck13 = d.k13 \n" +
                        "where d.k5 is null\n" +
                        ") tbl order by 1 desc limit 15";
        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
        Assert.assertTrue(plan, plan.contains("7:Project\n" +
                "  |  output columns:\n" +
                "  |  39 <-> [39: k13, DECIMAL128(27,9), true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  6:OlapScanNode\n" +
                "     table: duplicate_par_tbl, rollup: duplicate_par_tbl\n" +
                "     preAggregation: on\n" +
                "     Predicates: [39: k13, DECIMAL128(27,9), true] > 60, 31: k5 IS NULL\n" +
                "     partitionsRatio=1/3, tabletsRatio=3/3\n" +
                "     tabletList=10015,10017,10019\n" +
                "     actualRows=0, avgRowSize=2.0\n" +
                "     cardinality: 1\n" +
                "     probe runtime filters:\n" +
                "     - filter_id = 1, probe_expr = (39: k13)"));
    }
}