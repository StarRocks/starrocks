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


package com.starrocks.sql.analyzer;

import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public class DDLTestBase {

    protected static ConnectContext ctx;
    protected static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);

        FeConstants.runningUnitTest = true;
        Config.enable_new_publish_mechanism = false;
        Config.tablet_sched_checker_interval_seconds = 10;
        Config.tablet_sched_repair_delay_factor_second = 10;
        Config.alter_scheduler_interval_millisecond = 1000;
    }

    @BeforeEach
    public void setUp() throws Exception {
        starRocksAssert.withDatabase(GlobalStateMgrTestUtil.testDb1)
                .useDatabase(GlobalStateMgrTestUtil.testDb1);
        starRocksAssert.withTable("CREATE TABLE `testTable1` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `testTable7` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE testTable2\n" +
                        "(\n" +
                        "    v1 date,\n" +
                        "    v2 int,\n" +
                        "    v3 int\n" +
                        ")\n" +
                        "DUPLICATE KEY(`v1`)\n" +
                        "PARTITION BY RANGE(v1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(v1) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');");
    }
}
