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


package com.starrocks.analysis;

import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.server.GlobalStateMgr;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;


public class TruncateTableStmtTest {


    @BeforeClass
    public static void setUp() throws Exception {
        // set timeout to a really long time so that ut can pass even when load of ut machine is very high
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;
        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database test");
        cluster.runSql("test",
                "CREATE TABLE bucket1 (\n" +
                        "day date NOT NULL COMMENT \"\",\n" +
                        "date_time datetime NOT NULL COMMENT \"\",\n" +
                        "product varchar(64) NOT NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP\n" +
                        "UNIQUE KEY(day, date_time)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "PARTITION BY RANGE(day)\n" +
                        "(\n" +
                        "PARTITION p20220630 VALUES [('2022-06-30'), ('2022-07-01')))\n" +
                        "DISTRIBUTED BY HASH(date_time) BUCKETS 10\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\"\n" +
                        ");");
    }
    @Test
    public void testCatalogModifyColumn() throws Exception {
        String alterStmt = "alter table bucket1 add PARTITION p20220706 VALUES LESS THAN ('2022-07-07') " +
                "DISTRIBUTED BY HASH(date_time) BUCKETS 20;";
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql("test", alterStmt);
        List<Long> tablets = cluster.listTablets("test", "bucket1");
        Assert.assertEquals(30, tablets.size());
        String truncateStmt = "truncate table bucket1 PARTITION(p20220706)";
        cluster.runSql("test", truncateStmt);
        tablets = cluster.listTablets("test", "bucket1");
        Assert.assertEquals(30, tablets.size());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }
}
