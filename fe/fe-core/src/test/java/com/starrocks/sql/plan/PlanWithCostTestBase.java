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

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.BeforeClass;

public class PlanWithCostTestBase extends PlanTestBase {
    protected static final int NUM_TABLE2_ROWS = 10000;
    protected static final int NUM_TABLE0_ROWS = 10000;

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        Config.alter_scheduler_interval_millisecond = 1;

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table2 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("test_all_type");
        setTableStatistics(table2, NUM_TABLE2_ROWS);

        OlapTable t0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("t0");
        setTableStatistics(t0, NUM_TABLE0_ROWS);

        OlapTable colocateT0 = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("colocate_t0");
        setTableStatistics(colocateT0, NUM_TABLE0_ROWS);

        OlapTable lineitem = (OlapTable) globalStateMgr.getLocalMetastore().getDb("test").getTable("lineitem");
        setTableStatistics(lineitem, NUM_TABLE0_ROWS * NUM_TABLE0_ROWS);

        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE test_mv\n" +
                "    (\n" +
                "        event_day int,\n" +
                "        siteid INT,\n" +
                "        citycode SMALLINT,\n" +
                "        username VARCHAR(32),\n" +
                "        pv BIGINT SUM DEFAULT '0'\n" +
                "    )\n" +
                "    AGGREGATE KEY(event_day, siteid, citycode, username)\n" +
                "    DISTRIBUTED BY HASH(siteid) BUCKETS 10\n" +
                "    rollup (\n" +
                "    r1(event_day,siteid),\n" +
                "    r2(event_day,citycode),\n" +
                "    r3(event_day),\n" +
                "    r4(event_day,pv),\n" +
                "    r5(event_day,siteid,pv)\n" +
                "    )\n" +
                "    PROPERTIES(\"replication_num\" = \"1\");");

        starRocksAssert.withTable(" CREATE TABLE `duplicate_table_with_null` ( `k1`  date, `k2`  datetime, " +
                "`k3`  char(20), `k4`  varchar(20), `k5`  boolean, `k6`  tinyint, " +
                "`k7`  smallint, `k8`  int, `k9`  bigint, `k10` largeint, " +
                "`k11` float, `k12` double, `k13` decimal(27,9) ) " +
                "ENGINE=OLAP DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) " +
                "COMMENT \"OLAP\" DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) " +
                "BUCKETS 3 PROPERTIES ( \"replication_num\" = \"1\");");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW bitmap_mv\n" +
                "                             AS\n" +
                "                             SELECT k1,k2,k3,k4, bitmap_union(to_bitmap(k7)), " +
                "bitmap_union(to_bitmap(k8)) FROM duplicate_table_with_null group by k1,k2,k3,k4");

        starRocksAssert.withTable("CREATE TABLE `test_dict` (\n" +
                "  `name` varchar(65533) NOT NULL COMMENT \"\",\n" +
                "  `dt` date NOT NULL  ,\n" +
                "  `id` bigint(20) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`name`,`dt`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(\n" +
                "PARTITION p20221202 VALUES [('2022-12-02'), ('2022-12-03')),\n" +
                "PARTITION p20221203 VALUES [('2022-12-03'), ('2022-12-04')),\n" +
                "PARTITION p20221204 VALUES [('2022-12-04'), ('2022-12-05')),\n" +
                "PARTITION p20221205 VALUES [('2022-12-05'), ('2022-12-06')),\n" +
                "PARTITION p20221206 VALUES [('2022-12-06'), ('2022-12-07')),\n" +
                "PARTITION p20221207 VALUES [('2022-12-07'), ('2022-12-08')),\n" +
                "PARTITION p20221208 VALUES [('2022-12-08'), ('2022-12-09')),\n" +
                "PARTITION p20221209 VALUES [('2022-12-09'), ('2022-12-10')),\n" +
                "PARTITION p20221210 VALUES [('2022-12-10'), ('2022-12-11')),\n" +
                "PARTITION p20221211 VALUES [('2022-12-11'), ('2022-12-12')),\n" +
                "PARTITION p20221212 VALUES [('2022-12-12'), ('2022-12-13')),\n" +
                "PARTITION p20221213 VALUES [('2022-12-13'), ('2022-12-14')))\n" +
                "DISTRIBUTED BY HASH(`name`) BUCKETS 9 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE t1_single(\n" +
                "    id bigint  ,\n" +
                "    user_id  bigint  ,\n" +
                "    recharge_money decimal(32,2) , \n" +
                "    province varchar(255) not null ,\n" +
                "    dt varchar\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province) (\n" +
                "   PARTITION p1 VALUES IN (\"beijing\",\"chongqing\"),\n" +
                "   PARTITION p2 VALUES IN (\"shanghai\",\"tianjing\")\n" +
                ") PROPERTIES (\"replication_num\" = \"1\")");
        starRocksAssert.withTable("CREATE TABLE t1_multi_col(\n" +
                "    id bigint  ,\n" +
                "    user_id  bigint  ,\n" +
                "    recharge_money decimal(32,2) , \n" +
                "    province varchar(255) not null ,\n" +
                "    dt varchar(255) not null\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (dt,province) (\n" +
                "   PARTITION p1 VALUES IN ((\"2022-04-01\", \"beijing\"),(\"2022-04-01\", \"chongqing\")),\n" +
                "   PARTITION p2 VALUES IN ((\"2022-04-02\", \"beijing\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");
        FeConstants.runningUnitTest = true;
    }

}
