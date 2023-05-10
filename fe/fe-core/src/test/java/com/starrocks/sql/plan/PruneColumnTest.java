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

import com.starrocks.server.GlobalStateMgr;
import org.junit.BeforeClass;
import org.junit.Test;

public class PruneColumnTest extends PlanTestNoneDBBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        String dbName = "prune_column_test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        starRocksAssert.withTable("CREATE TABLE `pc0` (\n" +
                "  `v1` bigint NULL, \n" +
                "  `map1` MAP<INT, INT> NULL, \n" +
                "  `map2` MAP<INT, MAP<INT, INT>> NULL, " +
                "  `map3` MAP<INT, MAP<INT, MAP<INT, INT>>> NULL, " +
                "  `map4` MAP<INT, MAP<INT, MAP<INT, MAP<INT, INT>>>> NULL " +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
    }

    @Test
    public void testPruneMapColumn() throws Exception {
        String sql = "select map_keys(map1) from pc0";
        String plan = getVerboseExplain(sql);

        System.out.println(plan);

        sql = "select map_values(map_values(map2)) from pc0";
        plan = getVerboseExplain(sql);

        System.out.println(plan);

        sql = "select map_keys(map_values(map_values(map3))) from pc0";
        plan = getVerboseExplain(sql);
        sql = "select map_keys(map_values(map_values(map4))) from pc0";
        plan = getVerboseExplain(sql);
        sql = "select map_keys(map_values(map_values(map_values(map4)))) from pc0";
        plan = getVerboseExplain(sql);

        sql = "select map_keys(map_values(map_values(map_values(map4)))), " +
                "     map_keys(map_values(map_values(map4)))," +
                "     map_keys(map_values(map4))," +
                "     map_keys(map4)," +
                " from pc0";
        plan = getVerboseExplain(sql);


        sql = "select map_1, " +
                "     map_2, " +
                "     map_values(map_1), " +
                "     map_keys(map_values(map_2)), " +
                "     map_keys(map_values(map_values(map_values(map4)))), " +
                "     map_keys(map_values(map_values(map4)))," +
                "     map_keys(map_values(map4))," +
                "     map_keys(map4)," +
                " from pc0";
        plan = getVerboseExplain(sql);
    }

}
