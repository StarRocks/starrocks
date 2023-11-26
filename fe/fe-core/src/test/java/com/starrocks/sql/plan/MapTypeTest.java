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

import com.starrocks.utframe.StarRocksAssert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class MapTypeTest extends PlanTestBase {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("create table test_map(" +
                "c0 INT, " +
                "c1 map<int,varchar(65533)>, " +
                "c2 map<int, map<int,double>>) " +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
    }

    @Test
    public void testMapFunc() throws Exception { // get super common return type
        String sql = "select map_concat(map{16865432442:3},map{3.323777777:'3'})";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MAP<DECIMAL128(28,9),VARCHAR>");

        sql = "with t0 as (\n" +
                "    select c1 from (values(map())) as t(c1)\n" +
                ")\n" +
                "select map_concat(map('a',1, 'b',2), c1)\n" +
                "from t0;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "map_concat(map{'a':1,'b':2}, CAST(1: c1 AS MAP<VARCHAR,TINYINT>))");

        sql = "with t0 as (\n" +
                "    select c1 from (values(map())) as t(c1)\n" +
                ")\n" +
                "select map_concat(c1, map('a',1, 'b',2))\n" +
                "from t0;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "map_concat(CAST(1: c1 AS MAP<VARCHAR,TINYINT>), map{'a':1,'b':2})");
    }

    @Test
    public void testInsertErrorType() throws Exception {
        String sql = "insert into test_map values (1, map{1: map{1:2}}, map{1:1});";
        try {
            String plan = getFragmentPlan(sql);
        } catch (Exception e) {
            assertContains(e.getMessage(), 
                    "Cannot cast 'map{1:map{1:2}}' from " + 
                    "MAP<TINYINT,MAP<TINYINT,TINYINT>> to MAP<INT,VARCHAR(65533)>.");
        }
    }

    @Test
    public void testComplexAnyValue() throws Exception {
        String sql = "select any_value(c2) from test_map limit 1";
        String plan = getFragmentPlan(sql);
        assertContains("1:AGGREGATE (update finalize)\n" +
                "  |  output: any_value(3: c2)\n" +
                "  |  group by: \n" +
                "  |  limit: 1");
    }
}
