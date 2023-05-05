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
        starRocksAssert.withTable("create table test_map(c0 INT, c1 map<int,varchar(65533)>, c2 map<int, map<int,double>>) " +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
    }

    @Test
    public void testMapFunc() throws Exception { // get super common return type
        String sql = "select map_concat({16865432442:3},{3.323777777:'3'})";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MAP<DECIMAL128(28,9),VARCHAR>");

        sql = "select cardinality({1:3})";
        plan = getFragmentPlan(sql);
        assertContains(plan, "map_size("); // rewrite to map_size
    }
}
