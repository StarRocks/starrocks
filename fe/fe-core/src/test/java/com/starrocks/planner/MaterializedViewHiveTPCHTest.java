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

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MaterializedViewHiveTPCHTest extends MaterializedViewTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        MaterializedViewTestBase.beforeClass();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        executeSqlFile("sql/materialized-view/tpch-hive/ddl_tpch_mv1.sql");
    }

    @Test
    public void testQuery1() {
        runFileUnitTest("materialized-view/tpch-hive/q1");
    }

    @Test
    public void testQuery2() {
        runFileUnitTest("materialized-view/tpch-hive/q2");
    }

    @Test
    public void testQuery3() {
        runFileUnitTest("materialized-view/tpch-hive/q3");
    }

    @Test
    public void testQuery4() {
        runFileUnitTest("materialized-view/tpch-hive/q4");
    }

    @Test
    public void testQuery5() {
        runFileUnitTest("materialized-view/tpch-hive/q5");
    }

    @Test
    public void testQuery6() {
        runFileUnitTest("materialized-view/tpch-hive/q6");
    }

    @Test
    public void testQuery7() {
        runFileUnitTest("materialized-view/tpch-hive/q7");
    }

    @Test
    public void testQuery8() {
        runFileUnitTest("materialized-view/tpch-hive/q8");
    }

    @Test
    public void testQuery9() {
        runFileUnitTest("materialized-view/tpch-hive/q9");
    }

    @Test
    public void testQuery10() {
        runFileUnitTest("materialized-view/tpch-hive/q10");
    }

    @Test
    public void testQuery11() {
        runFileUnitTest("materialized-view/tpch-hive/q11");
    }

    @Test
    public void testQuery12() {
        runFileUnitTest("materialized-view/tpch-hive/q12");
    }

    @Test
    public void testQuery14() {
        runFileUnitTest("materialized-view/tpch-hive/q14");
    }

    @Test
    public void testQuery15() {
        runFileUnitTest("materialized-view/tpch-hive/q15");
    }

    @Test
    public void testQuery17() {
        runFileUnitTest("materialized-view/tpch-hive/q17");
    }

    @Test
    public void testQuery18() {
        runFileUnitTest("materialized-view/tpch-hive/q18");
    }

    @Test
    public void testQuery19() {
        runFileUnitTest("materialized-view/tpch-hive/q19");
    }

    @Test
    public void testQuery20() {
        runFileUnitTest("materialized-view/tpch-hive/q20");
    }

    @Test
    public void testQuery21() {
        runFileUnitTest("materialized-view/tpch-hive/q21");
    }

    @Test
    public void testQuery22() {
        runFileUnitTest("materialized-view/tpch-hive/q22");
    }
}
