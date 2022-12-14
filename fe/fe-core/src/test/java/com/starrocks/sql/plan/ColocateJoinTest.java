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

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

class ColocateJoinTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("CREATE TABLE `colocate_t2_1` (\n" +
                "  `v7` bigint NULL COMMENT \"\",\n" +
                "  `v8` bigint NULL COMMENT \"\",\n" +
                "  `v9` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v7`, `v8`, v9)\n" +
                "DISTRIBUTED BY HASH(`v7`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"colocate_with\" = \"colocate_group_1\"" +
                ");");
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("colocateJoinOnceSqls")
    void testColocateJoinOnce(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        int count = StringUtils.countMatches(plan, "INNER JOIN (COLOCATE)");
        Assert.assertEquals(plan, 1, count);
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("colocateJoinTwiceSqls")
    void testColocateJoinTwice(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        int count = StringUtils.countMatches(plan, "INNER JOIN (COLOCATE)");
        Assert.assertEquals(plan, 2, count);
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("otherJoinTypeColocateOnceSqls")
    void testOtherJoinColocateOnce(String sql) throws Exception {
        connectContext.getSessionVariable().disableJoinReorder();
        String plan = getFragmentPlan(sql);
        int colocateCount = StringUtils.countMatches(plan, "(COLOCATE)");
        int bucketShuffleCount = StringUtils.countMatches(plan, "(BUCKET_SHUFFLE)");

        Assert.assertEquals(plan, 1, colocateCount);
        Assert.assertEquals(plan, 1, bucketShuffleCount);

        connectContext.getSessionVariable().enableJoinReorder();
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("otherJoinTypeColocateTwiceSqls")
    void testOtherJoinColocateTwice(String sql) throws Exception {
        connectContext.getSessionVariable().disableJoinReorder();
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        int count = StringUtils.countMatches(plan, "COLOCATE");
        Assert.assertEquals(plan, 2, count);
        connectContext.getSessionVariable().enableJoinReorder();
    }


    private static Stream<Arguments> colocateJoinOnceSqls() {
        List<String> sqls = Lists.newArrayList();

        // sqls should colocate join but not support now
        List<String> unsupportedSqls = Lists.newArrayList();
        sqls.add("select * from colocate_t0 join colocate_t1 on v1 = v5 and v1 = v4");
        sqls.add("select * from colocate_t0 join colocate_t1 on v2 = v4 and v1 = v4");
        sqls.add("select * from colocate_t0 join colocate_t1 on v1 + v2 = v4 + v5 and v1 = v4 + 1 and v1 = v4");
        sqls.add("select * from colocate_t0, colocate_t1 where v1 = v5 and v1 = v4");
        sqls.add("select * from colocate_t0, colocate_t1 where v2 = v4 and v1 = v4");
        sqls.add("select * from colocate_t0, colocate_t1 where v1 + v2 = v4 + v5 and v1 = v4 + 1 and v1 = v4");
        sqls.add("select * from colocate_t0, colocate_t1, colocate_t2_1 where  v1 = v5 and v5 = v7");
        sqls.add("select * from colocate_t0 join colocate_t1 on v1 = v5 join colocate_t2_1 on v5 = v7");

        // TODO(packy) now we cannot derive v1 = v7 plan from the below sqls
        unsupportedSqls.add("select * from colocate_t0 join colocate_t1 on v1 = v5 + v6 join colocate_t2_1 on v5 + v6 = v7");
        unsupportedSqls.add("select * from colocate_t0, colocate_t1, colocate_t2_1 where  v1 = v5 + v6 and v5 + v6 = v7");
        return sqls.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> colocateJoinTwiceSqls() {
        List<String> sqls = Lists.newArrayList();
        // sqls should colocate join but not support now
        List<String> unsupportedSqls = Lists.newArrayList();
        sqls.add("select * from colocate_t0 join colocate_t1 on v1 = v4 join colocate_t2_1 on v4 = v7");
        sqls.add("select * from colocate_t0 join colocate_t1 on v1 = v5 and v1 = v4 join colocate_t2_1 on v5 = v7 and v7 = v2");
        sqls.add("select * from colocate_t0 join colocate_t1 on v1 = v5 join colocate_t2_1 on v1 = v4 and v1 = v7");


        sqls.add("select * from colocate_t0, colocate_t1, colocate_t2_1 where v1 = v4 and v4 = v7");
        sqls.add("select * from colocate_t0, colocate_t1, colocate_t2_1 where v1 = v5 and v1 = v4 and v5 = v7 and v7 = v2");
        sqls.add("select * from colocate_t0, colocate_t1, colocate_t2_1 where v1 = v5 and v1 = v4 and v1 = v7");


        // TODO(packy) the expr col seems not been equivalent conduction
        unsupportedSqls.add("select * from colocate_t0 join colocate_t1 on v1 = v5 and v1 = v4 + v6 and v1 = v4 " +
                "join colocate_t2_1 on v4 + v6 = v7");
        unsupportedSqls.add("select * from colocate_t0 join colocate_t1 on v1 + v2 = v4 and v1 + v2 = v5 - v4 " +
                "join colocate_t2_1 on v5 - v4 = v7 and v7 = v1");
        unsupportedSqls.add("select * from colocate_t0 join colocate_t1 on v1 + v2 = v4 - v3 and v1 = v4 + v5 " +
                "join colocate_t2_1 on v4 + v5 = v4 and v4 + v5 = v7");
        return sqls.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> otherJoinTypeColocateOnceSqls() {
        List<String> sqls = Lists.newArrayList();
        sqls.add("select * from colocate_t0 left join colocate_t1 on v1 = v4 left join colocate_t2_1 on v1 = v8");
        sqls.add("select * from colocate_t0 left semi join colocate_t1 on v1 = v4 left join colocate_t2_1 on v1 = v8");
        sqls.add("select * from colocate_t0 left anti join colocate_t1 on v1 = v4 left join colocate_t2_1 on v1 = v8");
        sqls.add("select * from colocate_t0 left join colocate_t1 on v1 = v4 and v2 = v5 left semi join colocate_t2_1 " +
                "on v1 = v8 and v3 = v9");

        sqls.add("select * from colocate_t0 right join colocate_t1 on v1 = v4 left join colocate_t2_1 on v4 = v8");
        sqls.add("select * from colocate_t0 right semi join colocate_t1 on v1 = v4 left join colocate_t2_1 on v4 = v8");
        sqls.add("select * from colocate_t0 right anti join colocate_t1 on v1 = v4 left join colocate_t2_1 on v4 = v8");
        sqls.add("select * from colocate_t0 right join colocate_t1 on v1 = v4 and v2 = v5 left semi join colocate_t2_1 " +
                "on v4 = v8 and v6 = v9");


        return sqls.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> otherJoinTypeColocateTwiceSqls() {
        List<String> sqls = Lists.newArrayList();
        sqls.add("select * from colocate_t0 left join colocate_t1 on v1 = v4 left join colocate_t2_1 on v1 = v7");
        sqls.add("select * from colocate_t0 left join colocate_t1 on v1 = v4 right join colocate_t2_1 on v1 = v7");
        sqls.add("select * from colocate_t0 left semi join colocate_t1 on v1 = v4 join colocate_t2_1 on v1 = v7");
        sqls.add("select * from colocate_t0 left anti join colocate_t1 on v1 = v4 join colocate_t2_1 on v1 = v7");
        sqls.add("select * from colocate_t0 left semi join colocate_t1 on v1 = v4 and v5 = v6 left join " +
                "colocate_t2_1 on v1 = v7 and v3 = v8");
        sqls.add("select * from colocate_t0 full outer join colocate_t1 on v1 = v4 and v5 > v6 left join " +
                "colocate_t2_1 on v1 = v7 and v3 = v8");

        sqls.add("select * from colocate_t0 right join colocate_t1 on v1 = v4 left join colocate_t2_1 on v4 = v7");
        sqls.add("select * from colocate_t0 right join colocate_t1 on v1 = v4 right join colocate_t2_1 on v4 = v7");
        sqls.add("select * from colocate_t0 right semi join colocate_t1 on v1 = v4 join colocate_t2_1 on v4 = v7");
        sqls.add("select * from colocate_t0 right anti join colocate_t1 on v1 = v4 join colocate_t2_1 on v4 = v7");
        sqls.add("select * from colocate_t0 right semi join colocate_t1 on v1 = v4 and v5 = v6 left join " +
                "colocate_t2_1 on v4 = v7 and v5 = v8");

        return sqls.stream().map(e -> Arguments.of(e));
    }

}
