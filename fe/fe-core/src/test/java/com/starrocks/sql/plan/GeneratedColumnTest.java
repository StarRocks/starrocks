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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GeneratedColumnTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE `tmc` (\n" +
                "  `v1` bigint(20) NULL COMMENT \"\",\n" +
                "  `v2` bigint(20) NULL COMMENT \"\",\n" +
                "  `v3` bigint(20) NULL AS v1 + 1 COMMENT \"\",\n" +
                "  `v4` bigint(20) NULL AS v1 + v2 COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"replicated_storage\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `tmc2` (\n" +
                "  `v1` bigint(20) NULL COMMENT \"\",\n" +
                "  `v2` array<bigint> NULL COMMENT \"\",\n" +
                "  `v3` bigint(20) NULL AS array_max(v2) COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"replicated_storage\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ")");

        starRocksAssert.withView("create view view_1 as select v3 from tmc;");

        starRocksAssert.withView("create view view_2 as select v3, v4 from tmc;");
    }

    @Test
    public void test() throws Exception {
        String sql = "select v1+v2 from tmc";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:4: v4");

        sql = "select tmc.v1 + tmc.v2 from tmc";
        plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:4: v4");

        sql = "select v1+1 from tmc";
        plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:3: v3");

        sql = "select v1+1 from tmc order by v1+v2;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:3: v3");
        assertContains(plan, "order by: <slot 4> 4: v4 ASC");

        sql = "select v1+1 as v from tmc order by v";
        plan = getFragmentPlan(sql);
        assertContains(plan, "order by: <slot 3> 3: v3 ASC");

        sql = "select v1+1 as v from tmc order by v1+v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:3: v3");
        assertContains(plan, "order by: <slot 4> 4: v4 ASC");

        sql = "select v1 from tmc where v1+v2 = v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:1: v1");
        assertContains(plan, "PREDICATES: 4: v4 = 2: v2");

        sql = "select v1+v2 from tmc where v1+v2 = 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:4: v4");
        assertContains(plan, "PREDICATES: 4: v4 = 2");

        sql = "select v1+v2 from tmc group by v1+v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:4: v4");
        assertContains(plan, "group by: 4: v4");

        sql = "select sum(v1+v2) from tmc";
        plan = getFragmentPlan(sql);
        assertContains(plan, "output: sum(4: v4)");

        sql = "select v1+v2 as v,sum(v1) from tmc group by v having v = 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "group by: 4: v4");

        sql = "select sum(v1+v2) over() from tmc";
        plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, sum(4: v4), ]");

        sql = "select array_max(v2) from tmc2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:3: v3");

        sql = "select tmc.v1 + 1 from tmc,tmc2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "<slot 3> : 3: v3");

        sql = "select v1+1 from tmc union all (select v1 from tmc2)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:8: v3");

        sql = "select v1+1 from tmc union all (select v1 from tmc2)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "OUTPUT EXPRS:8: v3");

        //Plan Can not be rewrite
        sql = "select v1+v2 from tmc group by v1,v2";
        plan = getFragmentPlan(sql);

        assertContains(plan, "OUTPUT EXPRS:5: expr");
        assertContains(plan, " group by: 1: v1, 2: v2");

        sql = " select v.v1 + 1 from tmc as v,tmc2 as tmc";
        plan = getFragmentPlan(sql);
        assertContains(plan, "<slot 3> : 3: v3");

        sql = " select v.v1 + 1 from tmc as v,tmc2 as tmc";
        plan = getFragmentPlan(sql);
        assertContains(plan, "<slot 3> : 3: v3");

        sql = " select * from view_1";
        plan = getFragmentPlan(sql);

        sql = " select * from view_2";
        plan = getFragmentPlan(sql);
    }

    // The generated column expression kept in Column is never analyzed, so collection constructors in it still
    // carry the parser's placeholder types: null for [...] and ANY_MAP for map{...} / map(k, v). SHOW CREATE TABLE
    // must render them as valid SQL that can be executed again.
    @Test
    public void testShowCreateTableWithCollectionLiteralInGeneratedColumn() throws Exception {
        String[][] cases = {
                // {column definition, expected rendering for non primary key tables}
                {"c boolean AS array_contains([a, b], 3)", "AS array_contains([`a`, `b`], 3)"},
                {"m varchar(20) AS map{'k': s}['k']", "AS map{'k':`s`}['k']"},
                {"m varchar(20) AS map('k', s)['k']", "AS map{'k':`s`}['k']"},
                {"x int AS named_struct('f', a).f", "AS named_struct('f', `a`).`f`"},
                {"d2 boolean AS array_contains(ARRAY<DATE>['2020-01-01'], d)",
                        "AS array_contains(ARRAY<DATE>['2020-01-01'], `d`)"},
        };
        String[] keyTypes = {"DUPLICATE KEY(id)", "PRIMARY KEY(id)"};
        int idx = 0;
        for (String keyType : keyTypes) {
            for (String[] c : cases) {
                String table = "gen_col_collection_" + idx;
                String copy = table + "_copy";
                idx++;
                starRocksAssert.withTable("CREATE TABLE " + table + " (id int NOT NULL, a int, b int, " +
                        "s varchar(20), d date, " + c[0] + ") " + keyType +
                        " DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
                try {
                    String ddl = starRocksAssert.showCreateTable("show create table " + table);
                    Assertions.assertFalse(ddl.contains("PseudoType"), ddl);
                    if (keyType.startsWith("DUPLICATE")) {
                        assertContains(ddl, c[1]);
                    }

                    // The rendered DDL must be executable and render the same way again.
                    starRocksAssert.withTable(ddl.replace("`" + table + "`", "`" + copy + "`"));
                    String copyDdl = starRocksAssert.showCreateTable("show create table " + copy);
                    Assertions.assertEquals(ddl.replace("`" + table + "`", "`" + copy + "`"), copyDdl);
                } finally {
                    starRocksAssert.dropTable("if exists " + table);
                    starRocksAssert.dropTable("if exists " + copy);
                }
            }
        }
    }
}
