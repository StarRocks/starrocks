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

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.wildfly.common.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Stream;

class SelectStmtWithCaseWhenTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp()
            throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        String createTblStmtStr = " CREATE TABLE `t0` (\n" +
                "  `region` varchar(128) NOT NULL COMMENT \"\",\n" +
                "  `order_date` date NOT NULL COMMENT \"\",\n" +
                "  `income` decimal(7, 0) NOT NULL COMMENT \"\",\n" +
                "  `ship_mode` int NOT NULL COMMENT \"\",\n" +
                "  `ship_code` int\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`region`, `order_date`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`order_date`)\n" +
                "(PARTITION p20220101 VALUES [(\"2022-01-01\"), (\"2022-01-02\")),\n" +
                "PARTITION p20220102 VALUES [(\"2022-01-02\"), (\"2022-01-03\")),\n" +
                "PARTITION p20220103 VALUES [(\"2022-01-03\"), (\"2022-01-04\")))\n" +
                "DISTRIBUTED BY HASH(`region`, `order_date`) BUCKETS 10 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"replicated_storage\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        String createTbl2StmtStr = " CREATE TABLE `t1` (\n" +
                "  `id` varchar(128) NOT NULL COMMENT \"\",\n" +
                "  `col_arr` array<varchar(100)> " +
                ") ENGINE=OLAP \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";

        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("test").useDatabase("test");
        starRocksAssert.withTable(createTblStmtStr);
        starRocksAssert.withTable(createTbl2StmtStr);
        FeConstants.enablePruneEmptyOutputScan = false;
        FeConstants.setLengthForVarchar = false;
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("caseWhenWithCaseClauses")
    void testCaseWhenWithCaseClause(String sql, List<String> patterns) throws Exception {
        test(sql, patterns);
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("caseWhenWithoutCaseClauses")
    void testCaseWhenWithoutCaseClause(String sql, List<String> patterns) throws Exception {
        test(sql, patterns);
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("caseWhenWithManyBranches")
    void testCaseWhenWithManyBranches(String sql, List<String> patterns) throws Exception {
        test(sql, patterns);
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("caseWhenWithNullableCol")
    void testCaseWhenWithNullableCol(String sql, List<String> patterns) throws Exception {
        test(sql, patterns);
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("ifTests")
    void testIf(String sql, List<String> patterns) throws Exception {
        test(sql, patterns);
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("caseInvolvingNull")
    void testCaseInvolvingNull(String sql, List<String> patterns) throws Exception {
        test(sql, patterns);
    }

    @Test
    void testConstantWhen() throws Exception {
        /// WHEN TRUE
        starRocksAssert.query("select case when random() > 10.0 then 1 when true then 2 else 10 end")
                .explainContains("if(random() > 10.0, 1, 2)");
        starRocksAssert.query("select case when random() > 10.0 then 1 " +
                        "   when true then 2 " +
                        "   when true then 3 " +
                        "   when random() <5 then 4 " +
                        "else 10 end")
                .explainContains("if(random() > 10.0, 1, 2)");

        // WHEN FALSE
        starRocksAssert.query("select case when random() > 10 then 1 when false then 2 else 10 end")
                .explainContains(" if(random() > 10.0, 1, 10)");
        starRocksAssert.query("select case when random() > 10 then 1 " +
                        "when false then 2 " +
                        "when false then 3 " +
                        "else 10 end")
                .explainContains(" if(random() > 10.0, 1, 10)");
    }

    private static Stream<Arguments> caseWhenWithCaseClauses() {
        String[][] testCases = new String[][] {
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) = 1",
                        "1: region = 'China'"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (1,2)",
                        "1: region IN ('China', 'Japan')"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) <> 1",
                        "1: region != 'China'"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) <> 2",
                        "1: region != 'Japan'"},
                {"select * from test.t0 where (case region when 'China' then 2 when 'Japan' then 2 else 3 end) <> 2",
                        "1: region NOT IN ('China', 'Japan')"},
                {"select * from test.t0 where (case region when 'China' then 2 when 'China' then 1 else 3 end) = 1", ""},
                {"select * from test.t0 where (case region when 'China' then 2 when 'China' then 1 else 3 end) <> 1", ""},
                {"select * from test.t0 where (case region when NULL then 2 when 'China' then 1 else 3 end) = 3",
                        "1: region != 'China'"},
                {"select * from test.t0 where (case region when NULL then 2 when 'China' then 1 else 3 end) = 1",
                        "1: region = 'China'"},
                {"select * from test.t0 where (case region when 'USA' then 5 when 'China' then 1 else 3 end) = 3",
                        "1: region != 'USA', 1: region != 'China'"},
                {"select * from test.t0 where (case region when 'USA' then 5 when 'UK' then 2 when 'China' then 1 end) =" + " 3",
                        ""},
                {"select * from test.t0 where (case region when 'USA' then 1 when 'UK' then 2 when 'China' then 2 end) =" + " 2",
                        "CASE 1: region WHEN 'USA' THEN 1 WHEN 'UK' THEN 2 WHEN 'China' THEN 2 END = 2"},
                {"select * from test.t0 where (case region when NULL then 2 when 'China' then 1 else 3 end) <> 1",
                        "1: region != 'China'"},
                {"select * from test.t0 where (case region when 'UK' then 2 when 'China' then 1 else 3 end) <> 3",
                        "1: region IN ('UK', 'China')"},
                {"select * from test.t0 where (case region when 'USA' then 5 when 'UK' then 2 when 'China' then 1 end) " + "<> 3",
                        "1: region IN ('USA', 'UK', 'China')"},
                {"select * from test.t0 where (case region when 'USA' then 1 when 'UK' then 2 when 'China' then 2 end) " + "<> 2",
                        "CASE 1: region WHEN 'USA' THEN 1 WHEN 'UK' THEN 2 WHEN 'China' THEN 2 END != 2"},
                {"select * from test.t0 where ((case region when 'USA' then 1 when 'UK' then 2 when 'China' then 2 end) "
                        + "<> 2) IS NULL",
                        "CASE 1: region WHEN 'USA' THEN 1 WHEN 'UK' THEN 2 WHEN 'China' THEN 2 END != 2 IS NULL"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (1)",
                        "1: region = 'China'"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in (1)",
                        "1: region != 'China'"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (3)",
                        "1: region != 'China', 1: region != 'Japan'"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in (3)",
                        "1: region IN ('China', 'Japan')"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) is null", ""},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) is not " + "null",
                        ""}, {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 end) is null",
                "1: region NOT IN ('China', 'Japan')"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 end) is not null",
                        "1: region IN ('China', 'Japan')"},
                {"select * from test.t0 where (case region when 'China' then NULL when 'Japan' then 2 end) is null",
                        "(1: region = 'China') OR (1: region NOT IN ('China', 'Japan'))"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then NULL end) is not null",
                        "1: region != 'Japan', 1: region IN ('China', 'Japan')"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  = 'b'",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN 'c' END = 'b'"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  != 'b'",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN 'c' END != 'b'"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  in "
                        + "('a', 'b', 'c', 'd')", "5: ship_code IN (1, 2, 3)"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' else 'e' "
                        + "end  in ('a', 'b', 'd')", "5: ship_code IN (1, 2)"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  in "
                        + "('e', 'f')", ""},
                {"select * from test.t0 where (case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  in "
                        + "('a', 'b', 'c', 'd')) is null",
                        "(((5: ship_code != 1) AND (5: ship_code != 2)) AND (5: ship_code != 3)) OR (((5: ship_code = 1) OR (5:"
                                + " ship_code = 2)) OR (5: ship_code = 3) IS NULL)"},
                {"select * from test.t0 where (case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  in "
                        + "('e', 'f')) is not null",
                        "((5: ship_code IN (1, 2, 3)) AND (5: ship_code IS NOT NULL)) OR ((5: ship_code NOT IN (1, 2, 3)) OR "
                                + "(5: ship_code IS NULL) IS NULL)"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then null  else 'e' "
                        + "end  = 'b'", "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN NULL ELSE 'e' END = 'b'"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then null else 'e' "
                        + "end  != 'b'",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN NULL ELSE 'e' END != 'b'"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then null else 'e' "
                        + "end in ('a', 'b', 'c', 'd')",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN NULL ELSE 'e' END IN ('a', 'b', 'c', "
                                + "'d')"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then null else 'e' "
                        + "end  in ('e', 'f')",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN NULL ELSE 'e' END IN ('e', 'f')"},
                {"select * from test.t0 where (case ship_code when 1 then 'a' when 2 then 'b' when 3 then null else 'e' "
                        + "end in ('a', 'b', 'c', 'd')) is null",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN NULL ELSE 'e' END IN ('a', 'b', 'c', "
                                + "'d') IS NULL"},
                {"select * from test.t0 where (case ship_code when 1 then 'a' when 2 then 'b' when 3 then null else 'e' "
                        + "end in ('a', 'b', 'c', 'd')) is not null",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN NULL ELSE 'e' END IN ('a', 'b', 'c', "
                                + "'d') IS NOT NULL"},
                {"select * from test.t0 where case ship_code when ship_mode + 1 then 'a' when ship_mode + 2 then 'b' "
                        + "when 3 then 'c' end in ('b', 'c', 'd')",
                        "CASE CAST(5: ship_code AS BIGINT) WHEN CAST(4: ship_mode AS BIGINT) + 1 THEN 'a' WHEN CAST(4: "
                                + "ship_mode AS BIGINT) + 2 THEN 'b' WHEN 3 THEN 'c' END IN ('b', 'c', 'd')"},
                {"select * from test.t0 where case ship_code when ship_mode + 1 then 'a' when ship_mode + 2 then 'b' "
                        + "else 'e' end in ('a', 'b', 'c', 'd')",
                        "(CAST(5: ship_code AS BIGINT) = CAST(4: ship_mode AS BIGINT) + 1) OR ((CAST(5: ship_code AS BIGINT) = "
                                + "CAST(4: ship_mode AS BIGINT) + 2) AND ((CAST(5: ship_code AS BIGINT) != CAST(4: ship_mode AS"
                                + " BIGINT) + 1) OR (CAST(5: ship_code AS BIGINT) IS NULL)))"},
                {"select * from test.t0 where (case ship_code when ship_mode + 1 then 'a' when ship_mode + 2 then 'b' "
                        + "else 'e' end in ('a', 'b', 'c', 'd')) is null", ""},
                {"select * from test.t0 where (case ship_code when ship_mode + 1 then 'a' when ship_mode + 2 then 'b' "
                        + "else 'e' end in ('a', 'b', 'c', 'd')) is not null", ""},
                {"select * from test.t0 where (case region when 'USA' then 1 when 'UK' then 2 else 3  end in (2, 3, "
                        + "null)) is null",
                        "CASE 1: region WHEN 'USA' THEN 1 WHEN 'UK' THEN 2 ELSE 3 END IN (2, 3, NULL) IS NULL"},

        };

        List<Arguments> argumentsList = Lists.newArrayList();
        for (String[] tc : testCases) {
            String sql = tc[0];
            argumentsList.add(Arguments.of(sql, Arrays.asList(tc).subList(1, tc.length)));
        }
        return argumentsList.stream();
    }

    private static Stream<Arguments> caseWhenWithoutCaseClauses() {
        String[][] testCases = new String[][] {
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) = 1",
                        "1: region = 'China'"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (1,2)",
                        "1: region IN ('China', 'Japan')"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) <> 1",
                        "1: region != 'China'"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) <> 2",
                        "1: region != 'Japan'"},
                {"select * from test.t0 where (case region when 'China' then 2 when 'Japan' then 2 else 3 end) <> 2",
                        "1: region NOT IN ('China', 'Japan')"},
                {"select * from test.t0 where (case region when 'China' then 2 when 'China' then 1 else 3 end) = 1", ""},
                {"select * from test.t0 where (case region when 'China' then 2 when 'China' then 1 else 3 end) <> 1", ""},
                {"select * from test.t0 where (case region when NULL then 2 when 'China' then 1 else 3 end) = 3",
                        "1: region != 'China'"},
                {"select * from test.t0 where (case region when NULL then 2 when 'China' then 1 else 3 end) = 1",
                        "1: region = 'China'"},
                {"select * from test.t0 where (case region when 'USA' then 5 when 'China' then 1 else 3 end) = 3",
                        "1: region != 'USA', 1: region != 'China'"},
                {"select * from test.t0 where (case region when 'USA' then 5 when 'UK' then 2 when 'China' then 1 end) =" + " 3",
                        ""},
                {"select * from test.t0 where (case region when 'USA' then 1 when 'UK' then 2 when 'China' then 2 end) =" + " 2",
                        "CASE 1: region WHEN 'USA' THEN 1 WHEN 'UK' THEN 2 WHEN 'China' THEN 2 END = 2"},
                {"select * from test.t0 where (case region when NULL then 2 when 'China' then 1 else 3 end) <> 1",
                        "1: region != 'China'"},
                {"select * from test.t0 where (case region when 'UK' then 2 when 'China' then 1 else 3 end) <> 3",
                        "1: region IN ('UK', 'China')"},
                {"select * from test.t0 where (case region when 'USA' then 5 when 'UK' then 2 when 'China' then 1 end) " + "<> 3",
                        "1: region IN ('USA', 'UK', 'China')"},
                {"select * from test.t0 where (case region when 'USA' then 1 when 'UK' then 2 when 'China' then 2 end) " + "<> 2",
                        "CASE 1: region WHEN 'USA' THEN 1 WHEN 'UK' THEN 2 WHEN 'China' THEN 2 END != 2"},
                {"select * from test.t0 where ((case region when 'USA' then 1 when 'UK' then 2 when 'China' then 2 end) "
                        + "<> 2) IS NULL",
                        "CASE 1: region WHEN 'USA' THEN 1 WHEN 'UK' THEN 2 WHEN 'China' THEN 2 END != 2 IS NULL"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (1)",
                        "1: region = 'China'"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in (1)",
                        "1: region != 'China'"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (3)",
                        "1: region != 'China', 1: region != 'Japan'"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in (3)",
                        "1: region IN ('China', 'Japan')"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) is null", ""},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) is not " + "null",
                        ""}, {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 end) is null",
                "1: region NOT IN ('China', 'Japan')"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 end) is not null",
                        "1: region IN ('China', 'Japan')"},
                {"select * from test.t0 where (case region when 'China' then NULL when 'Japan' then 2 end) is null",
                        "(1: region = 'China') OR (1: region NOT IN ('China', 'Japan'))"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then NULL end) is not null",
                        "1: region != 'Japan', 1: region IN ('China', 'Japan')"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  = 'b'",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN 'c' END = 'b'"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  != 'b'",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN 'c' END != 'b'"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  in "
                        + "('a', 'b', 'c', 'd')", "5: ship_code IN (1, 2, 3)"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' else 'e' "
                        + "end  in ('a', 'b', 'd')", "5: ship_code IN (1, 2)"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  in "
                        + "('e', 'f')", ""},
                {"select * from test.t0 where (case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  in "
                        + "('a', 'b', 'c', 'd')) is null",
                        "(((5: ship_code != 1) AND (5: ship_code != 2)) AND (5: ship_code != 3)) OR (((5: ship_code = 1) OR (5:"
                                + " ship_code = 2)) OR (5: ship_code = 3) IS NULL)"},
                {"select * from test.t0 where (case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  in "
                        + "('e', 'f')) is not null",
                        "((5: ship_code IN (1, 2, 3)) AND (5: ship_code IS NOT NULL)) OR ((5: ship_code NOT IN (1, 2, 3)) OR "
                                + "(5: ship_code IS NULL) IS NULL)"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then null  else 'e' "
                        + "end  = 'b'", "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN NULL ELSE 'e' END = 'b'"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then null else 'e' "
                        + "end  != 'b'",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN NULL ELSE 'e' END != 'b'"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then null else 'e' "
                        + "end in ('a', 'b', 'c', 'd')",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN NULL ELSE 'e' END IN ('a', 'b', 'c', "
                                + "'d')"},
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then null else 'e' "
                        + "end  in ('e', 'f')",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN NULL ELSE 'e' END IN ('e', 'f')"},
                {"select * from test.t0 where (case ship_code when 1 then 'a' when 2 then 'b' when 3 then null else 'e' "
                        + "end in ('a', 'b', 'c', 'd')) is null",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN NULL ELSE 'e' END IN ('a', 'b', 'c', "
                                + "'d') IS NULL"},
                {"select * from test.t0 where (case ship_code when 1 then 'a' when 2 then 'b' when 3 then null else 'e' "
                        + "end in ('a', 'b', 'c', 'd')) is not null",
                        "CASE 5: ship_code WHEN 1 THEN 'a' WHEN 2 THEN 'b' WHEN 3 THEN NULL ELSE 'e' END IN ('a', 'b', 'c', "
                                + "'d') IS NOT NULL"},
                {"select * from test.t0 where case ship_code when ship_mode + 1 then 'a' when ship_mode + 2 then 'b' "
                        + "when 3 then 'c' end in ('b', 'c', 'd')",
                        "CASE CAST(5: ship_code AS BIGINT) WHEN CAST(4: ship_mode AS BIGINT) + 1 THEN 'a' WHEN CAST(4: "
                                + "ship_mode AS BIGINT) + 2 THEN 'b' WHEN 3 THEN 'c' END IN ('b', 'c', 'd')"},
                {"select * from test.t0 where case ship_code when ship_mode + 1 then 'a' when ship_mode + 2 then 'b' "
                        + "else 'e' end in ('a', 'b', 'c', 'd')",
                        "(CAST(5: ship_code AS BIGINT) = CAST(4: ship_mode AS BIGINT) + 1) OR ((CAST(5: ship_code AS BIGINT) = "
                                + "CAST(4: ship_mode AS BIGINT) + 2) AND ((CAST(5: ship_code AS BIGINT) != CAST(4: ship_mode AS"
                                + " BIGINT) + 1) OR (CAST(5: ship_code AS BIGINT) IS NULL)))"},
                {"select * from test.t0 where (case ship_code when ship_mode + 1 then 'a' when ship_mode + 2 then 'b' "
                        + "else 'e' end in ('a', 'b', 'c', 'd')) is null", ""},
                {"select * from test.t0 where (case ship_code when ship_mode + 1 then 'a' when ship_mode + 2 then 'b' "
                        + "else 'e' end in ('a', 'b', 'c', 'd')) is not null", ""},
                {"select * from test.t0 where (case region when 'USA' then 1 when 'UK' then 2 else 3  end in (2, 3, "
                        + "null)) is null",
                        "CASE 1: region WHEN 'USA' THEN 1 WHEN 'UK' THEN 2 ELSE 3 END IN (2, 3, NULL) IS NULL"},
        };

        List<Arguments> argumentsList = Lists.newArrayList();
        for (String[] tc : testCases) {
            String sql = tc[0];
            argumentsList.add(Arguments.of(sql, Arrays.asList(tc).subList(1, tc.length)));
        }
        return argumentsList.stream();
    }

    private static Stream<Arguments> caseWhenWithManyBranches() {
        String sqlTemp = "select * from test.t0 where (case \n" +
                "   when ship_mode >= 90 then 'A'\n" +
                "   when ship_mode >= 80 then 'B'\n" +
                "   when ship_mode >= 70 then 'C'\n" +
                "   when ship_mode >= 60 then 'D'\n" +
                "   else 'E' end)  %s";
        String[][] testCases = new String[][] {
                {"= 'A'", "4: ship_mode >= 90"},
                {"= 'B'", "4: ship_mode >= 80, 4: ship_mode < 90"},
                {"= 'C'", "4: ship_mode >= 70, 4: ship_mode < 80"},
                {"= 'D'", "4: ship_mode >= 60, 4: ship_mode < 70"},
                {"= 'E'", "4: ship_mode < 60"}, {"<> 'A'", "4: ship_mode < 90"},
                {"<> 'B'", "(4: ship_mode < 80) OR (4: ship_mode >= 90)"},
                {"<> 'C'", "(4: ship_mode < 70) OR ((4: ship_mode >= 90) OR (4: ship_mode >= 80))"}, {"<> 'D'",
                        "(4: ship_mode < 60) OR (((4: ship_mode >= 90) OR (4: ship_mode >= 80)) OR (4: ship_mode >= 70))"},
                {"<> 'E'", "((4: ship_mode >= 90) OR ((4: ship_mode >= 80) AND (4: ship_mode < 90))) OR (((4: ship_mode < "
                                        + "90) AND "
                                        + "(4: ship_mode < 80)) AND ((4: ship_mode >= 70) OR ((4: ship_mode >= 60) AND (4: "
                                        + "ship_mode < "
                                        + "70)))), 4: ship_mode >= 60"}, {"in ('A','B')",
                        "(4: ship_mode >= 90) OR ((4: ship_mode >= 80) AND (4: ship_mode < 90)), 4: ship_mode >= 80"},
                {"in ('A','B', 'C')",
                        "((4: ship_mode >= 90) OR ((4: ship_mode >= 80) AND (4: ship_mode < 90))) OR ((4: ship_mode >= "
                                        + "70) AND "
                                        + "((4: ship_mode < 90) AND (4: ship_mode < 80))), 4: ship_mode >= 70"}, {"in ('D','E')",
                        "4: ship_mode < 90, 4: ship_mode < 80, 4: ship_mode < 70, (4: ship_mode >= 60) OR (4: ship_mode < 60)"},
                {"in ('E')", "4: ship_mode < 60"},
                {"in (NULL)", ""},
                {"in ('F')", ""},
                {"in ('A','B','C','D','E')", ""},
                {"in ('A','B','C','D','E','F')", ""},
                {"not in ('A','B')", "4: ship_mode < 90, (4: ship_mode < 80) OR (4: ship_mode >= 90)"},
                {"not in ('A','B', 'C')",
                        "4: ship_mode < 90, 4: ship_mode < 80, 4: ship_mode < 70, (4: ship_mode >= 60) OR (4: ship_mode"
                                        + " < 60)"},
                {"not in ('D','E')",
                        "((4: ship_mode >= 90) OR ((4: ship_mode >= 80) AND (4: ship_mode < 90))) OR ((4: ship_mode >= "
                                        + "70) AND "
                                + "((4: ship_mode < 90) AND (4: ship_mode < 80))), 4: ship_mode >= 70"},
                {"not in ('E')",
                        "((4: ship_mode >= 90) OR ((4: ship_mode >= 80) AND (4: ship_mode < 90))) OR (((4: ship_mode < 90) AND "
                                + "(4: ship_mode < 80)) AND ((4: ship_mode >= 70) OR ((4: ship_mode >= 60) AND (4: ship_mode < "
                                + "70)))), 4: ship_mode >= 60"},
                {"not in (NULL)", ""},
                {"not in ('A','B','C','D','E')", ""},
                {"not in ('A','B','C','D','E','F')", ""},
                {"is NULL", ""},
                {"is NOT NULL", ""},
        };

        List<Arguments> argumentsList = Lists.newArrayList();
        for (String[] tc : testCases) {
            String sql = String.format(sqlTemp, tc[0]);
            argumentsList.add(Arguments.of(sql, Arrays.asList(tc).subList(1, tc.length)));
        }

        return argumentsList.stream();
    }

    private static Stream<Arguments> caseWhenWithNullableCol() {
        String sqlTemp = "select * from test.t0 where (case \n" +
                "   when ship_code >= 90 then 'A'\n" +
                "   when ship_code >= 80 then 'B'\n" +
                "   when ship_code >= 70 then 'C'\n" +
                "   when ship_code >= 60 then 'D'\n" +
                "   else 'E' end)  %s";
        String[][] testCases = new String[][] {
                {"= 'A'", "5: ship_code >= 90"},
                {"= 'B'", "5: ship_code >= 80, (5: ship_code < 90) OR (5: ship_code IS NULL)"},
                {"= 'C'", "5: ship_code >= 70, (5: ship_code < 90) OR (5: ship_code IS NULL), (5: ship_code < 80) OR (5: "
                        + "ship_code IS NULL)"},
                {"<> 'D'",
                        "CASE WHEN 5: ship_code >= 90 THEN 'A' WHEN 5: ship_code >= 80 THEN 'B' WHEN 5: ship_code >= 70 THEN "
                                + "'C' WHEN 5: ship_code >= 60 THEN 'D' ELSE 'E' END != 'D'"},
                {"<> 'E'",
                        "((5: ship_code >= 90) OR ((5: ship_code >= 80) AND ((5: ship_code < 90) OR (5: ship_code IS NULL)))) "
                                + "OR ((((5: ship_code < 90) OR (5: ship_code IS NULL)) AND ((5: ship_code < 80) OR (5: "
                                + "ship_code IS NULL))) AND ((5: ship_code >= 70) OR ((5: ship_code >= 60) AND ((5: ship_code <"
                                + " 70) OR (5: ship_code IS NULL))))), 5: ship_code >= 60"},
                {"in ('A','B')",
                        "(5: ship_code >= 90) OR ((5: ship_code >= 80) AND ((5: ship_code < 90) OR (5: ship_code IS NULL))), 5:"
                                + " ship_code >= 80"},
                {"in ('A','B', 'C')",
                        "((5: ship_code >= 90) OR ((5: ship_code >= 80) AND ((5: ship_code < 90) OR (5: ship_code IS NULL)))) "
                                + "OR ((5: ship_code >= 70) AND (((5: ship_code < 90) OR (5: ship_code IS NULL)) AND ((5: "
                                + "ship_code < 80) OR (5: ship_code IS NULL)))), 5: ship_code >= 70"},
                {"in ('D','E')",
                        "CASE WHEN 5: ship_code >= 90 THEN 'A' WHEN 5: ship_code >= 80 THEN 'B' WHEN 5: ship_code >= 70 THEN "
                                + "'C' WHEN 5: ship_code >= 60 THEN 'D' ELSE 'E' END IN ('D', 'E')"},
                {"in ('E')",
                        "CASE WHEN 5: ship_code >= 90 THEN 'A' WHEN 5: ship_code >= 80 THEN 'B' WHEN 5: ship_code >= 70 THEN "
                                + "'C' WHEN 5: ship_code >= 60 THEN 'D' ELSE 'E' END = 'E'"},
                {"in (NULL)", ""},
                {"in ('F')", ""},
                {"in ('A','B','C','D','E')", ""},
                {"in ('A','B','C','D','E','F')", ""},
                {"not in ('A','B', 'C')",
                        "CASE WHEN 5: ship_code >= 90 THEN 'A' WHEN 5: ship_code >= 80 THEN 'B' WHEN 5: ship_code >= 70 THEN "
                                + "'C' WHEN 5: ship_code >= 60 THEN 'D' ELSE 'E' END NOT IN ('A', 'B', 'C')"},
                {"not in ('D','E')",
                        "((5: ship_code >= 90) OR ((5: ship_code >= 80) AND ((5: ship_code < 90) OR (5: ship_code IS NULL)))) "
                                + "OR ((5: ship_code >= 70) AND (((5: ship_code < 90) OR (5: ship_code IS NULL)) AND ((5: "
                                + "ship_code < 80) OR (5: ship_code IS NULL)))), 5: ship_code >= 70"},
                {"not in ('E')",
                        "((5: ship_code >= 90) OR ((5: ship_code >= 80) AND ((5: ship_code < 90) OR (5: ship_code IS NULL)))) "
                                + "OR ((((5: ship_code < 90) OR (5: ship_code IS NULL)) AND ((5: ship_code < 80) OR (5: "
                                + "ship_code IS NULL))) AND ((5: ship_code >= 70) OR ((5: ship_code >= 60) AND ((5: ship_code <"
                                + " 70) OR (5: ship_code IS NULL))))), 5: ship_code >= 60"},
                {"not in (NULL)", ""},
                {"not in ('A','B','C','D','E','F')", ""},
                {"is NULL", ""},
                {"is NOT NULL", ""},

        };

        List<Arguments> argumentsList = Lists.newArrayList();
        for (String[] tc : testCases) {
            String sql = String.format(sqlTemp, tc[0]);
            argumentsList.add(Arguments.of(sql, Arrays.asList(tc).subList(1, tc.length)));
        }

        return argumentsList.stream();
    }

    private static Stream<Arguments> ifTests() {
        String[][] testCases = new String[][] {
                {"select * from test.t0 where if(region='USA', 1, 0) = 1", "1: region = 'USA'"},
                {"select * from test.t0 where if(region='USA', 1, 0) = 0", "1: region != 'USA'"},
                {"select * from test.t0 where if(region='USA', 1, 0) = 2", ""},
                {"select * from test.t0 where if(region='USA', 1, 0) <> 1", "1: region != 'USA'"},
                {"select * from test.t0 where if(region='USA', 1, 0) <> 0", "1: region = 'USA'"},
                {"select * from test.t0 where if(region='USA', 1, 0) <> 2", ""},
                {"select * from test.t0 where if(region='USA', 1, 0) in (1)", "1: region = 'USA'"},
                {"select * from test.t0 where if(region='USA', 1, 0) in (1,0)", ""},
                {"select * from test.t0 where if(region='USA', 1, 0) in (2,3)", ""},
                {"select * from test.t0 where (if(region='USA', 1, 0) in (2,3, null))",
                        "if(1: region = 'USA', 1, 0) IN (2, 3, NULL)"},
                {"select * from test.t0 where (if(region='USA', 1, 0) in (2,3, null)) is null",
                        "if(1: region = 'USA', 1, 0) IN (2, 3, NULL) IS NULL"},
                {"select * from test.t0 where if(region='USA', 1, 0) not in (0)", "1: region = 'USA'"},
                {"select * from test.t0 where if(region='USA', 1, 0) not in (0,1)", ""},
                {"select * from test.t0 where if(region='USA', 1, 0) not in (2,3)", ""},
                {"select * from test.t0 where if(region='USA', 1, 0) is NULL", ""},
                {"select * from test.t0 where if(region='USA', 1, 0) is NOT NULL", ""},
                {"select * from test.t0 where if(ship_code is null, null, 0) is NULL", "5: ship_code IS NULL"},
                {"select * from test.t0 where if(ship_code is null or ship_code > 2, 2, 1) > 1",
                        "if((5: ship_code IS NULL) OR (5: ship_code > 2), 2, 1) > 1"},
                {"select * from test.t0 where if(ship_code is null or ship_code > 2, 2, 1) != 2",
                        "if((5: ship_code IS NULL) OR (5: ship_code > 2), 2, 1) != 2"},
                {"select * from test.t0 where if(ship_code is null or ship_code > 2, 1, 0) is NOT NULL", ""},
                {"with tmp as (select ship_mode, if(ship_code > 4, 1, 0) as layer0, if (ship_code >= 1 and ship_code <= "
                        + "4, 1, 0) as layer1,if(ship_code is null or ship_code < 1, 1, 0) as layer2 from t0) select * "
                        + "from tmp where layer2 = 1 and layer0 != 1 and layer1 !=1",
                        "(5: ship_code IS NULL) OR (5: ship_code < 1), if(5: ship_code > 4, 1, 0) != 1, if((5: ship_code >= 1) "
                                + "AND (5: ship_code <= 4), 1, 0) != 1"},
                {"select * from test.t0 where nullif('China', region) = 'China'", "1: region != 'China'"},
                {"select * from test.t0 where nullif('China', region) <> 'China'", ""},
                {"select * from test.t0 where nullif('China', region) is NULL", "1: region = 'China'"},
                {"select * from test.t0 where (nullif('China', region) is NULL) is NULL", ""},
                {"select * from test.t0 where (nullif('China', region) is NULL) is NOT NULL", ""},
                {"select * from test.t0 where nullif('China', region) is NOT NULL", "1: region != 'China'"},
                {"select * from test.t0 where (nullif('China', region) is NOT NULL) is NULL",
                        "1: region != 'China' IS NULL"},
                {"select * from test.t0 where (nullif('China', region) is NOT NULL) is NOT NULL",
                        "1: region != 'China' IS NOT NULL"}, {"select * from test.t0 where nullif('China', region) = 'USA'", ""},
                {"select * from test.t0 where nullif('China', region) <>  'USA'", "1: region != 'China'"},
                {"select * from test.t0 where nullif(1, ship_code) = 1",
                        "(5: ship_code != 1) OR (5: ship_code IS NULL)"},
                {"select * from test.t0 where nullif(1, ship_code) <> 1", ""},
                {"select * from test.t0 where nullif(1, ship_code) is NULL", "5: ship_code = 1"},
                {"select * from test.t0 where (nullif(1, ship_code) is NULL) is NULL", ""},
                {"select * from test.t0 where (nullif(1, ship_code) is NULL) is NOT NULL", ""},
                {"select * from test.t0 where nullif(1, ship_code) is NOT NULL",
                        "(5: ship_code != 1) OR (5: ship_code IS NULL)"},
                {"select * from test.t0 where (nullif(1, ship_code) is NOT NULL) is NULL",
                        "(5: ship_code != 1) OR (5: ship_code IS NULL) IS NULL"},
                {"select * from test.t0 where (nullif(1, ship_code) is NOT NULL) is NOT NULL",
                        "(5: ship_code != 1) OR (5: ship_code IS NULL) IS NOT NULL"},
                {"select * from test.t0 where nullif(1, ship_code) = 2", ""},
                {"select * from test.t0 where nullif(1, ship_code) <>  2",
                        "(5: ship_code != 1) OR (5: ship_code IS NULL)"},
        };

        List<Arguments> argumentsList = Lists.newArrayList();
        for (String[] tc : testCases) {
            String sql = tc[0];
            argumentsList.add(Arguments.of(sql, Arrays.asList(tc).subList(1, tc.length)));
        }
        return argumentsList.stream();
    }

    private static Stream<Arguments> caseInvolvingNull() {
        String[][] testCases = new String[][] {
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) = NULL", ""},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) <> NULL", ""},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) <=> NULL",
                        "CASE 1: region WHEN 'China' THEN 1 WHEN 'Japan' THEN 2 ELSE 3 END <=> NULL"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (1," + "NULL)",
                        "CASE 1: region WHEN 'China' THEN 1 WHEN 'Japan' THEN 2 ELSE 3 END IN (1, NULL)"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in "
                        + "(1,NULL)", "CASE 1: region WHEN 'China' THEN 1 WHEN 'Japan' THEN 2 ELSE 3 END NOT IN (1, NULL)"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (NULL,"
                        + "NULL)", "CASE 1: region WHEN 'China' THEN 1 WHEN 'Japan' THEN 2 ELSE 3 END IN (NULL, NULL)"},
                {"select * from test.t0 where (case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in "
                        + "(NULL,NULL)", "CASE 1: region WHEN 'China' THEN 1 WHEN 'Japan' THEN 2 ELSE 3 END NOT IN (NULL, NULL)"},
                {"select * from test.t0 where if (region = 'China', 1, 2) = NULL", ""},
                {"select * from test.t0 where if (region = 'China', 1, 2) not in (NULL, 1)",
                        "if(1: region = 'China', 1, 2) NOT IN (NULL, 1)"},
                {"select * from test.t0 where (case when ship_code is null then true when 1 then false end) is null", ""},
                {"select * from test.t0 where (case when ship_code is null then true when 0 then false end) is null",
                        "5: ship_code IS NOT NULL"},
                {"select * from test.t0 where (case when ship_code is null then true when null then false end) is null",
                        "5: ship_code IS NOT NULL"},
                {"select * from test.t0 where (case when ship_code is null then true when not null then false end) is " + "null",
                        "5: ship_code IS NOT NULL"},
                {"select * from test.t0 where (case when ship_code is null then true when not 3 then false end) is null",
                        "5: ship_code IS NOT NULL, (CAST(3 AS BOOLEAN)) OR (CAST(3 AS BOOLEAN) IS NULL)"},
                {"select * from test.t0 where (case when region = 'USA' then true when region like 'COM%' then false "
                        + "end) is null", "1: region != 'USA', NOT (1: region LIKE 'COM%')"},
                {"select * from test.t0 where (case when case when null then 'A' when false then 'B' when region like "
                        + "'COM%' then 'C' end = 'C' then true else false end) is null", ""},
        };
        List<Arguments> argumentsList = Lists.newArrayList();
        for (String[] tc : testCases) {
            String sql = tc[0];
            argumentsList.add(Arguments.of(sql, Arrays.asList(tc).subList(1, tc.length)));
        }
        return argumentsList.stream();
    }

    private void test(String sql, List<String> patterns) throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(3000000);
        starRocksAssert.getCtx().getSessionVariable().setEnablePredicateExprReuse(false);
        String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), sql);
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add(sql);
        joiner.add(patterns.toString());
        joiner.add(plan);

        if (patterns.stream().anyMatch(String::isBlank)) {
            Assertions.assertFalse(plan.contains("PREDICATES:"), joiner.toString());
        } else {
            Assertions.assertTrue(patterns.stream().anyMatch(plan::contains), joiner.toString());
        }

        //        String code = "{\n\"" + sql.replace("\n", " ") + "\",\n" + Arrays.stream(plan.split("\n"))
        //                .filter(c -> c.contains("PREDICATES:")).map(c -> c.replace("PREDICATES:", "").trim())
        //                .collect(Collectors.joining(", ", "\"", "\"")) + "},";
        //        System.out.println(code);
    }

    @Test
    public void testNotSimplifyCaseWhenSkipComplexFunctions() throws Exception {
        String sql = "WITH cte01 AS (\n" +
                "  SELECT\n" +
                "    id,\n" +
                "    (\n" +
                "      CASE\n" +
                "        WHEN (ARRAY_LENGTH(col_arr) < 2) THEN \"bucket1\"\n" +
                "        WHEN ((ARRAY_LENGTH(col_arr) >= 2) AND (ARRAY_LENGTH(col_arr) < 4)) THEN \"bucket2\"\n" +
                "        ELSE NULL\n" +
                "        END\n" +
                "      ) AS len_bucket\n" +
                "  FROM\n" +
                "    t1\n" +
                ")\n" +
                "SELECT id, len_bucket\n" +
                "FROM cte01\n" +
                "WHERE len_bucket IS NOT NULL;";
        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
        Assert.assertTrue(plan.contains("CASE WHEN array_length"));
    }
}
