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
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
                "  `ship_code` int" +
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
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";

        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("test").useDatabase("test");
        starRocksAssert.withTable(createTblStmtStr);
        FeConstants.enablePruneEmptyOutputScan = false;
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

    private static Stream<Arguments> caseWhenWithCaseClauses() {
        String[][] testCases = new String[][] {
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) = 1",
                        "Predicates: [1: region, VARCHAR, false] = 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (1,2)",
                        "Predicates: 1: region IN ('China', 'Japan')"
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) <> 1",
                        "Predicates: [1: region, VARCHAR, false] != 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) <> 2",
                        "Predicates: [1: region, VARCHAR, false] != 'Japan'",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 2 when 'Japan' then 2 else 3 end) <> 2",
                        "Predicates: 1: region NOT IN ('China', 'Japan')",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 2 when 'China' then 1 else 3 end) = 1",
                        "0:EMPTYSET",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 2 when 'China' then 1 else 3 end) <> 1",
                        "  0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on\n" +
                                "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                                "     tabletList=\n" +
                                "     actualRows=0, avgRowSize=5.0\n" +
                                "     cardinality: 1",
                },
                {"select * from test.t0 where \n" +
                        "(case region when NULL then 2 when 'China' then 1 else 3 end) = 3",
                        "Predicates: [1: region, VARCHAR, false] != 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case region when NULL then 2 when 'China' then 1 else 3 end) = 1",
                        "Predicates: [1: region, VARCHAR, false] = 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'USA' then 5 when 'China' then 1 else 3 end) = 3",
                        "[1: region, VARCHAR, false] != 'USA', [1: region, VARCHAR, false] != 'China'",
                },

                {"select * from test.t0 where \n" +
                        "(case region when 'USA' then 5 when 'UK' then 2 when 'China' then 1 end) = 3",
                        "0:EMPTYSET",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'USA' then 1 when 'UK' then 2 when 'China' then 2 end) = 2",
                        "Predicates: CASE 1: region WHEN",
                },
                {"select * from test.t0 where \n" +
                        "(case region when NULL then 2 when 'China' then 1 else 3 end) <> 1",
                        "Predicates: [1: region, VARCHAR, false] != 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'UK' then 2 when 'China' then 1 else 3 end) <> 3",
                        "Predicates: 1: region IN ('UK', 'China')"
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'USA' then 5 when 'UK' then 2 when 'China' then 1 end) <> 3",
                        "Predicates: 1: region IN ('USA', 'UK', 'China')",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'USA' then 1 when 'UK' then 2 when 'China' then 2 end) <> 2",
                        "Predicates: CASE 1: region WHEN 'USA'",
                },
                {"select * from test.t0 where \n" +
                        "((case region when 'USA' then 1 when 'UK' then 2 when 'China' then 2 end) <> 2) IS NULL",
                        "Predicates: CASE 1: region WHEN"
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (1)",
                        "Predicates: [1: region, VARCHAR, false] = 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in (1)",
                        "Predicates: [1: region, VARCHAR, false] != 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (3)",
                        "[1: region, VARCHAR, false] != 'China', [1: region, VARCHAR, false] != 'Japan'",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in (3)",
                        "Predicates: 1: region IN ('China', 'Japan')",
                },

                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) is null",
                        "  0:EMPTYSET\n" +
                                "     cardinality: 1",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) is not null",
                        "  0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on\n" +
                                "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                                "     tabletList=\n" +
                                "     actualRows=0, avgRowSize=5.0\n" +
                                "     cardinality: 1",
                },

                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 end) is null",
                        "Predicates: 1: region NOT IN ('China', 'Japan')"
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 end) is not null",
                        "Predicates: 1: region IN ('China', 'Japan')",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then NULL when 'Japan' then 2 end) is null",
                        "Predicates: (1: region = 'China') OR (1: region NOT IN ('China', 'Japan'))",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then NULL end) is not null",
                        "Predicates: [1: region, VARCHAR, false] != 'Japan', 1: region IN ('China', 'Japan')",
                },
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  = 'b'",
                        "CASE 5: ship_code WHEN"
                },
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  != 'b'",
                        "Predicates: CASE 5: ship_code WHEN"
                },
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  " +
                        "in ('a', 'b', 'c', 'd')", "5: ship_code IN (1, 2, 3)"
                },
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' else 'e' end  " +
                        "in ('a', 'b', 'd')", "5: ship_code IN (1, 2)"
                },

                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  " +
                        "in ('e', 'f')", "0:EMPTYSET"
                },
                {"select * from test.t0 where (case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  " +
                        "in ('a', 'b', 'c', 'd')) is null",
                        "(((5: ship_code != 1) AND (5: ship_code != 2)) AND (5: ship_code != 3))",
                        "OR (((5: ship_code = 1) OR (5: ship_code = 2)) OR (5: ship_code = 3) IS NULL)"
                },
                {"select * from test.t0 where (case ship_code when 1 then 'a' when 2 then 'b' when 3 then 'c' end  " +
                        "in ('e', 'f')) is not null",
                        "ship_code IN (1, 2, 3)"
                },
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then null  " +
                        "else 'e' end  = 'b'",
                        "CASE 5: ship_code WHEN"
                },
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then null " +
                        "else 'e' end  != 'b'",
                        "CASE 5: ship_code WHEN"
                },
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then null " +
                        "else 'e' end in ('a', 'b', 'c', 'd')",
                        "CASE 5: ship_code WHEN"
                },
                {"select * from test.t0 where case ship_code when 1 then 'a' when 2 then 'b' when 3 then null " +
                        "else 'e' end  in ('e', 'f')",
                        "CASE 5: ship_code WHEN"
                },
                {"select * from test.t0 where (case ship_code when 1 then 'a' when 2 then 'b' when 3 then null " +
                        "else 'e' end in ('a', 'b', 'c', 'd')) is null",
                        "CASE 5: ship_code WHEN"
                },
                {"select * from test.t0 where (case ship_code when 1 then 'a' when 2 then 'b' when 3 then null " +
                        "else 'e' end in ('a', 'b', 'c', 'd')) is not null",
                        "CASE 5: ship_code WHEN"
                },
                {"select * from test.t0 where case ship_code when ship_mode + 1 then 'a' when ship_mode + 2 then 'b' " +
                        "when 3 then 'c' end in ('b', 'c', 'd')",
                        "CASE CAST(5: ship_code AS BIGINT)"
                },

                {"select * from test.t0 where case ship_code when ship_mode + 1 then 'a' when ship_mode + 2 then 'b' " +
                        "else 'e' end in ('a', 'b', 'c', 'd')",
                        "(CAST(5: ship_code AS BIGINT) = CAST(4: ship_mode AS BIGINT) + 1)"
                },
                {"select * from test.t0 where (case ship_code when ship_mode + 1 then 'a' when ship_mode + 2 then 'b' " +
                        "else 'e' end in ('a', 'b', 'c', 'd')) is null",
                        "EMPTYSET"
                },
                {"select * from test.t0 where (case ship_code when ship_mode + 1 then 'a' when ship_mode + 2 then 'b' " +
                        "else 'e' end in ('a', 'b', 'c', 'd')) is not null",
                        "0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on\n" +
                                "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                                "     tabletList=\n" +
                                "     actualRows=0, avgRowSize=5.0\n" +
                                "     cardinality: 1"
                },

                {"select * from test.t0 where (case region when 'USA' then 1 when 'UK' then 2 else 3  end in (2, 3, null))" +
                        " is null",
                        " CASE 1: region WHEN 'USA' THEN 1 WHEN 'UK' THEN 2 ELSE 3 END IN (2, 3, NULL) IS NULL"},
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
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) = 1",
                        "Predicates: [1: region, VARCHAR, false] = 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) <> 1",
                        "Predicates: [1: region, VARCHAR, false] != 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region <> 'China' then 1 when region = 'Japan' then 2 else 3 end) <> 2",
                        "Predicates: (1: region != 'Japan') OR (1: region != 'China')",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 2 when region = 'Japan' then 2 else 3 end) <> 2",
                        "Predicates: [1: region, VARCHAR, false] != 'China', [1: region, VARCHAR, false] != 'Japan'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 2 when region = 'China' then 1 else 3 end) = 1",
                        "[1: region, VARCHAR, false] = 'China', [1: region, VARCHAR, false] != 'China'",
                },

                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 2 when region = 'China' then 1 else 3 end) <> 1",
                        "Predicates: (1: region != 'China') OR (1: region = 'China')",
                },
                {"select * from test.t0 where \n" +
                        "(case when region is NULL then 2 when region = 'China' then 1 else 3 end) = 3",
                        "Predicates: [1: region, VARCHAR, false] != 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = NULL then 2 when region = 'China' then 1 else 3 end) = 1",
                        "Predicates: [1: region, VARCHAR, false] = 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'USA' then 5 when region = 'China' then 1 else 3 end) = 3",
                        "Predicates: [1: region, VARCHAR, false] != 'USA', [1: region, VARCHAR, false] != 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'USA' then 5 when region = 'UK' then 2 \n" +
                        "when region = 'China' then 1 end) = 3",
                        "0:EMPTYSET",
                },

                {"select * from test.t0 where \n" +
                        "(case when region = 'USA' then 1 when region = 'UK' then 2 \n" +
                        "when region = 'China' then 2 end) = 2",
                        "Predicates: CASE WHEN 1:",
                },

                {"select * from test.t0 where \n" +
                        "(case when region = NULL then 2 when region = 'China' then 1 else 3 end) <> 1",
                        "Predicates: [1: region, VARCHAR, false] != 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'UK' then 2 when region = 'China' then 1 else 3 end) <> 3",
                        "Predicates: ((1: region = 'China') AND (1: region != 'UK')) OR (1: region = 'UK')",
                        "Predicates: (1: region = 'UK') OR ((1: region = 'China') AND (1: region != 'UK'))",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'USA' then 5 when region = 'UK' then 2 \n" +
                        "when region = 'China' then 1 end) <> 3",
                        "Predicates: CASE WHEN 1:",
                },

                {"select * from test.t0 where \n" +
                        "(case when region = 'USA' then 1 when region = 'UK' then 2 " +
                        "when region = 'China' then 2 end) <> 2",
                        "Predicates: CASE WHEN",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) in (1)",
                        "Predicates: [1: region, VARCHAR, false] = 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) not in (1)",
                        "Predicates: [1: region, VARCHAR, false] != 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) in (3)",
                        "Predicates: [1: region, VARCHAR, false] != 'China', [1: region, VARCHAR, false] != 'Japan'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) not in (3)",
                        "Predicates: (1: region = 'China') OR ((1: region = 'Japan') AND (1: region != 'China')), " +
                                "1: region IN ('China', 'Japan')",
                },

                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) is null",
                        "0:EMPTYSET"
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) is not null",
                        "  0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on\n" +
                                "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                                "     tabletList=\n" +
                                "     actualRows=0, avgRowSize=5.0\n" +
                                "     cardinality: 1",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 end) is null",
                        "Predicates: [1: region, VARCHAR, false] != 'China', [1: region, VARCHAR, false] != 'Japan'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 end) is not null",
                        "Predicates: (1: region = 'China') OR (1: region = 'Japan')",
                },

                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then NULL when region = 'Japan' then 2 end) is null",
                        "Predicates: (1: region = 'China') OR ((1: region != 'China') AND (1: region != 'Japan'))",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then NULL end) is not null",
                        "Predicates: (1: region != 'Japan') OR (1: region = 'China'), " +
                                "(1: region = 'China') OR (1: region = 'Japan')",
                },
                {"select * from test.t0 where case when ship_code is null then 'a' when ship_code = 1 then 'b' " +
                        "when ship_code = 2 then 'c' else 'd' end != 'c'",
                        "CASE WHEN 5: ship_code"
                },
                {"select * from test.t0 where case when ship_code is null then 'a' when ship_code = 1 then 'b' " +
                        "when ship_code = 2 then 'c' else 'd' end in ('a', 'b', 'c')",
                        "(5: ship_code IS NULL) OR ((5: ship_code = 1) AND (5: ship_code IS NOT NULL))"
                },
                {"select * from test.t0 where case when ship_code is null then 'a' when ship_code = 1 then 'b' " +
                        "when ship_code = 2 then 'c' else 'd' end not in ('a', 'b', 'c')",
                        "Predicates: CASE WHEN 5: ship_code"
                },
                {"select * from test.t0 where case when ship_code is null then 'a' when ship_code = 1 then 'b' " +
                        "when ship_code = 2 then 'c' end != 'c'",
                        "Predicates: CASE WHEN 5:"

                },
                {"select * from test.t0 where case when ship_code is null then 'a' when ship_code = 1 then 'b' " +
                        "when ship_code = 2 then 'c' end in ('a', 'b', 'c')",
                        "Predicates: CASE WHEN 5:"
                },

                {"select * from test.t0 where case when ship_code is null then 'a' when ship_code = 1 then 'b' " +
                        "when ship_code = 2 then 'c' end not in ('a', 'b', 'c')",
                        "EMPTYSET"
                },
                {"select * from test.t0 where (case when ship_code is null then 'a' when ship_code = 1 then 'b' " +
                        "when ship_code = 2 then 'c' end in ('a', 'b', 'c')) is null",
                        "Predicates: CASE WHEN 5:"

                },
                {"select * from test.t0 where (case when ship_code is null then 'a' when ship_code = 1 then 'b' " +
                        "when ship_code = 2 then 'c' end not in ('a', 'b', 'c')) is null",
                        "5: ship_code IS NOT NULL, (5: ship_code != 1) OR (5: ship_code IS NULL), " +
                                "(5: ship_code != 2) OR (5: ship_code IS NULL)"
                },
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
                {"= 'A'", "[4: ship_mode, INT, false] >= 90"},
                {"= 'B'", "[4: ship_mode, INT, false] >= 80, [4: ship_mode, INT, false] < 90"},
                {"= 'C'", "[4: ship_mode, INT, false] >= 70, [4: ship_mode, INT, false] < 80"},
                {"= 'D'", "[4: ship_mode, INT, false] >= 60", "(4: ship_mode < 70) OR (4: ship_mode IS NULL)"},
                {"= 'E'", "[4: ship_mode, INT, false] < 6", "(4: ship_mode < 60) OR (4: ship_mode IS NULL)"},
                {"<> 'A'", "[4: ship_mode, INT, false] < 90"},
                {"<> 'B'", "(4: ship_mode < 80) OR (4: ship_mode >= 90)"},
                {"<> 'C'", "(4: ship_mode < 70) OR ((4: ship_mode >= 90) OR (4: ship_mode >= 80))"},
                {"<> 'D'", "(4: ship_mode < 60) OR (((4: ship_mode >= 90) OR (4: ship_mode >= 80)) OR (4: ship_mode >= 70))"},
                {"<> 'E'", "[4: ship_mode, INT, false] >= 60"},

                {"in ('A','B')",
                        "Predicates: (4: ship_mode >= 90) OR ((4: ship_mode >= 80) AND (4: ship_mode < 90)), " +
                                "[4: ship_mode, INT, false] >= 80"},
                {"in ('A','B', 'C')", "[4: ship_mode, INT, false] >= 70"},
                {"in ('D','E')", "[4: ship_mode, INT, false] < 90, [4: ship_mode, INT, false] < 80, " +
                        "[4: ship_mode, INT, false] < 70"},
                {"in ('E')", "[4: ship_mode, INT, false] < 60"},
                {"in (NULL)", "0:EMPTYSET"},
                {"in ('F')", "0:EMPTYSET"},
                {"in ('A','B','C','D','E')",
                        "  0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on\n" +
                                "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                                "     tabletList=\n" +
                                "     actualRows=0, avgRowSize=5.0\n" +
                                "     cardinality: 1"},
                {"in ('A','B','C','D','E','F')",
                        "  0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on\n" +
                                "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                                "     tabletList=\n" +
                                "     actualRows=0, avgRowSize=5.0\n" +
                                "     cardinality: 1"},
                {"not in ('A','B')", "[4: ship_mode, INT, false] < 90, (4: ship_mode < 80) OR (4: ship_mode >= 90)"},
                {"not in ('A','B', 'C')", "[4: ship_mode, INT, false] < 70"},

                {"not in ('D','E')", "[4: ship_mode, INT, false] >= 70"},
                {"not in ('E')", "[4: ship_mode, INT, false] >= 60"},
                {"not in (NULL)", "0:EMPTYSET"},
                {"not in ('A','B','C','D','E')", "0:EMPTYSET"},
                {"not in ('A','B','C','D','E','F')", "0:EMPTYSET"},
                {"is NULL", "0:EMPTYSET"},
                {"is NOT NULL",
                        "  0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on\n" +
                                "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                                "     tabletList=\n" +
                                "     actualRows=0, avgRowSize=5.0\n" +
                                "     cardinality: 1"},
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
                {"= 'A'", "[5: ship_code, INT, true] >= 90"},
                {"= 'B'", "[5: ship_code, INT, true] >= 80, (5: ship_code < 90) OR (5: ship_code IS NULL)"},
                {"= 'C'", "[5: ship_code, INT, true] >= 70, (5: ship_code < 90) OR (5: ship_code IS NULL), " +
                        "(5: ship_code < 80) OR (5: ship_code IS NULL)"},
                {"<> 'D'", "Predicates: CASE WHEN 5:"},
                {"<> 'E'", "[5: ship_code, INT, true] >= 60"},

                {"in ('A','B')",
                        "Predicates: (5: ship_code >= 90) OR ((5: ship_code >= 80) AND " +
                                "((5: ship_code < 90) OR (5: ship_code IS NULL))), [5: ship_code, INT, true] >= 80"},
                {"in ('A','B', 'C')", "[5: ship_code, INT, true] >= 70"},
                {"in ('D','E')", "Predicates: CASE WHEN 5: ship_code"},
                {"in ('E')", "Predicates: CASE WHEN 5: ship_code"},
                {"in (NULL)", "0:EMPTYSET"},
                {"in ('F')", "0:EMPTYSET"},
                {"in ('A','B','C','D','E')",
                        "  0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on\n" +
                                "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                                "     tabletList=\n" +
                                "     actualRows=0, avgRowSize=5.0\n" +
                                "     cardinality: 1"},
                {"in ('A','B','C','D','E','F')",
                        "  0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on\n" +
                                "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                                "     tabletList=\n" +
                                "     actualRows=0, avgRowSize=5.0\n" +
                                "     cardinality: 1"},
                {"not in ('A','B', 'C')",
                        "Predicates: CASE WHEN 5: ship_code"},

                {"not in ('D','E')", "[5: ship_code, INT, true] >= 70"},
                {"not in ('E')", "[5: ship_code, INT, true] >= 6"},
                {"not in (NULL)", "0:EMPTYSET"},
                {"not in ('A','B','C','D','E','F')", "0:EMPTYSET"},
                {"is NULL", "0:EMPTYSET"},
                {"is NOT NULL",
                        "  0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on\n" +
                                "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                                "     tabletList=\n" +
                                "     actualRows=0, avgRowSize=5.0\n" +
                                "     cardinality: 1"},
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
                {"select * from test.t0 where if(region='USA', 1, 0) = 1", "[1: region, VARCHAR, false] = 'USA'"},
                {"select * from test.t0 where if(region='USA', 1, 0) = 0", "[1: region, VARCHAR, false] != 'USA'"},
                {"select * from test.t0 where if(region='USA', 1, 0) = 2", "0:EMPTYSET"},
                {"select * from test.t0 where if(region='USA', 1, 0) <> 1", "[1: region, VARCHAR, false] != 'USA'"},
                {"select * from test.t0 where if(region='USA', 1, 0) <> 0", "[1: region, VARCHAR, false] = 'USA'"},

                {"select * from test.t0 where if(region='USA', 1, 0) <> 2", "0:OlapScanNode\n" +
                        "     table: t0, rollup: t0\n" +
                        "     preAggregation: on\n" +
                        "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                        "     tabletList=\n" +
                        "     actualRows=0, avgRowSize=5.0\n" +
                        "     cardinality: 1"},
                {"select * from test.t0 where if(region='USA', 1, 0) in (1)", "[1: region, VARCHAR, false] = 'USA'"},
                {"select * from test.t0 where if(region='USA', 1, 0) in (1,0)", "  0:OlapScanNode\n" +
                        "     table: t0, rollup: t0\n" +
                        "     preAggregation: on\n" +
                        "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                        "     tabletList=\n" +
                        "     actualRows=0, avgRowSize=5.0\n" +
                        "     cardinality: 1\n"},
                {"select * from test.t0 where if(region='USA', 1, 0) in (2,3)", "0:EMPTYSET"},
                {"select * from test.t0 where (if(region='USA', 1, 0) in (2,3, null))",
                        "if(1: region = 'USA', 1, 0) IN (2, 3, NULL)"},
                {"select * from test.t0 where (if(region='USA', 1, 0) in (2,3, null)) is null",
                        "if(1: region = 'USA', 1, 0) IN (2, 3, NULL) IS NULL"},
                {"select * from test.t0 where if(region='USA', 1, 0) not in (0)", "[1: region, VARCHAR, false] = 'USA'"},

                {"select * from test.t0 where if(region='USA', 1, 0) not in (0,1)", "0:EMPTYSET"},
                {"select * from test.t0 where if(region='USA', 1, 0) not in (2,3)", "  0:OlapScanNode\n" +
                        "     table: t0, rollup: t0\n" +
                        "     preAggregation: on\n" +
                        "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                        "     tabletList=\n" +
                        "     actualRows=0, avgRowSize=5.0\n" +
                        "     cardinality: 1\n"},
                {"select * from test.t0 where if(region='USA', 1, 0) is NULL", "0:EMPTYSET"},
                {"select * from test.t0 where if(region='USA', 1, 0) is NOT NULL", "  0:OlapScanNode\n" +
                        "     table: t0, rollup: t0\n" +
                        "     preAggregation: on\n" +
                        "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                        "     tabletList=\n" +
                        "     actualRows=0, avgRowSize=5.0\n" +
                        "     cardinality: 1\n"},
                {"select * from test.t0 where if(ship_code is null, null, 0) is NULL", "5: ship_code IS NULL"},
                {"select * from test.t0 where if(ship_code is null or ship_code > 2, 2, 1) > 1",
                        "if[((5: ship_code IS NULL) OR (5: ship_code > 2), 2, 1)"},

                {"select * from test.t0 where if(ship_code is null or ship_code > 2, 2, 1) != 2",
                        "if[((5: ship_code IS NULL) OR (5: ship_code > 2), 2, 1)"},
                {"select * from test.t0 where if(ship_code is null or ship_code > 2, 1, 0) is NOT NULL", "  0:OlapScanNode\n" +
                        "     table: t0, rollup: t0\n" +
                        "     preAggregation: on\n" +
                        "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                        "     tabletList=\n" +
                        "     actualRows=0, avgRowSize=5.0\n" +
                        "     cardinality: 1\n"},
                {"with tmp as (select ship_mode, if(ship_code > 4, 1, 0) as layer0, " +
                        "if (ship_code >= 1 and ship_code <= 4, 1, 0) as layer1," +
                        "if(ship_code is null or ship_code < 1, 1, 0) as layer2 from t0) " +
                        "select * from tmp where layer2 = 1 and layer0 != 1 and layer1 !=1",
                        "(5: ship_code IS NULL) OR (5: ship_code < 1), if[([5: ship_code, INT, true] > 4, 1, 0)",
                        "if[((5: ship_code >= 1) AND (5: ship_code <= 4), 1, 0)"
                },

                {"select * from test.t0 where nullif('China', region) = 'China'", "[1: region, VARCHAR, false] != 'China'"},
                {"select * from test.t0 where nullif('China', region) <> 'China'", "0:EMPTYSET"},
                {"select * from test.t0 where nullif('China', region) is NULL", "[1: region, VARCHAR, false] = 'China'"},
                {"select * from test.t0 where (nullif('China', region) is NULL) is NULL",
                        "0:EMPTYSET"},
                {"select * from test.t0 where (nullif('China', region) is NULL) is NOT NULL",
                        "0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on"},
                {"select * from test.t0 where nullif('China', region) is NOT NULL", "[1: region, VARCHAR, false] != 'China'"},
                {"select * from test.t0 where (nullif('China', region) is NOT NULL) is NULL",
                        "1: region != 'China' IS NULL"},
                {"select * from test.t0 where (nullif('China', region) is NOT NULL) is NOT NULL",
                        "1: region != 'China' IS NOT NULL"},

                {"select * from test.t0 where nullif('China', region) = 'USA'", "0:EMPTYSET"},
                {"select * from test.t0 where nullif('China', region) <>  'USA'", "[1: region, VARCHAR, false] != 'China'"},

                {"select * from test.t0 where nullif(1, ship_code) = 1", "(5: ship_code != 1) OR (5: ship_code IS NULL)"},
                {"select * from test.t0 where nullif(1, ship_code) <> 1", "0:EMPTYSET"},
                {"select * from test.t0 where nullif(1, ship_code) is NULL", "[5: ship_code, INT, true] = 1"},
                {"select * from test.t0 where (nullif(1, ship_code) is NULL) is NULL", "0:EMPTYSET"},
                {"select * from test.t0 where (nullif(1, ship_code) is NULL) is NOT NULL",
                        "0:OlapScanNode\n" +
                        "     table: t0, rollup: t0\n" +
                        "     preAggregation: on"},
                {"select * from test.t0 where nullif(1, ship_code) is NOT NULL", "(5: ship_code != 1) OR (5: ship_code IS NULL)"},
                {"select * from test.t0 where (nullif(1, ship_code) is NOT NULL) is NULL",
                        "(5: ship_code != 1) OR (5: ship_code IS NULL)"},
                {"select * from test.t0 where (nullif(1, ship_code) is NOT NULL) is NOT NULL",
                        "(5: ship_code != 1) OR (5: ship_code IS NULL)"},
                {"select * from test.t0 where nullif(1, ship_code) = 2", "0:EMPTYSET"},
                {"select * from test.t0 where nullif(1, ship_code) <>  2", "(5: ship_code != 1) OR (5: ship_code IS NULL)"},
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
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) = NULL",
                        "0:EMPTYSET",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) <> NULL",
                        "0:EMPTYSET",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) <=> NULL",
                        "Predicates: CASE",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (1,NULL)",
                        "CASE 1: region WHEN 'China' THEN 1 WHEN 'Japan' THEN 2 ELSE 3 END IN (1, NULL)",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in (1,NULL)",
                        "CASE 1: region WHEN 'China' THEN 1 WHEN 'Japan' THEN 2 ELSE 3 END NOT IN (1, NULL)",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (NULL,NULL)",
                        "CASE 1: region WHEN 'China' THEN 1 WHEN 'Japan' THEN 2 ELSE 3 END IN (NULL, NULL)",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in (NULL,NULL)",
                        "CASE 1: region WHEN 'China' THEN 1 WHEN 'Japan' THEN 2 ELSE 3 END NOT IN (NULL, NULL)",
                },
                {"select * from test.t0 where \n" +
                        "if (region = 'China', 1, 2) = NULL",
                        "0:EMPTYSET",
                },
                {"select * from test.t0 where \n" +
                        "if (region = 'China', 1, 2) not in (NULL, 1)",
                        "if(1: region = 'China', 1, 2) NOT IN (NULL, 1)",
                },
                {"select * from test.t0 where (case when ship_code is null then true when 1 then false end) is null",
                        "0:EMPTYSET"
                },
                {"select * from test.t0 where (case when ship_code is null then true when 0 then false end) is null",
                        "Predicates: 5: ship_code IS NOT NULL"
                },
                {"select * from test.t0 where (case when ship_code is null then true when null then false end) is null",
                        "Predicates: 5: ship_code IS NOT NULL"
                },
                {"select * from test.t0 where (case when ship_code is null then true when not null then false end) is null",
                        "Predicates: 5: ship_code IS NOT NULL"
                },
                {"select * from test.t0 where (case when ship_code is null then true when not 3 then false end) is null",
                        "Predicates: 5: ship_code IS NOT NULL, (CAST(3 AS BOOLEAN)) OR (CAST(3 AS BOOLEAN) IS NULL)"
                },
                {"select * from test.t0 where (case when region = 'USA' then true " +
                        "when region like 'COM%' then false end) is null",
                        "Predicates: [1: region, VARCHAR, false] != 'USA', NOT (1: region LIKE 'COM%')"
                },
                {"select * from test.t0 where (case when case when null then 'A' when false then 'B' " +
                        "when region like 'COM%' then 'C' end = 'C' then true else false end) is null",
                        "0:EMPTYSET"
                }


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
        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add(sql);
        joiner.add(patterns.toString());
        joiner.add(plan);
        Assert.assertTrue(joiner.toString(), patterns.stream().anyMatch(plan::contains));
    }
}
