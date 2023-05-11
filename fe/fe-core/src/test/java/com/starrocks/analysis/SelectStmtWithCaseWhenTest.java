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

import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.stream.Stream;

public class SelectStmtWithCaseWhenTest {
    private static StarRocksAssert starRocksAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void setUp()
            throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        String createTblStmtStr = " CREATE TABLE `t0` (\n" +
                "  `region` varchar(128) NOT NULL COMMENT \"\",\n" +
                "  `order_date` date NOT NULL COMMENT \"\",\n" +
                "  `income` decimal(7, 0) NOT NULL COMMENT \"\",\n" +
                "  `ship_mode` int NOT NULL COMMENT \"\"\n" +
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
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";

        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("test").useDatabase("test");
        starRocksAssert.withTable(createTblStmtStr);
    }

    @Test
    public void testCaseWhenWithCaseClause() throws Exception {
        String[][] testCases = new String[][] {
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) = 1",
                        "Predicates: [1: region, VARCHAR, false] = 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (1,2)",
                        "Predicates: 1: region IN ('China', 'Japan')",
                        "Predicates: 1: region IN ('Japan', 'China')"
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
                                "     actualRows=0, avgRowSize=4.0\n" +
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
                        "Predicates: 1: region NOT IN ('China', 'USA')",
                        "Predicates: 1: region NOT IN ('USA', 'China')",
                },
                // Q9
                {"select * from test.t0 where \n" +
                        "(case region when 'USA' then 5 when 'UK' then 2 when 'China' then 1 end) = 3",
                        "0:EMPTYSET",
                },
                // Q10
                {"select * from test.t0 where \n" +
                        "(case region when 'USA' then 1 when 'UK' then 2 when 'China' then 2 end) = 2",
                        "Predicates: 1: region IN ('UK', 'China')",
                        "Predicates: 1: region IN ('China', 'UK')",
                },

                {"select * from test.t0 where \n" +
                        "(case region when NULL then 2 when 'China' then 1 else 3 end) <> 1",
                        "Predicates: [1: region, VARCHAR, false] != 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'UK' then 2 when 'China' then 1 else 3 end) <> 3",
                        "Predicates: 1: region IN ('UK', 'China')",
                        "Predicates: 1: region IN ('China', 'UK')",
                },
                // Q13
                {"select * from test.t0 where \n" +
                        "(case region when 'USA' then 5 when 'UK' then 2 when 'China' then 1 end) <> 3",
                        "((1: region = 'China') OR (1: region = 'UK')) OR (1: region = 'USA')",
                        "((1: region = 'UK') OR (1: region = 'China')) OR (1: region = 'USA')",
                        "((1: region = 'USA') OR (1: region = 'UK')) OR (1: region = 'China')",
                        "((1: region = 'USA') OR (1: region = 'China')) OR (1: region = 'UK')",
                        "((1: region = 'China') OR (1: region = 'USA')) OR (1: region = 'UK')",
                        "((1: region = 'UK') OR (1: region = 'USA')) OR (1: region = 'China')",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'USA' then 1 when 'UK' then 2 when 'China' then 2 end) <> 2",
                        "[1: region, VARCHAR, false] = 'USA'",
                },

                {"select * from test.t0 where \n" +
                        "((case region when 'USA' then 1 when 'UK' then 2 when 'China' then 2 end) <> 2) IS NULL",
                        "(1: region = 'USA') OR (if(1: region NOT IN ('UK', 'China', 'USA'), NULL, FALSE)) IS NULL",
                        "(1: region = 'USA') OR (if(1: region NOT IN ('UK', 'USA', 'China'), NULL, FALSE)) IS NULL",
                        "(1: region = 'USA') OR (if(1: region NOT IN ('USA', 'China', 'UK'), NULL, FALSE)) IS NULL",
                        "(1: region = 'USA') OR (if(1: region NOT IN ('USA', 'UK', 'China'), NULL, FALSE)) IS NULL",
                        "(1: region = 'USA') OR (if(1: region NOT IN ('China', 'UK', 'USA'), NULL, FALSE)) IS NULL",
                        "(1: region = 'USA') OR (if(1: region NOT IN ('China', 'USA', 'UK'), NULL, FALSE)) IS NULL",
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
                        "Predicates: 1: region NOT IN ('Japan', 'China')",
                        "Predicates: 1: region NOT IN ('China', 'Japan')",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in (3)",
                        "Predicates: 1: region IN ('Japan', 'China')",
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
                                "     actualRows=0, avgRowSize=4.0\n" +
                                "     cardinality: 1",
                },

                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 end) is null",
                        "Predicates: 1: region NOT IN ('Japan', 'China')",
                        "Predicates: 1: region NOT IN ('China', 'Japan')",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 end) is not null",
                        "Predicates: 1: region IN ('Japan', 'China')",
                        "Predicates: 1: region IN ('China', 'Japan')",
                },

                {"select * from test.t0 where \n" +
                        "(case region when 'China' then NULL when 'Japan' then 2 end) is null",
                        "Predicates: (1: region = 'China') OR (1: region NOT IN ('China', 'Japan'))",
                        "Predicates: (1: region = 'China') OR (1: region NOT IN ('Japan', 'China'))",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then NULL end) is not null",
                        "Predicates: [1: region, VARCHAR, false] != 'Japan', 1: region IN ('Japan', 'China')",
                        "Predicates: [1: region, VARCHAR, false] != 'Japan', 1: region IN ('China', 'Japan')",
                }
        };
        for (String[] tc : testCases) {
            String sql = tc[0];
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assert.assertTrue(plan, Stream.of(tc).skip(1).anyMatch(plan::contains));
        }
    }

    @Test
    public void testCaseWhenWithoutCaseClause() throws Exception {
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
                //Q3
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 2 when region = 'Japan' then 2 else 3 end) <> 2",
                        "[1: region, VARCHAR, false] != 'China', " +
                                "(1: region != 'Japan') OR (1: region = 'China'), " +
                                "(1: region != 'China') AND ((1: region != 'Japan') OR (1: region = 'China')) " +
                                "IS NOT NULL",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 2 when region = 'China' then 1 else 3 end) = 1",
                        "[1: region, VARCHAR, false] = 'China', [1: region, VARCHAR, false] != 'China'",
                },
                // Q5
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 2 when region = 'China' then 1 else 3 end) <> 1",
                        "Predicates: (1: region != 'China') OR (1: region = 'China')",
                },
                // Q6
                {"select * from test.t0 where \n" +
                        "(case when region is NULL then 2 when region = 'China' then 1 else 3 end) = 3",
                        "Predicates: [1: region, VARCHAR, false] != 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = NULL then 2 when region = 'China' then 1 else 3 end) = 1",
                        "Predicates: [1: region, VARCHAR, false] = 'China'",
                },
                // Q8
                {"select * from test.t0 where \n" +
                        "(case when region = 'USA' then 5 when region = 'China' then 1 else 3 end) = 3",
                        "[1: region, VARCHAR, false] != 'USA', (1: region != 'China') OR (1: region = 'USA')",
                        "(1: region != 'China') OR (1: region = 'USA'), [1: region, VARCHAR, false] != 'USA'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'USA' then 5 when region = 'UK' then 2 \n" +
                        "when region = 'China' then 1 end) = 3",
                        "0:EMPTYSET",
                },
                // Q10
                {"select * from test.t0 where \n" +
                        "(case when region = 'USA' then 1 when region = 'UK' then 2 \n" +
                        "when region = 'China' then 2 end) = 2",
                        "Predicates: CASE WHEN 1: region = 'USA' THEN 1 WHEN 1: region = 'UK' " +
                                "THEN 2 WHEN 1: region = 'China' THEN 2 END = 2",
                },

                {"select * from test.t0 where \n" +
                        "(case when region = NULL then 2 when region = 'China' then 1 else 3 end) <> 1",
                        "Predicates: [1: region, VARCHAR, false] != 'China'",
                },
                //Q12
                {"select * from test.t0 where \n" +
                        "(case when region = 'UK' then 2 when region = 'China' then 1 else 3 end) <> 3",
                        "Predicates: ((1: region = 'China') AND (1: region != 'UK')) OR (1: region = 'UK')",
                        "Predicates: (1: region = 'UK') OR ((1: region = 'China') AND (1: region != 'UK'))",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'USA' then 5 when region = 'UK' then 2 \n" +
                        "when region = 'China' then 1 end) <> 3",
                        " Predicates: CASE WHEN",
                },
                // Q14
                {"select * from test.t0 where \n" +
                        "(case when region = 'USA' then 1 when region = 'UK' then 2 " +
                        "when region = 'China' then 2 end) <> 2",
                        "[1: region, VARCHAR, false] = 'USA'",
                },

                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) in (1)",
                        "Predicates: [1: region, VARCHAR, false] = 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) not in (1)",
                        "Predicates: [1: region, VARCHAR, false] != 'China'",
                },
                // Q17
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) in (3)",
                        "Predicates: [1: region, VARCHAR, false] != 'China', " +
                                "(1: region != 'Japan') OR (1: region = 'China'), " +
                                "(1: region != 'China') AND ((1: region != 'Japan') " +
                                "OR (1: region = 'China')) IS NOT NULL",
                        "Predicates: (1: region != 'Japan') OR (1: region = 'China'), " +
                                "[1: region, VARCHAR, false] != 'China', " +
                                "((1: region != 'Japan') OR (1: region = 'China')) " +
                                "AND (1: region != 'China') IS NOT NULL",
                },
                // Q18
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) not in (3)",
                        "((1: region = 'Japan') AND (1: region != 'China')) OR (1: region = 'China')",
                        "(1: region = 'China') OR ((1: region = 'Japan') AND (1: region != 'China')), " +
                                "(1: region = 'China') OR ((1: region = 'Japan') AND (1: region != 'China')) " +
                                "IS NOT NULL",
                },

                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) is null",
                        "  0:EMPTYSET\n" +
                                "     cardinality: 1",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 else 3 end) is not null",
                        "  0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on\n" +
                                "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                                "     tabletList=\n" +
                                "     actualRows=0, avgRowSize=4.0\n" +
                                "     cardinality: 1",
                },
                // Q21
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 end) is null",
                        "Predicates: ((1: region != 'China') AND (1: region != 'Japan')) " +
                                "OR ((1: region != 'China') AND (1: region != 'Japan') IS NULL)",
                },
                //Q22
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then 2 end) is not null",
                        "Predicates: (1: region = 'China') OR (1: region = 'Japan'), " +
                                "(1: region != 'China') AND (1: region != 'Japan') IS NOT NULL",
                },

                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then NULL when region = 'Japan' then 2 end) is null",
                        " Predicates: (1: region = 'China') OR (((1: region != 'China') AND " +
                                "(1: region != 'Japan')) OR ((1: region != 'China') AND " +
                                "(1: region != 'Japan') IS NULL))",
                },
                {"select * from test.t0 where \n" +
                        "(case when region = 'China' then 1 when region = 'Japan' then NULL end) is not null",
                        "Predicates: (1: region != 'Japan') OR (1: region = 'China'), (1: region = 'China') OR " +
                                "(1: region = 'Japan'), (1: region != 'China') AND (1: region != 'Japan') IS NOT NULL",
                }
        };
        for (String[] tc : testCases) {
            String sql = tc[0];
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assert.assertTrue(plan, Stream.of(tc).skip(1).anyMatch(plan::contains));
        }
    }

    @Test
    public void testCaseWhenWithManyBranches() throws Exception {
        String sqlTemp = "select * from test.t0 where (case \n" +
                "   when ship_mode >= 90 then 'A'\n" +
                "   when ship_mode >= 80 then 'B'\n" +
                "   when ship_mode >= 70 then 'C'\n" +
                "   when ship_mode >= 60 then 'D'\n" +
                "   else 'E' end)  %s";

        String[][] testCases = new String[][] {
                // Q0
                {"= 'A'", "[4: ship_mode, INT, false] >= 90, 4: ship_mode >= 90 IS NOT NULL"},
                {"= 'B'",
                        "[4: ship_mode, INT, false] >= 80, [4: ship_mode, INT, false] < 90, " +
                                "(4: ship_mode >= 80) AND (4: ship_mode < 90) IS NOT NULL"},
                {"= 'C'", "Predicates: CASE WHEN"},
                {"= 'D'", "Predicates: CASE WHEN"},
                {"= 'E'", "Predicates: CASE WHEN"},
                // Q5
                {"<> 'A'", "[4: ship_mode, INT, false] < 90, 4: ship_mode < 90 IS NOT NULL"},
                {"<> 'B'",
                        "(4: ship_mode < 80) OR (4: ship_mode >= 90), " +
                                "(4: ship_mode < 80) OR (4: ship_mode >= 90) IS NOT NULL"},
                {"<> 'C'", "Predicates: CASE WHEN"},
                {"<> 'D'", "Predicates: CASE WHEN"},
                {"<> 'E'", "Predicates: CASE WHEN"},
                // Q10
                {"in ('A','B')",
                        "((4: ship_mode >= 80) AND (4: ship_mode < 90)) OR (4: ship_mode >= 90), " +
                                "((4: ship_mode >= 80) AND (4: ship_mode < 90)) OR (4: ship_mode >= 90) IS NOT NULL",
                        "(4: ship_mode >= 90) OR ((4: ship_mode >= 80) AND (4: ship_mode < 90)), " +
                                "(4: ship_mode >= 90) OR ((4: ship_mode >= 80) AND (4: ship_mode < 90)) IS NOT NULL"},
                {"in ('A','B', 'C')", "Predicates: CASE WHEN"},
                {"in ('D','E')", "Predicates: CASE WHEN"},
                {"in ('E')", "Predicates: CASE WHEN"},
                {"in (NULL)", "0:EMPTYSET"},
                // Q15
                {"in ('F')", "0:EMPTYSET"},
                {"in ('A','B','C','D','E')",
                        "  0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on\n" +
                                "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                                "     tabletList=\n" +
                                "     actualRows=0, avgRowSize=4.0\n" +
                                "     cardinality: 1"},
                {"in ('A','B','C','D','E','F')",
                        "  0:OlapScanNode\n" +
                                "     table: t0, rollup: t0\n" +
                                "     preAggregation: on\n" +
                                "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                                "     tabletList=\n" +
                                "     actualRows=0, avgRowSize=4.0\n" +
                                "     cardinality: 1"},
                {"not in ('A','B')",
                        "(4: ship_mode < 80) OR (4: ship_mode >= 90), [4: ship_mode, INT, false] < 90, " +
                                "((4: ship_mode < 80) OR (4: ship_mode >= 90)) AND (4: ship_mode < 90) IS NOT NULL",
                        "[4: ship_mode, INT, false] < 90, (4: ship_mode < 80) OR (4: ship_mode >= 90), " +
                                "(4: ship_mode < 90) AND ((4: ship_mode < 80) OR (4: ship_mode >= 90)) IS NOT NULL"},
                // Q19
                {"not in ('A','B', 'C')", "Predicates: CASE WHEN"},
                {"not in ('D','E')", "Predicates: CASE WHEN"},
                {"not in ('E')", "Predicates: CASE WHEN"},
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
                                "     actualRows=0, avgRowSize=4.0\n" +
                                "     cardinality: 1"},
        };
        for (String[] tc : testCases) {
            String sql = String.format(sqlTemp, tc[0]);
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assert.assertTrue(plan, Stream.of(tc).skip(1).anyMatch(plan::contains));
        }
    }

    @Test
    public void testIf() throws Exception {
        String[][] testCases = new String[][] {
                {"select * from test.t0 where if(region='USA', 1, 0) = 1",
                        "[1: region, VARCHAR, false] = 'USA', 1: region = 'USA' IS NOT NULL"},
                //{"select * from test.t0 where (case when region='USA' then 1 else 0 end) = 0", "foobar" },
                {"select * from test.t0 where if(region='USA', 1, 0) = 0",
                        "[1: region, VARCHAR, false] != 'USA', 1: region != 'USA' IS NOT NULL"},
                {"select * from test.t0 where if(region='USA', 1, 0) = 2", "0:EMPTYSET"},
                {"select * from test.t0 where if(region='USA', 1, 0) <> 1",
                        "[1: region, VARCHAR, false] != 'USA', 1: region != 'USA' IS NOT NULL"},
                {"select * from test.t0 where if(region='USA', 1, 0) <> 0",
                        "[1: region, VARCHAR, false] = 'USA', 1: region = 'USA' IS NOT NULL"},
                {"select * from test.t0 where if(region='USA', 1, 0) <> 2", "0:OlapScanNode\n" +
                        "     table: t0, rollup: t0\n" +
                        "     preAggregation: on\n" +
                        "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                        "     tabletList=\n" +
                        "     actualRows=0, avgRowSize=4.0\n" +
                        "     cardinality: 1"},
                {"select * from test.t0 where if(region='USA', 1, 0) in (1)",
                        "[1: region, VARCHAR, false] = 'USA', 1: region = 'USA' IS NOT NULL"},
                {"select * from test.t0 where if(region='USA', 1, 0) in (1,0)", "  0:OlapScanNode\n" +
                        "     table: t0, rollup: t0\n" +
                        "     preAggregation: on\n" +
                        "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                        "     tabletList=\n" +
                        "     actualRows=0, avgRowSize=4.0\n" +
                        "     cardinality: 1\n"},
                {"select * from test.t0 where if(region='USA', 1, 0) in (2,3)", "0:EMPTYSET"},
                {"select * from test.t0 where if(region='USA', 1, 0) not in (0)",
                        "[1: region, VARCHAR, false] = 'USA', 1: region = 'USA' IS NOT NULL"},
                {"select * from test.t0 where if(region='USA', 1, 0) not in (0,1)", "0:EMPTYSET"},
                {"select * from test.t0 where if(region='USA', 1, 0) not in (2,3)", "  0:OlapScanNode\n" +
                        "     table: t0, rollup: t0\n" +
                        "     preAggregation: on\n" +
                        "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                        "     tabletList=\n" +
                        "     actualRows=0, avgRowSize=4.0\n" +
                        "     cardinality: 1\n"},
                {"select * from test.t0 where if(region='USA', 1, 0) is NULL", "0:EMPTYSET"},
                {"select * from test.t0 where if(region='USA', 1, 0) is NOT NULL", "  0:OlapScanNode\n" +
                        "     table: t0, rollup: t0\n" +
                        "     preAggregation: on\n" +
                        "     partitionsRatio=0/3, tabletsRatio=0/0\n" +
                        "     tabletList=\n" +
                        "     actualRows=0, avgRowSize=4.0\n" +
                        "     cardinality: 1\n"},
                // Q14
                {"select * from test.t0 where nullif('China', region) = 'China'",
                        "Predicates: nullif"},
                // Q15
                {"select * from test.t0 where nullif('China', region) <> 'China'",
                        "0:EMPTYSET"},
                {"select * from test.t0 where nullif('China', region) is NULL",
                        "[1: region, VARCHAR, false] = 'China'"},
                {"select * from test.t0 where nullif('China', region) is NOT NULL",
                        "[1: region, VARCHAR, false] != 'China'"},
                {"select * from test.t0 where nullif('China', region) = 'USA'", "0:EMPTYSET"},
                // Q19
                {"select * from test.t0 where nullif('China', region) <>  'USA'", "Predicates: nullif"},
        };

        for (String[] tc : testCases) {
            String sql = tc[0];
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assert.assertTrue(plan, Stream.of(tc).skip(1).anyMatch(plan::contains));
        }
    }

    @Test
    public void testCaseInvolvingNull() throws Exception {
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
                        "[1: region, VARCHAR, false] = 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in (1,NULL)",
                        "[1: region, VARCHAR, false] != 'China'",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) in (NULL,NULL)",
                        "0:EMPTYSET",
                },
                {"select * from test.t0 where \n" +
                        "(case region when 'China' then 1 when 'Japan' then 2 else 3 end) not in (NULL,NULL)",
                        "0:EMPTYSET",
                },
                {"select * from test.t0 where \n" +
                        "if (region = 'China', 1, 2) = NULL",
                        "0:EMPTYSET",
                },
                {"select * from test.t0 where \n" +
                        "if (region = 'China', 1, 2) not in (NULL, 1)",
                        "[1: region, VARCHAR, false] != 'China', 1: region != 'China' IS NOT NULL",
                },
        };
        for (String[] tc : testCases) {
            String sql = tc[0];
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assert.assertTrue(plan, Stream.of(tc).skip(1).anyMatch(plan::contains));
        }
    }
}
