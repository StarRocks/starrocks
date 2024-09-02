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

import com.starrocks.common.FeConstants;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class LowCardinalityArrayTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE supplier_nullable ( S_SUPPKEY     INTEGER NOT NULL,\n" +
                "                             S_NAME        CHAR(25) NOT NULL,\n" +
                "                             S_ADDRESS     ARRAY<VARCHAR(40)>, \n" +
                "                             S_NATIONKEY   INTEGER NOT NULL,\n" +
                "                             S_PHONE       ARRAY<CHAR(15)> NOT NULL,\n" +
                "                             S_ACCTBAL     double NOT NULL,\n" +
                "                             S_COMMENT     VARCHAR(101) NOT NULL,\n" +
                "                             PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`s_suppkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE table_int (id_int INT, id_bigint BIGINT) " +
                "DUPLICATE KEY(`id_int`) " +
                "DISTRIBUTED BY HASH(`id_int`) BUCKETS 1 " +
                "PROPERTIES (\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `s2` (    \n" +
                "  `v1` bigint(20) NULL COMMENT \"\",    \n" +
                "  `v2` int NULL,    \n" +
                "  `a1` array<varchar(65533)> NULL COMMENT \"\",    \n" +
                "  `a2` array<varchar(65533)> NULL COMMENT \"\"    \n" +
                ") ENGINE=OLAP    \n" +
                "DUPLICATE KEY(`v1`)    \n" +
                "COMMENT \"OLAP\"    \n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 10    \n" +
                "PROPERTIES (    \n" +
                "\"replication_num\" = \"1\",    \n" +
                "\"in_memory\" = \"false\",    \n" +
                "\"enable_persistent_index\" = \"false\",    \n" +
                "\"replicated_storage\" = \"false\",    \n" +
                "\"compression\" = \"LZ4\"    \n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `s1` (    \n" +
                "  `v1` bigint(20) NULL COMMENT \"\",    \n" +
                "  `v2` int(11) NULL COMMENT \"\",    \n" +
                "  `a1` array<varchar(65533)> NULL COMMENT \"\",    \n" +
                "  `a2` array<varchar(65533)> NULL COMMENT \"\"    \n" +
                ") ENGINE=OLAP    \n" +
                "DUPLICATE KEY(`v1`)    \n" +
                "COMMENT \"OLAP\"    \n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 10    \n" +
                "PROPERTIES (    \n" +
                "\"replication_num\" = \"1\",       \n" +
                "\"in_memory\" = \"false\",    \n" +
                "\"enable_persistent_index\" = \"false\",    \n" +
                "\"replicated_storage\" = \"false\",    \n" +
                "\"light_schema_change\" = \"true\",    \n" +
                "\"compression\" = \"LZ4\"    \n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `s3` (    \n" +
                "  `v1` bigint(20) NULL COMMENT \"\",    \n" +
                "  `v2` int(11) NULL COMMENT \"\",    \n" +
                "  `a1` array<varchar(65533)> NULL COMMENT \"\",    \n" +
                "  `a2` array<int> NULL COMMENT \"\",    \n" +
                "  `a3` array<varchar(65533)> NULL COMMENT \"\"    \n" +
                ") ENGINE=OLAP    \n" +
                "DUPLICATE KEY(`v1`)    \n" +
                "COMMENT \"OLAP\"    \n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 10    \n" +
                "PROPERTIES (    \n" +
                "\"replication_num\" = \"1\",       \n" +
                "\"in_memory\" = \"false\",    \n" +
                "\"enable_persistent_index\" = \"false\",    \n" +
                "\"replicated_storage\" = \"false\",    \n" +
                "\"light_schema_change\" = \"true\",    \n" +
                "\"compression\" = \"LZ4\"    \n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `s4` (    \n" +
                "  `v1` bigint(20) NULL COMMENT \"\",    \n" +
                "  `v2` int NULL,    \n" +
                "  `a1` array<string> NULL COMMENT \"\",    \n" +
                "  `a2` array<string> NULL COMMENT \"\"    \n" +
                ") ENGINE=OLAP    \n" +
                "UNIQUE KEY(`v1`)    \n" +
                "COMMENT \"OLAP\"    \n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 10    \n" +
                "PROPERTIES (    \n" +
                "\"replication_num\" = \"1\",    \n" +
                "\"in_memory\" = \"false\",    \n" +
                "\"enable_persistent_index\" = \"false\",    \n" +
                "\"replicated_storage\" = \"false\",    \n" +
                "\"compression\" = \"LZ4\"    \n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `s5` (    \n" +
                "  `v1` bigint(20) NULL COMMENT \"\",    \n" +
                "  `v2` int MAX NULL,    \n" +
                "  `a1` array<string> REPLACE NULL COMMENT \"\",    \n" +
                "  `a2` array<string> REPLACE NULL COMMENT \"\"    \n" +
                ") ENGINE=OLAP    \n" +
                "AGGREGATE KEY(`v1`)    \n" +
                "COMMENT \"OLAP\"    \n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 10    \n" +
                "PROPERTIES (    \n" +
                "\"replication_num\" = \"1\",    \n" +
                "\"in_memory\" = \"false\",    \n" +
                "\"enable_persistent_index\" = \"false\",    \n" +
                "\"replicated_storage\" = \"false\",    \n" +
                "\"compression\" = \"LZ4\"    \n" +
                ");");

        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setSqlMode(2);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setCboCteReuse(false);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
    }

    @AfterClass
    public static void afterClass() {
        connectContext.getSessionVariable().setSqlMode(0);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
    }

    @Test
    public void testArrayPredicate() throws Exception {
        String sql = "select array_min(S_ADDRESS), S_ADDRESS from supplier_nullable where S_ADDRESS[0] = 'a'";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains("  0:OlapScanNode\n" +
                "     table: supplier_nullable, rollup: supplier_nullable\n" +
                "     preAggregation: on\n" +
                "     Predicates: DictDecode(10: S_ADDRESS, [<place-holder> = 'a'], 10: S_ADDRESS[0])"));
    }

    @Test
    public void testArrayPredicate2() throws Exception {
        String sql = "select array_min(S_ADDRESS), S_ADDRESS from supplier_nullable where S_ADDRESS = ['a', 'b']";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains(" 0:OlapScanNode\n" +
                "     table: supplier_nullable, rollup: supplier_nullable\n" +
                "     preAggregation: on\n" +
                "     Predicates: DictDecode(10: S_ADDRESS, [<place-holder>]) = ['a','b']"));
        Assert.assertTrue(plan, plan.contains("  |  output columns:\n" +
                "  |  9 <-> DictDecode(10: S_ADDRESS, [<place-holder>], array_min(10: S_ADDRESS))\n" +
                "  |  10 <-> [10: S_ADDRESS, ARRAY<INT>, true]"));
    }

    @Test
    public void testArrayPredicate3() throws Exception {
        String sql = "select S_ADDRESS from supplier_nullable where S_ADDRESS = ['a', 'b']";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains("  0:OlapScanNode\n" +
                "     table: supplier_nullable, rollup: supplier_nullable\n" +
                "     preAggregation: on\n" +
                "     Predicates: [3: S_ADDRESS, ARRAY<VARCHAR(40)>, true] = ['a','b']"));
    }

    @Test
    public void testArrayPredicate4() throws Exception {
        String sql = "select array_min(S_ADDRESS), S_ADDRESS from supplier_nullable where S_ADDRESS[0] = 'a'";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains("  0:OlapScanNode\n" +
                "     table: supplier_nullable, rollup: supplier_nullable\n" +
                "     preAggregation: on\n" +
                "     Predicates: DictDecode(10: S_ADDRESS, [<place-holder> = 'a'], 10: S_ADDRESS[0])\n" +
                "     dict_col=S_ADDRESS\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=2.0\n" +
                "     Pruned type: 10 <-> [ARRAY<INT>]\n" +
                "     cardinality: 1\n"));
    }

    @Test
    public void testArrayMultiPredicate() throws Exception {
        String sql = "select array_min(S_ADDRESS), S_ADDRESS from supplier_nullable " +
                "where S_ADDRESS[0] = 'a' and S_ADDRESS[1] = 'b'";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "Predicates: DictDecode(10: S_ADDRESS, [<place-holder> = 'a'], 10: S_ADDRESS[0]), " +
                "DictDecode(10: S_ADDRESS, [<place-holder> = 'b'], 10: S_ADDRESS[1])");
    }

    @Test
    public void testArrayComplexPredicate() throws Exception {
        String sql = "select array_min(S_ADDRESS), S_ADDRESS from supplier_nullable " +
                "where ARRAY_DISTINCT(S_ADDRESS)[0] = 'a' and REVERSE(S_ADDRESS)[1] = 'b'";
        String plan = getVerboseExplain(sql);
        assertContains(plan,
                "Predicates: DictDecode(10: S_ADDRESS, [<place-holder> = 'a'], array_distinct(10: S_ADDRESS)[0]), " +
                        "DictDecode(10: S_ADDRESS, [<place-holder> = 'b'], reverse(10: S_ADDRESS)[1])");

        sql = "select array_min(S_ADDRESS), S_ADDRESS from supplier_nullable " +
                "where ARRAY_SLICE(S_ADDRESS, 2, 4)[0] = 'a' and ARRAY_FILTER(S_ADDRESS, [TRUE,FALSE])[1] = 'b'";
        plan = getVerboseExplain(sql);
        assertContains(plan,
                "Predicates: DictDecode(10: S_ADDRESS, [<place-holder> = 'a'], array_slice(10: S_ADDRESS, 2, 4)[0]), " +
                        "DictDecode(10: S_ADDRESS, [<place-holder> = 'b'], array_filter(10: S_ADDRESS, [TRUE,FALSE])" +
                        "[1])");

        sql = "select S_ADDRESS from supplier_nullable " +
                "where ARRAY_MIN(S_ADDRESS) = 'a' and ARRAY_MAX(S_ADDRESS) = 'b'";
        plan = getVerboseExplain(sql);
        assertContains(plan, "DictDecode(9: S_ADDRESS, [<place-holder> = 'a'], array_min(9: S_ADDRESS)), " +
                "DictDecode(9: S_ADDRESS, [<place-holder> = 'b'], array_max(9: S_ADDRESS))");

        sql = "select array_min(S_ADDRESS), S_ADDRESS from supplier_nullable " +
                "where ARRAY_DISTINCT(ARRAY_SLICE(S_ADDRESS, 2, 4))[0] = 'a'";
        plan = getVerboseExplain(sql);
        assertContains(plan, "DictDecode(10: S_ADDRESS, [<place-holder> = 'a'], " +
                "array_distinct(array_slice(10: S_ADDRESS, 2, 4))[0])");
    }

    @Test
    public void testArrayProject() throws Exception {
        String sql = "select array_min(S_ADDRESS), ARRAY_DISTINCT(ARRAY_SLICE(S_ADDRESS, 2, 4))[0], " +
                "array_max(S_ADDRESS), ARRAY_LENGTH(S_ADDRESS)" +
                "from supplier_nullable where S_ADDRESS[0] = 'a'";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains("  |  output columns:\n" +
                "  |  9 <-> DictDecode(13: S_ADDRESS, [<place-holder>], array_min(13: S_ADDRESS))\n" +
                "  |  10 <-> DictDecode(13: S_ADDRESS, [<place-holder>], array_distinct(array_slice(13: S_ADDRESS, 2," +
                " 4))[0])\n" +
                "  |  11 <-> DictDecode(13: S_ADDRESS, [<place-holder>], array_max(13: S_ADDRESS))\n" +
                "  |  12 <-> array_length[([13: S_ADDRESS, ARRAY<INT>, true]); args: INVALID_TYPE; result: INT; " +
                "args nullable: true; result nullable: true]"));
    }

    @Test
    public void testArrayProjectOrderLimit() throws Exception {
        String sql = "select array_length(a1), array_max(a2), array_min(a1), array_distinct(a1), array_sort(a2),\n" +
                "       reverse(a1), array_slice(a2, 2, 4), cardinality(a2)\n" +
                "from s2 where a1[1] = 'Jiangsu' and a2[2] = 'GD' order by v1 limit 2;";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains("  Global Dict Exprs:\n" +
                "    23: DictDefine(22: a2, [<place-holder>])\n" +
                "    24: DictDefine(21: a1, [<place-holder>])\n" +
                "    25: DictDefine(21: a1, [<place-holder>])\n" +
                "    26: DictDefine(22: a2, [<place-holder>])\n" +
                "    27: DictDefine(21: a1, [<place-holder>])\n" +
                "    28: DictDefine(22: a2, [<place-holder>])\n" +
                "\n" +
                "  5:Decode\n" +
                "  |  <dict id 23> : <string id 6>"));
    }

    @Test
    public void testArrayProjectOrderLimit2() throws Exception {
        String sql = "explain verbose  select lower(upper(array_min(reverse(array_sort(a1)))))      \n" +
                "    from s2 where a2[2] = 'GD' order by v1 limit 2; ";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains("  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: v1, BIGINT, true]\n" +
                "  |  8 <-> DictDefine(6: a1, [lower(upper(<place-holder>))], array_min(reverse(array_sort(6: a1))))"));
    }

    @Test
    public void testArrayShuffleProject() throws Exception {
        String sql = "select ARRAY_MIN(S_ADDRESS), " +
                "            ARRAY_DISTINCT(ARRAY_SLICE(S_ADDRESS, 2, 4))[0], " +
                "            ARRAY_MAX(S_ADDRESS), " +
                "            ARRAY_LENGTH(S_ADDRESS) " +
                "from supplier_nullable xx join[shuffle] table_int t on S_NATIONKEY = id_int " +
                "where S_ADDRESS[0] = 'a'";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains(" Global Dict Exprs:\n" +
                "    21: DictDefine(20: S_ADDRESS, [<place-holder>])\n" +
                "    22: DictDefine(20: S_ADDRESS, [<place-holder>])\n" +
                "    23: DictDefine(20: S_ADDRESS, [<place-holder>])\n" +
                "\n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  4 <-> [4: S_NATIONKEY, INT, false]\n" +
                "  |  17 <-> array_length[([20: S_ADDRESS, ARRAY<INT>, true]); args: INVALID_TYPE; result: INT; args " +
                "nullable: true; result nullable: true]\n" +
                "  |  20 <-> [20: S_ADDRESS, ARRAY<INT>, true]\n" +
                "  |  21 <-> array_distinct(array_slice(20: S_ADDRESS, 2, 4))[0]\n" +
                "  |  22 <-> array_min[([20: S_ADDRESS, ARRAY<INT>, true]); args: INVALID_TYPE; result: INT; args " +
                "nullable: true; result nullable: true]\n" +
                "  |  23 <-> array_max[([20: S_ADDRESS, ARRAY<INT>, true]); args: INVALID_TYPE; result: INT; args " +
                "nullable: true; result nullable: true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode"));
    }

    @Test
    public void testArrayShuffleProject2() throws Exception {
        String sql = "select ARRAY_MIN(S_ADDRESS), " +
                "            ARRAY_DISTINCT(ARRAY_SLICE(S_ADDRESS, 2, 4))[0], " +
                "            HEX(ARRAY_SLICE(S_ADDRESS, 1, 2)[0]), " +
                "            UPPER(ARRAY_MAX(S_ADDRESS)), " +
                "            ARRAY_DISTINCT(ARRAY_FILTER(S_ADDRESS, [TRUE, FALSE])), " +
                "            REVERSE(ARRAY_DISTINCT(REVERSE(S_ADDRESS))), " +
                "            ARRAY_MAX(REVERSE(ARRAY_DISTINCT(REVERSE(S_ADDRESS)))), " +
                "            REVERSE(ARRAY_DISTINCT(REVERSE(S_ADDRESS)))[2] " +
                "from supplier_nullable xx join[shuffle] table_int t on S_NATIONKEY = id_int " +
                "where S_ADDRESS[0] = 'a'";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains(" Global Dict Exprs:\n" +
                "    32: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "    33: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "    34: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "    35: DictDefine(29: S_ADDRESS, [hex(<place-holder>)])\n" +
                "    36: DictDefine(29: S_ADDRESS, [upper(<place-holder>)])\n" +
                "    37: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "    38: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "    30: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "    31: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "\n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  4 <-> [4: S_NATIONKEY, INT, false]\n" +
                "  |  29 <-> [29: S_ADDRESS, ARRAY<INT>, true]\n" +
                "  |  30 <-> 41: reverse[2]\n" +
                "  |  31 <-> array_distinct(array_slice(29: S_ADDRESS, 2, 4))[0]\n" +
                "  |  32 <-> array_slice(29: S_ADDRESS, 1, 2)[0]\n" +
                "  |  33 <-> array_max[([41: reverse, ARRAY<INT>, true]); args: INVALID_TYPE; result: INT; args " +
                "nullable: true; result nullable: true]\n" +
                "  |  34 <-> array_min[([29: S_ADDRESS, ARRAY<INT>, true]); args: INVALID_TYPE; result: INT; args " +
                "nullable: true; result nullable: true]\n" +
                "  |  35 <-> DictDefine(29: S_ADDRESS, [hex(<place-holder>)], array_slice(29: S_ADDRESS, 1, 2)[0])\n" +
                "  |  36 <-> DictDefine(29: S_ADDRESS, [upper(<place-holder>)], array_max(29: S_ADDRESS))\n" +
                "  |  37 <-> array_distinct[(array_filter[([29: S_ADDRESS, ARRAY<INT>, true], [TRUE,FALSE]); args: " +
                "INVALID_TYPE,INVALID_TYPE; result: ARRAY<INT>; args nullable: true; result nullable: true]); args: " +
                "INVALID_TYPE; result: ARRAY<INT>; args nullable: true; result nullable: true]\n" +
                "  |  38 <-> [41: reverse, ARRAY<INT>, true]\n" +
                "  |  common expressions:\n" +
                "  |  39 <-> reverse[([29: S_ADDRESS, ARRAY<INT>, true]); args: INVALID_TYPE; result: ARRAY<INT>; " +
                "args nullable: true; result nullable: true]\n" +
                "  |  40 <-> array_distinct[([39: reverse, ARRAY<INT>, true]); args: INVALID_TYPE; result: " +
                "ARRAY<INT>; args nullable: true; result nullable: true]\n" +
                "  |  41 <-> reverse[([40: array_distinct, ARRAY<INT>, true]); args: INVALID_TYPE; result: " +
                "ARRAY<INT>; args nullable: true; result nullable: true]\n" +
                "  |  cardinality: 1"));

        assertContains(plan, "  Global Dict Exprs:\n" +
                "    32: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "    33: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "    34: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "    35: DictDefine(29: S_ADDRESS, [hex(<place-holder>)])\n" +
                "    36: DictDefine(29: S_ADDRESS, [upper(<place-holder>)])\n" +
                "    37: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "    38: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "    30: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "    31: DictDefine(29: S_ADDRESS, [<place-holder>])\n" +
                "\n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  4 <-> [4: S_NATIONKEY, INT, false]\n" +
                "  |  29 <-> [29: S_ADDRESS, ARRAY<INT>, true]\n" +
                "  |  30 <-> 41: reverse[2]\n" +
                "  |  31 <-> array_distinct(array_slice(29: S_ADDRESS, 2, 4))[0]\n" +
                "  |  32 <-> array_slice(29: S_ADDRESS, 1, 2)[0]\n" +
                "  |  33 <-> array_max[([41: reverse, ARRAY<INT>, true]); args: INVALID_TYPE; result: INT; args " +
                "nullable: true; result nullable: true]\n" +
                "  |  34 <-> array_min[([29: S_ADDRESS, ARRAY<INT>, true]); args: INVALID_TYPE; result: INT; args " +
                "nullable: true; result nullable: true]\n" +
                "  |  35 <-> DictDefine(29: S_ADDRESS, [hex(<place-holder>)], array_slice(29: S_ADDRESS, 1, 2)[0])\n" +
                "  |  36 <-> DictDefine(29: S_ADDRESS, [upper(<place-holder>)], array_max(29: S_ADDRESS))\n" +
                "  |  37 <-> array_distinct[(array_filter[([29: S_ADDRESS, ARRAY<INT>, true], [TRUE,FALSE]); args: " +
                "INVALID_TYPE,INVALID_TYPE; result: ARRAY<INT>; args nullable: true; result nullable: true]); args: " +
                "INVALID_TYPE; result: ARRAY<INT>; args nullable: true; result nullable: true]\n" +
                "  |  38 <-> [41: reverse, ARRAY<INT>, true]\n" +
                "  |  common expressions:\n" +
                "  |  39 <-> reverse[([29: S_ADDRESS, ARRAY<INT>, true]); args: INVALID_TYPE; result: ARRAY<INT>; " +
                "args nullable: true; result nullable: true]\n" +
                "  |  40 <-> array_distinct[([39: reverse, ARRAY<INT>, true]); args: INVALID_TYPE; result: " +
                "ARRAY<INT>; args nullable: true; result nullable: true]\n" +
                "  |  41 <-> reverse[([40: array_distinct, ARRAY<INT>, true]); args: INVALID_TYPE; result: " +
                "ARRAY<INT>; args nullable: true; result nullable: true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testArrayShuffleProjectStringCountDistinct() throws Exception {
        String sql = "SELECT count(distinct x1), " +
                "            count(distinct x2), " +
                "            count(distinct x3), " +
                "            count(distinct x4), " +
                "            MAX(x5), " +
                "            MIN(x6), " +
                "            COUNT(x7), " +
                "            APPROX_COUNT_DISTINCT(x8)  " +
                "FROM (select ARRAY_MIN(S_ADDRESS) x1, " +
                "            ARRAY_DISTINCT(ARRAY_SLICE(S_ADDRESS, 2, 4))[0] x2, " +
                "            ARRAY_SLICE(S_ADDRESS, 1, 2)[0] x3, " +
                "            ARRAY_MAX(S_ADDRESS) x4, " +
                "            ARRAY_DISTINCT(ARRAY_FILTER(S_ADDRESS, [TRUE, FALSE]))[3] x5, " +
                "            REVERSE(ARRAY_DISTINCT(REVERSE(S_ADDRESS)))[1] x6, " +
                "            ARRAY_MAX(REVERSE(ARRAY_DISTINCT(REVERSE(S_ADDRESS)))) x7, " +
                "            REVERSE(ARRAY_DISTINCT(REVERSE(S_ADDRESS)))[2] x8 " +
                "from supplier_nullable xx join[shuffle] table_int t on S_NATIONKEY = id_int " +
                "where S_ADDRESS[0] = 'a' ) as yyy";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  Global Dict Exprs:\n" +
                "    49: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    50: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "\n" +
                "  10:Decode\n" +
                "  |  <dict id 49> : <string id 23>\n" +
                "  |  <dict id 50> : <string id 24>\n" +
                "  |  cardinality: 1");
        Assert.assertTrue(plan, plan.contains("Global Dict Exprs:\n" +
                "    37: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    38: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    39: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    40: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    41: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    42: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    43: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    44: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    45: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    46: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    47: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    48: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    49: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    50: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    51: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    52: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    53: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    54: DictDefine(36: S_ADDRESS, [<place-holder>])"));
        assertContains(plan, "  6:Project\n" +
                "  |  output columns:\n" +
                "  |  41 <-> DictDefine(39: array_min, [<place-holder>])\n" +
                "  |  42 <-> DictDefine(52: expr, [<place-holder>])\n" +
                "  |  43 <-> DictDefine(53: expr, [<place-holder>])\n" +
                "  |  44 <-> DictDefine(40: array_max, [<place-holder>])\n" +
                "  |  45 <-> DictDefine(54: expr, [<place-holder>])\n" +
                "  |  46 <-> DictDefine(37: expr, [<place-holder>])\n" +
                "  |  47 <-> DictDefine(38: array_max, [<place-holder>])\n" +
                "  |  48 <-> DictDefine(51: expr, [<place-holder>])\n" +
                "  |  cardinality: 1");
        assertContains(plan, "Global Dict Exprs:\n" +
                "    51: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    52: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    53: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    37: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    54: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    38: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    39: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "    40: DictDefine(36: S_ADDRESS, [<place-holder>])\n" +
                "\n" +
                "  1:Project");
        assertContains(plan, "  0:OlapScanNode\n" +
                "     table: supplier_nullable, rollup: supplier_nullable\n" +
                "     preAggregation: on\n" +
                "     Predicates: DictDecode(36: S_ADDRESS, [<place-holder> = 'a'], 36: S_ADDRESS[0])");
    }

    @Test
    public void testArrayShuffleProjectStringProject() throws Exception {
        String sql = "SELECT upper(x1), " +
                "            LTRIM(x2), " +
                "            IF(x3, 'a', 'b'), " +
                "            LOWER(x4), " +
                "            CONCAT(x5) " +
                "FROM (select ARRAY_MIN(S_ADDRESS) x1, " +
                "            ARRAY_DISTINCT(ARRAY_SLICE(S_ADDRESS, 2, 4))[0] x2, " +
                "            ARRAY_SLICE(S_ADDRESS, 1, 2)[0] x3, " +
                "            ARRAY_MAX(S_ADDRESS) x4, " +
                "            ARRAY_DISTINCT(ARRAY_FILTER(S_ADDRESS, [TRUE, FALSE]))[3] x5 " +
                "from supplier_nullable xx join[shuffle] table_int t on S_NATIONKEY = id_int " +
                "where S_ADDRESS[0] = 'a' ) as yyy";
        String plan = getVerboseExplain(sql);
        assertContains(plan, " Global Dict Exprs:\n" +
                "    32: DictDefine(30: S_ADDRESS, [<place-holder>])\n" +
                "    33: DictDefine(30: S_ADDRESS, [<place-holder>])\n" +
                "    34: DictDefine(30: S_ADDRESS, [ltrim(<place-holder>)])\n" +
                "    35: DictDefine(30: S_ADDRESS, [if(CAST(<place-holder> AS BOOLEAN), 'a', 'b')])\n" +
                "    36: DictDefine(30: S_ADDRESS, [lower(<place-holder>)])\n" +
                "    37: DictDefine(30: S_ADDRESS, [concat(<place-holder>)])\n" +
                "    38: DictDefine(30: S_ADDRESS, [upper(<place-holder>)])\n" +
                "    31: DictDefine(30: S_ADDRESS, [<place-holder>])");

        assertContains(plan, " Global Dict Exprs:\n" +
                "    32: DictDefine(30: S_ADDRESS, [<place-holder>])\n" +
                "    33: DictDefine(30: S_ADDRESS, [<place-holder>])\n" +
                "    34: DictDefine(30: S_ADDRESS, [ltrim(<place-holder>)])\n" +
                "    35: DictDefine(30: S_ADDRESS, [if(CAST(<place-holder> AS BOOLEAN), 'a', 'b')])\n" +
                "    36: DictDefine(30: S_ADDRESS, [lower(<place-holder>)])\n" +
                "    37: DictDefine(30: S_ADDRESS, [concat(<place-holder>)])\n" +
                "    38: DictDefine(30: S_ADDRESS, [upper(<place-holder>)])\n" +
                "    31: DictDefine(30: S_ADDRESS, [<place-holder>])\n" +
                "\n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  4 <-> [4: S_NATIONKEY, INT, false]\n" +
                "  |  30 <-> [30: S_ADDRESS, ARRAY<INT>, true]\n" +
                "  |  31 <-> array_distinct(array_slice(30: S_ADDRESS, 2, 4))[0]\n" +
                "  |  32 <-> array_slice(30: S_ADDRESS, 1, 2)[0]\n" +
                "  |  33 <-> array_distinct(array_filter(30: S_ADDRESS, [TRUE,FALSE]))[3]\n" +
                "  |  34 <-> DictDefine(30: S_ADDRESS, [ltrim(<place-holder>)], array_distinct(array_slice(30: " +
                "S_ADDRESS, 2, 4))[0])\n" +
                "  |  35 <-> DictDefine(30: S_ADDRESS, [if(CAST(<place-holder> AS BOOLEAN), 'a', 'b')], array_slice" +
                "(30: S_ADDRESS, 1, 2)[0])\n" +
                "  |  36 <-> DictDefine(30: S_ADDRESS, [lower(<place-holder>)], array_max(30: S_ADDRESS))\n" +
                "  |  37 <-> DictDefine(30: S_ADDRESS, [concat(<place-holder>)], array_distinct(array_filter(30: " +
                "S_ADDRESS, [TRUE,FALSE]))[3])\n" +
                "  |  38 <-> DictDefine(30: S_ADDRESS, [upper(<place-holder>)], array_min(30: S_ADDRESS))\n" +
                "  |  cardinality: 1\n" +
                "  |  ");
    }

    @Test
    public void testArrayScanLimit() throws Exception {
        String sql = "select * from supplier_nullable limit 10";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "Decode");
    }

    @Test
    public void testArrayUnWorthScan() throws Exception {
        String sql = "select * from supplier_nullable";
        String plan = getVerboseExplain(sql);
        assertNotContains(plan, "Decode");
    }

    @Test
    public void testArrayMultiOrPredicate() throws Exception {
        String sql = "select array_min(S_ADDRESS), S_ADDRESS from supplier_nullable " +
                "where S_ADDRESS[0] = 'a' or S_ADDRESS[1] = 'b'";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains("  0:OlapScanNode\n" +
                "     table: supplier_nullable, rollup: supplier_nullable\n" +
                "     preAggregation: on\n" +
                "     Predicates: (DictDecode(10: S_ADDRESS, [<place-holder> = 'a'], 10: S_ADDRESS[0]))" +
                " OR (DictDecode(10: S_ADDRESS, [<place-holder> = 'b'], 10: S_ADDRESS[1]))\n" +
                "     dict_col=S_ADDRESS\n" +
                "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                "     tabletList=\n" +
                "     actualRows=0, avgRowSize=2.0\n" +
                "     Pruned type: 10 <-> [ARRAY<INT>]\n" +
                "     cardinality: 1"));
    }

    @Test
    public void testCaseWhen() throws Exception {
        String sql = "select case when S_ADDRESS[1] = '5-LOW' " +
                "then 2 when S_ADDRESS[1] = '3-MEDIUM' then 1 else 0 end " +
                "from supplier_nullable";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "DictDecode(10: S_ADDRESS, [CASE WHEN <place-holder> = '5-LOW' THEN 2 " +
                "WHEN <place-holder> = '3-MEDIUM' THEN 1 ELSE 0 END], 10: S_ADDRESS[1])");

        sql = "select case when S_ADDRESS[1] = '5-LOW' " +
                "then 2 when S_ADDRESS[2] = '3-MEDIUM' then 1 else 0 end " +
                "from supplier_nullable";
        plan = getVerboseExplain(sql);
        assertContains(plan, "CASE " +
                "WHEN DictDecode(10: S_ADDRESS, [<place-holder> = '5-LOW'], 10: S_ADDRESS[1]) THEN 2 " +
                "WHEN DictDecode(10: S_ADDRESS, [<place-holder> = '3-MEDIUM'], 10: S_ADDRESS[2]) THEN 1 ELSE 0 END");
    }

    @Test
    public void testArrayToStringProject() throws Exception {
        String sql = "select MIN(x2), LOWER(x1) from (" +
                "   select HEX(ARRAY_SLICE(S_ADDRESS, 1, 2)[0]) as x1, " +
                "               UPPER(ARRAY_MAX(S_ADDRESS)) as x2 " +
                "   from supplier_nullable xx " +
                "   where S_ADDRESS[0] = 'a' " +
                ") yy " +
                "group by x1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("  1:Project\n" +
                "  |  <slot 14> : DictDefine(13: S_ADDRESS, [hex(<place-holder>)], array_slice(13: S_ADDRESS, 1, 2)[0])\n" +
                "  |  <slot 15> : DictDefine(13: S_ADDRESS, [upper(<place-holder>)], array_max(13: S_ADDRESS))"));
    }

    @Test
    public void testThriftDict() throws Exception {
        try {
            connectContext.getSessionVariable().setNewPlanerAggStage(2);
            String sql = "select lower(x1), upper(MIN(HEX(x2))) " +
                    "from (select a1[1] x1, array_max(a2) x2 from s1) y " +
                    "group by x1;";
            String plan = getThriftPlan(sql);
            assertContains(plan, "is_nullable:true, is_monotonic:true, is_index_only_filter:false)])]), " +
                    "query_global_dicts:[TGlobalDict(columnId:11, strings:[6D 6F 63 6B]");
            assertContains(plan, "(type:RANDOM, partition_exprs:[]), " +
                    "query_global_dicts:[TGlobalDict(columnId:11, strings:[6D 6F 63 6B]");
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(0);
        }
    }

    @Test
    public void testArrayIfNullArray() throws Exception {
        String sql = "select ifnull(a1, a2), a1, a2 from s2 order by v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "ifnull(DictDecode(9: a1, [<place-holder>]), DictDecode(10: a2, [<place-holder>]))");
    }

    @Test
    public void testArrayIfNullString() throws Exception {
        String sql = "select ifnull(a1[1], a2[1]), a1, a2 from s2 order by v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "ifnull(DictDecode(10: a1, [<place-holder>], 10: a1[1]), " +
                "DictDecode(11: a2, [<place-holder>], 11: a2[1]))");
    }

    @Test
    public void testArrayCharOnScan() throws Exception {
        String sql = "select array_slice(S_PHONE,-1,2) from supplier_nullable where S_SUPPKEY = 1";
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "DictDecode");
        assertContains(plan, "<slot 9> : array_slice(5: S_PHONE, -1, 2)");
    }

    @Test
    public void testArrayPruneSubfield() throws Exception {
        String sql = "select S_NAME from supplier_nullable where array_length(S_ADDRESS) = 2";
        String plan = getVerboseExplain(sql);
        assertNotContains(plan, "dict_col=");
        assertContains(plan, "ColumnAccessPath: [/S_ADDRESS/OFFSET]");
    }

    @Test
    public void testUnnestArray() throws Exception {
        String sql = "select S_ADDRESS[2], col.unnest from supplier_nullable, unnest(S_ADDRESS) col;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  1:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [INT]\n");
        assertContains(plan, "  2:Project\n" +
                "  |  output columns:\n" +
                "  |  10 <-> DictDecode(12: S_ADDRESS, [<place-holder>], 12: S_ADDRESS[2])\n" +
                "  |  13 <-> [13: unnest, INT, true]");

        sql = "select S_ADDRESS[2], lower(col.unnest) from supplier_nullable, unnest(S_ADDRESS) col;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  1:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [INT]\n");
        assertContains(plan, "  2:Project\n" +
                "  |  output columns:\n" +
                "  |  10 <-> DictDecode(13: S_ADDRESS, [<place-holder>], 13: S_ADDRESS[2])\n" +
                "  |  11 <-> DictDecode(14: unnest, [lower(<place-holder>)])");
    }

    @Test
    public void testMultiUnnestArray() throws Exception {
        String sql = "select S_ADDRESS[2], unnest.a, unnest.b " +
                "from supplier_nullable, unnest(S_ADDRESS, S_PHONE) as unnest(a, b) ;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  1:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [INT, CHAR(15)]\n");
        assertContains(plan, "  2:Project\n" +
                "  |  output columns:\n" +
                "  |  10 <-> [10: b, CHAR(15), true]\n" +
                "  |  11 <-> DictDecode(13: S_ADDRESS, [<place-holder>], 13: S_ADDRESS[2])\n" +
                "  |  14 <-> [14: a, INT, true]");

        sql = "select *" +
                "from s3, unnest(a1, a2, a3) as unnest(a, b, c) ;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  1:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [INT, INT, INT]");
        assertContains(plan, "  2:Decode\n" +
                "  |  <dict id 12> : <string id 3>\n" +
                "  |  <dict id 13> : <string id 5>\n" +
                "  |  <dict id 14> : <string id 6>\n" +
                "  |  <dict id 15> : <string id 8>");
        assertContains(plan, "  Global Dict Exprs:\n" +
                "    14: DictDefine(12: a1, [<place-holder>])\n" +
                "    15: DictDefine(13: a3, [<place-holder>])");

        sql = "select *" +
                "from s3, unnest(a1, a2, array_map(x -> concat(x, 'abc'), a3)) as unnest(a, b, c) ;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  |  10 <-> array_map[(" +
                "[9, VARCHAR(65533), true] -> concat[([9, VARCHAR(65533), true], 'abc'); " +
                "args: VARCHAR; result: VARCHAR; args nullable: true; result nullable: true], " +
                "DictDecode(15: a3, [<place-holder>])); " +
                "args: FUNCTION,INVALID_TYPE; result: ARRAY<VARCHAR>; args nullable: true; result nullable: true]");
        assertContains(plan, "dict_col=a1,a3");
        assertContains(plan, "  2:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [INT, INT, VARCHAR]");
    }

    @Test
    public void testAggreagateOrUnique() throws Exception {
        String sql = "select array_length(a1), array_max(a2), array_min(a1), array_distinct(a1), array_sort(a2),\n" +
                "       reverse(a1), array_slice(a2, 2, 4), cardinality(a2)\n" +
                "from s4 where a1[1] = 'Jiangsu' and a2[2] = 'GD' order by v1 limit 2;";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains("  Global Dict Exprs:\n" +
                "    23: DictDefine(22: a2, [<place-holder>])\n" +
                "    24: DictDefine(21: a1, [<place-holder>])\n" +
                "    25: DictDefine(21: a1, [<place-holder>])\n" +
                "    26: DictDefine(22: a2, [<place-holder>])\n" +
                "    27: DictDefine(21: a1, [<place-holder>])\n" +
                "    28: DictDefine(22: a2, [<place-holder>])\n" +
                "\n" +
                "  5:Decode\n" +
                "  |  <dict id 23> : <string id 6>"));

        sql = "select array_length(a1), array_max(a2), array_min(a1), array_distinct(a1), array_sort(a2),\n" +
                "       reverse(a1), array_slice(a2, 2, 4), cardinality(a2)\n" +
                "from s5 where a1[1] = 'Jiangsu' and a2[2] = 'GD' order by v1 limit 2;";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan, plan.contains("  Global Dict Exprs:\n" +
                "    23: DictDefine(22: a2, [<place-holder>])\n" +
                "    24: DictDefine(21: a1, [<place-holder>])\n" +
                "    25: DictDefine(21: a1, [<place-holder>])\n" +
                "    26: DictDefine(22: a2, [<place-holder>])\n" +
                "    27: DictDefine(21: a1, [<place-holder>])\n" +
                "    28: DictDefine(22: a2, [<place-holder>])\n" +
                "\n" +
                "  5:Decode\n" +
                "  |  <dict id 23> : <string id 6>"));
    }

    @Test
    public void testCastStringToArray() throws Exception {
        String sql = "select cast( S_COMMENT as array<string>) from supplier_nullable";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("1:Project\n" +
                "  |  <slot 9> : CAST(7: S_COMMENT AS ARRAY<VARCHAR(65533)>)"));

        sql = "select cast( S_COMMENT as array<string>) from supplier_nullable limit 1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("1:Project\n" +
                "  |  <slot 9> : CAST(7: S_COMMENT AS ARRAY<VARCHAR(65533)>)\n" +
                "  |  limit: 1"));

        sql = "select cast( S_COMMENT as array<array<string>>) from supplier_nullable limit 1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan, plan.contains("1:Project\n" +
                "  |  <slot 9> : CAST(7: S_COMMENT AS ARRAY<ARRAY<VARCHAR(65533)>>)\n" +
                "  |  limit: 1"));
    }
}
