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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LowCardinalityStructTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE T (KEY_COL     INTEGER NOT NULL,\n" +
                "                             VARCHAR_COL        VARCHAR(25),\n" +
                "                             ARRAY_VARCHAR_COL  ARRAY<VARCHAR(40)>, \n" +
                "                             INTEGER_COL        INTEGER)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`KEY_COL`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`KEY_COL`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setSqlMode(2);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setCboCteReuse(false);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setSqlMode(0);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
    }

    @Test
    public void testDictField() throws Exception {
        String sql = "select UPPER(STRUCT(VARCHAR_COL, ARRAY_VARCHAR_COL, INTEGER_COL).col1) from T";
        String plan = getVerboseExplain(sql);
        String expected = "5 <-> DictDecode([6: VARCHAR_COL, INT, true], [upper[(<place-holder>); args: VARCHAR; " +
                "result: VARCHAR; args nullable: true; result nullable: true]], row[([6: VARCHAR_COL, INT, true], " +
                "[7: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [4: INTEGER_COL, INT, true]); args: INT,INVALID_TYPE,INT; " +
                "result: struct<col1 int(11), col2 array<int(11)>, col3 int(11)>; args nullable: true; result " +
                "nullable: true].col1[true])";
        Assertions.assertTrue(plan.contains(expected));
    }

    @Test
    public void testNonDictField() throws Exception {
        String sql = "select STRUCT(VARCHAR_COL, INTEGER_COL).col2 from T";
        String plan = getVerboseExplain(sql);
        String expected = "5 <-> row[(DictDecode([6: VARCHAR_COL, INT, true], [<place-holder>]), " +
                "[4: INTEGER_COL, INT, true]); args: VARCHAR,INT; result: struct<col1 varchar(25), col2 int(11)>; " +
                "args nullable: true; result nullable: true].col2[true]";
        Assertions.assertTrue(plan.contains(expected));
    }

    @Test
    public void testAllFields() throws Exception {
        String sql = "WITH T AS (select STRUCT(VARCHAR_COL, ARRAY_VARCHAR_COL, INTEGER_COL) AS S from T) " +
                "select S.col1, S.col2, S.col3 from T";
        String plan = getVerboseExplain(sql);
        String expected = "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  6 <-> DictDecode([9: VARCHAR_COL, INT, true], [<place-holder>], " +
                "row[([9: VARCHAR_COL, INT, true], [10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], " +
                "[4: INTEGER_COL, INT, true]); args: INT,INVALID_TYPE,INT; result: struct<col1 int(11), " +
                "col2 array<int(11)>, col3 int(11)>; args nullable: true; result nullable: true].col1[true])\n" +
                "  |  7 <-> DictDecode([10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>], " +
                "row[([9: VARCHAR_COL, INT, true], [10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], " +
                "[4: INTEGER_COL, INT, true]); args: INT,INVALID_TYPE,INT; result: struct<col1 int(11), " +
                "col2 array<int(11)>, col3 int(11)>; args nullable: true; result nullable: true].col2[true])\n" +
                "  |  8 <-> row[(DictDecode([9: VARCHAR_COL, INT, true], [<place-holder>]), " +
                "DictDecode([10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>]), " +
                "[4: INTEGER_COL, INT, true]); args: VARCHAR,INVALID_TYPE,INT; result: struct<col1 varchar(25), " +
                "col2 array<varchar(40)>, col3 int(11)>; args nullable: true; result nullable: true].col3[true]";
        Assertions.assertTrue(plan.contains(expected));
    }

    @Test
    public void testStructDecode() throws Exception {
        String sql = "select TO_JSON(STRUCT(VARCHAR_COL, INTEGER_COL, ARRAY_VARCHAR_COL)) from T";
        String plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("5 <-> to_json[(row[(DictDecode([6: VARCHAR_COL, INT, true], " +
                "[<place-holder>]), [4: INTEGER_COL, INT, true], DictDecode([7: ARRAY_VARCHAR_COL, ARRAY<INT>, true]," +
                " [<place-holder>])); args: VARCHAR,INT,INVALID_TYPE; result: struct<col1 varchar(25), col2 int(11), " +
                "col3 array<varchar(40)>>; args nullable: true; result nullable: true]); args: INVALID_TYPE; result: " +
                "JSON; args nullable: true; result nullable: true]"));
    }

    @Test
    public void testTopLevelStruct() throws Exception {
        String sql = "select STRUCT(VARCHAR_COL, ARRAY_VARCHAR_COL, INTEGER_COL) from T";
        String plan = getVerboseExplain(sql);
        String expected = "5 <-> named_struct[('col1', DictDecode([6: VARCHAR_COL, INT, true], [<place-holder>], " +
                "[8: row, struct<col1 int(11), col2 array<int(11)>, col3 int(11)>, true].col1[true]), 'col2', " +
                "DictDecode([7: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>], [8: row, struct<col1 int(11)," +
                " col2 array<int(11)>, col3 int(11)>, true].col2[true]), 'col3', [8: row, struct<col1 int(11), " +
                "col2 array<int(11)>, col3 int(11)>, true].col3[true]); args: VARCHAR,VARCHAR,VARCHAR,INVALID_TYPE," +
                "VARCHAR,INT; result: struct<col1 varchar(25), col2 array<varchar(40)>, col3 int(11)>; " +
                "args nullable: true; result nullable: true]";
        Assertions.assertTrue(plan.contains(expected));
    }

    @Test
    public void testNamedStructTopLevel() throws Exception {
        String sql = "select NAMED_STRUCT('c1', VARCHAR_COL, 'c2', ARRAY_VARCHAR_COL, 'c3', INTEGER_COL) from T";
        String plan = getVerboseExplain(sql);
        String expected = "5 <-> named_struct[('c1', DictDecode([6: VARCHAR_COL, INT, true], [<place-holder>], " +
                "[8: named_struct, struct<c1 int(11), c2 array<int(11)>, c3 int(11)>, true].c1[true]), 'c2', " +
                "DictDecode([7: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>], [8: named_struct, " +
                "struct<c1 int(11), c2 array<int(11)>, c3 int(11)>, true].c2[true]), 'c3', [8: named_struct, " +
                "struct<c1 int(11), c2 array<int(11)>, c3 int(11)>, true].c3[true]); args: VARCHAR,VARCHAR,VARCHAR," +
                "INVALID_TYPE,VARCHAR,INT; result: struct<c1 varchar(25), c2 array<varchar(40)>, c3 int(11)>; " +
                "args nullable: true; result nullable: true]";
        Assertions.assertTrue(plan.contains(expected));
    }

    @Test
    public void testNamedStructGetFields() throws Exception {
        String sql = "WITH T2 AS (" +
                "select NAMED_STRUCT('c1', VARCHAR_COL, 'c2', ARRAY_VARCHAR_COL, 'c3', INTEGER_COL) AS S from T) " +
                "select S.c1, S.c2, S.c3 FROM T2";
        String plan = getVerboseExplain(sql);
        String expected = "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  6 <-> DictDecode([9: VARCHAR_COL, INT, true], [<place-holder>], named_struct[('c1', " +
                "[9: VARCHAR_COL, INT, true], 'c2', [10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], 'c3', [4: INTEGER_COL," +
                " INT, true]); args: VARCHAR,INT,VARCHAR,INVALID_TYPE,VARCHAR,INT; result: struct<c1 int(11), " +
                "c2 array<int(11)>, c3 int(11)>; args nullable: true; result nullable: true].c1[true])\n" +
                "  |  7 <-> DictDecode([10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>], named_struct[" +
                "('c1', [9: VARCHAR_COL, INT, true], 'c2', [10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], 'c3', " +
                "[4: INTEGER_COL, INT, true]); args: VARCHAR,INT,VARCHAR,INVALID_TYPE,VARCHAR,INT; result: struct" +
                "<c1 int(11), c2 array<int(11)>, c3 int(11)>; args nullable: true; result nullable: true].c2[true])\n" +
                "  |  8 <-> named_struct[('c1', DictDecode([9: VARCHAR_COL, INT, true], [<place-holder>]), 'c2', " +
                "DictDecode([10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>]), 'c3', [4: INTEGER_COL, " +
                "INT, true]); args: VARCHAR,VARCHAR,VARCHAR,INVALID_TYPE,VARCHAR,INT; result: struct<c1 varchar(25)," +
                " c2 array<varchar(40)>, c3 int(11)>; args nullable: true; result nullable: true].c3[true]";
        Assertions.assertTrue(plan.contains(expected));
    }

    @Test
    public void testExchangeDictField() throws Exception {
        String sql = "select UPPER(STRUCT(VARCHAR_COL, ARRAY_VARCHAR_COL, INTEGER_COL).col1) from T " +
                "ORDER BY INTEGER_COL";
        String plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("Global Dict Exprs:\n" +
                "    8: DictDefine(6: VARCHAR_COL, [upper(<place-holder>)])\n" +
                "\n" +
                "  5:Decode\n" +
                "  |  <dict id 8> : <string id 5>\n" +
                "  |  cardinality: 1"));
    }

    @Test
    public void testExchangeTopLevelStruct() throws Exception {
        String sql = "select STRUCT(VARCHAR_COL, ARRAY_VARCHAR_COL, INTEGER_COL) from T ORDER BY INTEGER_COL";
        String plan = getVerboseExplain(sql);
        String expected = "5 <-> named_struct[('col1', DictDecode([6: VARCHAR_COL, INT, true], [<place-holder>], " +
                "[8: row, struct<col1 int(11), col2 array<int(11)>, col3 int(11)>, true].col1[true]), 'col2', " +
                "DictDecode([7: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>], [8: row, struct<col1 int(11)," +
                " col2 array<int(11)>, col3 int(11)>, true].col2[true]), 'col3', [8: row, struct<col1 int(11), col2 " +
                "array<int(11)>, col3 int(11)>, true].col3[true]); args: VARCHAR,VARCHAR,VARCHAR,INVALID_TYPE,VARCHAR" +
                ",INT; result: struct<col1 varchar(25), col2 array<varchar(40)>, col3 int(11)>; args nullable: true; " +
                "result nullable: true]\n";
        Assertions.assertTrue(plan.contains(expected));
    }

    @Test
    public void testExchangeDefineDictField() throws Exception {
        String sql = "select UPPER(STRUCT(VARCHAR_COL, INTEGER_COL).col1) from T " +
                "ORDER BY UPPER(STRUCT(VARCHAR_COL, INTEGER_COL).col1)";
        String plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("  Global Dict Exprs:\n" +
                "    7: DictDefine(6: VARCHAR_COL, [upper(<place-holder>)])\n" +
                "\n" +
                "  4:Decode\n" +
                "  |  <dict id 7> : <string id 5>"));
    }

    @Test
    public void testArrayDictField() throws Exception {
        String sql = "select STRUCT(ARRAY_VARCHAR_COL, VARCHAR_COL).col1 from T";
        String plan = getVerboseExplain(sql);
        String expected = "5 <-> DictDecode([6: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>], row[([6: " +
                "ARRAY_VARCHAR_COL, ARRAY<INT>, true], [7: VARCHAR_COL, INT, true]); args: INVALID_TYPE,INT; result: " +
                "struct<col1 array<int(11)>, col2 int(11)>; args nullable: true; result nullable: true].col1[true])";
        Assertions.assertTrue(plan.contains(expected));
    }

    @Test
    public void testArrayElementDictField() throws Exception {
        String sql = "select STRUCT(ARRAY_VARCHAR_COL, VARCHAR_COL).col1[1] from T";
        String plan = getVerboseExplain(sql);
        String expected = "5 <-> DictDecode([6: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>], row[([6: " +
                "ARRAY_VARCHAR_COL, ARRAY<INT>, true], [7: VARCHAR_COL, INT, true]); args: INVALID_TYPE,INT; result: " +
                "struct<col1 array<int(11)>, col2 int(11)>; args nullable: true; result nullable: true].col1[true][1])";
        Assertions.assertTrue(plan.contains(expected));
    }
}