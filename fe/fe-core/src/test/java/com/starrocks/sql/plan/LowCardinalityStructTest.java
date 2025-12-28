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
        starRocksAssert.withTable("""
                CREATE TABLE T (
                    KEY_COL             INTEGER NOT NULL,
                    VARCHAR_COL         VARCHAR(25),
                    ARRAY_VARCHAR_COL   ARRAY<VARCHAR(40)>,
                    INTEGER_COL         INTEGER)
                ENGINE=OLAP
                DUPLICATE KEY(`KEY_COL`)
                COMMENT "OLAP"
                DISTRIBUTED by HASH(`KEY_COL`) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "in_memory" = "false"
                );
                """);

        starRocksAssert.withTable("""
                CREATE TABLE T2 (
                    KEY_COL2            INTEGER NOT NULL,
                    VARCHAR_COL2        VARCHAR(25),
                    ARRAY_VARCHAR_COL2  ARRAY<VARCHAR(40)>,
                    INTEGER_COL2        INTEGER)
                ENGINE=OLAP
                DUPLICATE KEY(`KEY_COL2`)
                COMMENT "OLAP"
                DISTRIBUTED by HASH(`KEY_COL2`) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "in_memory" = "false"
                );
                """);

        starRocksAssert.withTable("""
                CREATE TABLE T_WITH_STRUCT (
                    KEY_COL             INTEGER NOT NULL,
                    VARCHAR_COL         VARCHAR(25),
                    ARRAY_VARCHAR_COL   ARRAY<VARCHAR(40)>,
                    INTEGER_COL         INTEGER,
                    STRUCT_COL          STRUCT<VARCHAR_FIELD VARCHAR(50), INTEGER_FIELD INTEGER>)
                ENGINE=OLAP
                DUPLICATE KEY(`KEY_COL`)
                COMMENT "OLAP"
                DISTRIBUTED by HASH(`KEY_COL`) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "in_memory" = "false"
                );
                """);

        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setSqlMode(2);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setCboCteReuse(false);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
        connectContext.getSessionVariable().setEnableStructLowCardinalityOptimize(true);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setSqlMode(0);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
        connectContext.getSessionVariable().setEnableStructLowCardinalityOptimize(false);
    }

    @Test
    public void testExpressionDictField() throws Exception {
        String sql = """
                SELECT UPPER(STRUCT(VARCHAR_COL, ARRAY_VARCHAR_COL, INTEGER_COL).col1)
                FROM T
                """;
        String plan = getVerboseExplain(sql);
        String expected = "5 <-> DictDecode([6: VARCHAR_COL, INT, true], [upper[(<place-holder>); args: VARCHAR; " +
                "result: VARCHAR; args nullable: true; result nullable: true]], row[([6: VARCHAR_COL, INT, true], " +
                "[3: ARRAY_VARCHAR_COL, ARRAY<VARCHAR(40)>, true], [4: INTEGER_COL, INT, true]);";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testExpressionNonDictField() throws Exception {
        String sql = """
                SELECT STRUCT(VARCHAR_COL, INTEGER_COL).col2
                FROM T
                """;
        String plan = getVerboseExplain(sql);
        String expected = "5 <-> row[([2: VARCHAR_COL, VARCHAR, true], [4: INTEGER_COL, INT, true]); " +
                "args: VARCHAR,INT; result: struct<col1 varchar(25), col2 int(11)>; args nullable: true; " +
                "result nullable: true].col2[true";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testExpressionAllFields() throws Exception {
        String sql = """
                WITH T AS (
                    SELECT STRUCT(VARCHAR_COL, ARRAY_VARCHAR_COL, INTEGER_COL) AS S
                    FROM T
                )
                SELECT S.col1, S.col2, S.col3 FROM T
                """;
        String plan = getVerboseExplain(sql);
        String expected = "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  6 <-> DictDecode([9: VARCHAR_COL, INT, true], [<place-holder>], row[([9: VARCHAR_COL, INT, " +
                "true], [10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [4: INTEGER_COL, INT, true]); args: " +
                "INT,INVALID_TYPE,INT; result: struct<col1 int(11), col2 array<int(11)>, col3 int(11)>; args " +
                "nullable: true; result nullable: true].col1[true])\n" +
                "  |  7 <-> DictDecode([10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>], row[([9: " +
                "VARCHAR_COL, INT, true], [10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [4: INTEGER_COL, INT, true]); " +
                "args: INT,INVALID_TYPE,INT; result: struct<col1 int(11), col2 array<int(11)>, col3 int(11)>; args " +
                "nullable: true; result nullable: true].col2[true])\n" +
                "  |  8 <-> row[(DictDecode([9: VARCHAR_COL, INT, true], [<place-holder>]), DictDecode([10: " +
                "ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>]), [4: INTEGER_COL, INT, true]); args: " +
                "VARCHAR,INVALID_TYPE,INT; result: struct<col1 varchar(25), col2 array<varchar(40)>, col3 int(11)>;" +
                " args nullable: true; result nullable: true].col3[true]\n";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testStructArgument() throws Exception {
        String sql = """
                SELECT TO_JSON(STRUCT(VARCHAR_COL, INTEGER_COL, ARRAY_VARCHAR_COL))
                FROM T
                """;
        String plan = getVerboseExplain(sql);
        String expected = "5 <-> to_json[(row[([2: VARCHAR_COL, VARCHAR, true], [4: INTEGER_COL, INT, true], " +
                "[3: ARRAY_VARCHAR_COL, ARRAY<VARCHAR(40)>, true]); args: VARCHAR,INT,INVALID_TYPE; result: " +
                "struct<col1 varchar(25), col2 int(11), col3 array<varchar(40)>>; args nullable: true; result " +
                "nullable: true]); args: INVALID_TYPE; result: JSON; args nullable: true; result nullable: true]";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testTopLevelStructNonProfitable() throws Exception {
        String sql = "SELECT STRUCT(VARCHAR_COL, ARRAY_VARCHAR_COL, INTEGER_COL) FROM T";
        String plan = getVerboseExplain(sql);
        String expected = "5 <-> row[([2: VARCHAR_COL, VARCHAR, true], [3: ARRAY_VARCHAR_COL, " +
                "ARRAY<VARCHAR(40)>, true], [4: INTEGER_COL, INT, true])";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testNamedStructTopLevel() throws Exception {
        String sql = """
                SELECT named_struct('c1', VARCHAR_COL, 'c2', ARRAY_VARCHAR_COL, 'c3', INTEGER_COL)
                FROM T
                ORDER BY VARCHAR_COL
                """;
        String plan = getVerboseExplain(sql);
        System.out.println(plan);
        String expected = "5 <-> named_struct[('c1', DictDecode([6: VARCHAR_COL, INT, true], [<place-holder>], " +
                "[8: named_struct, struct<c1 int(11), c2 array<int(11)>, c3 int(11)>, true].c1[true]), 'c2', " +
                "DictDecode([7: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>], [8: named_struct, " +
                "struct<c1 int(11), c2 array<int(11)>, c3 int(11)>, true].c2[true]), 'c3', [8: named_struct, " +
                "struct<c1 int(11), c2 array<int(11)>, c3 int(11)>, true].c3[true]); args: " +
                "VARCHAR,VARCHAR,VARCHAR,INVALID_TYPE,VARCHAR,INT; result: struct<c1 varchar(25), c2 " +
                "array<varchar(40)>, c3 int(11)>; args nullable: true; result nullable: true]";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testNamedStructGetFields() throws Exception {
        String sql = """
                WITH TB2 AS (
                    SELECT named_struct('c1', VARCHAR_COL, 'c2', ARRAY_VARCHAR_COL, 'c3', INTEGER_COL) AS S
                    FROM T
                )
                SELECT S.c1, S.c2, S.c3 FROM TB2
                """;
        String plan = getVerboseExplain(sql);
        String expected = "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  6 <-> DictDecode([9: VARCHAR_COL, INT, true], [<place-holder>], named_struct[('c1', [9: " +
                "VARCHAR_COL, INT, true], 'c2', [10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], 'c3', [4: INTEGER_COL, " +
                "INT, true]); args: VARCHAR,INT,VARCHAR,INVALID_TYPE,VARCHAR,INT; result: struct<c1 int(11), c2 " +
                "array<int(11)>, c3 int(11)>; args nullable: true; result nullable: true].c1[true])\n" +
                "  |  7 <-> DictDecode([10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>], named_struct[('c1'" +
                ", [9: VARCHAR_COL, INT, true], 'c2', [10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], 'c3', [4: " +
                "INTEGER_COL, INT, true]); args: VARCHAR,INT,VARCHAR,INVALID_TYPE,VARCHAR,INT; result: struct<c1 " +
                "int(11), c2 array<int(11)>, c3 int(11)>; args nullable: true; result nullable: true].c2[true])\n" +
                "  |  8 <-> named_struct[('c1', DictDecode([9: VARCHAR_COL, INT, true], [<place-holder>]), 'c2', " +
                "DictDecode([10: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>]), 'c3', [4: INTEGER_COL, INT," +
                " true]); args: VARCHAR,VARCHAR,VARCHAR,INVALID_TYPE,VARCHAR,INT; result: struct<c1 varchar(25), c2" +
                " array<varchar(40)>, c3 int(11)>; args nullable: true; result nullable: true].c3[true]\n";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testOrderByDefineDictField() throws Exception {
        String sql = """
                SELECT UPPER(STRUCT(VARCHAR_COL, INTEGER_COL).col1)
                FROM T
                ORDER BY UPPER(STRUCT(VARCHAR_COL, INTEGER_COL).col1)
                """;
        String plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("  Global Dict Exprs:\n" +
                "    7: DictDefine(6: VARCHAR_COL, [upper(<place-holder>)])\n" +
                "\n" +
                "  4:Decode\n" +
                "  |  <dict id 7> : <string id 5>"), plan);
    }

    @Test
    public void testArrayDictField() throws Exception {
        String sql = "SELECT STRUCT(ARRAY_VARCHAR_COL, VARCHAR_COL).col1 FROM T";
        String plan = getVerboseExplain(sql);
        String expected = "5 <-> DictDecode([6: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>], " +
                "row[([6: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [2: VARCHAR_COL, VARCHAR, true]);";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testArrayElementDictField() throws Exception {
        String sql = "SELECT STRUCT(ARRAY_VARCHAR_COL, VARCHAR_COL).col1[1] FROM T";
        String plan = getVerboseExplain(sql);
        String expected = "5 <-> DictDecode([6: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>], " +
                "row[([6: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [2: VARCHAR_COL, VARCHAR, true]); args: " +
                "INVALID_TYPE,VARCHAR; result: struct<col1 array<int(11)>, col2 varchar(25)>; args nullable: true; " +
                "result nullable: true].col1[true][1])";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testStructField() throws Exception {
        String sql = """
                SELECT STRUCT(STRUCT(VARCHAR_COL), ARRAY_VARCHAR_COL), UPPER(VARCHAR_COL)
                FROM T
                ORDER BY INTEGER_COL
                """;
        String plan = getVerboseExplain(sql);
        String expected = "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  4 <-> [4: INTEGER_COL, INT, true]\n" +
                "  |  5 <-> row[(row[(DictDecode([7: VARCHAR_COL, INT, true], [<place-holder>])); args: VARCHAR; " +
                "result: struct<col1 varchar(25)>; args nullable: true; result nullable: true], [3: " +
                "ARRAY_VARCHAR_COL, ARRAY<VARCHAR(40)>, true]); args: INVALID_TYPE,INVALID_TYPE; result: " +
                "struct<col1 struct<col1 varchar(25)>, col2 array<varchar(40)>>; args nullable: true;" +
                " result nullable: true]\n" +
                "  |  8 <-> DictDefine([7: VARCHAR_COL, INT, true], [upper[(<place-holder>); args: VARCHAR; result:" +
                " VARCHAR; args nullable: true; result nullable: true]])";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testJoin() throws Exception {
        String sql = """
                WITH TB1 AS (
                    SELECT struct(VARCHAR_COL) S1
                    FROM T
                    ORDER BY INTEGER_COL
                    limit 10
                ),
                TB2 AS (
                    SELECT struct(VARCHAR_COL2) S2
                    FROM T2
                    ORDER BY INTEGER_COL2
                    limit 10
                )
                SELECT S1, S2
                FROM TB1 CROSS JOIN TB2;
                """;
        String plan = getVerboseExplain(sql);
        String expected = "  10:Project\n" +
                "  |  output columns:\n" +
                "  |  5 <-> named_struct[('col1', DictDecode([11: VARCHAR_COL, INT, true], [<place-holder>], [13: " +
                "row, struct<col1 int(11)>, true].col1[true])); args: VARCHAR,VARCHAR; result: struct<col1 " +
                "varchar(25)>; args nullable: true; result nullable: true]\n" +
                "  |  10 <-> named_struct[('col1', DictDecode([12: VARCHAR_COL2, INT, true], [<place-holder>], [14:" +
                " row, struct<col1 int(11)>, true].col1[true])); args: VARCHAR,VARCHAR; result: struct<col1 " +
                "varchar(25)>; args nullable: true; result nullable: true]\n";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testTwoStageDecode() throws Exception {
        String sql = """
                SELECT STRUCT(VARCHAR_COL), ARRAY_VARCHAR_COL, INTEGER_COL
                FROM T
                ORDER BY KEY_COL
                """;
        String plan = getVerboseExplain(sql);
        String expected = "  6:Decode\n" +
                "  |  <dict id 7> : <string id 3>\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  output columns:\n" +
                "  |  4 <-> [4: INTEGER_COL, INT, true]\n" +
                "  |  5 <-> named_struct[('col1', DictDecode([6: VARCHAR_COL, INT, true], [<place-holder>], [8: row," +
                " struct<col1 int(11)>, true].col1[true])); args: VARCHAR,VARCHAR; result: struct<col1 varchar(25)>; " +
                "args nullable: true; result nullable: true]\n" +
                "  |  7 <-> [7: ARRAY_VARCHAR_COL, ARRAY<INT>, true]";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testAggregation() throws Exception {
        String sql = """
                WITH TB AS (
                    SELECT STRUCT(VARCHAR_COL, INTEGER_COL) S
                        FROM T
                ),
                TB2 AS (
                    SELECT ANY_VALUE(S) A
                    FROM TB
                )
                SELECT A
                FROM TB2
                """;
        ExecPlan plan2 = getExecPlan(sql);
        Assertions.assertNotNull(plan2);
        String plan = getVerboseExplain(sql);
        String expected = "  3:Project\n" +
                "  |  output columns:\n" +
                "  |  6 <-> named_struct[('col1', DictDecode([7: VARCHAR_COL, INT, true], [<place-holder>], [9: " +
                "any_value, struct<col1 int(11), col2 int(11)>, true].col1[true]), 'col2', [9: any_value, struct<col1" +
                " int(11), col2 int(11)>, true].col2[true]); args: VARCHAR,VARCHAR,VARCHAR,INT; result: struct<col1" +
                " varchar(25), col2 int(11)>; args nullable: true; result nullable: true]\n";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testAggregationGetFields() throws Exception {
        String sql = """
                WITH TB AS (
                    SELECT STRUCT(VARCHAR_COL, INTEGER_COL, ARRAY_VARCHAR_COL) S
                        FROM T
                ),
                TB2 AS (
                    SELECT ANY_VALUE(S) A
                    FROM TB
                )
                SELECT A.col1, A.col2, A.col3
                FROM TB2
                """;
        ExecPlan plan2 = getExecPlan(sql);
        Assertions.assertNotNull(plan2);
        String plan = getVerboseExplain(sql);
        String expected = "  3:Project\n" +
                "  |  output columns:\n" +
                "  |  7 <-> DictDecode([13: VARCHAR_COL, INT, true], [<place-holder>], [16: any_value, struct<col1 " +
                "int(11), col2 int(11), col3 array<int(11)>>, true].col1[true])\n" +
                "  |  8 <-> [16: any_value, struct<col1 int(11), col2 int(11), col3 array<int(11)>>, true].col2" +
                "[false]\n" +
                "  |  9 <-> DictDecode([14: ARRAY_VARCHAR_COL, ARRAY<INT>, true], [<place-holder>], [16: any_value, " +
                "struct<col1 int(11), col2 int(11), col3 array<int(11)>>, true].col3[true])\n";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testJoinAggregation() throws Exception {
        String sql = """
                WITH TB AS (
                    SELECT STRUCT(VARCHAR_COL, INTEGER_COL) S
                        FROM T
                ),
                TB2 AS (
                    SELECT ANY_VALUE(S) A
                    FROM TB
                ),
                TB3 AS (
                    SELECT STRUCT(VARCHAR_COL2, INTEGER_COL2) S
                    FROM T2
                ),
                TB4 AS (
                    SELECT ANY_VALUE(S) A
                    from TB3
                )
                SELECT TB2.A c1, TB4.A c2
                FROM TB2 CROSS JOIN TB4
                """;
        String plan = getVerboseExplain(sql);
        String expected = "  8:Project\n" +
                "  |  output columns:\n" +
                "  |  6 <-> named_struct[('col1', DictDecode([13: VARCHAR_COL, INT, true], [<place-holder>], [16: " +
                "any_value, struct<col1 int(11), col2 int(11)>, true].col1[true]), 'col2', [16: any_value, struct" +
                "<col1 int(11), col2 int(11)>, true].col2[true]); args: VARCHAR,VARCHAR,VARCHAR,INT; result: struct" +
                "<col1 varchar(25), col2 int(11)>; args nullable: true; result nullable: true]\n" +
                "  |  12 <-> named_struct[('col1', DictDecode([14: VARCHAR_COL2, INT, true], [<place-holder>], [18: " +
                "any_value, struct<col1 int(11), col2 int(11)>, true].col1[true]), 'col2', [18: any_value, struct" +
                "<col1 int(11), col2 int(11)>, true].col2[true]); args: VARCHAR,VARCHAR,VARCHAR,INT; result: struct" +
                "<col1 varchar(25), col2 int(11)>; args nullable: true; result nullable: true]\n";
        Assertions.assertTrue(plan.contains(expected), plan);
    }

    @Test
    public void testNoStringFieldsInStruct() throws Exception {
        String sql = """
                SELECT STRUCT(INTEGER_COL), UPPER(VARCHAR_COL), STRUCT(INTEGER_COL).col1, STRUCT(1).col1
                FROM T;
                """;
        String plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("  1:Project\n" +
                "  |  output columns:\n" +
                "  |  5 <-> [10: row, struct<col1 int(11)>, true]\n" +
                "  |  6 <-> DictDecode([9: VARCHAR_COL, INT, true], [upper[(<place-holder>); args: VARCHAR; result: " +
                "VARCHAR; args nullable: true; result nullable: true]])\n" +
                "  |  7 <-> row[([4: INTEGER_COL, INT, true]); args: INT; result: struct<col1 int(11)>; args " +
                "nullable: true; result nullable: true].col1[true]\n" +
                "  |  8 <-> row[(1); args: TINYINT; result: struct<col1 tinyint(4)>; args nullable: false; " +
                "result nullable: true].col1[true]"), plan);
    }

    @Test
    public void testAggregatedStructAsArgument() throws Exception {
        String sql = """
                WITH TB AS (
                    SELECT STRUCT(VARCHAR_COL, INTEGER_COL) S
                        from T
                ),
                TB2 AS (
                    SELECT ANY_VALUE(S) A
                    from TB
                )
                SELECT TO_JSON(A)
                FROM TB2
                """;
        String plan = getVerboseExplain(sql);
        Assertions.assertTrue(plan.contains("  3:Project\n" +
                "  |  output columns:\n" +
                "  |  7 <-> to_json[(named_struct[('col1', DictDecode([8: VARCHAR_COL, INT, true], [<place-holder>], " +
                "[10: any_value, struct<col1 int(11), col2 int(11)>, true].col1[true]), 'col2', [10: any_value, " +
                "struct<col1 int(11), col2 int(11)>, true].col2[true]); args: VARCHAR,VARCHAR,VARCHAR,INT; result: " +
                "struct<col1 varchar(25), col2 int(11)>; args nullable: true; result nullable: true]); args: " +
                "INVALID_TYPE; result: JSON; args nullable: true; result nullable: true]\n"), plan);
    }

    @Test
    public void testStructInScan() throws Exception {
        String sql = """
                SELECT STRUCT_COL, UPPER(STRUCT_COL.VARCHAR_FIELD), UPPER(VARCHAR_COL)
                FROM T_WITH_STRUCT
                """;
        String plan = getVerboseExplain(sql);
        String expected = "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  5 <-> [5: STRUCT_COL, struct<VARCHAR_FIELD varchar(50), INTEGER_FIELD int(11)>, true]\n" +
                "  |  6 <-> upper[([5: STRUCT_COL, struct<VARCHAR_FIELD varchar(50), INTEGER_FIELD int(11)>, true]." +
                "VARCHAR_FIELD[true]); args: VARCHAR; result: VARCHAR; args nullable: true; result nullable: true]\n" +
                "  |  7 <-> DictDecode([8: VARCHAR_COL, INT, true], [upper[(<place-holder>); args: VARCHAR; " +
                "result: VARCHAR; args nullable: true; result nullable: true]])\n";
        Assertions.assertTrue(plan.contains(expected), plan);
    }
}