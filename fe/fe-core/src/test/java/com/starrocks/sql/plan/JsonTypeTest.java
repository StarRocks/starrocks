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

import com.starrocks.catalog.Type;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class JsonTypeTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        starRocksAssert.withTable("CREATE TABLE tjson_test (" +
                " v_id INT," +
                " v_json json, " +
                " v_SMALLINT SMALLINT," +
                " v_TINYINT TINYINT," +
                " v_INT INT," +
                " v_BIGINT BIGINT," +
                " v_LARGEINT LARGEINT," +
                " v_BOOLEAN BOOLEAN," +
                " v_DOUBLE DOUBLE," +
                " v_FLOAT FLOAT," +
                " v_VARCHAR VARCHAR," +
                " v_CHAR CHAR," +
                " v_decimal decimal128(32,1)" +
                ") DUPLICATE KEY (v_id) " +
                "DISTRIBUTED BY HASH (v_id) " +
                "properties(\"replication_num\"=\"1\") ;"
        );
    }

    @Test
    public void testJoin() {
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                Type.NOT_SUPPORT_JOIN_ERROR_MSG,
                () -> getFragmentPlan("select * from tjson_test t1 join tjson_test t2 using(v_json)"));

        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                Type.NOT_SUPPORT_JOIN_ERROR_MSG,
                () -> getFragmentPlan("select * from tjson_test t1 join tjson_test t2 on t1.v_json = t2.v_json"));
        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                Type.NOT_SUPPORT_JOIN_ERROR_MSG,
                () -> getFragmentPlan("select * from tjson_test t1 join tjson_test t2 on t1.v_json > t2.v_json"));

        ExceptionChecker.expectThrowsNoException(
                () -> getFragmentPlan("select * from tjson_test t1 join tjson_test t2 on t1.v_id = t2.v_id"));

        ExceptionChecker.expectThrowsNoException(
                () -> getFragmentPlan("select * from tjson_test t1 join tjson_test t2 on" +
                        " cast(t1.v_json->'a' as int) = cast(t2.v_json->'a' as int)"));

        ExceptionChecker.expectThrowsNoException(
                () -> getFragmentPlan("select * from tjson_test t1 join tjson_test t2 on" +
                        " cast(t1.v_json->'a' as int) = cast(t2.v_json->'a' as int) and t1.v_id = t2.v_id"));

        ExceptionChecker.expectThrowsWithMsg(SemanticException.class,
                Type.NOT_SUPPORT_JOIN_ERROR_MSG,
                () -> getFragmentPlan("select * from tjson_test t1 join tjson_test t2 on" +
                        " t1.v_id = t2.v_id and t1.v_json = t2.v_json"));
    }

    /**
     * Arrow expression should be rewrite to json_query
     */
    @Test
    public void testRewriteArrowExpr() throws Exception {
        assertPlanContains("select parse_json('1') -> '$.k1' ",
                "json_query(parse_json('1'), '$.k1')");
        assertPlanContains("select v_json -> '$.k1' from tjson_test ",
                "json_query(2: v_json, '$.k1')");

        // arrow and cast
        assertPlanContains("select cast(parse_json('1') -> '$.k1' as int) ",
                "json_query(parse_json('1'), '$.k1')");
        assertPlanContains("select cast(v_json -> '$.k1' as int) from tjson_test",
                "json_query(2: v_json, '$.k1')");
    }

    @Test
    public void testPredicateImplicitCast() throws Exception {
        assertPlanContains("select parse_json('1') between 0.5 and 0.9",
                "CAST(3: parse_json AS DOUBLE)");

        List<String> operators = Arrays.asList("<", "<=", "=", ">=", "!=");
        for (String operator : operators) {
            assertPlanContains(String.format("select parse_json('1') %s 1", operator),
                    String.format("parse_json('1') %s CAST(1 AS JSON)", operator));
            assertPlanContains(String.format("select parse_json('1') %s 3.14", operator),
                    String.format("parse_json('1') %s CAST(3.14 AS JSON)", operator));
            assertPlanContains(String.format("select parse_json('1') %s 'a'", operator),
                    String.format("parse_json('1') %s CAST('a' AS JSON)", operator));
            assertPlanContains(String.format("select parse_json('1') %s false", operator),
                    String.format("parse_json('1') %s CAST(false AS JSON)", operator));
        }

        assertPlanContains("select parse_json('1') in (1, 2, 3)", "IN");
        assertPlanContains("select parse_json('1') in (parse_json('1'), parse_json('2'))", "OR");
    }

    /**
     * Test various type cast
     */
    @Test
    public void testCastJson() throws Exception {
        List<String> allowedCastTypes =
                Arrays.asList("SMALLINT", "TINYINT", "INT", "BIGINT", "LARGEINT",
                        "BOOLEAN",
                        "DOUBLE", "FLOAT",
                        "VARCHAR", "CHAR");
        for (int i = 0; i < allowedCastTypes.size(); i++) {
            String allowType = allowedCastTypes.get(i);
            int slot = i + 3;
            try {
                // cast json to sql type
                {
                    String expected = String.format("CAST(2: v_json AS %s)", allowType);
                    String columnCastSql = String.format("select cast(v_json as %s) from tjson_test", allowType);
                    assertPlanContains(columnCastSql, expected);
                }

                {
                    String functionCastSql = String.format("select cast(parse_json('') as %s)", allowType);
                    String expected = String.format("CAST(parse_json('') AS %s)", allowType);
                    assertPlanContains(functionCastSql, expected);
                }

                // cast sql type to json
                {
                    String expected = String.format("CAST(%d: v_%s AS JSON)", slot, allowType);
                    String columnCastSql = String.format("select cast(v_%s AS JSON) from tjson_test", allowType);
                    assertPlanContains(columnCastSql, expected);
                }
            } catch (Exception e) {
                throw new Exception("failed in case " + allowType, e);
            }
        }

        // special for decimal
        {
            assertPlanContains("select cast(v_json as DECIMAL128(32,1)) from tjson_test",
                    "CAST(2: v_json AS DECIMAL128(32,1))");
            assertPlanContains("select cast(v_decimal AS JSON) from tjson_test",
                    "CAST(13: v_decimal AS JSON)");
        }

        List<String> notAllowedCastTypes = Arrays.asList("date", "datetime");
        for (String notAllowType : notAllowedCastTypes) {
            String columnCastSql = String.format("select cast(v_json as %s) from tjson_test", notAllowType);
            ExceptionChecker.expectThrowsWithMsg(
                    SemanticException.class,
                    String.format(
                            "Invalid type cast from json to %s in sql `test.tjson_test.v_json`",
                            notAllowType),
                    () -> getFragmentPlan(columnCastSql)
            );

            String functionCastSql = String.format("select cast(parse_json('') as %s)", notAllowType);
            ExceptionChecker.expectThrowsWithMsg(
                    SemanticException.class,
                    String.format("Invalid type cast from json to %s in sql `parse_json('')`", notAllowType),
                    () -> getFragmentPlan(functionCastSql)
            );
        }

    }

    /**
     * Test type cast for json_array
     * Add implicit cast for all SQL type to JSON type
     */
    @Test
    public void testCastJsonArray() throws Exception {
        assertPlanContains("select json_array(parse_json('1'), parse_json('2'))",
                "json_array(parse_json('1'), parse_json('2'))");
        assertPlanContains("select json_array(1, 1)", "json_array(3: cast, 3: cast)");
        assertPlanContains("select json_array(1, '1')", "json_array(CAST(1 AS JSON), CAST('1' AS JSON))");
        assertPlanContains("select json_array(1.1)", "json_array(CAST(1.1 AS JSON))");
        assertPlanContains("select json_array(NULL)", "NULL");
        assertPlanContains("select json_array(true)", "json_array(CAST(true AS JSON))");
        assertPlanContains("select json_array(1.0E8)", "json_array(CAST(100000000 AS JSON))");

        assertPlanContains("select json_array(NULL, NULL, 1)",
                "json_array(NULL, NULL, CAST(1 AS JSON))");
        assertPlanContains("select json_array(1, NULL)",
                "json_array(CAST(1 AS JSON), NULL)");
        assertPlanContains("select json_array(1, '1', true, false, 1.1, null)",
                "json_array(CAST(1 AS JSON), CAST('1' AS JSON), CAST(TRUE AS JSON), " +
                        "CAST(FALSE AS JSON), CAST(1.1 AS JSON), NULL)");

        assertPlanContains(
                "select json_array(v_smallint, v_tinyint, v_int, v_boolean, v_double, v_varchar) from tjson_test",
                "json_array(CAST(3: v_SMALLINT AS JSON), CAST(4: v_TINYINT AS JSON), " +
                        "CAST(5: v_INT AS JSON), CAST(8: v_BOOLEAN AS JSON), " +
                        "CAST(9: v_DOUBLE AS JSON), CAST(11: v_VARCHAR AS JSON))");
    }

    @Test
    public void testCastJsonToArray() throws Exception {
        assertPlanContains("select cast(json_array(1,2,3) as array<int>)",
                "CAST(json_array(CAST(1 AS JSON), CAST(2 AS JSON), CAST(3 AS JSON)) AS ARRAY<INT>)");
        assertPlanContains("select cast(json_array(1,2,3) as array<varchar>)",
                "CAST(json_array(CAST(1 AS JSON), CAST(2 AS JSON), CAST(3 AS JSON)) AS ARRAY<VARCHAR>)");
        assertPlanContains("select cast(json_array(1,2,3) as array<JSON>)",
                "CAST(json_array(CAST(1 AS JSON), CAST(2 AS JSON), CAST(3 AS JSON)) AS ARRAY<JSON>)");

        // Multi-dimension array casting is not supported
        assertExceptionMsgContains("select cast(json_array(1,2,3) as array<array<int>>)",
                "Getting analyzing error from line 1, column 7 to line 1, column 50. Detail message: " +
                        "Invalid type cast from json to array<array<int(11)>> in sql `json_array(1, 2, 3)`.");
    }
}
