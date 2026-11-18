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

import com.starrocks.common.ExceptionChecker;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class JsonTypeTest extends PlanTestBase {

    @BeforeAll
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
    public void testPredicateImplicitCast() throws Exception {
        assertPlanContains("select parse_json('1') between 0.5 and 0.9",
                "CAST(parse_json('1') AS DOUBLE)");

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
        assertPlanContains("select parse_json('1') in (parse_json('1'), parse_json('2'))",
                "parse_json('1') IN (parse_json('1'), parse_json('2'))");
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
        assertPlanContains("select json_array(1, 1)", "json_array(CAST(1 AS JSON), CAST(1 AS JSON))");
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

    @Test
    public void testCastComplexTypeToJson() throws Exception {
        starRocksAssert.withTable("CREATE TABLE tjson_complex (" +
                " v_id INT," +
                " v_array ARRAY<INT>," +
                " v_nested ARRAY<ARRAY<VARCHAR(10)>>," +
                " v_array_json ARRAY<JSON>," +
                " v_map MAP<VARCHAR(10), BIGINT>," +
                " v_map_int MAP<INT, DOUBLE>," +
                " v_map_decimal MAP<DECIMAL(10, 2), INT>," +
                " v_map_boolean MAP<BOOLEAN, INT>," +
                " v_array_decimal ARRAY<DECIMAL(10, 2)>," +
                " v_array_boolean ARRAY<BOOLEAN>," +
                " v_struct STRUCT<a INT, b DECIMAL(10, 2), c ARRAY<DATETIME>>" +
                ") DUPLICATE KEY (v_id) " +
                "DISTRIBUTED BY HASH (v_id) " +
                "properties(\"replication_num\"=\"1\") ;");

        String plan = getFragmentPlan("select cast(v_array as json), cast(v_nested as json), " +
                "cast(v_array_json as json), cast(v_map as json), cast(v_map_int as json), " +
                "cast(v_array_decimal as json), cast(v_array_boolean as json), cast(v_struct as json) " +
                "from tjson_complex");
        assertContains(plan, "to_json(2: v_array)", "to_json(3: v_nested)", "to_json(4: v_array_json)",
                "to_json(5: v_map)", "to_json(6: v_map_int)",
                "to_json(CAST(9: v_array_decimal AS ARRAY<JSON>))", "to_json(CAST(10: v_array_boolean AS ARRAY<JSON>))",
                "to_json(CAST(11: v_struct AS struct<`a` int(11), `b` json, `c` array<datetime>>))");
        assertPlanContains("select cast(map{'a': [1, 2]} as json), cast(named_struct('a', 1, 'b', 'x') as json)",
                "to_json(map{'a':[1,2]})", "to_json(named_struct('a', 1, 'b', 'x'))");

        // Aggregated values, as in CAST(map_agg(k, v) AS JSON).
        assertPlanContains("select v_id, cast(array_agg(v_array) as json), cast(map_agg(v_id, v_array_decimal) as json) " +
                        "from tjson_complex group by v_id",
                "to_json(12: array_agg)", "to_json(CAST(13: map_agg AS MAP<INT,ARRAY<JSON>>))");
        // A view keeps the conversion of decimals to JSON.
        starRocksAssert.withView("CREATE VIEW tjson_complex_view AS " +
                "SELECT cast(v_array_decimal as json) AS j FROM tjson_complex");
        assertPlanContains("select j from tjson_complex_view", "to_json(CAST(9: v_array_decimal AS ARRAY<JSON>))");

        assertExceptionMsgContains("select cast(v_map_decimal as json) from tjson_complex",
                "Invalid type cast from map<decimal(10, 2),int(11)> to json");
        assertExceptionMsgContains("select cast(v_map_boolean as json) from tjson_complex",
                "Invalid type cast from map<boolean,int(11)> to json");
        // Only explicit casts are rewritten: a complex value is still not implicitly converted to JSON.
        assertExceptionMsgContains("select json_length(v_array) from tjson_complex",
                "No matching function with signature: json_length(array<int(11)>)");
    }

    @Test
    public void testFlatJsonMeta() throws Exception {
        String sql = "select flat_json_meta(v_json) from tjson_test[_META_]";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: flat_json_meta");

        sql = "select flat_json_meta(v_json), count(1) from tjson_test[_META_]";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(1)\n" +
                "  |  group by: flat_json_meta");

        sql = "select array_join(flat_json_meta(v_json), ', '), count(1) from tjson_test[_META_]";
        plan = getFragmentPlan(sql);
        assertContains(plan, "array_join(14: flat_json_meta, ', ')");

        Assertions.assertThrows(SemanticException.class, () -> getFragmentPlan(
                "select flat_json_meta(12) from tjson_test[_META_]"));

        Assertions.assertThrows(SemanticException.class, () -> getFragmentPlan(
                "select flat_json_meta(v_json) from tjson_test[_META_] group by v_INT"));

        Assertions.assertThrows(SemanticException.class, () -> getFragmentPlan(
                "select flat_json_meta(json_query(v_json, '$.v1')) from tjson_test[_META_]"));
    }
}
