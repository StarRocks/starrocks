// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.common.ExceptionChecker;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TranslateOperatorTest extends PlanTestBase {

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

    @AfterClass
    public static void afterClass() {
        PlanTestBase.tearDown();
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
                            "Invalid type cast from json to %s in sql ``default_cluster:test`.`tjson_test`.`v_json``",
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
                "json_array(CAST(1 AS JSON), CAST('1' AS JSON), CAST(TRUE AS JSON), CAST(FALSE AS JSON), CAST(1.1 AS JSON), NULL)");

        assertPlanContains(
                "select json_array(v_smallint, v_tinyint, v_int, v_boolean, v_double, v_varchar) from tjson_test",
                "json_array(CAST(3: v_SMALLINT AS JSON), CAST(4: v_TINYINT AS JSON), CAST(5: v_INT AS JSON), CAST(8: v_BOOLEAN AS JSON), CAST(9: v_DOUBLE AS JSON), CAST(11: v_VARCHAR AS JSON))");
    }

    private void assertPlanContains(String sql, String expected) throws Exception {
        String plan = getFragmentPlan(sql);
        Assert.assertTrue("expected is: " + expected + " but plan is \n" + plan,
                StringUtils.containsIgnoreCase(plan.toLowerCase(), expected));
    }

}
