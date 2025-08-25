package com.starrocks.sql.plan;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class MinMaxMonotonicRewriteTest extends PlanTestBase {

    @ParameterizedTest
    @CsvSource(
            delimiter = '|',
            value = {
                    "select min(to_datetime(id_datetime)) from test_all_type " +
                            "| output: min(8: id_datetime) " +
                            "| to_datetime(",
                    "select max(to_datetime(id_datetime)) from test_all_type " +
                            "| output: max(8: id_datetime) " +
                            "| to_date(",
                    "select max(from_unixtime(id_datetime)) from test_all_type " +
                            "| output: max(8: id_datetime) " +
                            "| from_unixtime(",
            })
    public void testRewriteMinMaxMonotonic(String sql, String expectedAggregation, String expectedProject)
            throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, expectedAggregation);
        assertContains(plan, ":Project\n", expectedProject);
    }

    @ParameterizedTest
    @CsvSource(
            delimiter = '|',
            value = {
                    "select min(id_int + 1) from test_all_type " +
                            "| output: min(1: id_int + 1) " +
                            "| 1: id_int + 1",
                    "select max(id_int * 2) from test_all_type " +
                            "| output: max(1: id_int * 2) " +
                            "| 1: id_int * 2",
                    "select min(concat(id_varchar, 'suffix')) from test_all_type " +
                            "| output: min(2: concat(id_varchar, 'suffix')) " +
                            "| 2: concat(id_varchar, 'suffix')",
                    "select max(abs(id_int)) from test_all_type " +
                            "| output: max(abs(1: id_int)) " +
                            "| abs(1: id_int)",
                    "select min(if(id_int > 0, id_int, 0)) from test_all_type " +
                            "| output: min(if(1: id_int > 0, 1: id_int, 0)) " +
                            "| if(1: id_int > 0, 1: id_int, 0)",
                    "select min(cast(id_datetime as date)) from test_all_type " +
                            "| output: min(8: id_datetime) " +
                            "| CAST(8: id_datetime AS DATE)"
            })
    public void testNoRewriteForNonMonotonicExpressions(String sql, String expectedAggregation, String expectedProject)
            throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, expectedAggregation);
        assertNotContains(plan, ":Project\n");
    }
}