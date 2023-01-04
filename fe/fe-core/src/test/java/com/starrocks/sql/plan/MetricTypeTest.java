// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.catalog.Type;
import org.junit.Test;

public class MetricTypeTest extends PlanTestBase {
    @Test
    public void testBitmapQuery() throws Exception {
        starRocksAssert.query(
                "select * from test.bitmap_table;").explainContains(
                "OUTPUT EXPRS:1: id | 2: id2"
        );

        starRocksAssert.query("select count(id2) from test.bitmap_table;")
                .explainContains("OUTPUT EXPRS:3: count",
                        "1:AGGREGATE (update finalize)", "output: count(2: id2)", "group by:", "0:OlapScanNode",
                        "PREAGGREGATION: OFF. Reason: Aggregate Operator not match: COUNT <--> BITMAP_UNION");

        starRocksAssert.query("select group_concat(id2) from test.bitmap_table;")
                .analysisError("No matching function with signature: group_concat(bitmap).");

        starRocksAssert.query("select sum(id2) from test.bitmap_table;").analysisError(
                "No matching function with signature: sum(bitmap).");

        starRocksAssert.query("select avg(id2) from test.bitmap_table;")
                .analysisError("No matching function with signature: avg(bitmap).");

        starRocksAssert.query("select max(id2) from test.bitmap_table;").analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select min(id2) from test.bitmap_table;").analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select count(*) from test.bitmap_table group by id2;")
                .analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select count(*) from test.bitmap_table where id2 = 1;").analysisError(
                "binary type bitmap with type double is invalid.");
    }

    @Test
    public void testHLLTypeQuery() throws Exception {
        starRocksAssert.query("select * from test.hll_table;").explainContains(
                "OUTPUT EXPRS:1: id | 2: id2");

        starRocksAssert.query("select count(id2) from test.hll_table;").explainContains("OUTPUT EXPRS:3: count",
                "1:AGGREGATE (update finalize)", "output: count(2: id2)", "group by:", "0:OlapScanNode",
                "PREAGGREGATION: OFF. Reason: Aggregate Operator not match: COUNT <--> HLL_UNION");

        starRocksAssert.query("select group_concat(id2) from test.hll_table;")
                .analysisError("No matching function with signature: group_concat(hll).");

        starRocksAssert.query("select sum(id2) from test.hll_table;")
                .analysisError("No matching function with signature: sum(hll).");

        starRocksAssert.query("select avg(id2) from test.hll_table;")
                .analysisError("No matching function with signature: avg(hll).");

        starRocksAssert.query("select max(id2) from test.hll_table;").analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select min(id2) from test.hll_table;").analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select min(id2) from test.hll_table;").analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select count(*) from test.hll_table group by id2;")
                .analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select count(*) from test.hll_table where id2 = 1").analysisError(
                "binary type hll with type double is invalid.");
    }

    @Test
    public void TestJoinOnBitmapColumn() {
        String sql = "select * from test.bitmap_table a join test.bitmap_table b on a.id2 = b.id2";
        starRocksAssert.query(sql).analysisError("binary type bitmap with type varchar(-1) is invalid.");

        sql = "select * from test.bitmap_table a join test.bitmap_table b on a.id2 = b.id";
        starRocksAssert.query(sql).analysisError("binary type bitmap with type double is invalid.");

        sql = "select * from test.bitmap_table a join test.hll_table b on a.id2 = b.id2";
        starRocksAssert.query(sql).analysisError("binary type bitmap with type varchar(-1) is invalid.");

        sql = "select * from test.bitmap_table a join test.hll_table b where a.id2 in (1, 2, 3)";
        starRocksAssert.query(sql).analysisError("HLL, BITMAP, PERCENTILE and ARRAY, MAP, STRUCT type couldn't as Predicate");
    }

}
