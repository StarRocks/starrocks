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

        starRocksAssert.query("select max(id2) from test.bitmap_table;").analysisError(Type.NOT_SUPPORT_AGG_ERROR_MSG);

        starRocksAssert.query("select min(id2) from test.bitmap_table;").analysisError(Type.NOT_SUPPORT_AGG_ERROR_MSG);

        starRocksAssert.query("select count(*) from test.bitmap_table group by id2;")
                .analysisError(Type.NOT_SUPPORT_GROUP_BY_ERROR_MSG);

        starRocksAssert.query("select count(*) from test.bitmap_table where id2 = 1;").analysisError(
                "bitmap", "not support binary predicate");
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

        starRocksAssert.query("select max(id2) from test.hll_table;").analysisError(Type.NOT_SUPPORT_AGG_ERROR_MSG);

        starRocksAssert.query("select min(id2) from test.hll_table;").analysisError(Type.NOT_SUPPORT_AGG_ERROR_MSG);

        starRocksAssert.query("select min(id2) from test.hll_table;").analysisError(Type.NOT_SUPPORT_AGG_ERROR_MSG);

        starRocksAssert.query("select count(*) from test.hll_table group by id2;")
                .analysisError(Type.NOT_SUPPORT_GROUP_BY_ERROR_MSG);

        starRocksAssert.query("select count(*) from test.hll_table where id2 = 1").analysisError(
                "hll", "not support binary predicate");
    }

    @Test
    public void testJoinOnBitmapColumn() {
        String sql = "select * from test.bitmap_table a join test.bitmap_table b on a.id2 = b.id2";
        starRocksAssert.query(sql).analysisError("bitmap", "not support binary predicate");

        sql = "select * from test.bitmap_table a join test.bitmap_table b on a.id2 = b.id";
        starRocksAssert.query(sql).analysisError("bitmap", "not support binary predicate");

        sql = "select * from test.bitmap_table a join test.hll_table b on a.id2 = b.id2";
        starRocksAssert.query(sql).analysisError("bitmap", "not support binary predicate");

        sql = "select * from test.bitmap_table a join test.hll_table b where a.id2 in (1, 2, 3)";
        starRocksAssert.query(sql).analysisError("HLL, BITMAP, PERCENTILE and ARRAY, MAP, STRUCT type couldn't as Predicate");
    }

}
