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

package com.starrocks.statistic.predicate_columns;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Test;

class ColumnUsageTest extends PlanTestBase {

    @Test
    public void testColumnUsage() throws Exception {
        // normal predicate
        starRocksAssert.query("select * from t0 where v1 > 1").explainQuery();
        starRocksAssert.query("select * from information_schema.column_stats_usage where table_name = 't0'")
                .explainContains("constant exprs", "'v1' | 'predicate'");

        starRocksAssert.query("select * from test_all_type where lower(t1a) = '123' and t1e < 1.1").explainQuery();
        starRocksAssert.query("select * from information_schema.column_stats_usage where table_name = 'test_all_type'")
                .explainContains("constant exprs", "'t1a' | 'predicate'");

        // group by
        starRocksAssert.query("select v2, v3, count(*) from t0 group by v2, v3").explainQuery();
        starRocksAssert.query("select * from information_schema.column_stats_usage where table_name = 't0'")
                .explainContains("constant exprs", "'v1' | 'predicate'");

        // join
        starRocksAssert.query("select * from t0 join t1 on t0.v2 = t1.v4").explainQuery();
        starRocksAssert.query("select * from information_schema.column_stats_usage where table_name = 't0'")
                .explainContains("constant exprs", "'v2' | 'join'");
        starRocksAssert.query("select * from information_schema.column_stats_usage where table_name = 't1'")
                .explainContains("constant exprs", "'v4' | 'join'");
    }

}