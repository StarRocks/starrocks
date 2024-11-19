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

import com.google.common.base.Splitter;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AnalyzeStmt;
import com.starrocks.sql.plan.PlanTestBase;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

class ColumnUsageTest extends PlanTestBase {

    @BeforeEach
    public void before() {
        PredicateColumnsMgr.getInstance().reset();
    }

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

    @ParameterizedTest
    @CsvSource(delimiterString = "|", value = {
            "|analyze table t0 predicate columns| ",
            "|analyze table t0 all columns|v1,v2,v3",
            "|analyze table t0(v1,v3)|v1,v3",
            "select * from t0 where v1 > 1|analyze table t0 predicate columns|v1",
            "select * from t0 where v1 > 1 and v2 < 10|analyze table t0 predicate columns|v2,v1",
            "select * from t0 where v1 > 1 and v2 < 10|analyze table t0 update histogram on predicate columns|v2,v1",
    })
    public void testAnalyzePredicateColumns(String query, String analyzeStmt, String expectedColumns) throws Exception {
        AnalyzeTestUtil.init();
        if (StringUtils.isNotEmpty(query)) {
            starRocksAssert.query(query).explainQuery();
        }
        AnalyzeStmt stmt = (AnalyzeStmt) AnalyzeTestUtil.analyzeSuccess(analyzeStmt);
        List<String> expect =
                StringUtils.isNotEmpty(expectedColumns) ? Splitter.on(",").splitToList(expectedColumns) : List.of();
        Assertions.assertEquals(expect, stmt.getColumnNames());
    }
}