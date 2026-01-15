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

package com.starrocks.analysis;

import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

public class CreateAsyncMaterializedViewTest extends MVTestBase {
    private static final Logger LOG = LogManager.getLogger(CreateAsyncMaterializedViewTest.class);

    @Test
    public void testCreateMVWithError() throws Exception {
        starRocksAssert.withTable("create table t1(\n" +
                "                a date,\n" +
                "                b string,\n" +
                "                c int);");
        starRocksAssert.withTable("create table t3(\n" +
                "                a date,\n" +
                "                b string,\n" +
                "                c int)\n" +
                "                partition by b,date_trunc(\"day\", a);");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv1\n" +
                "                 DISTRIBUTED BY HASH(a) BUCKETS 3\n" +
                "                 partition by (date_trunc(\"day\", a), b)\n" +
                "                 REFRESH MANUAL\n" +
                "                 AS SELECT t3.a, t3.b, sum(t1.c+t3.c) as sum_cnt\n" +
                "                 FROM t1, t3\n" +
                "                 where t1.a = t3.a\n" +
                "                   and t1.b = t3.b\n" +
                "                 GROUP BY t3.a, t3.b;");

    }
}