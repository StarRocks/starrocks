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

package com.starrocks.connector.parser.trino;

import com.starrocks.sql.common.StarRocksPlannerException;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

// These tests are not supported for StarRocks trino parser,
// we could support it later
public class TrinoParserNotSupportTest extends TrinoTestBase {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        TrinoTestBase.beforeClass();
    }

    // refer to https://trino.io/docs/current/sql/select.html#window-clause
    @Test
    public void testWindowClause() {
        String sql = "select sum(v1) over w from t0 window w as (partition by v2)";
        analyzeFail(sql, "StarRocks does not support Window clause now");
    }

    // refer to https://trino.io/docs/current/sql/select.html#offset-clause
    @Test
    public void testOffsetWithoutLimit() {
        String sql = "select v1 from t0 order by v1 offset 2";
        analyzeFail(sql, "Trino Parser on StarRocks does not support OFFSET without LIMIT now");
    }

    // refer to https://trino.io/docs/current/sql/select.html#limit-or-fetch-first-clause
    @Test
    public void testLimitAll() {
        String sql = "select v1 from t0 limit ALL";
        analyzeFail(sql, "Unsupported expression [ALL]");
    }

    // refer to https://trino.io/docs/current/language/types.html#interval-year-to-month
    @Test
    public void testIntervalDataType() throws Exception {
        expectedEx.expect(StarRocksPlannerException.class);
        // StarRocks do not support this query.
        String sql = "select INTERVAL '2' DAY";
        // It will throw exception in optimize stage
        getFragmentPlan(sql);
    }

    // refer to https://trino.io/docs/current/sql/select.html#tablesample
    @Test
    public void testSampleTable() {
        String sql = "select * from t0 TABLESAMPLE BERNOULLI(10)";
        analyzeFail(sql, "Unsupported relation [SampledRelation");
    }

    // refer to https://trino.io/docs/current/language/types.html#timestamp-with-time-zone
    // https://trino.io/docs/current/functions/datetime.html#time-zone-conversion
    @Test
    public void testTimeStampWithTimeZone() {
        String sql = "select TIMESTAMP '2014-03-14 09:30:00 Europe/Berlin'";
        analyzeSuccess(sql);

        sql = "select TIMESTAMP '2014-09-17 Europe/Berlin'";
        analyzeSuccess(sql);
    }

    // refer to https://trino.io/docs/current/functions/conversion.html#format
    @Test
    public void testFormat() {
        String sql = "select format('%03d', 8);";
        analyzeFail(sql, "No matching function with signature: format");
    }

    // refer to https://trino.io/docs/current/functions/json.html#cast-to-json
    @Test
    public void testJsonCast() {
        String sql = "SELECT CAST(MAP(ARRAY['k1', 'k2', 'k3'], ARRAY[1, 23, 456]) AS JSON);";
        analyzeFail(sql, "Invalid type cast from map<varchar,smallint(6)> to json");
    }

    // refer to https://trino.io/docs/current/functions/json.html#json-value
    @Test
    public void testJsonValue() {
        String sql = "select json_value('[true, 12e-1, \"text\"]', 'strict $[1]')";
        analyzeFail(sql, "No matching function with signature: json_value(varchar, varchar)");
    }

    // refer to https://trino.io/docs/current/functions/json.html#json-object
    @Test
    public void testJsonObject() {
        String sql = "SELECT json_object('key1' : 1, 'key2' : true)";
        analyzeFail(sql, "Unsupported expression [JSON_OBJECT");
    }
}
