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

import com.starrocks.qe.SqlModeHelper;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ShareJsonParseTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE json_share_t (\n" +
                "  k INT,\n" +
                "  doc VARCHAR(65533),\n" +
                "  doc2 VARCHAR(65533),\n" +
                "  j JSON\n" +
                ") DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES(\"replication_num\" = \"1\")");
    }

    @AfterEach
    public void resetSession() {
        connectContext.getSessionVariable().setEnableShareJsonParse(true);
        connectContext.getSessionVariable().setSqlMode(sqlMode);
        connectContext.getSessionVariable().setShareJsonParseMinExtractions(2);
    }

    private long sqlMode;

    @BeforeEach
    public void saveSession() {
        sqlMode = connectContext.getSessionVariable().getSqlMode();
    }

    private static void assertParsedOnce(String plan, String doc) {
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "parse_json(" + doc + ")"), plan);
        Assertions.assertFalse(plan.contains("get_json_string(" + doc + ","), plan);
        Assertions.assertFalse(plan.contains("get_json_double(" + doc + ","), plan);
        Assertions.assertFalse(plan.contains("get_json_int(" + doc + ","), plan);
    }

    @Test
    public void testSharedDocumentIsParsedOnce() throws Exception {
        String sql = "select get_json_string(doc, '$.a.amount'), get_json_string(doc, '$.a.currency'), " +
                "get_json_string(doc, '$.b.amount') from json_share_t";
        String plan = getFragmentPlan(sql);
        assertParsedOnce(plan, "2: doc");
        assertContains(plan, "common expressions:");
    }

    @Test
    public void testNestedInsideOtherExpressions() throws Exception {
        String sql = "select named_struct('amount', cast(get_json_string(doc, '$.a.amount') as double) * 2, " +
                "'currency', get_json_string(doc, '$.a.currency')), " +
                "murmur_hash3_32(get_json_string(doc, '$.b.amount')) from json_share_t";
        assertParsedOnce(getFragmentPlan(sql), "2: doc");
    }

    @Test
    public void testMixedGettersKeepResultTypes() throws Exception {
        String sql = "select get_json_int(doc, '$.a'), get_json_double(doc, '$.b'), get_json_string(doc, '$.c'), " +
                "get_json_object(doc, '$.d'), get_json_scalar(doc, '$.e') from json_share_t";
        String plan = getVerboseExplain(sql);
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "parse_json[([2: doc, VARCHAR, true])"), plan);
        assertContains(plan, ": parse_json, JSON, true], '$.a'); args: JSON,VARCHAR; result: BIGINT;");
        assertContains(plan, ": parse_json, JSON, true], '$.b'); args: JSON,VARCHAR; result: DOUBLE;");
        assertContains(plan, ": parse_json, JSON, true], '$.e'); args: JSON,VARCHAR; result: VARCHAR;");
        assertNotContains(plan, "args: VARCHAR,VARCHAR");
    }

    @Test
    public void testSingleExtractionIsUnchanged() throws Exception {
        String plan = getFragmentPlan("select get_json_string(doc, '$.a'), get_json_string(doc2, '$.a') " +
                "from json_share_t");
        assertNotContains(plan, "parse_json");
        assertContains(plan, "get_json_string(2: doc, '$.a')");
        assertContains(plan, "get_json_string(3: doc2, '$.a')");
    }

    @Test
    public void testRepeatedIdenticalCallIsUnchanged() throws Exception {
        // The same call twice is already deduplicated by expression reuse; there is nothing to share.
        String plan = getFragmentPlan("select get_json_string(doc, '$.a'), concat(get_json_string(doc, '$.a'), 'x') " +
                "from json_share_t");
        assertNotContains(plan, "parse_json");
    }

    @Test
    public void testJsonColumnIsUnchanged() throws Exception {
        String plan = getFragmentPlan("select get_json_string(j, '$.a'), get_json_string(j, '$.b') from json_share_t");
        assertNotContains(plan, "parse_json");
    }

    @Test
    public void testConstantDocumentIsUnchanged() throws Exception {
        String plan = getFragmentPlan("select get_json_string('{\"a\": 1, \"b\": 2}', concat('$.', doc)), " +
                "get_json_string('{\"a\": 1, \"b\": 2}', concat('$.', doc2)) from json_share_t");
        assertNotContains(plan, "parse_json");
    }

    @Test
    public void testDisabledBySessionVariable() throws Exception {
        connectContext.getSessionVariable().setEnableShareJsonParse(false);
        String plan = getFragmentPlan("select get_json_string(doc, '$.a'), get_json_string(doc, '$.b') " +
                "from json_share_t");
        assertNotContains(plan, "parse_json");
        assertContains(plan, "get_json_string(2: doc, '$.a')");
    }

    @Test
    public void testDisabledWhenErrorsAreThrown() throws Exception {
        // Reuse can parse rows a CASE branch skips; with throwing enabled an invalid one would fail the query.
        connectContext.getSessionVariable().setSqlMode(sqlMode | SqlModeHelper.MODE_ALLOW_THROW_EXCEPTION);
        String plan = getFragmentPlan("select if(k > 0, get_json_string(doc, '$.a'), get_json_string(doc, '$.b')) " +
                "from json_share_t");
        assertNotContains(plan, "parse_json");
    }

    // Flat JSON only applies to JSON columns, and its path rewrite runs in the logical phase; the column access
    // paths the scan reads must be identical whether this rule runs or not.
    private void assertSameAccessPaths(String sql) throws Exception {
        String withRule = getVerboseExplain(sql);
        connectContext.getSessionVariable().setEnableShareJsonParse(false);
        String withoutRule = getVerboseExplain(sql);
        connectContext.getSessionVariable().setEnableShareJsonParse(true);
        Assertions.assertEquals(accessPaths(withoutRule), accessPaths(withRule), withRule);
        Assertions.assertFalse(accessPaths(withRule).isEmpty(), withRule);
    }

    private static String accessPaths(String plan) {
        StringBuilder sb = new StringBuilder();
        for (String line : plan.split("\n")) {
            if (line.contains("ColumnAccessPath")) {
                sb.append(line.trim()).append('\n');
            }
        }
        return sb.toString();
    }

    @Test
    public void testFlatJsonPathsOnJsonColumnAreKept() throws Exception {
        String sql = "select get_json_string(j, '$.a'), get_json_int(j, '$.b'), get_json_double(j, '$.c') " +
                "from json_share_t";
        assertSameAccessPaths(sql);
        assertNotContains(getFragmentPlan(sql), "parse_json");
    }

    @Test
    public void testFlatJsonSubfieldFeedingSharedParse() throws Exception {
        // The inner call reads a flat JSON subfield as VARCHAR; only the outer calls share one parse of it.
        String sql = "select get_json_string(get_json_string(j, '$.payload'), '$.x'), " +
                "get_json_string(get_json_string(j, '$.payload'), '$.y') from json_share_t";
        assertSameAccessPaths(sql);
        String plan = getVerboseExplain(sql);
        assertContains(plan, "/j(varchar)/payload(varchar)");
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "parse_json[("), plan);
    }

    @Test
    public void testJsonColumnAndVarcharDocumentInOneProjection() throws Exception {
        String sql = "select get_json_string(j, '$.a'), get_json_string(j, '$.b'), " +
                "get_json_string(doc, '$.a'), get_json_string(doc, '$.b') from json_share_t";
        assertSameAccessPaths(sql);
        String plan = getVerboseExplain(sql);
        Assertions.assertEquals(1, StringUtils.countMatches(plan, "parse_json[([2: doc, VARCHAR, true])"), plan);
    }

    @Test
    public void testMinExtractionsThreshold() throws Exception {
        String twoPaths = "select get_json_string(doc, '$.a'), get_json_string(doc, '$.b') from json_share_t";
        String threePaths = "select get_json_string(doc, '$.a'), get_json_string(doc, '$.b'), " +
                "get_json_string(doc, '$.c') from json_share_t";
        connectContext.getSessionVariable().setShareJsonParseMinExtractions(3);
        assertNotContains(getFragmentPlan(twoPaths), "parse_json");
        assertParsedOnce(getFragmentPlan(threePaths), "2: doc");

        // 1 rewrites a lone extraction too; values below 1 behave like 1.
        String onePath = "select get_json_string(doc, '$.a') from json_share_t";
        connectContext.getSessionVariable().setShareJsonParseMinExtractions(1);
        assertParsedOnce(getFragmentPlan(onePath), "2: doc");
        connectContext.getSessionVariable().setShareJsonParseMinExtractions(0);
        assertParsedOnce(getFragmentPlan(onePath), "2: doc");
    }

}
