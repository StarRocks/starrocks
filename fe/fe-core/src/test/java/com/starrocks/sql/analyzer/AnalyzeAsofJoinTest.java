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

package com.starrocks.sql.analyzer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeAsofJoinTest {

    @BeforeAll
    public static void beforeAll() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testRequireAtLeastOneEquality() {
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v2 <= t1.v5",
                "at least one equality condition in join ON clause");
    }

    @Test
    public void testRequireExactlyOneTemporalInequality() {
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 <= t1.v5 AND t0.v2 >= t1.v5",
                "ASOF JOIN supports only one inequality condition in join ON clause");
    }

    @Test
    public void testUnsupportedOperators() {
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 != t1.v5",
                "does not support '!=' operator in join ON clause");
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 <=> t1.v5",
                "does not support '<=>' operator in join ON clause");
    }

    @Test
    public void testOrNotSupported() {
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON (t0.v1 = t1.v4) OR (t0.v2 <= t1.v5)",
                "do not support OR operators in join ON clause");

    }

    @Test
    public void testValidAsofJoin() {
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 <= t1.v5");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF LEFT JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 >= t1.v5");
    }

    @Test
    public void testMissingOnClause() {
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1");
    }

    @Test
    public void testMissingInequalityCondition() {
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1 on t0.v1 = t1.v4",
                "ASOF JOIN requires exactly one temporal inequality condition in join ON clause");
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 WHERE t0.v2 > t1.v5",
                "ASOF JOIN requires exactly one temporal inequality condition in join ON clause");
    }

    @Test
    public void testTemporalTypeValidationInvalids() {
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN tall ON t0.v1 = tall.td AND t0.v2 <= tall.te",
                "supports only BIGINT, DATE, or DATETIME types in join ON clause");
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN tall ON t0.v1 = tall.td AND t0.v2 <= tall.tj",
                "supports only BIGINT, DATE, or DATETIME types in join ON clause");
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN tall ON t0.v1 = tall.td AND t0.v2 <= tall.ta",
                "supports only BIGINT, DATE, or DATETIME types in join ON clause");
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN tall ON t0.v1 = tall.td AND t0.v2 <= tall.tb",
                "supports only BIGINT, DATE, or DATETIME types in join ON clause");
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN tjson ON t0.v1 = tjson.v_int AND t0.v2 <= tjson.v_json",
                "supports only BIGINT, DATE, or DATETIME types in join ON clause");
    }

    @Test
    public void testTemporalTypeBigintVsBigint() {
        analyzeSuccess("SELECT t0.v1 FROM t0 asof JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 <= t1.v5");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 < t1.v5");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 >= t1.v5");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 > t1.v5");
    }

    @Test
    public void testSupportedAsofJoinType() {
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 > t1.v5");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF INNER JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 > t1.v5");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF LEFT JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 > t1.v5");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF LEFT OUTER JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 > t1.v5");
    }

    @Test
    public void testUnsupportedAsofJoinType() {
        analyzeFail("SELECT t0.v1 FROM t0 ASOF RIGHT JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 > t1.v5");
        analyzeFail("SELECT t0.v1 FROM t0 ASOF RIGHT OUTER JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 > t1.v5");
        analyzeFail("SELECT t0.v1 FROM t0 ASOF FULL JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 > t1.v5");
        analyzeFail("SELECT t0.v1 FROM t0 ASOF FULL OUTER JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 > t1.v5");
        analyzeFail("SELECT t0.v1 FROM t0 ASOF SEMI JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 > t1.v5");
        analyzeFail("SELECT t0.v1 FROM t0 ASOF ANTI JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 > t1.v5");
        analyzeFail("SELECT t0.v1 FROM t0 ASOF CROSS JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 > t1.v5");
    }

    // datetime vs datetime, date vs date, and mixed date<->datetime on the same table (self-join)
    @Test
    public void testTemporalTypeDateTimeAndDateSelfJoins() {
        // datetime vs datetime
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON a.td = b.td AND a.th <= b.th");
        analyzeSuccess("SELECT a.td FROM tall a ASOF LEFT JOIN tall b ON a.td = b.td AND a.th >= b.th");

        // date vs date
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON a.td = b.td AND a.ti < b.ti");
        analyzeSuccess("SELECT a.td FROM tall a ASOF LEFT JOIN tall b ON a.td = b.td AND a.ti >= b.ti");

        // mixed: datetime vs date and date vs datetime
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON a.td = b.td AND a.th <= b.ti");
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON a.td = b.td AND a.ti <= b.th");
    }

    @Test
    public void testMultipleEqualityConditions() {
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v3 = t1.v6 AND t0.v2 <= t1.v5");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF LEFT JOIN t1 ON t0.v1 = t1.v4 AND t0.v3 = t1.v6 AND t0.v2 >= t1.v5");
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON a.td = b.td AND a.tc = b.tc AND a.tg = b.tg" +
                " AND a.th <= b.th");
    }

    @Test
    public void testAsofJoinWithWhereClause() {
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 <= t1.v5 WHERE t0.v3 > 100");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF LEFT JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 >= t1.v5 WHERE t1.v6 IS NOT NULL");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 <= t1.v5 " +
                "WHERE t0.v3 > 100 AND t1.v6 < 200");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 <= t1.v5 " +
                "WHERE t0.v1 IN (SELECT v7 FROM t2)");
    }

    @Test
    public void testAsofJoinWithUsing() {
        analyzeFail("SELECT l.v1 FROM t0 l ASOF JOIN t0 r USING (v1)");
        analyzeFail("SELECT l.v1 FROM t0 l ASOF JOIN t0 r USING (v1, v2)");
    }

    @Test
    public void testFunctionInTemporalCondition() {
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF LEFT JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 + 10 >= t1.v5 - 5");
        analyzeSuccess("SELECT a.td FROM tall a ASOF LEFT JOIN tall b ON a.tc = b.tc AND a.td + 100 >= b.tg - 50");
        
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON a.td = b.td AND DATE_ADD(a.th, INTERVAL 1 DAY) <= b.th");
        analyzeSuccess("SELECT a.td FROM tall a ASOF LEFT JOIN tall b ON a.td = b.td" +
                " AND DATE_SUB(a.th, INTERVAL 1 HOUR) >= b.th");
        
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON a.td = b.td AND DATE_ADD(a.ti, INTERVAL 1 DAY) <= b.ti");
        analyzeSuccess("SELECT a.td FROM tall a ASOF LEFT JOIN tall b ON a.td = b.td AND " +
                "a.ti >= DATE_SUB(b.ti, INTERVAL 7 DAY)");
        
        analyzeSuccess("SELECT a.td FROM tall a ASOF LEFT JOIN tall b ON a.td = b.td AND" +
                " UNIX_TIMESTAMP(a.th) >= UNIX_TIMESTAMP(b.th)");
        
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 <= t1.v5");
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON a.tc = b.tc AND a.td <= b.tg");
    }

    @Test
    public void testFunctionInEqualityCondition() {
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 + 0 = t1.v4 + 0 AND t0.v2 <= t1.v5");
        analyzeSuccess("SELECT a.td FROM tall a ASOF LEFT JOIN tall b ON a.td + 1 = b.td + 1 AND a.tg >= b.tg");
        
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON DATE(a.th) = DATE(b.th) AND a.th <= b.th");
        analyzeSuccess("SELECT a.td FROM tall a ASOF LEFT JOIN tall b ON YEAR(a.ti) = YEAR(b.ti) AND a.ti >= b.ti");
        
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON a.tc = b.tc AND a.th <= b.th");
        
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 <= t1.v5");
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON a.td = b.td AND a.tg <= b.tg");
    }

    @Test
    public void testComplexAsofJoinScenarios() {
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON " +
                "t0.v1 = t1.v4 AND t0.v3 + 1 = t1.v6 + 1 AND " +
                "t0.v2 + 10 <= t1.v5 + 10");
        
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON " +
                "a.tc = b.tc AND a.td + 1 = b.td + 1 AND " +
                "a.tg + 100 <= b.tg + 200");
        
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON " +
                "a.td = b.td AND " +
                "CONVERT_TZ(a.th, '+00:00', '+08:00') <= CONVERT_TZ(b.th, '+00:00', '+08:00')");
        
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON " +
                "YEAR(a.ti) = YEAR(b.ti) AND MONTH(a.ti) = MONTH(b.ti) AND " +
                "a.ti <= DATE_ADD(b.ti, INTERVAL 1 DAY)");
        
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF LEFT JOIN t1 ON " +
                "CASE WHEN t0.v1 > 0 THEN t0.v1 ELSE 0 END = CASE WHEN t1.v4 > 0 THEN t1.v4 ELSE 0 END AND " +
                "t0.v2 >= t1.v5");
        
        analyzeSuccess("SELECT a.td FROM tall a ASOF LEFT JOIN tall b ON " +
                "a.tc = b.tc AND a.td = b.td AND " +
                "DATE_ADD(a.th, INTERVAL 1 HOUR) >= DATE_SUB(b.th, INTERVAL 1 DAY)");
    }

    @Test
    public void testInvalidFunctionInTemporalCondition() {
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN tall ON t0.v1 = tall.td AND t0.v2 <= UPPER(tall.ta)",
                "supports only BIGINT, DATE, or DATETIME types in join ON clause");
        
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND ABS(t0.v2) <= ABS(t1.v5)",
                "supports only BIGINT, DATE, or DATETIME types in join ON clause");
        
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 <= SQRT(t1.v5)",
                "supports only BIGINT, DATE, or DATETIME types in join ON clause");

        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 <= t1.v5 AND t0.v3 <= 'invalid'",
                "ASOF JOIN temporal condition supports only BIGINT, DATE, or DATETIME types in join ON clause");

        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN tall ON t0.v1 = tall.td AND t0.v2 <= CONCAT(tall.ta, 'suffix')",
                "ASOF JOIN temporal condition supports only BIGINT, DATE, or DATETIME types in join ON clause");
    }

    @Test
    public void testAsofJoinWithLateralJoin() {
        analyzeFail("SELECT tarray.v1 FROM tarray ASOF JOIN unnest(v3) AS u " +
                "ON tarray.v1 = u.unnest AND tarray.v2 <= u.unnest",
                "ASOF join is not supported with lateral join");
    }

    @Test
    public void testAsofJoinWithSubquery() {
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN (SELECT * FROM t1) sub " +
                "ON t0.v1 = sub.v4 AND t0.v2 <= sub.v5");

        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF LEFT JOIN " +
                "(SELECT v4, v5, v6 FROM t1 WHERE v4 > 10) sub " +
                "ON t0.v1 = sub.v4 AND t0.v2 >= sub.v5");
    }

    @Test
    public void testAsofJoinWithNullCondition() {
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON NULL",
                "ASOF JOIN requires at least one equality condition in join ON clause");
    }

    @Test
    public void testAsofJoinWithConstantConditions() {
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND 1 = 1 AND t0.v2 <= t1.v5");
        
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND 1 = 1",
                "ASOF JOIN requires exactly one temporal inequality condition in join ON clause");
    }

    @Test 
    public void testAsofJoinWithNestedConditions() {
        analyzeFail("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON " +
                "t0.v1 = t1.v4 AND (t0.v2 <= t1.v5 OR t0.v3 >= t1.v6)",
                "ASOF JOIN conditions do not support OR operators in join ON clause");
        
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON " +
                "((t0.v1 = t1.v4) AND (t0.v3 = t1.v6)) AND (t0.v2 <= t1.v5)");
    }

    @Test
    public void testAsofJoinWithDifferentOperators() {
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 < t1.v5");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 <= t1.v5");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 > t1.v5");
        analyzeSuccess("SELECT t0.v1 FROM t0 ASOF JOIN t1 ON t0.v1 = t1.v4 AND t0.v2 >= t1.v5");
    }

    @Test
    public void testAsofJoinWithMixedTypes() {
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON a.tc = b.tc AND a.td <= b.th");
        analyzeSuccess("SELECT a.td FROM tall a ASOF JOIN tall b ON a.tc = b.tc AND a.th >= b.td");

        analyzeFail("SELECT a.td FROM tall a ASOF JOIN tall b ON a.tc = b.tc AND a.td <= b.ta",
                "supports only BIGINT, DATE, or DATETIME types in join ON clause");
    }

    @Test
    public void testOtherPredicates() {
        analyzeSuccess("select * from t0 asof join t1 on t0.v1 = t1.v4 and  t1.v5 <= t0.v2 where (v1 > 4 and v5 < 2)");
        analyzeFail("select * from t0 asof join t1 on t0.v1 = t1.v4 and  t1.v5 <= t0.v2 and (v1 > 4 and v5 < 2)",
                "ASOF JOIN temporal condition supports only BIGINT, DATE, or DATETIME types in join ON clause");

        analyzeSuccess("select * from t0 asof join t1 on t0.v1 = t1.v4 and  t1.v5 <= t0.v2 where (v1 > 4 or v5 < 2)");
        analyzeFail("select * from t0 asof join t1 on t0.v1 = t1.v4 and  t1.v5 <= t0.v2 and (v1 > 4 or v5 < 2)",
                "ASOF JOIN temporal condition supports only BIGINT, DATE, or DATETIME types in join ON clause");

        analyzeSuccess("select * from t0 asof join t1 on t0.v1 = t1.v4 and t1.v5 <= t0.v2 where t0.v1 != 3");
        analyzeFail("select * from t0 asof join t1 on t0.v1 = t1.v4 and  t1.v5 <= t0.v2 and v1 != 3",
                "ASOF JOIN does not support '!=' operator in join ON clause");
    }

}


