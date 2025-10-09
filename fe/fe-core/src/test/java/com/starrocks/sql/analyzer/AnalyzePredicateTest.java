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

import com.starrocks.common.ExceptionChecker;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.connectContext;

public class AnalyzePredicateTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testArrayPredicate() {
        analyzeSuccess("select * from tarray where v3 is null");
        analyzeFail("select * from tarray where v3 between [1,2,3] and [4,5,6]",
                "HLL, BITMAP, PERCENTILE and ARRAY, MAP, STRUCT type couldn't as Predicate");
        analyzeSuccess("select * from tarray where v3 in ([1,2,3], [4,5,6])");
    }


    @Test
    public void testInPredicate() {
        analyzeSuccess("select * from t0 where TIMEDIFF('1970-01-16', '1969-12-24') in( cast (1.2 as decimal))");
    }

    @Test
    public void testLargeInPredicate()  {
        connectContext.getSessionVariable().setLargeInPredicateThreshold(3);
        analyzeSuccess("select * from test.t0 where v1 in (1,2,3,4)");
        analyzeSuccess("select * from test.t0 where v1 in (1,2,3,4,5,6,7,8,9,10)");
        analyzeSuccess("select * from test.t0 where v2 in (100,200,300,400,500)");
        analyzeSuccess("select * from test.tall where ta in ('a','b','c','d','e')");
        analyzeSuccess("select * from test.tprimary2 where v1 in ('str1','str2','str3','str4','str5')");
        analyzeSuccess("select * from test.t0 where v1 in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)");
        analyzeSuccess("select * from test.t0 where (v1 + 1) in (2,3,4,5,6,7,8,9,10,11)");
        analyzeSuccess("select * from test.t0 where cast(v1 as string) in ('1','2','3','4','5','6','7','8')");
        StringBuilder largeInClause = new StringBuilder("select * from test.t0 where v1 in (");
        for (int i = 1; i <= 100; i++) {
            if (i > 1) {
                largeInClause.append(",");
            }
            largeInClause.append(i);
        }
        largeInClause.append(")");
        analyzeSuccess(largeInClause.toString());
        connectContext.getSessionVariable().setLargeInPredicateThreshold(100000);
    }

    @Test
    public void testLargeInPredicateTypeValidation() {
        connectContext.getSessionVariable().setLargeInPredicateThreshold(3);
        ExceptionChecker.expectThrowsWithMsg(LargeInPredicateException.class,
                "LargeInPredicate only supports: (1) compare type is IntegerType and constant type is BIGINT," +
                        " (2) both compare and constant are STRING types. " +
                        "Current types: compareType=decimal(10, 2), constantValueType=bigint(20)",
                () -> analyzeFail("select * from test.t0 where cast(v1 as decimal(10,2)) in (1,2,3,4,5,6,7,8,9,10)"));

        ExceptionChecker.expectThrowsWithMsg(LargeInPredicateException.class,
                "Current types: compareType=bigint(20), constantValueType=varchar",
                () -> analyzeFail("select * from test.t0 where v1 in ('1', '2', '3')"));

        ExceptionChecker.expectThrowsWithMsg(LargeInPredicateException.class,
                "Current types: compareType=varchar(20), constantValueType=bigint(20)",
                () -> analyzeFail("select * from test.tall where ta in (1, 2, 3, 4, 5, 6, 7)"));

        // fallback to InPredicate
        analyzeSuccess("select * from test.t0 where cast(v1 as tinyint) in (1,2,3,41231231231231231231231123)");

        analyzeSuccess("select * from test.t0 where cast(v1 as int) in (1,2,3,4,5,6,7,8,9,10)");
        analyzeSuccess("select * from test.t0 where cast(v1 as smallint) in (1,2,3,4,5,6,7,8,9,10)");
        analyzeSuccess("select * from test.t0 where cast(v1 as tinyint) in (1,2,3,4,5,6,7,8,9,10)");
        analyzeSuccess("select * from test.tall where cast(ta as char(10)) in ('a','b','c','d','e','f','g','h')");
        analyzeSuccess("select * from test.auto_tbl1 where cast(col1 as text) in ('val1','val2','val3','val4','val5')");
        analyzeSuccess("select * from test.tall where upper(ta) in ('A','B','C','D','E','F','G','H')");
        analyzeSuccess("select * from test.t0 where (v1 * 2) in (2,4,6,8,10,12,14,16,18,20)");
        analyzeSuccess("select * from test.tall where concat(ta, '_suffix') in ('a_suffix','b_suffix')");
        connectContext.getSessionVariable().setLargeInPredicateThreshold(100000);
    }
}
