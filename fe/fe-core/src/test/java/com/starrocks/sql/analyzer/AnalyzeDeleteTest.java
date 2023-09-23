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

import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeDeleteTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testPartitions() {
        DeleteStmt st;
        st = (DeleteStmt) SqlParser.parse("delete from tjson partition (p0)", 0).get(0);
        Assert.assertEquals(1, st.getPartitionNamesList().size());
        st = (DeleteStmt) SqlParser.parse("delete from tjson partition p0", 0).get(0);
        Assert.assertEquals(1, st.getPartitionNamesList().size());
        st = (DeleteStmt) SqlParser.parse("delete from tjson partition (p0, p1)", 0).get(0);
        Assert.assertEquals(2, st.getPartitionNamesList().size());
    }

    @Test
    public void testAnalyzeNonPrimaryKey() {
        analyzeFail("delete from tjson where v_json like 'abc'",
                "Where clause only supports");

        analyzeFail("delete from tjson where v_int > 20 and v_json like 'abc'",
                "Where clause only supports");

        analyzeFail("delete from tjson where v_int < 10 or v_int > 20",
                "should be AND");

        analyzeFail("delete from tjson where v_int = v_int and v_int = v_int",
                "Right expr of binary predicate should be value");

        analyzeFail("delete from tjson where 10 = v_int and 10 = v_int",
                "Left expr of binary predicate should be column name");

        analyzeSuccess("delete from tjson where v_json in ('1','2','3') and v_int > 10 and v_int < 40");
    }

    @Test
    public void testSingle() {
        StatementBase stmt = analyzeSuccess("delete from tjson where v_int = 1");
        Assert.assertEquals(true, ((DeleteStmt) stmt).shouldHandledByDeleteHandler());

        analyzeFail("delete from tjson",
                "Where clause is not set");

        stmt = analyzeSuccess("delete from tprimary where pk = 1");
        Assert.assertEquals(false, ((DeleteStmt) stmt).shouldHandledByDeleteHandler());

        analyzeFail("delete from tprimary partitions (p1, p2) where pk = 1",
                "Delete for primary key table do not support specifying partitions");

        analyzeFail("delete from tprimary",
                "Delete must specify where clause to prevent full table delete");
    }

    @Test
    public void testUsing() {
        analyzeSuccess("delete from tprimary using tprimary2 tp2 where tprimary.pk = tp2.pk");

        analyzeSuccess(
                "delete from tprimary using tprimary2 tp2 join t0 where tprimary.pk = tp2.pk " +
                        "and tp2.pk = t0.v1 and t0.v2 > 0");
    }

    @Test
    public void testCTE() {
        analyzeSuccess(
                "with tp2cte as (select * from tprimary2 where v2 < 10) delete from tprimary using " +
                        "tp2cte where tprimary.pk = tp2cte.pk");
    }
}
