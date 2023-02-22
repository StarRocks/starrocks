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


package com.starrocks.sql.parser;

import com.starrocks.qe.SessionVariable;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.plan.PlanTestBase.assertContains;
import static org.junit.Assert.fail;

class ParserTest {

    @Test
    void tokensExceedLimitTest() {
        String sql = "select 1";
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setParseTokensLimit(1);
        try {
            SqlParser.parse(sql, sessionVariable);
        } catch (Exception e) {
            assertContains(e.getMessage(), "Getting syntax error. Detail message: " +
                    "Statement exceeds maximum length limit");
        }
    }

    @Test
    void sqlParseErrorInfoTest() {
        String sql = "select 1 form tbl";
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable);
            fail("sql should fail to parse.");
        } catch (Exception e) {
            assertContains(e.getMessage(), "Getting syntax error at line 1, column 14. " +
                    "Detail message: Input 'tbl' is not valid at this position, please check the SQL Reference.");
        }
    }

    /**
     * Test that FE code can parse queries for databases in the MySQL-family that support SQL 2011's system versioning
     * temporal queries. Although MySQL doesn't yet support this syntax directly, multiple MySQL compatible databases do.
     */
    @Test
    void sqlParseTemporalQueriesTest() {
        String[] temporalQueries = new String[] {
                // DoltDB temporal query syntax
                // https://docs.dolthub.com/sql-reference/version-control/querying-history
                "SELECT * FROM t AS OF 'kfvpgcf8pkd6blnkvv8e0kle8j6lug7a';",
                "SELECT * FROM t AS OF 'myBranch';",
                "SELECT * FROM t AS OF 'HEAD^2';",
                "SELECT * FROM t AS OF TIMESTAMP('2020-01-01');",
                "SELECT * from `mydb/ia1ibijq8hq1llr7u85uivsi5lh3310p`.myTable;",

                // MariaDB temporal query syntax
                // https://mariadb.com/kb/en/system-versioned-tables/
                "SELECT * FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '2016-10-09 08:07:06';",
                "SELECT * FROM t FOR SYSTEM_TIME BETWEEN (NOW() - INTERVAL 1 YEAR) AND NOW();",
                "SELECT * FROM t FOR SYSTEM_TIME FROM '2016-01-01 00:00:00' TO '2017-01-01 00:00:00';",
                "SELECT * FROM t FOR SYSTEM_TIME ALL;",
        };

        for (String query : temporalQueries) {
            try {
                com.starrocks.sql.parser.SqlParser.parse(query, 0).get(0);
            } catch (ParsingException e) {
                fail("Unexpected parsing exception for query: " + query);
            } catch (Exception e) {
                fail("Unexpected exception for query: " + query);
            }
        }
    }

    @Test
    void testInvalidDbName() {
        String sql = "use a.b.c";
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable);
            fail("sql should fail to parse.");
        } catch (Exception e) {
            assertContains(e.getMessage(), "Getting syntax error from line 1, column 4 to line 1, column 8. " +
                    "Detail message: Invalid db name format 'a.b.c'.");
        }
    }

    @Test
    void testInvalidTaskName() {
        String sql = "submit task a.b.c as create table a.b (v1, v2) as select * from t1";
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable);
            fail("sql should fail to parse.");
        } catch (Exception e) {
            assertContains(e.getMessage(), "Getting syntax error from line 1, column 12 to line 1, column 16." +
                    " Detail message: Invalid task name format 'a.b.c'.");
        }
    }
}


