// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.parser;

import com.starrocks.qe.SessionVariable;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.plan.PlanTestBase.assertContains;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

class TokenLimitTest {

    @Test
    void tokensExceedLimitTest() {
        String sql = "select 1";
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setParseTokensLimit(1);
        assertThrows(OperationNotAllowedException.class, () -> SqlParser.parse(sql, sessionVariable));
    }

    @Test
    void sqlParseErrorInfoTest() {
        String sql = "select 1 form tbl";
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable);
            fail("sql should fail to parse.");
        } catch (Exception e) {
            assertContains(e.getMessage(), "You have an error in your SQL syntax");
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
}


