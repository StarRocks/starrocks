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
}


