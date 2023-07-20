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

package com.starrocks.sql.ast;

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.List;

public class ExplainAnalyzeStmtTest {

    @Test(expected = ParsingException.class)
    public void testTrinoDialectFailure() {
        SessionVariable sessionVariable = new SessionVariable();
        SqlParser.parse("explain analyze select array[1,2,3] as numbers", sessionVariable);
    }

    @Test
    public void testTrinoDialect() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlDialect("trino");
        List<StatementBase> statementBases =
                SqlParser.parse("explain analyze select array[1,2,3] as numbers", sessionVariable);
        Assert.assertEquals(statementBases.size(), 1);
        Assert.assertNotNull(statementBases.get(0));
    }

    @Test
    public void testNonsupportInsert() {
        Exception exception = Assertions.assertThrows(ParsingException.class, () -> {
            SessionVariable sessionVariable = new SessionVariable();
            SqlParser.parse("explain analyze insert into t0(c1, c2) values(1, 2)", sessionVariable);
        });

        String expectedMessage = "Getting syntax error. Detail message: Unsupported operation analyze.";
        String actualMessage = exception.getMessage();
        Assert.assertEquals(expectedMessage, actualMessage);
    }

    @Test
    public void testNonsupportUpdate() {
        Exception exception = Assertions.assertThrows(ParsingException.class, () -> {
            SessionVariable sessionVariable = new SessionVariable();
            SqlParser.parse("explain analyze update t0 set id = 1", sessionVariable);
        });

        String expectedMessage = "Getting syntax error. Detail message: Unsupported operation analyze.";
        String actualMessage = exception.getMessage();
        Assert.assertEquals(expectedMessage, actualMessage);
    }

    @Test
    public void testNonsupportDelete() {
        Exception exception = Assertions.assertThrows(ParsingException.class, () -> {
            SessionVariable sessionVariable = new SessionVariable();
            SqlParser.parse("explain analyze delete from t0 where id = 1", sessionVariable);
        });

        String expectedMessage = "Getting syntax error. Detail message: Unsupported operation analyze.";
        String actualMessage = exception.getMessage();
        Assert.assertEquals(expectedMessage, actualMessage);
    }

    @Test
    public void testSupportQuery() {
        SessionVariable sessionVariable = new SessionVariable();
        List<StatementBase> statementBases =
                SqlParser.parse("explain analyze select * from t0 where id = 1", sessionVariable);
        Assert.assertEquals(statementBases.size(), 1);
        Assert.assertNotNull(statementBases.get(0));
    }
}
