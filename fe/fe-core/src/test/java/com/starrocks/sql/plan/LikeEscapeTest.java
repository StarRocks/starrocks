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

import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LikeEscapeTest extends PlanTestBase {

    // The pattern that the scan gets, written with the backslash as the escape character.
    private void assertPattern(String predicate, String pattern) throws Exception {
        String plan = getFragmentPlan("select t1a from test_all_type where t1a " + predicate);
        assertContains(plan, "1: t1a LIKE " + literal(pattern));
    }

    // A string as the plan prints it.
    private static String literal(String value) {
        return "'" + value.replace("\\", "\\\\") + "'";
    }

    @Test
    public void testEscapeCharacter() throws Exception {
        assertPattern("like 'a^%b^_c^^d^e%_' escape '^'", "a\\%b\\_c^de%_");
        assertPattern("like 'a|%b' escape '|'", "a\\%b");
        assertPattern("like 'a%%b%_c%' escape '%'", "a\\%b\\_c\\%");
        // The pattern ends with the escape character, which then stands for itself.
        assertPattern("like 'a%^' escape '^'", "a%^");

        // Without the ESCAPE clause, and with ESCAPE '\\', a backslash escapes. Otherwise it is an ordinary character.
        assertPattern("like 'a\\%b'", "a\\%b");
        assertPattern("like 'a\\%b' escape '\\\\'", "a\\%b");
        assertPattern("like 'a\\%b' escape '^'", "a\\\\%b");
        assertPattern("like 'a\\%b^%' escape ''", "a\\\\%b^%");
    }

    // The filters "contains", "starts with", "ends with" and "does not contain" of Looker.
    @Test
    public void testLookerFilters() throws Exception {
        String value = "replace(replace(replace('50%_^', '^', '^^'), '%', '^%'), '_', '^_')";
        assertPattern("like concat('%', " + value + ", '%') escape '^'", "%50\\%\\_^%");
        assertPattern("like concat(" + value + ", '%') escape '^'", "50\\%\\_^%");
        assertPattern("like concat('%', " + value + ") escape '^'", "%50\\%\\_^");

        String plan = getFragmentPlan("select t1a from test_all_type where t1a not like concat('%', " + value +
                ", '%') escape '^'");
        assertContains(plan, "NOT (1: t1a LIKE " + literal("%50\\%\\_^%") + ")");
    }

    @Test
    public void testHaving() throws Exception {
        String plan = getFragmentPlan("select t1a, count(*) from test_all_type group by t1a " +
                "having t1a like 'a^%' escape '^'");
        assertContains(plan, "1: t1a LIKE " + literal("a\\%"));
    }

    @Test
    public void testUnsupported() {
        String sql = "select t1a from test_all_type where t1a ";
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> getFragmentPlan(sql + "like concat(t1a, '%') escape '^'"));
        assertContains(e.getMessage(), "The pattern of LIKE ... ESCAPE must be a constant");

        ParsingException p = Assertions.assertThrows(ParsingException.class,
                () -> getFragmentPlan(sql + "like 'a' escape '^^'"));
        assertContains(p.getMessage(), "ESCAPE must be a single character or an empty string");
        p = Assertions.assertThrows(ParsingException.class, () -> getFragmentPlan(sql + "regexp 'a' escape '^'"));
        assertContains(p.getMessage(), "ESCAPE is only supported with LIKE");
    }

    @Test
    public void testToSql() throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("select t1a from test_all_type where " +
                "t1a like concat('%', 'a^%') escape '^' and t1a not like 'b|_' escape '|'", connectContext);
        assertContains(AstToSQLBuilder.toSQL(stmt), "LIKE (concat('%', 'a^%')) ESCAPE '^'", "LIKE 'b|_' ESCAPE '|'");
    }
}
