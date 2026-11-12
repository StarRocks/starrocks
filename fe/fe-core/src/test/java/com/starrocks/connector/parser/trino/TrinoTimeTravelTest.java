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

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.QueryPeriod;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * The Trino parser carries time travel on the table node (FOR VERSION/TIMESTAMP AS OF). Dropping it
 * during AST conversion made the query silently read the latest snapshot instead of the requested one.
 *
 * <p>Trino's grammar has exactly one temporal form, {@code FOR (TIMESTAMP|VERSION) AS OF <expr>}; the
 * StarRocks-only spellings (SYSTEM_TIME, BETWEEN/FROM..TO/ALL, and omitting FOR) are rejected by the
 * Trino parser and reach the StarRocks parser through the dialect downgrade, so both sets are covered here.
 */
public class TrinoTimeTravelTest {
    private static TableRelation parseTableRelation(String sql) {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlDialect("trino");
        List<StatementBase> statements = SqlParser.parse(sql, sessionVariable);
        Assertions.assertEquals(1, statements.size());
        QueryRelation queryRelation = ((QueryStatement) statements.get(0)).getQueryRelation();
        return (TableRelation) ((SelectRelation) queryRelation).getRelation();
    }

    private static QueryPeriod parseQueryPeriod(String sql, QueryPeriod.PeriodType expectedType) {
        QueryPeriod queryPeriod = parseTableRelation(sql).getQueryPeriod();
        Assertions.assertNotNull(queryPeriod, sql);
        Assertions.assertEquals(expectedType, queryPeriod.getPeriodType(), sql);
        Assertions.assertTrue(queryPeriod.getStart().isEmpty(), sql);
        return queryPeriod;
    }

    @Test
    public void testVersionAsOfSnapshotId() {
        QueryPeriod queryPeriod = parseQueryPeriod(
                "select * from time_travel_test for version as of 4392135601283712525",
                QueryPeriod.PeriodType.VERSION);
        Expr end = queryPeriod.getEnd().orElseThrow();
        Assertions.assertEquals(4392135601283712525L, ((IntLiteral) end).getValue());
    }

    @Test
    public void testVersionAsOfRefNameWithAlias() {
        // The aliased relation must keep the period that visitTable put on the table.
        QueryPeriod queryPeriod = parseQueryPeriod(
                "select * from time_travel_test for version as of 'tag1' as t", QueryPeriod.PeriodType.VERSION);
        Assertions.assertEquals("tag1", ((StringLiteral) queryPeriod.getEnd().orElseThrow()).getStringValue());
    }

    @Test
    public void testTimestampAsOfTimestampLiteral() {
        QueryPeriod queryPeriod = parseQueryPeriod(
                "select * from time_travel_test for timestamp as of timestamp '2024-01-01 00:00:00'",
                QueryPeriod.PeriodType.TIMESTAMP);
        Expr end = queryPeriod.getEnd().orElseThrow();
        Assertions.assertEquals("2024-01-01 00:00:00", ((DateLiteral) end).getStringValue());
    }

    @Test
    public void testTimestampAsOfDateLiteral() {
        QueryPeriod queryPeriod = parseQueryPeriod(
                "select * from time_travel_test for timestamp as of date '2024-01-01'",
                QueryPeriod.PeriodType.TIMESTAMP);
        Assertions.assertEquals("2024-01-01", ((DateLiteral) queryPeriod.getEnd().orElseThrow()).getStringValue());
    }

    @Test
    public void testTimestampAsOfStringLiteral() {
        QueryPeriod queryPeriod = parseQueryPeriod(
                "select * from time_travel_test for timestamp as of '2024-01-01 00:00:00'",
                QueryPeriod.PeriodType.TIMESTAMP);
        Assertions.assertEquals("2024-01-01 00:00:00",
                ((StringLiteral) queryPeriod.getEnd().orElseThrow()).getStringValue());
    }

    @Test
    public void testTimeTravelInsideJoinAndSubquery() {
        // visitTable is the single conversion point, so every nesting level must carry its own period.
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlDialect("trino");
        String sql = "select * from (select * from time_travel_test for version as of 1) z";
        StatementBase statement = SqlParser.parse(sql, sessionVariable).get(0);
        List<TableRelation> relations =
                com.starrocks.sql.analyzer.AnalyzerUtils.collectTableRelations(statement);
        Assertions.assertEquals(1, relations.size());
        Assertions.assertNotNull(relations.get(0).getQueryPeriod());
    }

    @Test
    public void testSurvivesSerializeAndReparse() {
        // A query dump / audit round trip goes through AstToSQLBuilder; if the formatter cannot render the
        // typed period, the regenerated SQL silently reads the latest snapshot instead of the pinned one.
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlDialect("trino");
        String sql = "select * from time_travel_test for version as of 4392135601283712525";
        String regenerated = AstToSQLBuilder.toSQL(SqlParser.parse(sql, sessionVariable).get(0));
        Assertions.assertTrue(regenerated.contains("FOR VERSION AS OF 4392135601283712525"), regenerated);

        // And the regenerated SQL must parse back to the same period under the StarRocks dialect.
        SessionVariable starRocksDialect = new SessionVariable();
        QueryRelation reparsed = ((QueryStatement) SqlParser.parse(regenerated, starRocksDialect).get(0))
                .getQueryRelation();
        QueryPeriod queryPeriod = ((TableRelation) ((SelectRelation) reparsed).getRelation()).getQueryPeriod();
        Assertions.assertNotNull(queryPeriod, regenerated);
        Assertions.assertEquals(QueryPeriod.PeriodType.VERSION, queryPeriod.getPeriodType());
        Assertions.assertEquals(4392135601283712525L, ((IntLiteral) queryPeriod.getEnd().orElseThrow()).getValue());
    }

    @Test
    public void testTimestampPeriodSurvivesSerializeAndReparse() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlDialect("trino");
        String sql = "select * from time_travel_test for timestamp as of timestamp '2024-01-01 00:00:00'";
        String regenerated = AstToSQLBuilder.toSQL(SqlParser.parse(sql, sessionVariable).get(0));
        Assertions.assertTrue(regenerated.contains("FOR TIMESTAMP AS OF"), regenerated);
        Assertions.assertTrue(regenerated.contains("2024-01-01 00:00:00"), regenerated);
    }

    @Test
    public void testNoQueryPeriod() {
        Assertions.assertNull(parseTableRelation("select * from time_travel_test").getQueryPeriod());
    }

    @Test
    public void testStarRocksOnlySpellingsFallBackToStarRocksParser() {
        // Rejected by the Trino grammar; the dialect downgrade hands them to the StarRocks parser, which
        // resolves SYSTEM_TIME AS OF to a TIMESTAMP period exactly as it does under the StarRocks dialect.
        QueryPeriod queryPeriod = parseQueryPeriod(
                "select * from time_travel_test for system_time as of '2024-01-01 00:00:00'",
                QueryPeriod.PeriodType.TIMESTAMP);
        Assertions.assertEquals("2024-01-01 00:00:00",
                ((StringLiteral) queryPeriod.getEnd().orElseThrow()).getStringValue());

        QueryPeriod noForKeyword = parseQueryPeriod(
                "select * from time_travel_test version as of 4392135601283712525", QueryPeriod.PeriodType.VERSION);
        Assertions.assertEquals(4392135601283712525L, ((IntLiteral) noForKeyword.getEnd().orElseThrow()).getValue());

        // BETWEEN/FROM..TO/ALL parse but build no QueryPeriod - the StarRocks parser keeps only the raw
        // text for MySQL external tables. Asserted so the trino path stays identical to the native one.
        TableRelation between = parseTableRelation("select * from time_travel_test for version between 1 and 2");
        Assertions.assertNull(between.getQueryPeriod());
        Assertions.assertFalse(between.getQueryPeriodString().isEmpty());
    }
}
