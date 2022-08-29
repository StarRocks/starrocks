// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.parser;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.ValuesRelation;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static com.starrocks.sql.plan.PlanTestBase.assertContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class InsertMultipleValuesTest {

    private static final int ROW_NUM = 20000;

    @Test
    public void parserRowsTest() {
        String sql = generateSql();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setParseTokensLimit(Integer.MAX_VALUE);
        List<StatementBase> sqlStatements = SqlParser.parse(sql, sessionVariable);
        InsertStmt insertStmt = (InsertStmt) sqlStatements.get(0);
        assertEquals(ROW_NUM, ((ValuesRelation) insertStmt.getQueryStatement()
                .getQueryRelation()).getRows().size());
    }

    private static String generateSql() {
        StringJoiner joiner = new StringJoiner(", ");
        List<String> list = new ArrayList<>();
        String value = "(6968246155471433667, 'k-1869445626llllllll', 'k-1869445626llllllll', " +
                "'k-1869445626llllllll', 'k-1869445626llllllll', 'k-1869445626llllllll', " +
                "'k-1869445626llllllll', 'k-1869445626llllllll', 'k-1869445626llllllll')";
        for (int i = 0; i < ROW_NUM; i++) {
            joiner.add(value);
            list.add(value);
        }
        String insertClause = "insert into tbl values ";
        return insertClause + joiner;
    }

    @Test
    public void parseInsertValuesTest() {
        List<String> values = ImmutableList.of("default", "null", "1", "-1", "+1", "1.23", "-1.23",
                ".23", "'abc'", "'abcd'", "true", "false", "datetime '2021-01-12 12:00:00'", "date '2021-01-12'",
                "1+1", "1-1", "1*1", "1/1", "1%1", "-1+2", "-1*2", "+1+2*1+1/3", "cast (1 as boolean)", "now()",
                "abs(1)", "JSON_OBJECT('a', '1')", "-abs(abs(1) + abs(-1))", "PARSE_JSON('{\"a\": 1}')",
                "date_add(cast ('2021-01-01' as date), INTERVAL 2 DAY)", "[1,2,3]", "[]",
                "array_intersect(['SQL'], ['MySQL'], array_slice([1,2,4,5,6], 3, 2))");
        StringJoiner joiner = new StringJoiner(",", "(", ")");

        for (String value : values) {
            joiner.add(value);
        }

        String insertSql = "insert into tbl values " + joiner.toString() + "," + joiner.toString();
        SessionVariable sessionVariable = new SessionVariable();
        List<StatementBase> sqlStatements = SqlParser.parse(insertSql, sessionVariable);
        InsertStmt insertStmt = (InsertStmt) sqlStatements.get(0);
        ValuesRelation valuesRelation = (ValuesRelation) insertStmt.getQueryStatement().getQueryRelation();
        List<Expr> exprList = valuesRelation.getRow(0).stream().filter(expr -> expr != null).collect(Collectors.toList());
        assertEquals(values.size(), exprList.size());
    }

    @Test
    public void tokensExceedLimitTest() {
        String sql = "select 1";
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setParseTokensLimit(1);
        assertThrows(ParsingException.class, () -> SqlParser.parse(sql, sessionVariable));
    }

    @Test
    public void sqlParseErrorInfoTest() {
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
