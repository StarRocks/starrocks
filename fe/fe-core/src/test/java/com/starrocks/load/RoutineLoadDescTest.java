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


package com.starrocks.load;

import com.starrocks.persist.OriginStatementInfo;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.ColumnSeparator;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class RoutineLoadDescTest {

    private static Expr parseExpr(String sql) {
        return SqlParser.parseSqlToExpr(sql, 0);
    }

    private static RoutineLoadDesc parseDesc(String loadProperties) {
        RoutineLoadDesc desc = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(
                "CREATE ROUTINE LOAD job ON tbl " + loadProperties
                        + " PROPERTIES (\"format\"=\"json\") FROM KAFKA (\"kafka_topic\" = \"my_topic\")", 0), null);
        Assertions.assertNotNull(desc, loadProperties);
        return desc;
    }

    // Expression as the user writes it in COLUMNS / WHERE -> what exprToSql renders. Every operand that is not
    // a column, a literal, a function call or a CAST is parenthesized, so operator precedence survives the
    // re-parse. Function names are written in lower case because the parser lower-cases them, which is not what
    // is under test here.
    private static final String[][] RENDERINGS = {
            {"floor((ts + 32400) / 86400)", "floor((`ts` + 32400) / 86400)"},
            {"from_unixtime(floor((ts + 32400) / 86400) * 86400)",
                    "from_unixtime(floor((`ts` + 32400) / 86400) * 86400)"},
            {"(a + b) * 2", "(`a` + `b`) * 2"},
            {"a + b * 2", "`a` + (`b` * 2)"},
            {"(a * b) + c", "(`a` * `b`) + `c`"},
            {"a - (b - c)", "`a` - (`b` - `c`)"},
            {"a / (b * c)", "`a` / (`b` * `c`)"},
            {"a % (b + 1)", "`a` % (`b` + 1)"},
            {"(a | b) & 1", "(`a` | `b`) & 1"},
            {"(a + b) * 2 > 10", "((`a` + `b`) * 2) > 10"},
            {"(x > 1 OR y > 1) AND z = 1", "((`x` > 1) OR (`y` > 1)) AND (`z` = 1)"},
            {"date_trunc('day', from_unixtime(floor(ts) + 32400))",
                    "date_trunc('day', from_unixtime(floor(`ts`) + 32400))"},
            {"CAST(a AS INT) + 1", "CAST(`a` AS INT) + 1"},
            {"CAST(a AS DECIMALV2(10, 2))", "CAST(`a` AS DECIMALV2(10,2))"},
            {"`order` + 1", "`order` + 1"},
            {"t.a + 1", "`t`.`a` + 1"},
            {"default_catalog.db.t.a + 1", "`default_catalog`.`db`.`t`.`a` + 1"},
            {"a > -10", "`a` > -10"},
            {"concat(a, 'it\\'s', 'back\\\\slash')", "concat(`a`, 'it\\'s', 'back\\\\slash')"},
            {"CASE WHEN a > 1 THEN (b + c) * 2 ELSE 0 END", "CASE WHEN (`a` > 1) THEN ((`b` + `c`) * 2) ELSE 0 END"},
            {"NOT (a > 1 AND b < 2)", "NOT ((`a` > 1) AND (`b` < 2))"},
            {"a IN (1, 2, 3) AND b IS NOT NULL", "(`a` IN (1, 2, 3)) AND (`b` IS NOT NULL)"},
            {"dt >= DATE '2024-01-01'", "`dt` >= DATE '2024-01-01'"},
            {"x'ab'", "x'AB'"},
            {"extract(YEAR FROM ts)", "year(`ts`)"},
            {"UPPER(a)", "upper(`a`)"},
            {"dictionary_get('d', (a + b) * 2)", "dictionary_get('d', (`a` + `b`) * 2)"},
            {"array_contains(ARRAY<BIGINT>[1, 2], a)", "array_contains(ARRAY<BIGINT>[1, 2], `a`)"},
            {"INTERVAL 1 DAY + (ts + INTERVAL 1 MONTH)", "INTERVAL 1 DAY + (`ts` + INTERVAL 1 MONTH)"},
    };

    // Shapes whose exact rendering is not interesting, but which must parse back to the same tree.
    private static final String[] ROUND_TRIPS = {
            "FLOOR((`timestamp` + 32400) / 86400) * 86400",
            "a DIV (b + 1)",
            "-(a + b)",
            "~a",
            "a * 1.5 + 100.0",
            "if(a IS NULL, NULL, TRUE)",
            "CAST(a AS INT) + 1",
            "CAST(a AS VARCHAR)",
            "CAST(a AS VARCHAR(10))",
            "CAST(a AS STRING)",
            "CAST(a AS DECIMAL(10, 2)) * 2",
            "CAST(a AS DATETIME)",
            "CAST(parse_json(a) AS ARRAY<JSON>)",
            "ts + INTERVAL 1 DAY",
            "date_add(ts, INTERVAL 1 DAY)",
            "timestampdiff(DAY, a, b)",
            "INTERVAL 1 DAY + (DATE '2024-01-30' + INTERVAL 1 MONTH)",
            "(ts + INTERVAL 1 DAY) - INTERVAL (a + b) * 2 HOUR",
            "date_add(ts, INTERVAL (a + b) * 2 DAY)",
            "current_timestamp(3)",
            "ts < DATETIME '2024-01-01 10:00:00'",
            "parse_json(a)->'$.x'",
            "a.b.c.d.e + 1",
            "`a.b` + 1",
            "array_map(x -> (x + 1) * 2, arr)",
            "get_json_string(array_filter(item -> get_json_string(item, '$.type') = 'type1', "
                    + "CAST(parse_json(tmp1) AS ARRAY<JSON>))[1], '$.vid')",
            "get_json_string(map_values(map_filter((k, v) -> get_json_string(v, '$.type') = 'type1', "
                    + "CAST(parse_json(tmp2) AS MAP<STRING, JSON>)))[1], '$.cid')",
            "map{'a': 1}",
            "a BETWEEN (b - 1) AND (b + 1)",
            "a LIKE concat('%', b, '%')",
            "a REGEXP '^[0-9]+$'",
            "CASE a WHEN 1 THEN 2 END",
            "str_to_date(a, '%Y-%m-%d %H:%i:%s')",
            "dictionary_get('d', k)",
            "dictionary_get('d', k, true)",
            "FROM_UNIXTIME(a)",
            "CONCAT(a, 'x')",
            "Get_Json_String(a, '$.x')",
            "CAST(a AS ARRAY<DECIMALV2(10, 2)>)",
            "CAST(a AS STRUCT<x DECIMALV2(10, 2), y MAP<INT, ARRAY<DECIMAL128(10, 2)>>>)",
            "1e3",
            "170141183460469231731687303715884105728.",
            "170141183460469231731687303715884105729",
            "123456789012345678901234567890123456789012345678901234567890",
    };

    @Test
    public void testExprToSqlKeepsOperatorPrecedence() {
        for (String[] rendering : RENDERINGS) {
            Expr parsed = parseExpr(rendering[0]);
            String sql = RoutineLoadDesc.exprToSql(parsed);
            Assertions.assertEquals(rendering[1], sql, rendering[0]);
            Assertions.assertTrue(RoutineLoadDesc.sameExpression(parsed, parseExpr(sql)),
                    "re-parsing '" + sql + "' changed the tree");
        }
    }

    @Test
    public void testExprToSqlRoundTrips() {
        for (String input : ROUND_TRIPS) {
            Expr parsed = parseExpr(input);
            String sql = RoutineLoadDesc.exprToSql(parsed);
            Expr reparsed = parseExpr(sql);
            Assertions.assertTrue(RoutineLoadDesc.sameExpression(parsed, reparsed), input + " -> " + sql);
            Assertions.assertEquals(sql, RoutineLoadDesc.exprToSql(reparsed), input);
        }
    }

    // The parser keeps the case the user wrote for function names, the printer lower-cases them, and
    // Expr.equals compares them case-sensitively. The comparison behind the persistence check must not
    // fail on that, or every load-property ALTER on a job created with FROM_UNIXTIME(...) is refused.
    @Test
    public void testSameExpressionIgnoresFunctionNameCase() {
        Expr upper = parseExpr("FROM_UNIXTIME(FLOOR((ts + 32400) / 86400) * 86400)");
        Expr lower = parseExpr("from_unixtime(floor((ts + 32400) / 86400) * 86400)");
        Assertions.assertNotEquals(upper, lower);
        Assertions.assertTrue(RoutineLoadDesc.sameExpression(upper, lower));
        Assertions.assertTrue(RoutineLoadDesc.sameExpression(parseExpr("db.My_Udf(a)"), parseExpr("db.my_udf(a)")));
        Assertions.assertFalse(RoutineLoadDesc.sameExpression(parseExpr("floor(a)"), parseExpr("ceil(a)")));
        Assertions.assertFalse(RoutineLoadDesc.sameExpression(parseExpr("db1.f(a)"), parseExpr("db2.f(a)")));
        Assertions.assertTrue(parseDesc("COLUMNS(d = FROM_UNIXTIME(ts)), WHERE Get_Json_String(k, '$.x') IS NOT NULL")
                .hasSameExpressions(parseDesc("COLUMNS(d = from_unixtime(ts)), WHERE get_json_string(k, '$.x') IS NOT NULL")));
    }

    @Test
    public void testLargeIntegerDecimalRoundTripWithDoubleLiteralMode() {
        String input = "123456789012345678901234567890123456789012345678901234567890";
        for (long sqlMode : new long[] {0, SqlModeHelper.MODE_DOUBLE_LITERAL}) {
            Expr parsed = SqlParser.parseSqlToExpr(input, sqlMode);
            String sql = RoutineLoadDesc.exprToSql(parsed);
            Expr reparsed = SqlParser.parseSqlToExpr(sql, sqlMode);
            Assertions.assertEquals(parsed, reparsed, sql + " with sql_mode=" + sqlMode);
            Assertions.assertEquals(parsed.getType(), reparsed.getType());
        }
    }

    // Expr.equals does not compare types, so the collection constructors are checked on the type itself.
    @Test
    public void testExprToSqlKeepsCollectionTypes() {
        ArrayExpr array = (ArrayExpr) parseExpr("ARRAY<BIGINT>[1, 2]");
        String arraySql = RoutineLoadDesc.exprToSql(array);
        Assertions.assertEquals("ARRAY<BIGINT>[1, 2]", arraySql);
        Assertions.assertEquals(array.getType(), ((ArrayExpr) parseExpr(arraySql)).getType());

        MapExpr map = (MapExpr) parseExpr("MAP<VARCHAR(10), INT>{'a': 1}");
        String mapSql = RoutineLoadDesc.exprToSql(map);
        Assertions.assertEquals("MAP<VARCHAR(10),INT>{'a':1}", mapSql);
        Assertions.assertEquals(map.getType(), ((MapExpr) parseExpr(mapSql)).getType());

        // untyped constructors stay untyped
        Assertions.assertNull(((ArrayExpr) parseExpr("[1, 2]")).getType());
        Assertions.assertEquals("[1, 2]", RoutineLoadDesc.exprToSql(parseExpr("[1, 2]")));
        Assertions.assertEquals("map{'a':1}", RoutineLoadDesc.exprToSql(parseExpr("map{'a': 1}")));

        for (String input : new String[] {
                "ARRAY<DECIMALV2(10, 2)>[1.0]",
                "MAP<INT, DECIMALV2(10, 2)>{1: 1.0}",
                "ARRAY<STRUCT<x DECIMALV2(10, 2), y DECIMAL128(10, 2)>>[row(1.0, 2.0)]",
                "MAP<INT, ARRAY<DECIMALV2(10, 2)>>{1: [1.0]}",
        }) {
            Expr parsed = parseExpr(input);
            String sql = RoutineLoadDesc.exprToSql(parsed);
            Expr reparsed = parseExpr(sql);
            Assertions.assertEquals(parsed, reparsed, input + " -> " + sql);
            Assertions.assertEquals(parsed.getType(), reparsed.getType(), input + " -> " + sql);
        }
    }

    @Test
    public void testStructFieldCommentRendering() {
        // Struct comments can exist in programmatically built types, although the CAST grammar does
        // not accept them. Retain the existing printer behavior and escape their literal contents.
        StructType type = new StructType(List.of(new StructField("x", IntegerType.INT, "it's \\path")), true);
        String typeSql = "STRUCT<`x` INT COMMENT 'it\\'s \\\\path'>";
        Assertions.assertEquals("CAST(`a` AS " + typeSql + ")",
                RoutineLoadDesc.exprToSql(new CastExpr(new TypeDef(type), parseExpr("a"))));
        Assertions.assertEquals("ARRAY<" + typeSql + ">[row(1)]",
                RoutineLoadDesc.exprToSql(new ArrayExpr(new ArrayType(type), List.of(parseExpr("row(1)")))));
    }

    @Test
    public void testIsEmpty() {
        Assertions.assertTrue(new RoutineLoadDesc().isEmpty());

        RoutineLoadDesc separatorOnly = new RoutineLoadDesc();
        separatorOnly.setColumnSeparator(new ColumnSeparator(","));
        Assertions.assertFalse(separatorOnly.isEmpty());
        Assertions.assertFalse(parseDesc("COLUMNS(a, b = a + 1)").isEmpty());
        Assertions.assertFalse(parseDesc("WHERE a > 1").isEmpty());

        // What the analyzer builds for an ALTER that only changes PROPERTIES: a desc, not null, with nothing in it.
        RoutineLoadDesc propertiesOnly = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(
                "ALTER ROUTINE LOAD FOR job PROPERTIES (\"desired_concurrent_number\"=\"2\")", 0), null);
        Assertions.assertNotNull(propertiesOnly);
        Assertions.assertTrue(propertiesOnly.isEmpty());
    }

    @Test
    public void testHasSameExpressions() {
        String columns = "COLUMNS(k, ts, r = floor((ts + 32400) / 86400))";
        String where = "WHERE (a + b) * 2 > 10";
        RoutineLoadDesc desc = parseDesc(columns + ", " + where);

        Assertions.assertTrue(desc.hasSameExpressions(parseDesc(columns + ", " + where)));
        // column names are case-insensitive
        Assertions.assertTrue(desc.hasSameExpressions(
                parseDesc("COLUMNS(K, TS, R = floor((ts + 32400) / 86400)), " + where)));
        // clauses that carry no expression do not take part
        Assertions.assertTrue(desc.hasSameExpressions(
                parseDesc("COLUMNS TERMINATED BY ';', " + columns + ", PARTITION(p1), " + where)));

        // the lossy rendering of the same text is a different load
        Assertions.assertFalse(desc.hasSameExpressions(
                parseDesc("COLUMNS(k, ts, r = floor(ts + 32400 / 86400)), " + where)));
        Assertions.assertFalse(desc.hasSameExpressions(parseDesc(columns + ", WHERE a + b * 2 > 10")));
        // a clause missing on one side, or a different column list
        Assertions.assertFalse(desc.hasSameExpressions(parseDesc(columns)));
        Assertions.assertFalse(desc.hasSameExpressions(parseDesc(where)));
        Assertions.assertFalse(desc.hasSameExpressions(parseDesc("COLUMNS(k, ts), " + where)));
        Assertions.assertFalse(desc.hasSameExpressions(parseDesc("COLUMNS(k, ts, x = floor((ts + 32400) / 86400)), " + where)));
    }

    @Test
    public void testFindExpressionDifferenceNamesTheNodeKindWhenRenderingsMatch() {
        // 1.5 is a DecimalLiteral by default and a FloatLiteral under MODE_DOUBLE_LITERAL; both print as 1.5.
        String stmt = "CREATE ROUTINE LOAD job ON tbl COLUMNS(b = a * 1.5) "
                + "PROPERTIES (\"format\"=\"json\") FROM KAFKA (\"kafka_topic\" = \"my_topic\")";
        RoutineLoadDesc decimal = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(stmt, 0), null);
        RoutineLoadDesc dbl = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(stmt, 0),
                Map.of(SessionVariable.SQL_MODE, Long.toString(SqlModeHelper.MODE_DOUBLE_LITERAL)));
        String difference = decimal.findExpressionDifference(dbl).orElseThrow();
        Assertions.assertTrue(difference.startsWith("COLUMNS b = `a` * 1.5 vs `a` * 1.5 ("), difference);
        Assertions.assertTrue(difference.endsWith("(DecimalLiteral vs FloatLiteral)"), difference);

        // renderings that differ are left alone
        RoutineLoadDesc other = parseDesc("COLUMNS(b = a * 2)");
        Assertions.assertEquals("COLUMNS b = `a` * 1.5 vs `a` * 2", decimal.findExpressionDifference(other).orElseThrow());
    }

    @Test
    public void testHasSameExpressionsChecksCollectionTypes() {
        for (String[] expressions : new String[][] {
                {"ARRAY<DECIMALV2(10, 2)>[1.0]", "ARRAY<DECIMAL64(10, 2)>[1.0]"},
                {"MAP<INT, DECIMALV2(10, 2)>{1: 1.0}", "MAP<INT, DECIMAL64(10, 2)>{1: 1.0}"},
                {"ARRAY<INT>[1]", "[1]"},
                {"MAP<INT, INT>{1: 2}", "map{1: 2}"},
                {"array_length([ARRAY<INT>[1]])", "array_length([ARRAY<BIGINT>[1]])"},
                {"ARRAY<STRUCT<x DECIMALV2(10, 2)>>[row(1.0)]", "ARRAY<STRUCT<x DECIMAL64(10, 2)>>[row(1.0)]"},
        }) {
            RoutineLoadDesc left = parseDesc("COLUMNS(result = " + expressions[0] + ")");
            RoutineLoadDesc right = parseDesc("COLUMNS(result = " + expressions[1] + ")");
            Assertions.assertFalse(left.hasSameExpressions(right), expressions[0] + " vs " + expressions[1]);
            Assertions.assertTrue(left.hasSameExpressions(parseDesc(left.toSql())), expressions[0]);

            RoutineLoadDesc whereLeft = parseDesc("WHERE " + expressions[0] + " IS NOT NULL");
            RoutineLoadDesc whereRight = parseDesc("WHERE " + expressions[1] + " IS NOT NULL");
            Assertions.assertFalse(whereLeft.hasSameExpressions(whereRight), expressions[0] + " in WHERE");
        }
    }

    @Test
    public void testToSql() throws Exception {
        RoutineLoadDesc originLoad = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo("CREATE ROUTINE LOAD job ON tbl " +
                "COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n', " +
                "COLUMNS(`a`, `b`, `c`=1), " +
                "TEMPORARY PARTITION(`p1`, `p2`), " +
                "WHERE a = 1 " +
                "PROPERTIES (\"desired_concurrent_number\"=\"3\") " +
                "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", 0), null);

        RoutineLoadDesc desc = new RoutineLoadDesc();
        // set column separator and check
        desc.setColumnSeparator(originLoad.getColumnSeparator());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';'", desc.toSql());
        // set row delimiter and check
        desc.setRowDelimiter(originLoad.getRowDelimiter());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n'", desc.toSql());
        // set columns and check
        desc.setColumnsInfo(originLoad.getColumnsInfo());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';', " +
                "ROWS TERMINATED BY '\n', " +
                "COLUMNS(`a`, `b`, `c` = 1)", desc.toSql());
        // set partitions and check
        desc.setPartitionNames(originLoad.getPartitionNames());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';', " +
                        "ROWS TERMINATED BY '\n', " +
                        "COLUMNS(`a`, `b`, `c` = 1), " +
                        "TEMPORARY PARTITION(`p1`, `p2`)",
                desc.toSql());
        // set where and check
        desc.setWherePredicate(originLoad.getWherePredicate());
        Assertions.assertEquals("COLUMNS TERMINATED BY ';', " +
                        "ROWS TERMINATED BY '\n', " +
                        "COLUMNS(`a`, `b`, `c` = 1), " +
                        "TEMPORARY PARTITION(`p1`, `p2`), " +
                        "WHERE `a` = 1",
                desc.toSql());
    }

    @Test
    public void testIncludeMetadataRoundTrip() throws Exception {
        // parse + AstBuilder.visitIncludeMetadata + buildLoadDesc populate the clause.
        RoutineLoadDesc originLoad = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(
                "CREATE ROUTINE LOAD job ON tbl " +
                        "INCLUDE METADATA(KEY AS k, PARTITION AS p, OFFSET AS o, HEADERS AS h), " +
                        "COLUMNS(a, b) " +
                        "PROPERTIES (\"format\"=\"json\") " +
                        "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", 0), null);

        Assertions.assertNotNull(originLoad.getMetadata());
        Assertions.assertEquals(4, originLoad.getMetadata().getItems().size());
        Assertions.assertEquals("KEY", originLoad.getMetadata().getItems().get(0).getKey());
        Assertions.assertEquals("k", originLoad.getMetadata().getItems().get(0).getAlias());

        // toSql renders the clause (write leg of the persistence round-trip).
        RoutineLoadDesc desc = new RoutineLoadDesc();
        desc.setMetadata(originLoad.getMetadata());
        Assertions.assertEquals("INCLUDE METADATA(KEY AS `k`, PARTITION AS `p`, OFFSET AS `o`, HEADERS AS `h`)",
                desc.toSql());

        // re-parse the rendered SQL (read leg) -> the clause survives an origStmt round-trip.
        RoutineLoadDesc reparsed = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(
                "CREATE ROUTINE LOAD job ON tbl " + desc.toSql() +
                        " PROPERTIES (\"format\"=\"json\") FROM KAFKA (\"kafka_topic\" = \"my_topic\")", 0), null);
        Assertions.assertNotNull(reparsed.getMetadata());
        Assertions.assertEquals(4, reparsed.getMetadata().getItems().size());
        Assertions.assertEquals("OFFSET", reparsed.getMetadata().getItems().get(2).getKey());
        Assertions.assertEquals("o", reparsed.getMetadata().getItems().get(2).getAlias());

        // A reserved-word alias round-trips because ParseUtil.backquote quotes it; without the backquotes
        // the rendered `AS from` would fail to re-parse.
        RoutineLoadDesc reserved = new RoutineLoadDesc();
        reserved.setMetadata(CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(
                "CREATE ROUTINE LOAD job ON tbl INCLUDE METADATA(KEY AS `from`), COLUMNS(a, b) " +
                        "PROPERTIES (\"format\"=\"json\") FROM KAFKA (\"kafka_topic\" = \"my_topic\")", 0), null)
                .getMetadata());
        Assertions.assertEquals("INCLUDE METADATA(KEY AS `from`)", reserved.toSql());
        RoutineLoadDesc reservedReparsed = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(
                "CREATE ROUTINE LOAD job ON tbl " + reserved.toSql() +
                        " PROPERTIES (\"format\"=\"json\") FROM KAFKA (\"kafka_topic\" = \"my_topic\")", 0), null);
        Assertions.assertEquals("from", reservedReparsed.getMetadata().getItems().get(0).getAlias());
    }

    @Test
    public void testIncludeMetadataAliasOptional() throws Exception {
        RoutineLoadDesc originLoad = CreateRoutineLoadStmt.getLoadDesc(new OriginStatementInfo(
                "CREATE ROUTINE LOAD job ON tbl " +
                        "INCLUDE METADATA(topic, KEY AS k), " +
                        "COLUMNS(topic, k) " +
                        "PROPERTIES (\"format\"=\"json\") " +
                        "FROM KAFKA (\"kafka_topic\" = \"my_topic\")", 0), null);

        Assertions.assertNotNull(originLoad.getMetadata());
        Assertions.assertEquals(2, originLoad.getMetadata().getItems().size());
        Assertions.assertEquals("topic", originLoad.getMetadata().getItems().get(0).getKey());
        Assertions.assertEquals("topic", originLoad.getMetadata().getItems().get(0).getAlias());
        Assertions.assertEquals("KEY", originLoad.getMetadata().getItems().get(1).getKey());
        Assertions.assertEquals("k", originLoad.getMetadata().getItems().get(1).getAlias());

        RoutineLoadDesc desc = new RoutineLoadDesc();
        desc.setMetadata(originLoad.getMetadata());
        Assertions.assertEquals("INCLUDE METADATA(topic AS `topic`, KEY AS `k`)", desc.toSql());
    }
}
