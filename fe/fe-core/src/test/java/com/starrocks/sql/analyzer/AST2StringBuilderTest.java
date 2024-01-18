// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class AST2StringBuilderTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNot() {
        {
            String sql;
            sql = "CREATE VIEW v3 AS \n" +
                    "SELECT v1 FROM t0 WHERE ((NOT false) IS NULL);";
            List<StatementBase>
                    statementBase =
                    SqlParser.parse(sql, AnalyzeTestUtil.getConnectContext().getSessionVariable().getSqlMode());
            Assert.assertEquals(1, statementBase.size());
            StatementBase baseStmt = statementBase.get(0);
            Analyzer.analyze(baseStmt, AnalyzeTestUtil.getConnectContext());
            Assert.assertTrue(baseStmt instanceof CreateViewStmt);
            CreateViewStmt viewStmt = (CreateViewStmt) baseStmt;
            Assert.assertEquals(viewStmt.getInlineViewDef(),
                    "SELECT `test`.`t0`.`v1`\nFROM `test`.`t0`\nWHERE (NOT FALSE) IS NULL");
        }
        {
            String sql;
            sql = "CREATE VIEW v3 AS \n" +
                    "SELECT v1 FROM t0 WHERE ((NOT false) IS NOT NULL);";
            List<StatementBase>
                    statementBase =
                    SqlParser.parse(sql, AnalyzeTestUtil.getConnectContext().getSessionVariable().getSqlMode());
            Assert.assertEquals(1, statementBase.size());
            StatementBase baseStmt = statementBase.get(0);
            Analyzer.analyze(baseStmt, AnalyzeTestUtil.getConnectContext());
            Assert.assertTrue(baseStmt instanceof CreateViewStmt);
            CreateViewStmt viewStmt = (CreateViewStmt) baseStmt;
            Assert.assertEquals(viewStmt.getInlineViewDef(),
                    "SELECT `test`.`t0`.`v1`\nFROM `test`.`t0`\nWHERE (NOT FALSE) IS NOT NULL");
        }
    }

    @Test
    public void testSet() {
        // 1. one global statement
        String sql = "SET GLOBAL time_zone = 'Asia/Shanghai'";

        List<StatementBase> statementBase = SqlParser.parse(
                sql, AnalyzeTestUtil.getConnectContext().getSessionVariable().getSqlMode());
        Assert.assertEquals(1, statementBase.size());
        SetStmt originStmt = (SetStmt) statementBase.get(0);

        System.err.println(sql + " -> " + AstToStringBuilder.toString(originStmt));

        statementBase = SqlParser.parse(
                AstToStringBuilder.toString(originStmt), AnalyzeTestUtil.getConnectContext().getSessionVariable().getSqlMode());
        Assert.assertEquals(1, statementBase.size());
        SetStmt convertStmt = (SetStmt) statementBase.get(0);

        Assert.assertEquals(1, convertStmt.getSetVars().size());
        Assert.assertEquals(SetType.GLOBAL, convertStmt.getSetVars().get(0).getType());
        Assert.assertEquals(AstToStringBuilder.toString(originStmt), AstToStringBuilder.toString(convertStmt));

        // 2. two default statement
        sql = "SET time_zone = 'Asia/Shanghai', allow_default_partition=true;";
        statementBase = SqlParser.parse(
                sql, AnalyzeTestUtil.getConnectContext().getSessionVariable().getSqlMode());
        Assert.assertEquals(1, statementBase.size());
        originStmt = (SetStmt) statementBase.get(0);

        System.err.println(sql + " -> " + AstToStringBuilder.toString(originStmt));

        statementBase = SqlParser.parse(
                AstToStringBuilder.toString(originStmt), AnalyzeTestUtil.getConnectContext().getSessionVariable().getSqlMode());
        Assert.assertEquals(1, statementBase.size());
        convertStmt = (SetStmt) statementBase.get(0);

        Assert.assertEquals(2, convertStmt.getSetVars().size());
        Assert.assertEquals(SetType.DEFAULT, convertStmt.getSetVars().get(0).getType());
        Assert.assertEquals(SetType.DEFAULT, convertStmt.getSetVars().get(1).getType());
        Assert.assertEquals(AstToStringBuilder.toString(originStmt), AstToStringBuilder.toString(convertStmt));
    }

    @Test
    public void testUserVariable() {

        String sql = "SET time_zone = 'Asia/Shanghai', allow_default_partition=true, " +
                "@var1=1, " +
                "@var2 = cast('2020-01-01' as date)," +
                "@var3 = \"foo\"," +
                "@var4 = 1.23," +
                "@`select` = 1 + 2 * 3;";
        List<StatementBase> statementBase = SqlParser.parse(
                sql, AnalyzeTestUtil.getConnectContext().getSessionVariable().getSqlMode());
        Assert.assertEquals(1, statementBase.size());
        SetStmt originStmt = (SetStmt) statementBase.get(0);
        Analyzer.analyze(originStmt, AnalyzeTestUtil.getConnectContext());
        Assert.assertEquals("SET `time_zone` = 'Asia/Shanghai',`allow_default_partition` = TRUE," +
                "@`var1` = cast (1 as tinyint(4))," +
                "@`var2` = cast ('2020-01-01' as date)," +
                "@`var3` = cast ('foo' as varchar)," +
                "@`var4` = cast (1.23 as decimal(3, 2))," +
                "@`select` = cast (7 as int(11))", AstToStringBuilder.toString(originStmt));

        statementBase = SqlParser.parse(
                AstToStringBuilder.toString(originStmt), AnalyzeTestUtil.getConnectContext().getSessionVariable().getSqlMode());
        Assert.assertEquals(1, statementBase.size());
        SetStmt convertStmt = (SetStmt) statementBase.get(0);
        Analyzer.analyze(convertStmt, AnalyzeTestUtil.getConnectContext());

        Assert.assertEquals(7, convertStmt.getSetVars().size());
        Assert.assertEquals(SetType.DEFAULT, convertStmt.getSetVars().get(0).getType());
        Assert.assertEquals(SetType.DEFAULT, convertStmt.getSetVars().get(1).getType());
        Assert.assertEquals(AstToStringBuilder.toString(originStmt), AstToStringBuilder.toString(convertStmt));
    }
}
