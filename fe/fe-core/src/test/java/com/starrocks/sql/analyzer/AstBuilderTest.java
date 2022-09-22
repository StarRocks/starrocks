// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.analyzer;


import com.starrocks.analysis.ShowRoutineLoadStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.sql.ast.TruncatePartitionClause;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.CaseInsensitiveStream;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.parser.StarRocksLexer;
import com.starrocks.sql.parser.StarRocksParser;
import com.starrocks.utframe.UtFrameUtils;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;

public class AstBuilderTest {

    private static ConnectContext connectContext;


    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
    }

    @Test
    public void testModifyBackendHost() throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {
        String sql = "alter system modify backend host '127.0.0.1' to 'testHost'";
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);
        StarRocksParser.sqlMode = 32;
        StarRocksParser.SqlStatementsContext sqlStatements = parser.sqlStatements();
        StatementBase statement = (StatementBase) new AstBuilder(32).visitSingleStatement(sqlStatements.singleStatement(0));
        Field field = statement.getClass().getDeclaredField("alterClause");
        field.setAccessible(true);
        ModifyBackendAddressClause clause = (ModifyBackendAddressClause) field.get(statement);
        Assert.assertTrue(clause.getSrcHost().equals("127.0.0.1") && clause.getDestHost().equals("testHost"));
    }

    @Test
    public void testModifyFrontendHost() throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {
        String sql = "alter system modify frontend host '127.0.0.1' to 'testHost'";
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);
        StarRocksParser.sqlMode = 32;
        StarRocksParser.SqlStatementsContext sqlStatements = parser.sqlStatements();
        StatementBase statement = (StatementBase) new AstBuilder(32).visitSingleStatement(sqlStatements.singleStatement(0));
        Field field = statement.getClass().getDeclaredField("alterClause");
        field.setAccessible(true);
        ModifyFrontendAddressClause clause = (ModifyFrontendAddressClause) field.get(statement);
        Assert.assertTrue(clause.getSrcHost().equals("127.0.0.1") && clause.getDestHost().equals("testHost"));
    }

    @Test
    public void testTruncatePartition() throws Exception {
        String sql = "alter table db.test truncate partition p1";
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
        AlterTableStmt aStmt = (AlterTableStmt) statement;
        List<AlterClause> alterClauses = aStmt.getOps();
        TruncatePartitionClause c = (TruncatePartitionClause) alterClauses.get(0);
        Assert.assertTrue(c.getPartitionNames().getPartitionNames().get(0).equals("p1"));
    }

    @Test
    public void testShowRoutineLoad() throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {
        String sql = "SHOW ROUTINE LOAD FOR `rl_test`FROM `db_test` WHERE state == 'RUNNING' ORDER BY `CreateTime` desc";
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);
        StarRocksParser.sqlMode = 32;
        StarRocksParser.SqlStatementsContext sqlStatements = parser.sqlStatements();
        ShowRoutineLoadStmt stmt = (ShowRoutineLoadStmt) new AstBuilder(32).visit(sqlStatements.singleStatement(0));
        Assert.assertEquals("db_test", stmt.getDbFullName());
        Assert.assertEquals("rl_teet", stmt.getName());
    }

    @Test
    public void testStopRoutineLoad() throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {
        String sql = "STOP ROUTINE LOAD FOR `db_test`.`rl_test`";
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);
        StarRocksParser.sqlMode = 32;
        StarRocksParser.SqlStatementsContext sqlStatements = parser.sqlStatements();
        ShowRoutineLoadStmt stmt = (ShowRoutineLoadStmt) new AstBuilder(32).visit(sqlStatements.singleStatement(0));
        Assert.assertEquals("db_test", stmt.getDbFullName());
        Assert.assertEquals("rl_teet", stmt.getName());
    }
}
