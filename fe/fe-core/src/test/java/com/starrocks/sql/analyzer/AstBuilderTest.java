// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;


import com.starrocks.analysis.ModifyBackendAddressClause;
import com.starrocks.analysis.ModifyFrontendAddressClause;
import com.starrocks.analysis.StatementBase;

import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.CaseInsensitiveStream;
import com.starrocks.sql.parser.StarRocksLexer;
import com.starrocks.sql.parser.StarRocksParser;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class AstBuilderTest {
    
    @Test
    public void testModifyBackendHost() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
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
        Assert.assertTrue(clause.getToBeModifyHost().equals("127.0.0.1") && clause.getFqdn().equals("testHost"));
    }

    @Test
    public void testModifyFrontendHost() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
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
        Assert.assertTrue(clause.getToBeModifyHost().equals("127.0.0.1") && clause.getFqdn().equals("testHost"));
    }
}
