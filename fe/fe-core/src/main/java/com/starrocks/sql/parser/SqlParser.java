// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.parser;

import com.google.common.collect.Lists;
import com.starrocks.analysis.StatementBase;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.util.List;

public class SqlParser {
    public static List<StatementBase> parse(String sql) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorHandler());

        StarRocksParser.SqlStatementsContext sqlStatementsContext = parser.sqlStatements();

        AstBuilder astBuilder = new AstBuilder();
        List<StatementBase> statements = Lists.newArrayList();
        for (int i = 0; i < sqlStatementsContext.singleStatement().size(); ++i) {
            StarRocksParser.SingleStatementContext singleStatementContext = sqlStatementsContext.singleStatement(i);
            statements.add((StatementBase) astBuilder.visitSingleStatement(singleStatementContext));
        }
        return statements;
    }
}
