// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.parser;

import com.google.common.collect.Lists;
import com.starrocks.analysis.InsertStmt;
import com.starrocks.analysis.SqlScanner;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryStatement;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.StringReader;
import java.util.List;

public class SqlParser {
    public static List<StatementBase> parse(String sql, ConnectContext session) {
        List<StatementBase> statements = Lists.newArrayList();
        String[] splitSql = sql.split(";");

        for (int i = 0; i < splitSql.length; ++i) {
            try {
                StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(splitSql[i])));
                CommonTokenStream tokenStream = new CommonTokenStream(lexer);
                StarRocksParser parser = new StarRocksParser(tokenStream);
                parser.removeErrorListeners();
                parser.addErrorListener(new ErrorHandler());
                StarRocksParser.SqlStatementsContext sqlStatements = parser.sqlStatements();
                statements.add((StatementBase) new AstBuilder().visitSingleStatement(sqlStatements.singleStatement(0)));
            } catch (ParsingException parsingException) {
                StatementBase statementBase = parseWithOldParser(splitSql[i], session);
                if (statementBase instanceof QueryStatement || statementBase instanceof InsertStmt) {
                    throw parsingException;
                }
                statements.add(statementBase);
            }
        }

        return statements;
    }

    private static StatementBase parseWithOldParser(String originStmt, ConnectContext session) {
        SqlScanner input = new SqlScanner(new StringReader(originStmt), session.getSessionVariable().getSqlMode());
        com.starrocks.analysis.SqlParser parser = new com.starrocks.analysis.SqlParser(input);
        try {
            return SqlParserUtils.getFirstStmt(parser);
        } catch (Error e) {
            throw new ParsingException("Please check your sql, we meet an error when parsing.");
        } catch (AnalysisException e) {
            String errorMessage = parser.getErrorMsg(originStmt);
            if (errorMessage == null) {
                throw new ParsingException(e.getMessage());
            } else {
                throw new ParsingException(errorMessage);
            }
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage == null) {
                throw new ParsingException("Internal Error");
            } else {
                throw new ParsingException("Internal Error: " + errorMessage);
            }
        }
    }
}
