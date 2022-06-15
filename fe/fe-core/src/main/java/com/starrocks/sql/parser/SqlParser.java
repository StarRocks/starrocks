// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.parser;

import com.clearspring.analytics.util.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.QueryStmt;
import com.starrocks.analysis.SqlScanner;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.qe.OriginStatement;
import com.starrocks.sql.StatementPlanner;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.StringReader;
import java.util.List;

public class SqlParser {
    public static List<StatementBase> parse(String originSql, long sqlMode) {
        List<String> splitSql = splitSQL(originSql);
        List<StatementBase> statements = Lists.newArrayList();

        for (int idx = 0; idx < splitSql.size(); ++idx) {
            String sql = splitSql.get(idx);
            try {
                StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
                CommonTokenStream tokenStream = new CommonTokenStream(lexer);
                StarRocksParser parser = new StarRocksParser(tokenStream);
                StarRocksParser.sqlMode = sqlMode;
                parser.removeErrorListeners();
                parser.addErrorListener(new ErrorHandler());
                StarRocksParser.SqlStatementsContext sqlStatements = parser.sqlStatements();
                StatementBase statement = (StatementBase) new AstBuilder(sqlMode)
                        .visitSingleStatement(sqlStatements.singleStatement(0));
                statement.setOrigStmt(new OriginStatement(sql, idx));
                statements.add(statement);
            } catch (ParsingException parsingException) {
                try {
                    StatementBase statement = parseWithOldParser(sql, sqlMode, 0);
                    if (StatementPlanner.supportedByNewPlanner(statement) || statement instanceof QueryStmt) {
                        throw parsingException;
                    }
                    statements.add(statement);
                } catch (Throwable e) {
                    throw parsingException;
                }
            }
        }

        return statements;
    }

    /**
     * parse sql to expression, only supports new parser
     *
     * @param expressionSql expression sql
     * @param sqlMode       sqlMode
     * @return Expr
     */
    public static Expr parseSqlToExpr(String expressionSql, long sqlMode) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(expressionSql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);
        StarRocksParser.sqlMode = sqlMode;
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorHandler());
        StarRocksParser.ExpressionContext expressionContext = parser.expression();
        return ((Expr) new AstBuilder(sqlMode).visit(expressionContext));
    }

    public static StatementBase parseFirstStatement(String originSql, long sqlMode) {
        return parse(originSql, sqlMode).get(0);
    }

    public static StatementBase parseWithOldParser(String originStmt, long sqlMode, int idx) throws AnalysisException {
        SqlScanner input = new SqlScanner(new StringReader(originStmt), sqlMode);
        com.starrocks.analysis.SqlParser parser = new com.starrocks.analysis.SqlParser(input);
        try {
            return SqlParserUtils.getStmt(parser, idx);
        } catch (AnalysisException e) {
            throw e;
        } catch (Throwable e) {
            String errorMessage = e.getMessage();
            if (errorMessage == null) {
                throw new AnalysisException("Internal Error");
            } else {
                throw new AnalysisException("Internal Error: " + errorMessage);
            }
        }
    }

    private static List<String> splitSQL(String sql) {
        List<String> sqlLists = Lists.newArrayList();
        boolean inString = false;
        int sqlStartOffset = 0;
        char inStringStart = '-';
        for (int i = 0; i < sql.length(); ++i) {
            if (!inString && (sql.charAt(i) == '\"' || sql.charAt(i) == '\'' || sql.charAt(i) == '`')) {
                inString = true;
                inStringStart = sql.charAt(i);
            } else if (inString && (sql.charAt(i) == inStringStart)) {
                inString = false;
            }

            if (sql.charAt(i) == ';') {
                if (!inString) {
                    sqlLists.add(sql.substring(sqlStartOffset, i));
                    sqlStartOffset = i + 1;
                }
            }
        }

        String last = sql.substring(sqlStartOffset).trim();
        if (!last.isEmpty()) {
            sqlLists.add(last);
        }
        return sqlLists;
    }
}
