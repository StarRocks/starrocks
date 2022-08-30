// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.parser;

import com.clearspring.analytics.util.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SqlScanner;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.SqlParserUtils;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.StatementPlanner;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.StringReader;
import java.util.List;

public class SqlParser {

    public static List<StatementBase> parse(String originSql, SessionVariable sessionVariable) {
        List<String> splitSql = splitSQL(originSql);
        List<StatementBase> statements = Lists.newArrayList();
        for (int idx = 0; idx < splitSql.size(); ++idx) {
            String sql = splitSql.get(idx);
            StatementBase statement = parseSingleSql(sql, sessionVariable);
            statement.setOrigStmt(new OriginStatement(sql, idx));
            statements.add(statement);
        }
        return statements;
    }

    public static StatementBase parseSingleSql(String sql, SessionVariable sessionVariable) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);
        setParserProperty(parser, sessionVariable);
        StatementBase statement;
        try {
            StarRocksParser.SqlStatementsContext sqlStatements = parser.sqlStatements();
            statement = (StatementBase) new AstBuilder(sessionVariable.getSqlMode())
                    .visitSingleStatement(sqlStatements.singleStatement(0));
            return statement;
        } catch (OperationNotAllowedException e) {
            // sql forbidden to execute, so no need to parse again by the old parser.
            throw e;
        } catch (ParsingException parsingException) {
            try {
                statement = parseWithOldParser(sql, sessionVariable.getSqlMode(), 0);
            } catch (Exception e) {
                // both new and old parser failed. We return new parser error info to client.
                throw parsingException;
            }
            if (StatementPlanner.supportedByNewPlanner(statement)) {
                throw parsingException;
            }
            return statement;
        }
    }

    public static void setParserProperty(StarRocksParser parser, SessionVariable sessionVariable) {
        parser.sqlMode = sessionVariable.getSqlMode();
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorHandler());
        parser.removeParseListeners();
        parser.addParseListener(new TokenNumberListener(sessionVariable.getParseTokensLimit()));
    }

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
                StatementBase statement;
                try {
                    statement = parseWithOldParser(sql, sqlMode, 0);
                } catch (Exception e) {
                    // both new and old parser failed. We return new parser error info to client.
                    throw parsingException;
                }

                if (StatementPlanner.supportedByNewPlanner(statement)) {
                    throw parsingException;
                }
                statements.add(statement);
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

    public static StatementBase parseWithOldParser(String originStmt, long sqlMode, int idx) {
        SqlScanner input = new SqlScanner(new StringReader(originStmt), sqlMode);
        com.starrocks.analysis.SqlParser parser = new com.starrocks.analysis.SqlParser(input);
        try {
            return SqlParserUtils.getStmt(parser, idx);
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

    private static List<String> splitSQL(String sql) {
        sql = removeComment(sql);

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

    /*
     * The new version of parser can handle comments (discarded directly in lexical analysis).
     * But there are some special cases when the old and new parsers are compatible.
     * Because the parser does not support all statements, we split the sql according to ";".
     * For example, sql: --xxx;\nselect 1; According to the old version, it will be parsed into one statement,
     * but after splitting according to the statement,
     * two statements will appear. This leads to incompatibility.
     * Because originSql is stored in the old version of the materialized view, it is the index stored in the old version.
     * */
    private static String removeComment(String sql) {
        boolean inString = false;
        char inStringStart = '-';
        boolean isSimpleComment = false;
        boolean isBracketComment = false;

        boolean inComment = false;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < sql.length(); ++i) {
            if (inComment) {
                if (sql.charAt(i) == '\n' && isSimpleComment) {
                    inComment = false;
                }

                if (sql.charAt(i) == '*' && i != sql.length() - 1 && sql.charAt(i + 1) == '/' && isBracketComment) {
                    inComment = false;
                    i++;
                }

                continue;
            }

            if (sql.charAt(i) == '-' && i != sql.length() - 1 && sql.charAt(i + 1) == '-') {
                if (!inString) {
                    inComment = true;
                    isSimpleComment = true;
                    continue;
                }
            }

            if (sql.charAt(i) == '/' && i != sql.length() - 2 && sql.charAt(i + 1) == '*' && sql.charAt(i + 2) != '+') {
                if (!inString) {
                    inComment = true;
                    isBracketComment = true;
                    continue;
                }
            }

            sb.append(sql.charAt(i));

            if (!inString && (sql.charAt(i) == '\"' || sql.charAt(i) == '\'' || sql.charAt(i) == '`')) {
                inString = true;
                inStringStart = sql.charAt(i);
            } else if (inString && (sql.charAt(i) == inStringStart)) {
                inString = false;
            }
        }
        return sb.toString();
    }
}
