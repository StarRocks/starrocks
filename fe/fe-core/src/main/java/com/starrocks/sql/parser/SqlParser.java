// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.parser;

import com.clearspring.analytics.util.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.common.Config;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.ImportColumnsStmt;
import com.starrocks.sql.ast.StatementBase;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.util.List;

public class SqlParser {

    public static List<StatementBase> parse(String sql, SessionVariable sessionVariable) {
        StarRocksParser parser = parserBuilder(sql, sessionVariable);
        List<StatementBase> statements = Lists.newArrayList();
        List<StarRocksParser.SingleStatementContext> singleStatementContexts = parser.sqlStatements().singleStatement();
        for (int idx = 0; idx < singleStatementContexts.size(); ++idx) {
            StatementBase statement = (StatementBase) new AstBuilder(sessionVariable.getSqlMode())
                    .visitSingleStatement(singleStatementContexts.get(idx));
            statement.setOrigStmt(new OriginStatement(sql, idx));
            statements.add(statement);
        }
        return statements;
    }

    /**
     * We need not only sqlMode but also other parameters to define the property of parser.
     * Please consider use {@link #parse(String, SessionVariable)}
     */
    @Deprecated
    public static List<StatementBase> parse(String originSql, long sqlMode) {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlMode(sqlMode);
        return parse(originSql, sessionVariable);
    }

    /**
     * Please use {@link #parse(String, SessionVariable)}
     */
    @Deprecated
    public static StatementBase parseFirstStatement(String originSql, long sqlMode) {
        return parse(originSql, sqlMode).get(0);
    }

    public static StatementBase parseOneWithStarRocksDialect(String originSql, SessionVariable sessionVariable) {
        return parse(originSql, sessionVariable).get(0);
    }

    /**
     * parse sql to expression, only supports new parser
     *
     * @param expressionSql expression sql
     * @param sqlMode       sqlMode
     * @return Expr
     */
    public static Expr parseSqlToExpr(String expressionSql, long sqlMode) {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlMode(sqlMode);

        return (Expr) new AstBuilder(sqlMode)
                .visit(parserBuilder(expressionSql, sessionVariable).expressionSingleton().expression());
    }

    public static ImportColumnsStmt parseImportColumns(String expressionSql, long sqlMode) {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlMode(sqlMode);

        return (ImportColumnsStmt) new AstBuilder(sqlMode)
                .visit(parserBuilder(expressionSql, sessionVariable).importColumns());
    }

    private static StarRocksParser parserBuilder(String sql, SessionVariable sessionVariable) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        lexer.setSqlMode(sessionVariable.getSqlMode());
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        StarRocksParser parser = new StarRocksParser(tokenStream);

        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorHandler());
        parser.removeParseListeners();
        parser.addParseListener(new TokenNumberListener(sessionVariable.getParseTokensLimit(), 
                Math.max(Config.expr_children_limit, sessionVariable.getExprChildrenLimit())));

        return parser;
    }
}
