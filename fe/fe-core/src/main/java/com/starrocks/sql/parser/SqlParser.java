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

package com.starrocks.sql.parser;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.connector.parser.trino.TrinoParserUtils;
import com.starrocks.connector.trino.TrinoParserUnsupportedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ImportColumnsStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.UnsupportedException;
import io.trino.sql.parser.StatementSplitter;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class SqlParser {
    private static final Logger LOG = LogManager.getLogger(SqlParser.class);
    private static final String EOF = "<EOF>";
    private static final int MIN_TOKEN_LIMIT = 100;
    private final AstBuilder.AstBuilderFactory astBuilderFactory;

    public SqlParser(AstBuilder.AstBuilderFactory astBuilderFactory) {
        this.astBuilderFactory = astBuilderFactory;
    }

    public static List<StatementBase> parse(String sql, SessionVariable sessionVariable) {
        try {
            if (sessionVariable.getSqlDialect().equalsIgnoreCase("trino")) {
                return parseWithTrinoDialect(sql, sessionVariable);
            } else {
                return parseWithStarRocksDialect(sql, sessionVariable);
            }
        } catch (OutOfMemoryError e) {
            LOG.warn("parser out of memory, sql is:" + sql);
            throw e;
        }
    }

    private static List<StatementBase> parseWithTrinoDialect(String sql, SessionVariable sessionVariable) {
        List<StatementBase> statements = Lists.newArrayList();
        try {
            StatementSplitter splitter = new StatementSplitter(sql);
            for (int idx = 0; idx < splitter.getCompleteStatements().size(); ++idx) {
                StatementSplitter.Statement statement = splitter.getCompleteStatements().get(idx);
                StatementBase statementBase = TrinoParserUtils.toStatement(statement.statement(),
                        sessionVariable.getSqlMode());
                statementBase.setOrigStmt(new OriginStatement(sql, idx));
                statements.add(statementBase);
            }
            if (!splitter.getPartialStatement().isEmpty()) {
                StatementBase statement = TrinoParserUtils.toStatement(splitter.getPartialStatement(),
                        sessionVariable.getSqlMode());
                statement.setOrigStmt(new OriginStatement(sql, splitter.getCompleteStatements().size()));
                statements.add(statement);
            }
            if (ConnectContext.get() != null) {
                ConnectContext.get().setRelationAliasCaseInSensitive(true);
            }
        } catch (ParsingException e) {
            // In Trino parser AstBuilder, it could throw ParsingException for unexpected exception,
            // use StarRocks parser to parse now.
            LOG.warn("Trino parse sql [{}] error, cause by {}", sql, e);
            if (sessionVariable.isEnableDialectDowngrade()) {
                return tryParseWithStarRocksDialect(sql, sessionVariable, e);
            }
            throw e;
        } catch (io.trino.sql.parser.ParsingException e) {
            // This sql does not use Trino syntax，use StarRocks parser to parse now.
            if (sql.toLowerCase().contains("select")) {
                LOG.warn("Trino parse sql [{}] error, cause by {}", sql, e);
            }
            if (sessionVariable.isEnableDialectDowngrade()) {
                return tryParseWithStarRocksDialect(sql, sessionVariable, e);
            }
            throw e;
        } catch (TrinoParserUnsupportedException e) {
            // We only support Trino partial syntax now, and for Trino parser unsupported statement,
            // try to use StarRocks parser to parse
            if (sessionVariable.isEnableDialectDowngrade()) {
                return tryParseWithStarRocksDialect(sql, sessionVariable, e);
            }
            throw e;
        } catch (UnsupportedException e) {
            // For unsupported statement, it can not be parsed by trino or StarRocks parser, both parser
            // can not support it now, we just throw the exception here to give user more information
            LOG.warn("Sql [{}] are not supported by trino parser, cause by {}", sql, e);
            throw e;
        }
        if (statements.isEmpty() || statements.stream().anyMatch(Objects::isNull)) {
            return parseWithStarRocksDialect(sql, sessionVariable);
        }
        return statements;
    }

    private static List<StatementBase> tryParseWithStarRocksDialect(String sql, SessionVariable sessionVariable,
                                                                    Exception trinoException) {
        try {
            return parseWithStarRocksDialect(sql, sessionVariable);
        } catch (Exception starRocksException) {
            LOG.warn("StarRocks parse sql [{}] error, cause by {}", sql, starRocksException);
            if (trinoException instanceof UnsupportedException) {
                throw unsupportedException(String.format("Trino parser parse sql error: [%s], " +
                                "and StarRocks parser also can not parse: [%s]", trinoException, starRocksException));
            } else {
                throw new StarRocksPlannerException(ErrorType.USER_ERROR,
                        String.format("Trino parser parse sql error: [%s], and StarRocks parser also can not parse: [%s]",
                        trinoException, starRocksException));
            }
        }
    }

    private static List<StatementBase> parseWithStarRocksDialect(String sql, SessionVariable sessionVariable) {
        List<StatementBase> statements = Lists.newArrayList();
        Pair<ParserRuleContext, StarRocksParser> pair = invokeParser(sql, sessionVariable, StarRocksParser::sqlStatements);
        StarRocksParser.SqlStatementsContext sqlStatementsContext = (StarRocksParser.SqlStatementsContext) pair.first;
        List<StarRocksParser.SingleStatementContext> singleStatementContexts = sqlStatementsContext.singleStatement();
        for (int idx = 0; idx < singleStatementContexts.size(); ++idx) {
            // collect hint info
            HintCollector collector = new HintCollector((CommonTokenStream) pair.second.getTokenStream(), sessionVariable);
            collector.collect(singleStatementContexts.get(idx));
            AstBuilder astBuilder = GlobalStateMgr.getCurrentState().getSqlParser().astBuilderFactory
                    .create(sessionVariable.getSqlMode(), collector.getContextWithHintMap());
            StatementBase statement = (StatementBase) astBuilder.visitSingleStatement(singleStatementContexts.get(idx));
            if (astBuilder.getParameters() != null && astBuilder.getParameters().size() != 0
                    && !(statement instanceof PrepareStmt)) {
                // for prepare stm1 from  '', here statement is inner statement
                statement = new PrepareStmt("", statement, astBuilder.getParameters());
            } else {
                statement.setOrigStmt(new OriginStatement(sql, idx));
            }
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

    public static StatementBase parseSingleStatement(String originSql, long sqlMode) {
        List<StatementBase> statements = parse(originSql, sqlMode);
        if (statements.size() > 1) {
            throw new ParsingException("only single statement is supported");
        }
        return statements.get(0);
    }

    public static StatementBase parseOneWithStarRocksDialect(String originSql, SessionVariable sessionVariable) {
        return parseWithStarRocksDialect(originSql, sessionVariable).get(0);
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
        ParserRuleContext expressionContext = invokeParser(expressionSql, sessionVariable,
                StarRocksParser::expressionSingleton).first;
        return (Expr) GlobalStateMgr.getCurrentState().getSqlParser().astBuilderFactory
                .create(sqlMode).visit(expressionContext);
    }

    public static List<Expr> parseSqlToExprs(String expressions, SessionVariable sessionVariable) {
        StarRocksParser.ExpressionListContext expressionListContext = (StarRocksParser.ExpressionListContext)
                invokeParser(expressions, sessionVariable, StarRocksParser::expressionList).first;
        AstBuilder astBuilder = GlobalStateMgr.getCurrentState().getSqlParser().astBuilderFactory
                .create(sessionVariable.getSqlMode());
        return expressionListContext.expression().stream()
                .map(e -> (Expr) astBuilder.visit(e))
                .collect(Collectors.toList());
    }

    public static ImportColumnsStmt parseImportColumns(String expressionSql, long sqlMode) {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlMode(sqlMode);
        ParserRuleContext importColumnsContext = invokeParser(expressionSql, sessionVariable,
                StarRocksParser::importColumns).first;
        return (ImportColumnsStmt) GlobalStateMgr.getCurrentState().getSqlParser().astBuilderFactory
                .create(sqlMode).visit(importColumnsContext);
    }

    private static Pair<ParserRuleContext, StarRocksParser> invokeParser(
            String sql, SessionVariable sessionVariable,
            Function<StarRocksParser, ParserRuleContext> parseFunction) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        lexer.setSqlMode(sessionVariable.getSqlMode());
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        int exprLimit = Math.max(Config.expr_children_limit, sessionVariable.getExprChildrenLimit());
        int tokenLimit = Math.max(MIN_TOKEN_LIMIT, sessionVariable.getParseTokensLimit());
        StarRocksParser parser = new StarRocksParser(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorHandler());
        parser.removeParseListeners();
        parser.addParseListener(new PostProcessListener(tokenLimit, exprLimit));
        if (!Config.enable_parser_context_cache) {
            DFA[] decisionDFA = new DFA[parser.getATN().getNumberOfDecisions()];
            for (int i = 0; i < parser.getATN().getNumberOfDecisions(); i++) {
                decisionDFA[i] = new DFA(parser.getATN().getDecisionState(i), i);
            }
            parser.setInterpreter(new ParserATNSimulator(parser, parser.getATN(), decisionDFA, new PredictionContextCache()));
        }

        try {
            // inspire by https://github.com/antlr/antlr4/issues/192#issuecomment-15238595
            // try SLL mode with BailErrorStrategy firstly
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            parser.setErrorHandler(new StarRocksBailErrorStrategy());
            return Pair.create(parseFunction.apply(parser), parser);
        } catch (ParseCancellationException e) {
            // if we fail, parse with LL mode with our own error strategy
            // rewind input stream
            tokenStream.seek(0);
            parser.reset();
            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            parser.setErrorHandler(new StarRocksDefaultErrorStrategy());
            return Pair.create(parseFunction.apply(parser), parser);
        }
    }

    public static String getTokenDisplay(Token t) {
        if (t == null) {
            return "<no token>";
        }

        String s = t.getText();
        if (s == null) {
            if (t.getType() == Token.EOF) {
                s = EOF;
            } else {
                s = "<" + t.getType() + ">";
            }
        }
        return s;
    }
}
