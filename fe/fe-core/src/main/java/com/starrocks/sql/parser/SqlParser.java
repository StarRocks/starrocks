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
<<<<<<< HEAD
=======
import com.starrocks.common.Pair;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.connector.parser.trino.TrinoParserUtils;
import com.starrocks.connector.trino.TrinoParserUnsupportedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
<<<<<<< HEAD
import com.starrocks.sql.ast.ImportColumnsStmt;
=======
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ImportColumnsStmt;
import com.starrocks.sql.ast.PrepareStmt;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.UnsupportedException;
import io.trino.sql.parser.StatementSplitter;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
<<<<<<< HEAD
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.misc.IntervalSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.similarity.JaroWinklerDistance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;
=======
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

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import static com.starrocks.sql.common.UnsupportedException.unsupportedException;

public class SqlParser {
    private static final Logger LOG = LogManager.getLogger(SqlParser.class);
<<<<<<< HEAD
    private static final int MIN_TOKEN_LIMIT = 100;
    private static final String EOF = "<EOF>";

    public static List<StatementBase> parse(String sql, SessionVariable sessionVariable) {
        if (sessionVariable.getSqlDialect().equalsIgnoreCase("trino")) {
            return parseWithTrinoDialect(sql, sessionVariable);
        } else {
            return parseWithStarRocksDialect(sql, sessionVariable);
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
    }

    private static List<StatementBase> parseWithTrinoDialect(String sql, SessionVariable sessionVariable) {
        List<StatementBase> statements = Lists.newArrayList();
        try {
            StatementSplitter splitter = new StatementSplitter(sql);
<<<<<<< HEAD
            for (StatementSplitter.Statement statement : splitter.getCompleteStatements()) {
                statements.add(TrinoParserUtils.toStatement(statement.statement(), sessionVariable.getSqlMode()));
            }
            if (!splitter.getPartialStatement().isEmpty()) {
                statements.add(TrinoParserUtils.toStatement(splitter.getPartialStatement(),
                        sessionVariable.getSqlMode()));
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            }
            if (ConnectContext.get() != null) {
                ConnectContext.get().setRelationAliasCaseInSensitive(true);
            }
        } catch (ParsingException e) {
            // In Trino parser AstBuilder, it could throw ParsingException for unexpected exception,
            // use StarRocks parser to parse now.
            LOG.warn("Trino parse sql [{}] error, cause by {}", sql, e);
<<<<<<< HEAD
            return tryParseWithStarRocksDialect(sql, sessionVariable, e);
=======
            if (sessionVariable.isEnableDialectDowngrade()) {
                return tryParseWithStarRocksDialect(sql, sessionVariable, e);
            }
            throw e;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        } catch (io.trino.sql.parser.ParsingException e) {
            // This sql does not use Trino syntax，use StarRocks parser to parse now.
            if (sql.toLowerCase().contains("select")) {
                LOG.warn("Trino parse sql [{}] error, cause by {}", sql, e);
            }
<<<<<<< HEAD
            return tryParseWithStarRocksDialect(sql, sessionVariable, e);
        } catch (TrinoParserUnsupportedException e) {
            // We only support Trino partial syntax now, and for Trino parser unsupported statement,
            // try to use StarRocks parser to parse
            return tryParseWithStarRocksDialect(sql, sessionVariable, e);
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
        StarRocksParser parser = parserBuilder(sql, sessionVariable);
        List<StarRocksParser.SingleStatementContext> singleStatementContexts =
                parser.sqlStatements().singleStatement();
        for (int idx = 0; idx < singleStatementContexts.size(); ++idx) {
            HintCollector collector = new HintCollector((CommonTokenStream) parser.getTokenStream());
            collector.collect(singleStatementContexts.get(idx));

            AstBuilder astBuilder = new AstBuilder(sessionVariable.getSqlMode(), collector.getContextWithHintMap());
            StatementBase statement = (StatementBase) astBuilder.visitSingleStatement(singleStatementContexts.get(idx));
            statement.setOrigStmt(new OriginStatement(sql, idx));
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

<<<<<<< HEAD
=======
    public static StatementBase parseSingleStatement(String originSql, long sqlMode) {
        List<StatementBase> statements = parse(originSql, sqlMode);
        if (statements.size() > 1) {
            throw new ParsingException("only single statement is supported");
        }
        return statements.get(0);
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD

        return (Expr) new AstBuilder(sqlMode)
                .visit(parserBuilder(expressionSql, sessionVariable).expressionSingleton().expression());
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public static ImportColumnsStmt parseImportColumns(String expressionSql, long sqlMode) {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setSqlMode(sqlMode);
<<<<<<< HEAD

        return (ImportColumnsStmt) new AstBuilder(sqlMode)
                .visit(parserBuilder(expressionSql, sessionVariable).importColumns());
    }

    private static StarRocksParser parserBuilder(String sql, SessionVariable sessionVariable) {
=======
        ParserRuleContext importColumnsContext = invokeParser(expressionSql, sessionVariable,
                StarRocksParser::importColumns).first;
        return (ImportColumnsStmt) GlobalStateMgr.getCurrentState().getSqlParser().astBuilderFactory
                .create(sqlMode).visit(importColumnsContext);
    }

    private static Pair<ParserRuleContext, StarRocksParser> invokeParser(
            String sql, SessionVariable sessionVariable,
            Function<StarRocksParser, ParserRuleContext> parseFunction) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        lexer.setSqlMode(sessionVariable.getSqlMode());
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        int exprLimit = Math.max(Config.expr_children_limit, sessionVariable.getExprChildrenLimit());
        int tokenLimit = Math.max(MIN_TOKEN_LIMIT, sessionVariable.getParseTokensLimit());
        StarRocksParser parser = new StarRocksParser(tokenStream);
<<<<<<< HEAD

        // Unify the error message
        parser.setErrorHandler(new DefaultErrorStrategy() {
            @Override
            public Token recoverInline(Parser recognizer)
                    throws RecognitionException {
                if (nextTokensContext == null) {
                    throw new InputMismatchException(recognizer);
                } else {
                    throw new InputMismatchException(recognizer, nextTokensState, nextTokensContext);
                }
            }

            @Override
            public void reportNoViableAlternative(Parser recognizer, NoViableAltException e) {
                TokenStream tokens = recognizer.getInputStream();
                String input;
                if (tokens != null) {
                    if (e.getStartToken().getType() == Token.EOF) {
                        input = EOF;
                    } else {
                        input = tokens.getText(e.getStartToken(), e.getOffendingToken());
                    }
                } else {
                    input = "<unknown input>";
                }
                String msg = PARSER_ERROR_MSG.noViableStatement(input);
                recognizer.notifyErrorListeners(e.getOffendingToken(), msg, e);
            }

            @Override
            public void reportInputMismatch(Parser recognizer, InputMismatchException e) {
                Token t = e.getOffendingToken();
                String tokenName = getTokenDisplay(t);
                IntervalSet expecting = getExpectedTokens(recognizer);
                String expects = filterExpectingToken(tokenName, expecting, recognizer.getVocabulary());
                String msg = PARSER_ERROR_MSG.unexpectedInput(tokenName, expects);
                recognizer.notifyErrorListeners(e.getOffendingToken(), msg, e);
            }

            @Override
            public void reportUnwantedToken(Parser recognizer) {
                if (inErrorRecoveryMode(recognizer)) {
                    return;
                }
                beginErrorCondition(recognizer);
                Token t = recognizer.getCurrentToken();
                String tokenName = getTokenDisplay(t);
                IntervalSet expecting = getExpectedTokens(recognizer);
                String expects = filterExpectingToken(tokenName, expecting, recognizer.getVocabulary());
                String msg = PARSER_ERROR_MSG.unexpectedInput(tokenName, expects);
                recognizer.notifyErrorListeners(t, msg, null);
            }

            private String filterExpectingToken(String token, IntervalSet expecting, Vocabulary vocabulary) {
                List<String> symbols = Lists.newArrayList();
                List<String> words = Lists.newArrayList();

                List<String> result = Lists.newArrayList();
                StringJoiner joiner = new StringJoiner(", ", "{", "}");
                JaroWinklerDistance jaroWinklerDistance = new JaroWinklerDistance();

                if (expecting.isNil()) {
                    return joiner.toString();
                }

                Iterator<Interval> iter = expecting.getIntervals().iterator();
                while (iter.hasNext()) {
                    Interval interval = iter.next();
                    int a = interval.a;
                    int b = interval.b;
                    if (a == b) {
                        addToken(vocabulary, a, symbols, words);
                    } else {
                        for (int i = a; i <= b; i++) {
                            addToken(vocabulary, i, symbols, words);
                        }
                    }
                }

                // if there exists an expect word in nonReserved words, there should be a legal identifier.
                if (words.contains("'ACCESS'")) {
                    result.add("a legal identifier");
                } else {
                    String upperToken = StringUtils.upperCase(token);
                    Collections.sort(words, Comparator.comparingDouble(s -> jaroWinklerDistance.apply(s, upperToken)));
                    int limit = Math.min(5, words.size());
                    result.addAll(words.subList(0, limit));
                    result.addAll(symbols);
                }

                result.forEach(joiner::add);
                return joiner.toString();
            }

            private void addToken(Vocabulary vocabulary, int a, Collection<String> symbols, Collection<String> words) {
                if (a == Token.EOF) {
                    symbols.add(EOF);
                } else if (a == Token.EPSILON) {
                    // do nothing
                } else {
                    String token = vocabulary.getDisplayName(a);
                    // ensure it's a word
                    if (token.length() > 1 && token.charAt(1) >= 'A' && token.charAt(1) <= 'Z') {
                        words.add(token);
                    } else {
                        symbols.add(token);
                    }
                }
            }
        });

=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorHandler());
        parser.removeParseListeners();
        parser.addParseListener(new PostProcessListener(tokenLimit, exprLimit));
<<<<<<< HEAD
        return parser;
=======
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
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
