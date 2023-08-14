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
import com.starrocks.connector.parser.trino.TrinoParserUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.ImportColumnsStmt;
import com.starrocks.sql.ast.StatementBase;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.StatementSplitter;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
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

public class SqlParser {
    private static final Logger LOG = LogManager.getLogger(SqlParser.class);

    private static final String EOF = "<EOF>";

    public static List<StatementBase> parse(String sql, SessionVariable sessionVariable) {
        if (sessionVariable.getSqlDialect().equalsIgnoreCase("trino")) {
            return parseWithTrinoDialect(sql, sessionVariable);
        } else {
            return parseWithStarRocksDialect(sql, sessionVariable);
        }
    }

    private static List<StatementBase> parseWithTrinoDialect(String sql, SessionVariable sessionVariable) {
        List<StatementBase> statements = Lists.newArrayList();
        try {
            StatementSplitter splitter = new StatementSplitter(sql);
            for (StatementSplitter.Statement statement : splitter.getCompleteStatements()) {
                statements.add(TrinoParserUtils.toStatement(statement.statement(), sessionVariable.getSqlMode()));
            }
            if (!splitter.getPartialStatement().isEmpty()) {
                statements.add(TrinoParserUtils.toStatement(splitter.getPartialStatement(),
                        sessionVariable.getSqlMode()));
            }
            if (ConnectContext.get() != null) {
                ConnectContext.get().setRelationAliasCaseInSensitive(true);
            }
        } catch (ParsingException e) {
            // we only support trino partial syntax, use StarRocks parser to parse now
            if (sql.toLowerCase().contains("select")) {
                LOG.warn("Trino parse sql [{}] error, cause by {}", sql, e);
            }
            return parseWithStarRocksDialect(sql, sessionVariable);
        } catch (UnsupportedOperationException e) {
            // For unsupported statement, use StarRocks parser to parse
            return parseWithStarRocksDialect(sql, sessionVariable);
        }
        if (statements.isEmpty() || statements.stream().anyMatch(Objects::isNull)) {
            return parseWithStarRocksDialect(sql, sessionVariable);
        }
        return statements;
    }

    private static List<StatementBase> parseWithStarRocksDialect(String sql, SessionVariable sessionVariable) {
        List<StatementBase> statements = Lists.newArrayList();
        StarRocksParser parser = parserBuilder(sql, sessionVariable);
        List<StarRocksParser.SingleStatementContext> singleStatementContexts =
                parser.sqlStatements().singleStatement();
        for (int idx = 0; idx < singleStatementContexts.size(); ++idx) {
            StatementBase statement = (StatementBase) new AstBuilder(sessionVariable.getSqlMode())
                    .visitSingleStatement(singleStatementContexts.get(idx));
            statement.setOrigStmt(new OriginStatement(sql, idx));
            statements.add(statement);
        }
        if (ConnectContext.get() != null) {
            ConnectContext.get().setRelationAliasCaseInSensitive(false);
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

        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorHandler());
        parser.removeParseListeners();
        parser.addParseListener(new PostProcessListener(sessionVariable.getParseTokensLimit(),
                Math.max(Config.expr_children_limit, sessionVariable.getExprChildrenLimit())));
        return parser;
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
