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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class StarRocksDefaultErrorStrategy extends DefaultErrorStrategy {

    private static final String EOF = "<EOF>";

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
        String tokenName = SqlParser.getTokenDisplay(t);
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
        String tokenName = SqlParser.getTokenDisplay(t);
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
}
