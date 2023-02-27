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

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.FailedPredicateException;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.LexerNoViableAltException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

class ErrorHandler extends BaseErrorListener {

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
                            String message, RecognitionException e) {
        String detailMsg = message == null ? "" : message;
        NodePosition pos = new NodePosition(line, charPositionInLine);
        String tokenName;

        if (e instanceof NoViableAltException || e instanceof LexerNoViableAltException) {
            // it means parser cannot find a suitable rule for the input.
            tokenName = SqlParser.getTokenDisplay(e.getOffendingToken());
            detailMsg = PARSER_ERROR_MSG.noViableStatement(tokenName);
        } else if (e instanceof InputMismatchException) {
            // it means parser find the input only partially matches the rule.
            tokenName = SqlParser.getTokenDisplay(e.getOffendingToken());
            detailMsg = PARSER_ERROR_MSG.inputMismatch(tokenName);
        } else if (e instanceof FailedPredicateException) {
            tokenName = SqlParser.getTokenDisplay(e.getOffendingToken());
            detailMsg = PARSER_ERROR_MSG.failedPredicate(tokenName, ((FailedPredicateException) e).getPredicate());
        } else {
            // other unknown error, use the message return by Antlr
        }

        throw new ParsingException(detailMsg, pos);
    }
}
