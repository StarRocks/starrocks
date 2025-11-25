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
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

import static com.starrocks.sql.parser.ErrorMsgProxy.PARSER_ERROR_MSG;

class ErrorHandler extends BaseErrorListener {

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
                            String message, RecognitionException e) {
        String detailMsg = message == null ? "" : message;
        NodePosition pos = new NodePosition(line, charPositionInLine);
        String tokenName;

        if (e instanceof FailedPredicateException) {
            tokenName = getTokenDisplay(e.getOffendingToken());
            detailMsg = PARSER_ERROR_MSG.failedPredicate(tokenName, ((FailedPredicateException) e).getPredicate());
        } else {
            // other unknown error, use the message return by Antlr
        }

        throw new ParsingException(detailMsg, pos);
    }

    public static String getTokenDisplay(Token t) {
        if (t == null) {
            return "<no token>";
        }

        String s = t.getText();
        if (s == null) {
            if (t.getType() == Token.EOF) {
                s = "<EOF>";
            } else {
                s = "<" + t.getType() + ">";
            }
        }
        return s;
    }
}
