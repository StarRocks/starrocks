// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

class ErrorHandler extends BaseErrorListener {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
                            String message, RecognitionException e) {
        String errorMessage = String.format("You have an error in your SQL syntax; " +
                "check the manual that corresponds to your MySQL server version for the right syntax to use " +
                "near '%s' at line %d", ((Token) offendingSymbol).getText(), line);
        throw new ParsingException(errorMessage);    }
}
