// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.Expr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;

import static java.lang.String.format;

public class SemanticException extends StarRocksPlannerException {
    public SemanticException(String formatString) {
        super(formatString, ErrorType.USER_ERROR);
    }

    public SemanticException(String formatString, Object... args) {
        super(format(formatString, args), ErrorType.USER_ERROR);
    }

    public static SemanticException missingAttributeException(Expr node) throws SemanticException {
        throw new SemanticException("Column '%s' cannot be resolved", node.toSql());
    }
}
