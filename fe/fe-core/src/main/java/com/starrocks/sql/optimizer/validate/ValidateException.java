// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.validate;

import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;

public class ValidateException extends StarRocksPlannerException {

    public ValidateException(String message, ErrorType type) {
        super(message, type);
    }

}
