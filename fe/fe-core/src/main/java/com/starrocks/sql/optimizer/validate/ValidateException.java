// This file is made available under Elastic License 2.0.

package com.starrocks.sql.optimizer.validate;

import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;

public class ValidateException extends StarRocksPlannerException {

    public ValidateException(String message, ErrorType type) {
        super(message, type);
    }

}
