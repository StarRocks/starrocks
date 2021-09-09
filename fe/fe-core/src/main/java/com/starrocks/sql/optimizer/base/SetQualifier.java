// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.base;

import com.starrocks.analysis.SetOperationStmt;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;

public enum SetQualifier {
    ALL,
    DISTINCT;

    public static SetQualifier convert(SetOperationStmt.Qualifier qualifier) {
        if (qualifier.equals(SetOperationStmt.Qualifier.ALL)) {
            return ALL;
        } else if (qualifier.equals(SetOperationStmt.Qualifier.DISTINCT)) {
            return DISTINCT;
        } else {
            throw new StarRocksPlannerException("Not support qualifier", ErrorType.INTERNAL_ERROR);
        }
    }
}
