// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

public class PhysicalStreamJoinOperator extends PhysicalJoinOperator {

    public PhysicalStreamJoinOperator(JoinOperator joinType,
                                      ScalarOperator onPredicate, String joinHint,
                                      long limit, ScalarOperator predicate,
                                      Projection projection) {
        super(OperatorType.PHYSICAL_STREAM_JOIN, joinType, onPredicate, joinHint, limit, predicate, projection);
    }
}
