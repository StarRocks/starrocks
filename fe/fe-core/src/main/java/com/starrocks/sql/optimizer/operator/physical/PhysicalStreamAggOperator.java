// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

public class PhysicalStreamAggOperator extends PhysicalOperator {

    public PhysicalStreamAggOperator(AggType type,
                                     List<ColumnRefOperator> groupBys,
                                     List<ColumnRefOperator> partitionByColumns,
                                     Map<ColumnRefOperator, CallOperator> aggregations,
                                     int singleDistinctFunctionPos,
                                     boolean isSplit,
                                     long limit,
                                     ScalarOperator predicate,
                                     Projection projection) {
        super(OperatorType.PHYSICAL_STREAM_AGG);
    }
}
