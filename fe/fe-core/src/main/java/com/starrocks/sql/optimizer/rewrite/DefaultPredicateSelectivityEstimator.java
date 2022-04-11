// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Predicate selectivity estimator
 */
public class DefaultPredicateSelectivityEstimator {

    private static final Logger LOG = LogManager.getLogger(DefaultPredicateSelectivityEstimator.class);

    public double estimate(ScalarOperator scalarOperator, Statistics statistics) {
        //todo need to implement
        return 0.0d;
    }


}
