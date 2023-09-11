// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RangeSimplifier {
    protected static final Logger LOG = LogManager.getLogger(RangeSimplifier.class);

    private final ScalarOperator srcPredicate;

    public RangeSimplifier(ScalarOperator srcPredicate) {
        this.srcPredicate = srcPredicate;
    }

    // check whether target range predicates are contained in srcPredicates
    // all ScalarOperator should be BinaryPredicateOperator,
    // left is ColumnRefOperator and right is ConstantOperator
    public ScalarOperator simplify(ScalarOperator target) {
        try {
            RangePredicate srcRangePredicate = extractRangePredicate(srcPredicate);
            RangePredicate targetRangePredicate = extractRangePredicate(target);
            return srcRangePredicate.simplify(targetRangePredicate);
        } catch (Exception e) {
            LOG.debug("Simplify scalar operator {} failed:", target, e);
            return null;
        }
    }

    private RangePredicate extractRangePredicate(ScalarOperator scalarOperator) {
        PredicateExtractor extractor = new PredicateExtractor();
        return scalarOperator.accept(extractor, new PredicateExtractor.PredicateExtractorContext());
    }
}
