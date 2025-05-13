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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.Assert;
import org.junit.Test;

public class ColumnRangePredicateTest {

    @Test
    public void testNonCanonicalBigintRangePredicate() {
        ColumnRefOperator columnRef = new ColumnRefOperator(1, Type.BIGINT, "col", true);
        ConstantOperator maxValue = ConstantOperator.createBigint(9223372036854775807L);
        BinaryPredicateOperator pred = new BinaryPredicateOperator(BinaryType.GT, columnRef, maxValue);
        PredicateExtractor extractor = new PredicateExtractor();
        PredicateExtractor.PredicateExtractorContext context = new PredicateExtractor.PredicateExtractorContext();
        RangePredicate rangePredicate = extractor.visitBinaryPredicate(pred, context);
        String s = rangePredicate.toScalarOperator().toString();
        Assert.assertEquals(s, "1: col > 9223372036854775807", s);
    }
}