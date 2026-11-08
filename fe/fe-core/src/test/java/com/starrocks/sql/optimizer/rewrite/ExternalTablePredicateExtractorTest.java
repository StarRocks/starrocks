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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExternalTablePredicateExtractorTest {
    @Test
    public void testDecimalPrecisionCastIsNotPushedDown() {
        // A cast to DECIMAL(3, 0) turns 9999 into NULL, where PostgreSQL raises an error and MySQL clamps it
        Type decimal3 = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 3, 0);
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.BIGINT, "k", true);
        for (boolean isMySQL : new boolean[] {false, true}) {
            ScalarOperator isNull = new IsNullPredicateOperator(new CastOperator(decimal3, column));
            ExternalTablePredicateExtractor extractor = new ExternalTablePredicateExtractor(isMySQL);
            extractor.extract(isNull);
            Assertions.assertNull(extractor.getPushPredicate());
            Assertions.assertEquals(isNull, extractor.getReservePredicate());
        }

        // An implicit cast is removed and the predicate pushed down, as before
        ScalarOperator implicitIsNull = new IsNullPredicateOperator(new CastOperator(decimal3, column, true));
        ExternalTablePredicateExtractor extractor = new ExternalTablePredicateExtractor(false);
        extractor.extract(implicitIsNull);
        Assertions.assertEquals(new IsNullPredicateOperator(column), extractor.getPushPredicate());
        Assertions.assertNull(extractor.getReservePredicate());
    }
}
