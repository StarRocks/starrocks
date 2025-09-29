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

import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test case for issue #63669: FE Assertion Failure when querying on int32 column in WHERE clause
 */
public class Int32PredicateTest {
    
    @Test
    public void testInt32PredicateWithLargeValue() {
        // Reproduce the issue scenario: WHERE collect_api_receive_time = 1234567890
        ColumnRefOperator columnRef = new ColumnRefOperator(1, Type.INT, "collect_api_receive_time", true);
        ConstantOperator value = ConstantOperator.createInt(1234567890);
        
        // Create a binary predicate: collect_api_receive_time = 1234567890
        BinaryPredicateOperator pred = new BinaryPredicateOperator(BinaryType.EQ, columnRef, value);
        
        // This should not throw an assertion failure anymore
        Assertions.assertNotNull(pred);
        Assertions.assertEquals(BinaryType.EQ, pred.getBinaryType());
        Assertions.assertEquals(columnRef, pred.getChild(0));
        Assertions.assertEquals(value, pred.getChild(1));
        
        // Test the distance calculation that was causing the issue
        ConstantOperator intMin = ConstantOperator.createInt(Integer.MIN_VALUE);
        long distance = value.distance(intMin);
        Assertions.assertEquals(3382051538L, distance);
    }
    
    @Test
    public void testInt32PredicateExtraction() {
        // Test predicate extraction which involves range canonicalization
        ColumnRefOperator columnRef = new ColumnRefOperator(1, Type.INT, "test_column", true);
        ConstantOperator value = ConstantOperator.createInt(1234567890);
        
        // Create equality predicate
        BinaryPredicateOperator eqPred = new BinaryPredicateOperator(BinaryType.EQ, columnRef, value);
        
        // Create greater than predicate
        BinaryPredicateOperator gtPred = new BinaryPredicateOperator(BinaryType.GT, columnRef, value);
        
        // Create less than predicate
        BinaryPredicateOperator ltPred = new BinaryPredicateOperator(BinaryType.LT, columnRef, value);
        
        // These should not cause assertion failures
        PredicateExtractor extractor = new PredicateExtractor();
        PredicateExtractor.PredicateExtractorContext context = new PredicateExtractor.PredicateExtractorContext();
        
        try {
            RangePredicate eqRange = extractor.visitBinaryPredicate(eqPred, context);
            RangePredicate gtRange = extractor.visitBinaryPredicate(gtPred, context);
            RangePredicate ltRange = extractor.visitBinaryPredicate(ltPred, context);
            
            Assertions.assertNotNull(eqRange);
            Assertions.assertNotNull(gtRange);
            Assertions.assertNotNull(ltRange);
        } catch (Exception e) {
            Assertions.fail("Predicate extraction should not throw exception: " + e.getMessage());
        }
    }
    
    @Test
    public void testInt32EdgeCasesInPredicates() {
        // Test edge cases that could cause overflow in distance calculation
        ColumnRefOperator columnRef = new ColumnRefOperator(1, Type.INT, "test_column", true);
        
        // Test with INT_MAX
        ConstantOperator intMax = ConstantOperator.createInt(Integer.MAX_VALUE);
        BinaryPredicateOperator maxPred = new BinaryPredicateOperator(BinaryType.EQ, columnRef, intMax);
        
        // Test with INT_MIN
        ConstantOperator intMin = ConstantOperator.createInt(Integer.MIN_VALUE);
        BinaryPredicateOperator minPred = new BinaryPredicateOperator(BinaryType.EQ, columnRef, intMin);
        
        // Test with the specific value from the issue
        ConstantOperator issueValue = ConstantOperator.createInt(1234567890);
        BinaryPredicateOperator issuePred = new BinaryPredicateOperator(BinaryType.EQ, columnRef, issueValue);
        
        PredicateExtractor extractor = new PredicateExtractor();
        PredicateExtractor.PredicateExtractorContext context = new PredicateExtractor.PredicateExtractorContext();
        
        try {
            RangePredicate maxRange = extractor.visitBinaryPredicate(maxPred, context);
            RangePredicate minRange = extractor.visitBinaryPredicate(minPred, context);
            RangePredicate issueRange = extractor.visitBinaryPredicate(issuePred, context);
            
            Assertions.assertNotNull(maxRange);
            Assertions.assertNotNull(minRange);
            Assertions.assertNotNull(issueRange);
        } catch (Exception e) {
            Assertions.fail("Edge case predicate extraction should not throw exception: " + e.getMessage());
        }
    }
}