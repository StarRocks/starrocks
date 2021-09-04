// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/BinaryPredicateTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.jmockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class BinaryPredicateTest {

    @Mocked
    Analyzer analyzer;

    @Test
    public void testMultiColumnSubquery(@Injectable Expr child0,
                                        @Injectable Subquery child1) {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, child0, child1);
        new Expectations() {
            {
                child1.returnsScalarColumn();
                result = false;
            }
        };

        try {
            binaryPredicate.analyzeImpl(analyzer);
            Assert.fail();
        } catch (AnalysisException e) {
        }
    }

    @Test
    public void testSingleColumnSubquery(@Injectable Expr child0,
                                         @Injectable QueryStmt subquery,
                                         @Injectable SlotRef slotRef) {
        Subquery child1 = new Subquery(subquery);
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, child0, child1);
        new Expectations() {
            {
                subquery.getResultExprs();
                result = Lists.newArrayList(slotRef);
                slotRef.getType();
                result = Type.INT;
            }
        };

        try {
            binaryPredicate.analyzeImpl(analyzer);
            Assert.assertSame(null, Deencapsulation.getField(binaryPredicate, "fn"));
        } catch (AnalysisException e) {
            Assert.fail(e.getMessage());
        }
    }
}