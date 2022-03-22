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

import com.starrocks.common.AnalysisException;
import com.starrocks.common.jmockit.Deencapsulation;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class BinaryPredicateTest {

    @Mocked
    Analyzer analyzer;

    @Test(expected = AnalysisException.class)
    public void testMultiColumnSubquery(@Injectable Expr child0,
                                        @Injectable Subquery child1) throws AnalysisException {
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, child0, child1);
        binaryPredicate.analyzeImpl(analyzer);
    }

    @Test(expected = AnalysisException.class)
    public void testSingleColumnSubquery(@Injectable Expr child0,
                                         @Injectable QueryStmt subquery,
                                         @Injectable SlotRef slotRef) throws AnalysisException {
        Subquery child1 = new Subquery(subquery);
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, child0, child1);

        binaryPredicate.analyzeImpl(analyzer);
        Assert.assertSame(null, Deencapsulation.getField(binaryPredicate, "fn"));
    }
}