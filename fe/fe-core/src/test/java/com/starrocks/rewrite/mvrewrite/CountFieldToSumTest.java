// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/rewrite/mvrewrite/CountFieldToSumTest.java

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

package com.starrocks.rewrite.mvrewrite;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.AnalysisException;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CountFieldToSumTest {

    @Test
    public void testCountDistinct(@Injectable Analyzer analyzer,
                                  @Injectable FunctionCallExpr functionCallExpr) {
        TableName tableName = new TableName("db1", "table1");
        SlotRef slotRef = new SlotRef(tableName, "c1");
        List<Expr> params = Lists.newArrayList();
        params.add(slotRef);

        new Expectations() {
            {
                functionCallExpr.getFnName().getFunction();
                result = FunctionSet.COUNT;
                functionCallExpr.getChildren();
                result = params;
                functionCallExpr.getChild(0);
                result = slotRef;
                functionCallExpr.getParams().isDistinct();
                result = true;
            }
        };
        CountFieldToSum countFieldToSum = new CountFieldToSum();
        try {
            Expr rewrittenExpr = countFieldToSum.apply(functionCallExpr, analyzer);
            Assert.assertTrue(rewrittenExpr instanceof FunctionCallExpr);
            Assert.assertEquals(FunctionSet.COUNT, ((FunctionCallExpr) rewrittenExpr).getFnName().getFunction());
        } catch (AnalysisException e) {
            System.out.println(e.getMessage());
        }
    }
}
