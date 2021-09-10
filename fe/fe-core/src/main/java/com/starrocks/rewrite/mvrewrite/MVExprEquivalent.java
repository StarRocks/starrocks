// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/rewrite/mvrewrite/MVExprEquivalent.java

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

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Expr;

/**
 * Only support the once match from originExpr and newExpr
 * TODO: one query expr could be calculate by a group by mv columns
 * TODO: mvExprEqual(queryexpr, mvColumnExprList)
 */
// Our new cost based query optimizer is more powerful and stable than old query optimizer,
// The old query optimizer related codes could be deleted safely.
// TODO: Remove old query optimizer related codes before 2021-09-30
@Deprecated
public class MVExprEquivalent {

    private static final ImmutableList<MVExprEqualRule> exprRewriteRuleList = ImmutableList
            .<MVExprEqualRule>builder()
            .add(FunctionCallEqualRule.INSTANCE)
            .add(SlotRefEqualRule.INSTANCE)
            .build();

    public static boolean mvExprEqual(Expr queryExpr, Expr mvColumnExpr) {
        for (MVExprEqualRule rule : exprRewriteRuleList) {
            if (rule.equal(queryExpr, mvColumnExpr)) {
                return true;
            }
        }
        return false;
    }
}
