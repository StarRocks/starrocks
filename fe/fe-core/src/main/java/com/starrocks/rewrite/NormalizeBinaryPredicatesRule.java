// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/rewrite/NormalizeBinaryPredicatesRule.java

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

package com.starrocks.rewrite;

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.common.AnalysisException;

/**
 * Normalizes binary predicates of the form <expr> <op> <slot> so that the slot is
 * on the left hand side. Predicates where <slot> is wrapped in a cast (implicit or
 * explicit) are normalized, too.
 * <p>
 * Examples:
 * 5 > id -> id < 5
 */
@Deprecated
public class NormalizeBinaryPredicatesRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new NormalizeBinaryPredicatesRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
        if (!(expr instanceof BinaryPredicate)) {
            return expr;
        }
        if (expr.getChild(0).unwrapSlotRef(false) != null) {
            return expr;
        }
        if (expr.getChild(1).unwrapSlotRef(false) == null) {
            return expr;
        }

        BinaryPredicate.Operator op = ((BinaryPredicate) expr).getOp();

        return new BinaryPredicate(op.converse(), expr.getChild(1), expr.getChild(0));
    }
}
