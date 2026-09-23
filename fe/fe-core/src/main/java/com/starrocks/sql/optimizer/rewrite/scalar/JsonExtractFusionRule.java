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


package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.type.JsonType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;

/**
 * Extract a constant path from VARCHAR JSON without materializing the entire VPack document.
 * Casts remain outside the extraction to preserve their target type and error policy.
 */
public class JsonExtractFusionRule extends BottomUpScalarOperatorRewriteRule {

    @Override
    public ScalarOperator visitCall(CallOperator call, ScalarOperatorRewriteContext context) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null || !connectContext.getSessionVariable().isEnableJsonExtractFusion()) {
            return call;
        }
        if (FunctionSet.JSON_QUERY.equalsIgnoreCase(call.getFnName()) && call.getChildren().size() == 2) {
            ScalarOperator inner = call.getChild(0);
            ScalarOperator path = call.getChild(1);
            if (isParseJsonOverVarchar(inner) && isConstStringPath(path)) {
                ScalarOperator x = inner.getChild(0);
                Type[] argTypes = new Type[] {x.getType(), path.getType()};
                Function fn = ExprUtils.getBuiltinFunction(FunctionSet.JSON_QUERY_FROM_STRING, argTypes,
                        Function.CompareMode.IS_IDENTICAL);
                if (fn != null) {
                    return new CallOperator(fn.functionName(), JsonType.JSON,
                            Lists.newArrayList(x, path), fn);
                }
            }
        }
        return call;
    }

    private static boolean isParseJsonOverVarchar(ScalarOperator op) {
        if (!(op instanceof CallOperator)) {
            return false;
        }
        CallOperator call = (CallOperator) op;
        if (!FunctionSet.PARSE_JSON.equalsIgnoreCase(call.getFnName()) || call.getChildren().size() != 1) {
            return false;
        }
        return isVarcharLike(call.getChild(0).getType());
    }

    private static boolean isVarcharLike(Type type) {
        if (type == null) {
            return false;
        }
        PrimitiveType pt = type.getPrimitiveType();
        return pt == PrimitiveType.VARCHAR || pt == PrimitiveType.CHAR;
    }

    private static boolean isConstStringPath(ScalarOperator op) {
        if (!(op instanceof ConstantOperator)) {
            return false;
        }
        ConstantOperator c = (ConstantOperator) op;
        if (c.isNull()) {
            return false;
        }
        return isVarcharLike(c.getType());
    }
}
