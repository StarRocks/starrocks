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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.type.JsonType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;

/**
 * Fuse {@code parse_json(x) + json_query/cast} chains over VARCHAR input into the BE fast path.
 *
 * <p>The arrow operator {@code parse_json(x) -> 'p'} is already lowered by the analyzer
 * ({@code RelationFields::visitArrowExpr}) to {@code json_query(parse_json(x), 'p')}. This rule
 * matches the post-analysis tree and rewrites it to a single fused call:
 *
 * <pre>
 *   json_query(parse_json(x), const_path)                  ->  json_query_from_string(x, const_path)
 *   cast(json_query(parse_json(x), const_path) as T)        ->  get_json_T(x, const_path)
 *   cast(json_query_from_string(x, const_path) as T)        ->  get_json_T(x, const_path)
 * </pre>
 *
 * Fusion is gated on:
 * <ul>
 *   <li>{@code x} is VARCHAR/STRING-typed (not already JSON);</li>
 *   <li>{@code const_path} is a non-null constant string operand;</li>
 *   <li>The cast target type T is one of {TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE,
 *       CHAR, VARCHAR/STRING, BOOLEAN}. Integer-narrower-than-BIGINT and FLOAT targets fuse
 *       via {@code get_json_int}/{@code get_json_double} with an outer cast preserved by the
 *       optimizer; CHAR fuses via {@code get_json_string}.</li>
 * </ul>
 *
 * Multi-path note: this rule operates locally on each scalar tree. If the same
 * {@code parse_json(x)} value appears in multiple places in a query, each location is fused
 * independently. The current legacy code already re-parses per occurrence, so this rule does
 * not regress the multi-path baseline; future multi-path-aware extraction is tracked separately.
 */
public class JsonExtractFusionRule extends BottomUpScalarOperatorRewriteRule {

    private static final ImmutableMap<PrimitiveType, String> CAST_TARGET_TO_GET_JSON_FN =
            ImmutableMap.<PrimitiveType, String>builder()
                    .put(PrimitiveType.BIGINT, FunctionSet.GET_JSON_INT)
                    .put(PrimitiveType.INT, FunctionSet.GET_JSON_INT)
                    .put(PrimitiveType.SMALLINT, FunctionSet.GET_JSON_INT)
                    .put(PrimitiveType.TINYINT, FunctionSet.GET_JSON_INT)
                    .put(PrimitiveType.DOUBLE, FunctionSet.GET_JSON_DOUBLE)
                    .put(PrimitiveType.FLOAT, FunctionSet.GET_JSON_DOUBLE)
                    .put(PrimitiveType.VARCHAR, FunctionSet.GET_JSON_STRING)
                    .put(PrimitiveType.CHAR, FunctionSet.GET_JSON_STRING)
                    .put(PrimitiveType.BOOLEAN, FunctionSet.GET_JSON_BOOL)
                    .build();

    @Override
    public ScalarOperator visitCall(CallOperator call, ScalarOperatorRewriteContext context) {
        // Pattern A: json_query(parse_json(x), const_path)  ->  json_query_from_string(x, const_path)
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

    @Override
    public ScalarOperator visitCastOperator(CastOperator cast, ScalarOperatorRewriteContext context) {
        // Pattern B: cast(<json_extracting_call> as T)  ->  get_json_T(x, const_path)
        if (cast.getChildren().size() != 1 || !(cast.getChild(0) instanceof CallOperator)) {
            return cast;
        }
        CallOperator inner = (CallOperator) cast.getChild(0);

        ScalarOperator x = null;
        ScalarOperator path = null;
        if (FunctionSet.JSON_QUERY.equalsIgnoreCase(inner.getFnName())
                && inner.getChildren().size() == 2
                && isParseJsonOverVarchar(inner.getChild(0))
                && isConstStringPath(inner.getChild(1))) {
            x = inner.getChild(0).getChild(0);
            path = inner.getChild(1);
        } else if (FunctionSet.JSON_QUERY_FROM_STRING.equalsIgnoreCase(inner.getFnName())
                && inner.getChildren().size() == 2
                && isVarcharLike(inner.getChild(0).getType())
                && isConstStringPath(inner.getChild(1))) {
            x = inner.getChild(0);
            path = inner.getChild(1);
        } else {
            return cast;
        }

        PrimitiveType target = cast.getType().getPrimitiveType();
        String fusedName = CAST_TARGET_TO_GET_JSON_FN.get(target);
        if (fusedName == null) {
            return cast;
        }

        Type[] argTypes = new Type[] {x.getType(), path.getType()};
        Function fn = ExprUtils.getBuiltinFunction(fusedName, argTypes, Function.CompareMode.IS_IDENTICAL);
        if (fn == null) {
            return cast;
        }

        ScalarOperator fused = new CallOperator(fn.functionName(), fn.getReturnType(),
                Lists.newArrayList(x, path), fn);

        // get_json_int returns BIGINT; get_json_double returns DOUBLE; get_json_string returns VARCHAR.
        // For narrower or non-canonical targets (INT, SMALLINT, TINYINT, FLOAT, CHAR) we must keep an
        // explicit cast around the fused call so the resulting expression's type matches the original
        // cast target exactly. Dropping the cast would silently widen INT to BIGINT etc.
        if (!fn.getReturnType().equals(cast.getType())) {
            return new CastOperator(cast.getType(), fused, cast.isImplicit());
        }
        return fused;
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
