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

package com.starrocks.sql.optimizer.rule.tree.exprreuse;

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.type.JsonType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The VARCHAR overloads of get_json_xxx(doc, path) parse the whole document inside every call, so a
 * projection that extracts N paths from one document parses it N times. Each of them has a JSON overload
 * with the same result type, prepare/close functions and semantics once the document is parsed by
 * parse_json, which uses the same parser and the same NULL-or-error handling for invalid input.
 *
 * When one VARCHAR document feeds at least share_json_parse_min_extractions (default 2) distinct
 * extractions in a projection, this rule rewrites them to get_json_xxx(parse_json(doc), path). It runs
 * right before {@link ScalarOperatorsReuseRule}, which then evaluates the shared parse_json(doc) once as a
 * common sub-expression.
 */
public class ShareJsonParseRule implements TreeRewriteRule {
    private static final Set<String> JSON_GETTERS = ImmutableSet.of(
            FunctionSet.GET_JSON_INT,
            FunctionSet.GET_JSON_DOUBLE,
            FunctionSet.GET_JSON_STRING,
            FunctionSet.GET_JSON_OBJECT,
            FunctionSet.GET_JSON_SCALAR);

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SessionVariable sessionVariable = taskContext.getOptimizerContext().getSessionVariable();
        // Expression reuse may evaluate the shared parse_json on rows a CASE branch would have skipped. That
        // never changes a result, but in ALLOW_THROW_EXCEPTION mode an invalid document on such a row would
        // now fail the query instead of being ignored.
        if (!sessionVariable.isEnableShareJsonParse()
                || (sessionVariable.getSqlMode() & SqlModeHelper.MODE_ALLOW_THROW_EXCEPTION) != 0) {
            return root;
        }
        // Below 1 would mean "rewrite documents nobody extracts from", which is the same as 1.
        int minExtractions = Math.max(1, sessionVariable.getShareJsonParseMinExtractions());
        Deque<OptExpression> stack = new ArrayDeque<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            OptExpression opt = stack.pop();
            Projection projection = opt.getOp().getProjection();
            if (projection != null) {
                opt.getOp().setProjection(rewriteProjection(projection, minExtractions));
            }
            opt.getInputs().forEach(stack::push);
        }
        return root;
    }

    static Projection rewriteProjection(Projection projection, int minExtractions) {
        Map<ScalarOperator, Set<CallOperator>> extractionsByDoc = new HashMap<>();
        projection.getColumnRefMap().values().forEach(op -> collectExtractions(op, extractionsByDoc));
        projection.getCommonSubOperatorMap().values().forEach(op -> collectExtractions(op, extractionsByDoc));

        Set<ScalarOperator> sharedDocs = new HashSet<>();
        extractionsByDoc.forEach((doc, calls) -> {
            if (calls.size() >= minExtractions) {
                sharedDocs.add(doc);
            }
        });
        if (sharedDocs.isEmpty()) {
            return projection;
        }

        Rewriter rewriter = new Rewriter(sharedDocs);
        return new Projection(rewriteAll(projection.getColumnRefMap(), rewriter),
                rewriteAll(projection.getCommonSubOperatorMap(), rewriter),
                projection.needReuseLambdaDependentExpr());
    }

    private static <K> Map<K, ScalarOperator> rewriteAll(Map<K, ScalarOperator> operators, Rewriter rewriter) {
        Map<K, ScalarOperator> result = new LinkedHashMap<>();
        operators.forEach((key, op) -> result.put(key, op.accept(rewriter, null)));
        return result;
    }

    private static void collectExtractions(ScalarOperator root, Map<ScalarOperator, Set<CallOperator>> result) {
        Deque<ScalarOperator> stack = new ArrayDeque<>();
        stack.push(root);
        while (!stack.isEmpty()) {
            ScalarOperator op = stack.pop();
            if (op instanceof CallOperator call && isVarcharExtraction(call)) {
                result.computeIfAbsent(call.getChild(0), k -> new HashSet<>()).add(call);
            }
            op.getChildren().forEach(stack::push);
        }
    }

    private static boolean isVarcharExtraction(CallOperator call) {
        if (!JSON_GETTERS.contains(call.getFnName()) || call.getArguments().size() != 2) {
            return false;
        }
        Function fn = call.getFunction();
        if (fn == null || fn.getArgs().length != 2 || !fn.getArgs()[0].isVarchar()) {
            return false;
        }
        // A constant document is folded or evaluated once anyway.
        ScalarOperator doc = call.getChild(0);
        return doc.getType().isVarchar() && !doc.isConstant();
    }

    private static class Rewriter extends BaseScalarOperatorShuttle {
        private final Set<ScalarOperator> sharedDocs;

        Rewriter(Set<ScalarOperator> sharedDocs) {
            this.sharedDocs = sharedDocs;
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, Void context) {
            ScalarOperator rewritten = super.visitCall(call, context);
            if (!isVarcharExtraction(call) || !sharedDocs.contains(call.getChild(0))
                    || !(rewritten instanceof CallOperator rewrittenCall)) {
                return rewritten;
            }
            Function jsonFn = ExprUtils.getBuiltinFunction(call.getFnName(),
                    new Type[] {JsonType.JSON, VarcharType.VARCHAR}, Function.CompareMode.IS_IDENTICAL);
            Function parseFn = ExprUtils.getBuiltinFunction(FunctionSet.PARSE_JSON,
                    new Type[] {VarcharType.VARCHAR}, Function.CompareMode.IS_IDENTICAL);
            // get_json_int has a deprecated INT overload; only rewrite when the JSON overload keeps the type.
            if (jsonFn == null || parseFn == null || !jsonFn.getReturnType().matchesType(call.getType())) {
                return rewritten;
            }
            CallOperator parsed = new CallOperator(FunctionSet.PARSE_JSON, JsonType.JSON,
                    List.of(rewrittenCall.getChild(0)), parseFn);
            return new CallOperator(call.getFnName(), call.getType(), List.of(parsed, rewrittenCall.getChild(1)),
                    jsonFn);
        }
    }
}
