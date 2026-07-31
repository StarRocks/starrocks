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

package com.starrocks.connector.starrocks;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictQueryOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictionaryGetOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Converts a planner-level predicate {@link ScalarOperator} into a SQL fragment that can be
 * appended to the remote FE's synthesized scan SQL.
 *
 * <p>The trust boundary is a <em>deny list</em>: any conjunct whose subtree contains an
 * expression that would be unsafe to evaluate on a different cluster (UDFs, non-deterministic
 * built-ins, optimizer-internal placeholders, subqueries) falls back to residual evaluation on
 * the local BE. Everything else is converted to its SQL form via {@link AstToSQLBuilder#toSQL}
 * and pushed down to the remote FE, which re-parses and re-analyzes it through its own
 * optimizer, mirroring how StarRocks already persists {@code Expr}s across boundaries
 * (e.g. {@code ExpressionSerializedObject}, {@code DefaultExpr}, {@code MysqlScanNode}).
 *
 * <p>The remote-side trust check {@code StarRocksRemoteScanService.isSafePushdownPredicate}
 * mirrors this deny list and rejects any predicate that ships in unsafe shapes even if the
 * local serializer is buggy or compromised.
 */
public class StarRocksRemotePredicateSerializer {
    private static final Logger LOG = LogManager.getLogger(StarRocksRemotePredicateSerializer.class);

    private StarRocksRemotePredicateSerializer() {
    }

    public static Result serialize(ScalarOperator predicate,
                                   Map<ColumnRefOperator, Column> colRefToColumn) {
        if (predicate == null) {
            return new Result(null, Collections.emptyList(), Collections.emptyList());
        }

        // The FormatterContext used by the planner binds ColumnRefOperators to slot-bound
        // SlotRefs whose colName is null (the SlotDescriptor carries the Column but no label),
        // which makes AstToSQLBuilder emit `null` instead of the column name. Build a
        // parallel mapping that wraps each column in a bare-name SlotRef so the SQL fragment
        // round-trips through the remote parser as a normal column reference.
        Map<ColumnRefOperator, Expr> nameOnlyColRefToExpr = new HashMap<>();
        for (Map.Entry<ColumnRefOperator, Column> entry : colRefToColumn.entrySet()) {
            SlotRef slotRef = new SlotRef(null, entry.getValue().getName());
            // Carry the column type so subfield access (e.g. struct_col.f1) can render to SQL;
            // without a type SubfieldExpr.toSql() fails and the predicate falls to residual.
            slotRef.setType(entry.getValue().getType());
            nameOnlyColRefToExpr.put(entry.getKey(), slotRef);
        }
        ScalarOperatorToExpr.FormatterContext sqlFormatter =
                new ScalarOperatorToExpr.FormatterContext(nameOnlyColRefToExpr);

        List<String> pushdownFragments = new ArrayList<>();
        List<ScalarOperator> residualPredicates = new ArrayList<>();
        List<String> unsupportedReasons = new ArrayList<>();

        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            String unsafeReason = findUnsafeReason(conjunct);
            if (unsafeReason != null) {
                residualPredicates.add(conjunct);
                unsupportedReasons.add(unsafeReason);
                continue;
            }
            try {
                Expr expr = ScalarOperatorToExpr.buildExecExpression(conjunct, sqlFormatter);
                String sql = AstToSQLBuilder.toSQL(expr);
                if (sql == null || sql.isEmpty()) {
                    residualPredicates.add(conjunct);
                    unsupportedReasons.add("toSQL produced empty fragment");
                    continue;
                }
                pushdownFragments.add(sql);
            } catch (Exception e) {
                LOG.debug("falling back to residual: failed to convert predicate {} to SQL", conjunct, e);
                residualPredicates.add(conjunct);
                unsupportedReasons.add("toSQL failed: " + e);
            }
        }

        String joined = null;
        if (!pushdownFragments.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < pushdownFragments.size(); i++) {
                if (i > 0) {
                    sb.append(" AND ");
                }
                sb.append('(').append(pushdownFragments.get(i)).append(')');
            }
            joined = sb.toString();
        }
        return new Result(joined, residualPredicates, unsupportedReasons);
    }

    private static String findUnsafeReason(ScalarOperator op) {
        UnsafeReasonFinder finder = new UnsafeReasonFinder();
        op.accept(finder, null);
        return finder.reason;
    }

    /**
     * Walks the scalar operator tree and records the first unsafe-to-push-down node it finds.
     * Implements the deny list described in the class javadoc.
     */
    private static final class UnsafeReasonFinder extends ScalarOperatorVisitor<Void, Void> {
        private String reason;

        @Override
        public Void visit(ScalarOperator op, Void context) {
            if (reason != null) {
                return null;
            }
            if (op instanceof SubqueryOperator) {
                reason = "subquery predicates are evaluated locally";
                return null;
            }
            if (op instanceof CloneOperator) {
                reason = "internal CloneOperator is not portable across clusters";
                return null;
            }
            if (op instanceof DictMappingOperator) {
                reason = "internal DictMappingOperator is not portable across clusters";
                return null;
            }
            // dict_mapping() / dictionary_get() reference cluster-local dictionary
            // objects; a same-named dictionary on the remote could silently produce
            // different values, so they must be evaluated locally.
            if (op instanceof DictQueryOperator) {
                reason = "dict_mapping is not portable across clusters";
                return null;
            }
            if (op instanceof DictionaryGetOperator) {
                reason = "dictionary_get is not portable across clusters";
                return null;
            }
            if (op instanceof CallOperator call) {
                String fnName = call.getFnName();
                if (fnName != null) {
                    String lower = fnName.toLowerCase(Locale.ROOT);
                    if (FunctionSet.allNonDeterministicFunctions.contains(lower)) {
                        reason = "non-deterministic function: " + lower;
                        return null;
                    }
                }
                Function fn = call.getFunction();
                if (fn != null && fn.isUdf()) {
                    reason = "user-defined function: " + fnName;
                    return null;
                }
            }
            for (ScalarOperator child : op.getChildren()) {
                if (reason != null) {
                    return null;
                }
                child.accept(this, context);
            }
            return null;
        }
    }

    public static class Result {
        private final String pushdownSql;
        private final List<ScalarOperator> residualPredicates;
        private final List<String> unsupportedReasons;

        Result(String pushdownSql, List<ScalarOperator> residualPredicates, List<String> unsupportedReasons) {
            this.pushdownSql = pushdownSql;
            this.residualPredicates = residualPredicates;
            this.unsupportedReasons = unsupportedReasons;
        }

        public String getPushdownSql() {
            return pushdownSql;
        }

        public List<ScalarOperator> getResidualPredicates() {
            return residualPredicates;
        }

        public List<String> getUnsupportedReasons() {
            return unsupportedReasons;
        }
    }
}
