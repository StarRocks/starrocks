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

package com.starrocks.sql.optimizer.rule.transformation.materialization.common;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorTypeReDeriver;
import com.starrocks.sql.optimizer.rewrite.TypeReDeriveException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

/**
 * MV-rewrite-specific substitutor. Wraps leaf substitution via
 * {@link ReplaceColumnRefRewriter} and bottom-up type re-derivation via
 * {@link ScalarOperatorTypeReDeriver} into a single entry point.
 *
 * <p>Returns {@link Optional#empty()} when re-derivation throws. The caller
 * surfaces the failure via a substitutionFailed flag and logs at ERROR level
 * upstream so MV recall regressions are visible in production. The caller
 * does NOT silently drop the candidate based on this signal — see
 * MvRewriteOutputValidator for the upstream observability contract.
 */
public final class MvColumnRefSubstitutor {

    private static final Logger LOG = LogManager.getLogger(MvColumnRefSubstitutor.class);

    private MvColumnRefSubstitutor() {}

    /**
     * Replace each {@link ColumnRefOperator} key in {@code expr} with its mapped
     * {@link ScalarOperator}, then bottom-up re-derive the resulting tree's types.
     */
    public static Optional<ScalarOperator> substitute(
            ScalarOperator expr,
            Map<ColumnRefOperator, ScalarOperator> columnMap) {
        ReplaceColumnRefRewriter replacer = new ReplaceColumnRefRewriter(columnMap);
        ScalarOperator afterReplace = replacer.rewrite(expr);
        try {
            return Optional.of(ScalarOperatorTypeReDeriver.reDerive(afterReplace));
        } catch (TypeReDeriveException e) {
            LOG.warn("MvColumnRefSubstitutor rejected expr after leaf substitution: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Same as {@link #substitute}, but additionally synchronizes the output
     * column ref's type and nullability to match the rewritten expression.
     * Used by sync/async MV rewriters when the substituted expression sits in a
     * projection or aggregation map whose output ColumnRef still claims the
     * pre-substitution type.
     */
    public static Optional<ScalarOperator> substituteAndSyncOutput(
            ColumnRefOperator outputRef,
            ScalarOperator expr,
            Map<ColumnRefOperator, ScalarOperator> columnMap) {
        Optional<ScalarOperator> out = substitute(expr, columnMap);
        out.ifPresent(s -> {
            outputRef.setType(s.getType());
            outputRef.setNullable(s.isNullable());
        });
        return out;
    }
}
