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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.Column;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The connector-agnostic normalization of an external table scan's conjuncts before a connector maps them onto its
 * own pruning:
 * <ul>
 *   <li>which conjuncts may be pushed to a native engine at all ({@link #splitStringPartitionCast});</li>
 *   <li>the plain {@code column op constant} predicates implied by function predicates ({@link FunctionPreimage}),
 *   per pruning level: file skipping ({@link #forFileSkipping}), partition pruning ({@link #forPartitionPruning})
 *   and BE row-group pruning ({@link #forBeConjuncts}).</li>
 * </ul>
 * Derived predicates are supersets of the conjuncts they came from and are used for pruning only, never in the
 * optimizer's predicate, so the selectivity of a conjunct is not counted twice. Deriving them never fails a query:
 * an unexpected error derives nothing.
 */
public final class ExternalScanPredicates {
    private static final Logger LOG = LogManager.getLogger(ExternalScanPredicates.class);

    private ExternalScanPredicates() {
    }

    /**
     * A connector with function-predicate pruning, and the session switch that enables it.
     */
    public enum Connector {
        ICEBERG(SessionVariable::isEnableIcebergFunctionPredicatePruning),
        DELTA_LAKE(SessionVariable::isEnableDeltaLakeFunctionPredicatePruning),
        HIVE(SessionVariable::isEnableHiveFunctionPredicatePruning);

        private final Predicate<SessionVariable> enabled;

        Connector(Predicate<SessionVariable> enabled) {
            this.enabled = enabled;
        }
    }

    /**
     * Whether function preimages are enabled for {@code connector} in the current session. Without a session (e.g. a
     * metadata-prefetch thread) neither the switch nor the session time zone is known, so nothing is derived.
     */
    public static boolean isFunctionPruningEnabled(Connector connector) {
        ConnectContext connectContext = ConnectContext.get();
        return connectContext != null && connector.enabled.test(connectContext.getSessionVariable());
    }

    /**
     * Temporal preimages on the scan's own columns ({@code colRefToColumn}), the columns the BE and a partition
     * pruner see.
     */
    public static FunctionPreimage.ColumnResolver scanTemporalColumns(Map<ColumnRefOperator, Column> colRefToColumn) {
        return (operand, kind) -> kind == FunctionPreimage.Kind.TEMPORAL
                && operand instanceof ColumnRefOperator ref && colRefToColumn.containsKey(ref);
    }

    /**
     * Predicates to push to a native engine next to {@code conjuncts}, to skip files on partition values and file
     * statistics. Disjunctions (the ranges of an IN list) are kept: they are not evaluated on rows.
     */
    public static List<ScalarOperator> forFileSkipping(Connector connector, List<ScalarOperator> conjuncts,
                                                       FunctionPreimage.ColumnResolver resolver) {
        if (conjuncts.isEmpty() || !isFunctionPruningEnabled(connector)) {
            return List.of();
        }
        try {
            return FunctionPreimage.deriveColumnPredicates(conjuncts, resolver);
        } catch (RuntimeException e) {
            LOG.debug("failed to derive {} file-skipping preimages", connector, e);
            return List.of();
        }
    }

    /**
     * Predicates to classify with the scan's conjuncts for partition pruning: those on partition columns prune
     * partitions, the others go to the BE. A disjunction is kept only on {@code partitionColumns} alone, so no OR is
     * evaluated on every row (see {@link FunctionPreimage#deriveRangePredicates}).
     */
    public static List<ScalarOperator> forPartitionPruning(Connector connector, List<ScalarOperator> conjuncts,
                                                           FunctionPreimage.ColumnResolver resolver,
                                                           Set<ColumnRefOperator> partitionColumns) {
        List<ScalarOperator> result = new ArrayList<>();
        for (ScalarOperator predicate : forFileSkipping(connector, conjuncts, resolver)) {
            if (!FunctionPreimage.isDisjunction(predicate)
                    || partitionColumns.containsAll(Utils.extractColumnRef(predicate))) {
                result.add(predicate);
            }
        }
        return result;
    }

    /**
     * Predicates to add to the BE scan conjuncts, so the BE prunes row groups (it only does so on a bare column);
     * without disjunctions, see {@link FunctionPreimage#deriveRangePredicates}.
     */
    public static List<ScalarOperator> forBeConjuncts(Connector connector, List<ScalarOperator> conjuncts,
                                                      FunctionPreimage.ColumnResolver resolver) {
        if (conjuncts.isEmpty() || !isFunctionPruningEnabled(connector)) {
            return List.of();
        }
        try {
            return FunctionPreimage.deriveRangePredicates(conjuncts, resolver);
        } catch (RuntimeException e) {
            LOG.debug("failed to derive {} scan preimages", connector, e);
            return List.of();
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // string partition columns compared with a temporal value
    // ------------------------------------------------------------------------------------------------------------

    /**
     * A conjunct "contains a string-to-temporal cast" if somewhere in it a string column is wrapped in a cast to
     * a temporal type, e.g. {@code CAST(<string col> AS DATETIME)}. Native pushdown unwraps such casts and then
     * compares in the STRING domain, which over-prunes, so these conjuncts must not be pushed.
     */
    public static boolean containsStringToTemporalCast(ScalarOperator operator) {
        if (operator instanceof CastOperator) {
            ScalarOperator child = operator.getChild(0);
            if (child instanceof ColumnRefOperator && child.getType().isStringType()
                    && (operator.getType().isDate() || operator.getType().isDatetime())) {
                return true;
            }
        }
        for (ScalarOperator child : operator.getChildren()) {
            if (containsStringToTemporalCast(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean allColumnsAreIdentityStringPartition(ScalarOperator conjunct,
                                                                Set<String> identityStringPartitionColumns) {
        List<ColumnRefOperator> refs = conjunct.getColumnRefs();
        if (refs.isEmpty()) {
            return false;
        }
        for (ColumnRefOperator ref : refs) {
            if (!identityStringPartitionColumns.contains(ref.getName().toLowerCase(Locale.ROOT))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Classifies conjuncts for a connector that pushes predicates to a native engine. When a query compares a STRING
     * partition column with a DATE/DATETIME value, coercion wraps the column in a cast, e.g.
     * {@code CAST(c AS DATETIME) = '2020-06-14 00:00:00'}; native pushdown would unwrap the cast and compare strings,
     * which never matches a {@code 'yyyy-MM-dd'} partition value and prunes every file.
     * <ul>
     *   <li>no string-to-temporal cast -> {@code pushable} (pushed to the native engine as usual);</li>
     *   <li>has such a cast and every referenced column is an identity string partition column -> {@code residual}
     *       (not pushed; evaluated against partition values, see
     *       {@link com.starrocks.connector.PartitionCastPredicatePruner});</li>
     *   <li>has such a cast but references a non-(identity string partition) column - e.g. a data column or a
     *       mixed OR - is dropped from both: not pushed (so the native engine cannot over-prune in the string
     *       domain) and not residual (it cannot be evaluated from partition values alone). Correctness is kept by
     *       the backend filter; only that conjunct's native pruning is given up.</li>
     * </ul>
     * {@code identityStringPartitionColumns} may be passed in any case; membership is case-insensitive.
     */
    public static PartitionResidual splitStringPartitionCast(List<ScalarOperator> conjuncts,
                                                             Set<String> identityStringPartitionColumns) {
        // Normalize to lower case here so callers need not pre-normalize the set.
        Set<String> normalizedPartitionColumns = new HashSet<>();
        for (String name : identityStringPartitionColumns) {
            normalizedPartitionColumns.add(name.toLowerCase(Locale.ROOT));
        }
        List<ScalarOperator> pushable = new ArrayList<>();
        List<ScalarOperator> residual = new ArrayList<>();
        for (ScalarOperator conjunct : conjuncts) {
            if (!containsStringToTemporalCast(conjunct)) {
                pushable.add(conjunct);
            } else if (allColumnsAreIdentityStringPartition(conjunct, normalizedPartitionColumns)) {
                residual.add(conjunct);
            }
            // else: contains a string-to-temporal cast that cannot be evaluated against partition values;
            // drop it from pushdown to avoid unsound native pruning (the backend filter still applies it).
        }
        return new PartitionResidual(pushable, residual);
    }

    /** Result of {@link #splitStringPartitionCast}. */
    public static class PartitionResidual {
        public final List<ScalarOperator> pushable;
        public final List<ScalarOperator> residual;

        public PartitionResidual(List<ScalarOperator> pushable, List<ScalarOperator> residual) {
            this.pushable = pushable;
            this.residual = residual;
        }

        public boolean hasResidual() {
            return !residual.isEmpty();
        }
    }
}
