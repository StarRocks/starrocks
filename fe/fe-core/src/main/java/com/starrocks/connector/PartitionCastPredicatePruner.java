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

package com.starrocks.connector;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * StarRocks-side residual partition pruning for STRING partition columns compared against a temporal value.
 *
 * <p>When a query compares a STRING partition column with a DATE/DATETIME value, binary-predicate coercion
 * wraps the column in a cast, e.g. {@code CAST(c AS DATETIME) = '2020-06-14 00:00:00'}. The backend evaluates
 * this in the DATETIME domain (parses the string then compares). Several connectors' native predicate
 * pushdown, however, only compare the column in its declared STRING type: they unwrap the cast and render the
 * temporal constant back to a string ({@code '2020-06-14 00:00:00'}), which never equals a {@code 'yyyy-MM-dd'}
 * partition value, so every data file is pruned and the query wrongly returns empty.
 *
 * <p>To keep pruning consistent with the backend filter, such conjuncts are kept out of the pushed predicate
 * and evaluated here against each file's partition values, reusing StarRocks' own {@code CAST(VARCHAR AS
 * DATETIME)} folding (identical parsing semantics to the backend). A file is dropped only when a residual
 * conjunct definitively folds to {@code false}; anything indeterminate (unfoldable, parse failure, non-boolean)
 * keeps the file, so the pruning can only ever be more conservative than the backend filter (never unsound).
 *
 * <p>The connector supplies the identity string partition column names and, per file, the raw string partition
 * values; the split/evaluation logic here is connector agnostic.
 */
public class PartitionCastPredicatePruner {
    private static final Logger LOG = LogManager.getLogger(PartitionCastPredicatePruner.class);

    private PartitionCastPredicatePruner() {
    }

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
     * Classifies conjuncts for a connector that pushes predicates to a native engine:
     * <ul>
     *   <li>no string-to-temporal cast -> {@code pushable} (pushed to the native engine as usual);</li>
     *   <li>has such a cast and every referenced column is an identity string partition column -> {@code residual}
     *       (not pushed; pruned here against partition values, consistent with the backend filter);</li>
     *   <li>has such a cast but references a non-(identity string partition) column - e.g. a data column or a
     *       mixed OR - is dropped from both: not pushed (so the native engine cannot over-prune in the string
     *       domain) and not residual (it cannot be evaluated from partition values alone). Correctness is kept by
     *       the backend filter; only that conjunct's native pruning is given up.</li>
     * </ul>
     * {@code identityStringPartitionColumns} may be passed in any case; membership is case-insensitive.
     */
    public static PartitionResidual split(List<ScalarOperator> conjuncts, Set<String> identityStringPartitionColumns) {
        // Normalize to lower case here so callers need not pre-normalize the set.
        Set<String> normalizedPartitionColumns = new HashSet<>();
        for (String name : identityStringPartitionColumns) {
            normalizedPartitionColumns.add(name.toLowerCase(Locale.ROOT));
        }
        List<ScalarOperator> pushable = Lists.newArrayList();
        List<ScalarOperator> residual = Lists.newArrayList();
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

    /**
     * Returns {@code true} if the partition (given as column name -> raw string value) may satisfy the residual
     * conjuncts, i.e. no conjunct definitively evaluates to {@code false}. Any conjunct that cannot be folded to
     * a boolean constant (parse failure, unbound reference, etc.) is treated as a possible match.
     *
     * @param partitionValues column name (any case) -> raw partition string value.
     */
    public static boolean partitionMayMatch(List<ScalarOperator> residualConjuncts,
                                            Map<String, String> partitionValues) {
        if (residualConjuncts.isEmpty()) {
            return true;
        }

        Map<String, String> lowerValues = new HashMap<>();
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            if (entry.getValue() != null) {
                lowerValues.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
            }
        }

        for (ScalarOperator conjunct : residualConjuncts) {
            Map<ColumnRefOperator, ScalarOperator> replaceMap = new HashMap<>();
            boolean allBound = true;
            for (ColumnRefOperator ref : conjunct.getColumnRefs()) {
                String value = lowerValues.get(ref.getName().toLowerCase(Locale.ROOT));
                if (value == null) {
                    allBound = false;
                    break;
                }
                replaceMap.put(ref, ConstantOperator.createVarchar(value));
            }
            if (!allBound) {
                // Cannot bind every reference to a partition value; be conservative and keep the file.
                continue;
            }

            Boolean result = tryFoldToBoolean(conjunct, replaceMap);
            if (result != null && !result) {
                return false;
            }
        }
        return true;
    }

    private static Boolean tryFoldToBoolean(ScalarOperator conjunct,
                                            Map<ColumnRefOperator, ScalarOperator> replaceMap) {
        try {
            ScalarOperator replaced = new ReplaceColumnRefRewriter(replaceMap).rewrite(conjunct);
            ScalarOperator folded = new ScalarOperatorRewriter().rewrite(
                    replaced, ScalarOperatorRewriter.FOLD_CONSTANT_RULES);
            if (folded instanceof ConstantOperator) {
                ConstantOperator constant = (ConstantOperator) folded;
                if (constant.isNull()) {
                    // NULL predicate = not true -> this partition value does not match.
                    return false;
                }
                if (constant.getType().isBoolean()) {
                    return constant.getBoolean();
                }
            }
        } catch (Exception e) {
            // Strict cast parse failure or any other folding issue: fall back to keeping the file.
            LOG.debug("residual partition predicate not foldable, keep file: {}", conjunct, e);
        }
        return null;
    }

    /**
     * Coarse manifest/partition-group level check for the residual conjuncts, given each referenced column's
     * temporal value range {@code [min, max]} (parsed from the group's partition summary). Returns {@code false}
     * only when some residual conjunct provably cannot be satisfied by ANY value within the ranges, so the group
     * can be pruned; returns {@code true} (keep) for anything it cannot decide. This never prunes a group that
     * might contain a match, so the resulting row-count estimate stays a safe over-estimate.
     *
     * @param partitionDateRanges column name (any case) -> [minDate, maxDate] for the group.
     */
    public static boolean rangeMayMatch(List<ScalarOperator> residualConjuncts,
                                        Map<String, LocalDateTime[]> partitionDateRanges) {
        Map<String, LocalDateTime[]> lowerRanges = new HashMap<>();
        for (Map.Entry<String, LocalDateTime[]> entry : partitionDateRanges.entrySet()) {
            lowerRanges.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
        }
        for (ScalarOperator conjunct : residualConjuncts) {
            if (!conjunctMayMatchRange(conjunct, lowerRanges)) {
                return false;
            }
        }
        return true;
    }

    // False only if the conjunct provably cannot be satisfied by any value within the ranges. Handles the simple
    // CAST(<string col>) <op> <temporal const> shape (column on the left); everything else keeps the group.
    private static boolean conjunctMayMatchRange(ScalarOperator conjunct, Map<String, LocalDateTime[]> lowerRanges) {
        if (!(conjunct instanceof BinaryPredicateOperator)) {
            return true;
        }
        BinaryPredicateOperator bp = (BinaryPredicateOperator) conjunct;
        String columnName = temporalCastColumnName(bp.getChild(0));
        ConstantOperator constant = asTemporalConstant(bp.getChild(1));
        if (columnName == null || constant == null) {
            return true;
        }
        LocalDateTime[] range = lowerRanges.get(columnName.toLowerCase(Locale.ROOT));
        if (range == null || range.length != 2 || range[0] == null || range[1] == null) {
            return true;
        }
        LocalDateTime lo = range[0];
        LocalDateTime hi = range[1];
        // The comparisons below assume an ordered range (lo <= hi). A caller may supply an inverted
        // range when min/max was computed in a different ordering domain than the parsed temporal
        // values (e.g. Iceberg's string-domain partition summary parsed back into dates for non
        // lexicographically-sortable date strings). Treat it as unknown and conservatively keep, so
        // pruning stays a safe over-estimate instead of silently under-counting.
        if (lo.isAfter(hi)) {
            return true;
        }
        LocalDateTime c = constant.getDatetime();
        // Does some value d in [lo, hi] satisfy (d <op> c)?
        switch (bp.getBinaryType()) {
            case EQ:
                return !c.isBefore(lo) && !c.isAfter(hi);
            case LT:
                return lo.isBefore(c);
            case LE:
                return !lo.isAfter(c);
            case GT:
                return hi.isAfter(c);
            case GE:
                return !hi.isBefore(c);
            default:
                return true;
        }
    }

    private static String temporalCastColumnName(ScalarOperator operator) {
        // Only DATETIME casts are range-pruned. CAST(col AS DATE) compares with day-truncation semantics,
        // but the manifest bounds/constant here are full LocalDateTimes; range-pruning such a cast could drop
        // a day-matching manifest (e.g. bound 2020-06-14T12:00 vs a midnight DATE constant), turning the safe
        // over-estimate into an under-estimate. So DATE casts are left unhandled here (conservatively kept).
        // string-vs-temporal binary predicates coerce to DATETIME, so a bare CAST(col AS DATE) only arises
        // from explicit user casts and is rare.
        if (operator instanceof CastOperator && operator.getType().isDatetime()) {
            ScalarOperator child = operator.getChild(0);
            if (child instanceof ColumnRefOperator && child.getType().isStringType()) {
                return ((ColumnRefOperator) child).getName();
            }
        }
        return null;
    }

    private static ConstantOperator asTemporalConstant(ScalarOperator operator) {
        if (operator instanceof ConstantOperator) {
            ConstantOperator constant = (ConstantOperator) operator;
            if (!constant.isNull() && (constant.getType().isDate() || constant.getType().isDatetime())) {
                return constant;
            }
        }
        return null;
    }

    /** Result of splitting conjuncts for partition pruning. */
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