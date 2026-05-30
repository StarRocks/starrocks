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
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.type.Type;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Normalizes comparisons between a STRING date column (canonical 'yyyy-MM-dd' values, common for
 * lake/partition columns such as datestr / datestr_ist) and a temporal constant into a pure
 * string-domain comparison, so that connector predicate push-down and partition pruning work
 * consistently.
 *
 * <p>Background: {@code current_date()} folds to a DATE constant, while {@code current_date() - interval
 * '1' day} folds to a DATETIME constant (the analyzer casts the interval-arithmetic operand to
 * DATETIME). When such a constant is compared with a string column, the optimizer either wraps the
 * column in {@code CAST(col AS DATETIME)} (equality path) or renders the constant to a string via the
 * IN/BETWEEN common-type path. A DATETIME constant always renders with a time component
 * ('2026-05-30 00:00:00'), which never string-matches a stored 'yyyy-MM-dd' value, producing empty
 * results and inconsistent behaviour between '=', 'IN', and ranges.
 *
 * <p>This rule runs after constant folding and rewrites, only when the constant time-of-day is
 * midnight (always true for current_date()/current_date()-interval):
 * <pre>
 *   CAST(strcol AS DATE|DATETIME) op '2026-05-30 00:00:00'  -> strcol op '2026-05-30'
 *   strcol op '2026-05-30 00:00:00'                         -> strcol op '2026-05-30'
 *   strcol IN ('2026-05-30', '2026-05-18 00:00:00')         -> strcol IN ('2026-05-30', '2026-05-18')
 * </pre>
 *
 * <p>Soundness: relies on the string column storing canonical zero-padded 'yyyy-MM-dd' values, for
 * which lexicographic order equals chronological order. Gated by the session variable
 * {@code enable_rewrite_string_date_predicate}; disable it for string columns that store
 * non-canonical date formats or genuine datetime strings.
 */
public class SimplifiedStringDatePredicateRule extends BottomUpScalarOperatorRewriteRule {
    // 'yyyy-MM-dd HH:mm:ss' with midnight time, optional fractional seconds (also all-zero).
    private static final Pattern MIDNIGHT_DATETIME_STRING =
            Pattern.compile("(\\d{4}-\\d{2}-\\d{2}) 00:00:00(?:\\.0+)?");
    // 'yyyy-MM-dd'
    private static final Pattern DATE_STRING = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

    private static boolean enabled() {
        return ConnectContext.get() == null
                || ConnectContext.get().getSessionVariable().isEnableRewriteStringDatePredicate();
    }

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        if (!enabled()) {
            return predicate;
        }
        BinaryType type = predicate.getBinaryType();
        // Only equality and range comparisons; for canonical 'yyyy-MM-dd' strings lexicographic order
        // matches chronological order, so ranges remain correct.
        if (!(type.isEqual() || type == BinaryType.NE || type.isRange())) {
            return predicate;
        }

        ScalarOperator column = extractStringDateColumn(predicate.getChild(0));
        if (column == null) {
            return predicate;
        }
        Optional<ConstantOperator> dateString = toDateOnlyString(predicate.getChild(1));
        if (dateString.isEmpty()) {
            return predicate;
        }
        // Idempotency: if neither side changed, return the original so the rewriter reaches a fixpoint.
        // toDateOnlyString() returns the same constant instance when it is already a canonical date string.
        if (column == predicate.getChild(0) && dateString.get() == predicate.getChild(1)) {
            return predicate;
        }
        return new BinaryPredicateOperator(type, column, dateString.get());
    }

    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, ScalarOperatorRewriteContext context) {
        if (!enabled() || predicate.isSubquery()) {
            return predicate;
        }
        ScalarOperator column = extractStringDateColumn(predicate.getChild(0));
        if (column == null) {
            return predicate;
        }

        List<ScalarOperator> newChildren = Lists.newArrayList();
        newChildren.add(column);
        boolean changed = column != predicate.getChild(0);
        for (ScalarOperator value : predicate.getListChildren()) {
            Optional<ConstantOperator> dateString = toDateOnlyString(value);
            if (dateString.isEmpty()) {
                return predicate;
            }
            changed |= dateString.get() != value;
            newChildren.add(dateString.get());
        }
        // Idempotency: only rebuild when something actually changed, so the rewriter reaches a fixpoint.
        if (!changed) {
            return predicate;
        }
        return new InPredicateOperator(predicate.isNotIn(), newChildren);
    }

    // Returns the bare string column if the operator is a string column, or CAST(stringColumn AS DATE|DATETIME).
    private static ScalarOperator extractStringDateColumn(ScalarOperator op) {
        if (op.isColumnRef() && op.getType().isStringType()) {
            return op;
        }
        if (op instanceof CastOperator
                && (op.getType().isDate() || op.getType().isDatetime())) {
            ScalarOperator child = op.getChild(0);
            if (child.isColumnRef() && child.getType().isStringType()) {
                return child;
            }
        }
        return null;
    }

    // Renders a temporal/date-string constant to a canonical 'yyyy-MM-dd' varchar constant, but only
    // when it has no (non-midnight) time component. Returns empty otherwise.
    private static Optional<ConstantOperator> toDateOnlyString(ScalarOperator op) {
        if (!op.isConstantRef()) {
            return Optional.empty();
        }
        ConstantOperator constant = (ConstantOperator) op;
        if (constant.isNull()) {
            return Optional.empty();
        }
        Type type = constant.getType();
        if (type.isDate()) {
            return Optional.of(ConstantOperator.createVarchar(toDateString(constant.getDate())));
        }
        if (type.isDatetime()) {
            LocalDateTime dt = constant.getDatetime();
            if (!dt.toLocalTime().equals(LocalTime.MIDNIGHT)) {
                return Optional.empty();
            }
            return Optional.of(ConstantOperator.createVarchar(toDateString(dt)));
        }
        if (type.isStringType()) {
            String value = constant.getVarchar();
            if (DATE_STRING.matcher(value).matches()) {
                return Optional.of(constant);
            }
            java.util.regex.Matcher m = MIDNIGHT_DATETIME_STRING.matcher(value);
            if (m.matches()) {
                return Optional.of(ConstantOperator.createVarchar(m.group(1)));
            }
        }
        return Optional.empty();
    }

    private static String toDateString(LocalDateTime dateTime) {
        return dateTime.toLocalDate().toString(); // ISO-8601 -> yyyy-MM-dd
    }
}
