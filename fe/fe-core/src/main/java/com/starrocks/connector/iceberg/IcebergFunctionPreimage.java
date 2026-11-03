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

package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.type.IntegerType;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.zone.ZoneOffsetTransition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Preimages of function predicates: maps a predicate on a function of a column to an Iceberg expression on the column
 * itself, so Iceberg prunes files through every partition transform of the column and through file column metrics.
 * <pre>
 *   year(c) = 2024                         -> c >= 2024-01-01 and c < 2025-01-01
 *   date_trunc('month', c) <= D            -> c < date_trunc('month', D) + 1 month
 *   date_format(c, '%Y-%m') = '2024-03'    -> c >= 2024-03-01 and c < 2024-04-01
 *   datediff(now(), c) <= 30               -> c >= to_date(now()) - 30 days
 *   hours_add(c, 2) < T                    -> c < T - 2 hours
 *   left(s, 3) = 'abc', starts_with(s, 'abc') -> s starts with 'abc'
 *   year(c) = 2024 and month(c) = 3        -> c >= 2024-03-01 and c < 2024-04-01
 * </pre>
 * Every handler maps to a superset of the matching values (all but the timestamptz DST overlap case are exact). The
 * result is only used for pruning: the original predicate is still evaluated on the rows. Callers must not use it
 * under NOT or for strict (exact) conversion.
 */
final class IcebergFunctionPreimage {
    private IcebergFunctionPreimage() {
    }

    /**
     * An operand resolved to an Iceberg column.
     */
    static final class Column {
        final String name;
        final Type type;

        Column(String name, Type type) {
            this.name = name;
            this.type = type;
        }

        boolean isTemporal() {
            return type.typeId() == Type.TypeID.DATE || type.typeId() == Type.TypeID.TIMESTAMP;
        }

        boolean isString() {
            return type.typeId() == Type.TypeID.STRING;
        }
    }

    /**
     * Resolves an operand to an Iceberg column of the table, or null.
     */
    interface ColumnResolver {
        Column resolve(ScalarOperator operand);
    }

    /**
     * The preimage of {@code f(column, constants...) op value} for one function family.
     */
    private interface ComparisonPreimage {
        Expression derive(CallOperator call, BinaryType op, ConstantOperator value, ColumnResolver resolver);
    }

    private static final Map<String, ComparisonPreimage> COMPARISON_PREIMAGES = buildComparisonPreimages();

    // the OR of a long IN list is a deep expression tree that Iceberg binds recursively; leave such lists unpruned
    private static final int MAX_IN_VALUES = 128;

    private static Map<String, ComparisonPreimage> buildComparisonPreimages() {
        ImmutableMap.Builder<String, ComparisonPreimage> builder = ImmutableMap.builder();
        for (String fn : List.of(FunctionSet.YEAR, FunctionSet.TO_DATE, FunctionSet.DATE, FunctionSet.DATE_TRUNC)) {
            builder.put(fn, TruncPreimage::derive);
        }
        builder.put(FunctionSet.DATE_FORMAT, DateFormatPreimage::derive);
        builder.put(FunctionSet.DATEDIFF, DateDiffPreimage::derive);
        for (String fn : ShiftPreimage.UNITS.keySet()) {
            builder.put(fn, ShiftPreimage::derive);
        }
        for (String fn : List.of(FunctionSet.LEFT, FunctionSet.SUBSTR, FunctionSet.SUBSTRING)) {
            builder.put(fn, PrefixPreimage::derive);
        }
        return builder.build();
    }

    /**
     * {@code call op value}; an IN list passes EQ and all its values.
     *
     * @return null when nothing safe can be derived.
     */
    static Expression deriveComparison(ScalarOperator operand, BinaryType op, List<ScalarOperator> values,
                                       ColumnResolver resolver) {
        if (values.isEmpty() || values.size() > MAX_IN_VALUES) {
            return null;
        }
        if (!(operand instanceof CallOperator call)) {
            return null;
        }
        ComparisonPreimage preimage = COMPARISON_PREIMAGES.get(call.getFnName().toLowerCase(Locale.ROOT));
        if (preimage == null) {
            return null;
        }
        Expression result = null;
        for (ScalarOperator value : values) {
            if (!(value instanceof ConstantOperator) || ((ConstantOperator) value).isNull()) {
                return null;
            }
            Expression expr;
            try {
                expr = preimage.derive(call, op, (ConstantOperator) value, resolver);
            } catch (RuntimeException e) {
                // an out-of-range boundary or an unparsable constant: leave the predicate unpruned
                return null;
            }
            if (expr == null) {
                // every value of an IN list must be covered, or the OR would prune matching files
                return null;
            }
            result = result == null ? expr : Expressions.or(result, expr);
        }
        return result;
    }

    /**
     * A boolean function used as a predicate: {@code starts_with(s, 'abc')}.
     */
    static Expression derivePredicateCall(CallOperator call, ColumnResolver resolver) {
        if (!FunctionSet.STARTS_WITH.equalsIgnoreCase(call.getFnName()) || call.getChildren().size() != 2) {
            return null;
        }
        Column column = resolver.resolve(call.getChild(0));
        String prefix = stringConstant(call.getChild(1));
        if (column == null || !column.isString() || prefix == null) {
            return null;
        }
        return Expressions.startsWith(column.name, prefix);
    }

    /**
     * Preimages that need several conjuncts together: {@code year(c) = Y and month(c) = M} (or {@code quarter(c)}).
     */
    static List<Expression> deriveConjuncts(List<ScalarOperator> conjuncts, ColumnResolver resolver) {
        return YearMonthPreimage.derive(conjuncts, resolver);
    }

    // ------------------------------------------------------------------------------------------------------------
    // year(c), to_date(c), date(c), date_trunc(unit, c)
    // ------------------------------------------------------------------------------------------------------------

    private static final class TruncPreimage {
        private static final Set<String> UNITS =
                Set.of("year", "quarter", "month", "week", "day", "hour", "minute", "second");

        static Expression derive(CallOperator call, BinaryType op, ConstantOperator value, ColumnResolver resolver) {
            String fn = call.getFnName().toLowerCase(Locale.ROOT);
            String unit;
            ScalarOperator operand;
            if (fn.equals(FunctionSet.DATE_TRUNC)) {
                String u = call.getChildren().size() == 2 ? stringConstant(call.getChild(0)) : null;
                if (u == null || !UNITS.contains(u.toLowerCase(Locale.ROOT))) {
                    return null;
                }
                unit = u.toLowerCase(Locale.ROOT);
                operand = call.getChild(1);
            } else if (call.getChildren().size() == 1) {
                unit = fn.equals(FunctionSet.YEAR) ? "year" : "day";
                operand = call.getChild(0);
            } else {
                return null;
            }
            Column column = resolver.resolve(unwrapDateCast(operand));
            if (column == null || !column.isTemporal()) {
                return null;
            }
            LocalDateTime floor;
            boolean aligned;
            if (fn.equals(FunctionSet.YEAR)) {
                // year(c) compares against an integer year; 1995.5 would need rounding by direction, so it is skipped
                Integer year = intConstant(value);
                if (year == null || year < 1 || year > 9998) {
                    return null;
                }
                floor = LocalDateTime.of(year, 1, 1, 0, 0);
                aligned = true;
            } else if (value.getType().isDate() || value.getType().isDatetime()) {
                LocalDateTime d = value.getDatetime();
                floor = truncate(unit, d);
                aligned = floor.equals(d);
            } else {
                return null;
            }
            return truncRange(column, op, floor, aligned, plusUnit(unit, floor));
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // date_format(c, '%Y-%m') and the other zero-padded, most-significant-first formats
    // ------------------------------------------------------------------------------------------------------------

    private static final class DateFormatPreimage {
        private static final Map<String, String[]> FORMATS = ImmutableMap.<String, String[]>builder()
                .put("%Y", new String[] {"year", "uuuu"})
                .put("%Y-%m", new String[] {"month", "uuuu-MM"})
                .put("%Y%m", new String[] {"month", "uuuuMM"})
                .put("%Y-%m-%d", new String[] {"day", "uuuu-MM-dd"})
                .put("%Y%m%d", new String[] {"day", "uuuuMMdd"})
                .put("%Y-%m-%d %H", new String[] {"hour", "uuuu-MM-dd HH"})
                .put("%Y%m%d%H", new String[] {"hour", "uuuuMMddHH"})
                .put("%Y-%m-%d %H:%i", new String[] {"minute", "uuuu-MM-dd HH:mm"})
                .put("%Y-%m-%d %H:%i:%s", new String[] {"second", "uuuu-MM-dd HH:mm:ss"})
                .build();

        static Expression derive(CallOperator call, BinaryType op, ConstantOperator value, ColumnResolver resolver) {
            if (call.getChildren().size() != 2) {
                return null;
            }
            String format = stringConstant(call.getChild(1));
            String[] spec = format == null ? null : FORMATS.get(format);
            String text = stringConstant(value);
            Column column = resolver.resolve(unwrapDateCast(call.getChild(0)));
            if (spec == null || text == null || column == null || !column.isTemporal()) {
                return null;
            }
            DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern(spec[1])
                    .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
                    .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                    .toFormatter(Locale.ROOT)
                    .withResolverStyle(ResolverStyle.STRICT);
            LocalDateTime floor = LocalDateTime.parse(text, formatter);
            // Only a canonical spelling sorts like the time it names: '2024-3' > '2024-12' as strings.
            if (floor.getYear() < 1 || floor.getYear() > 9998 || !formatter.format(floor).equals(text)) {
                return null;
            }
            return truncRange(column, op, floor, true, plusUnit(spec[0], floor));
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // datediff(c, K) / datediff(K, c): the difference of the date parts in days
    // ------------------------------------------------------------------------------------------------------------

    private static final class DateDiffPreimage {
        static Expression derive(CallOperator call, BinaryType op, ConstantOperator value, ColumnResolver resolver) {
            Integer days = intConstant(value);
            if (call.getChildren().size() != 2 || days == null) {
                return null;
            }
            boolean columnFirst = !call.getChild(0).isConstantRef();
            ScalarOperator operand = call.getChild(columnFirst ? 0 : 1);
            ScalarOperator other = call.getChild(columnFirst ? 1 : 0);
            Column column = resolver.resolve(unwrapDateCast(operand));
            if (column == null || !column.isTemporal() || !(other instanceof ConstantOperator)
                    || ((ConstantOperator) other).isNull()
                    || !(other.getType().isDate() || other.getType().isDatetime())) {
                return null;
            }
            LocalDateTime base = ((ConstantOperator) other).getDatetime().truncatedTo(ChronoUnit.DAYS);
            LocalDateTime day;
            if (columnFirst) {
                // day(c) - day(K) op n  <=>  day(c) op day(K) + n
                day = base.plusDays(days);
            } else {
                // day(K) - day(c) op n  <=>  day(c) flip(op) day(K) - n
                day = base.minusDays(days);
                op = flip(op);
                if (op == null) {
                    return null;
                }
            }
            return truncRange(column, op, day, true, day.plusDays(1));
        }

        private static BinaryType flip(BinaryType op) {
            switch (op) {
                case EQ:
                    return BinaryType.EQ;
                case GE:
                    return BinaryType.LE;
                case GT:
                    return BinaryType.LT;
                case LE:
                    return BinaryType.GE;
                case LT:
                    return BinaryType.GT;
                default:
                    return null;
            }
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // weeks/hours/minutes/seconds_add|sub(c, n): a shift is a bijection, c op K - n
    // (days/months/years and date_add/date_sub are already rewritten by ArithmeticCommutativeRule)
    // ------------------------------------------------------------------------------------------------------------

    private static final class ShiftPreimage {
        static final Map<String, ChronoUnit> UNITS = ImmutableMap.<String, ChronoUnit>builder()
                .put(FunctionSet.WEEKS_ADD, ChronoUnit.WEEKS)
                .put(FunctionSet.WEEKS_SUB, ChronoUnit.WEEKS)
                .put(FunctionSet.HOURS_ADD, ChronoUnit.HOURS)
                .put(FunctionSet.HOURS_SUB, ChronoUnit.HOURS)
                .put(FunctionSet.MINUTES_ADD, ChronoUnit.MINUTES)
                .put(FunctionSet.MINUTES_SUB, ChronoUnit.MINUTES)
                .put(FunctionSet.SECONDS_ADD, ChronoUnit.SECONDS)
                .put(FunctionSet.SECONDS_SUB, ChronoUnit.SECONDS)
                .build();

        static Expression derive(CallOperator call, BinaryType op, ConstantOperator value, ColumnResolver resolver) {
            String fn = call.getFnName().toLowerCase(Locale.ROOT);
            Integer amount = call.getChildren().size() == 2 ? intConstant(call.getChild(1)) : null;
            Column column = resolver.resolve(unwrapDateCast(call.getChild(0)));
            if (amount == null || column == null || !column.isTemporal()
                    || !(value.getType().isDate() || value.getType().isDatetime())) {
                return null;
            }
            boolean add = fn.endsWith("_add");
            if (!add && amount == Integer.MIN_VALUE) {
                // the BE negates the INT amount, which overflows: do not guess what it computes
                return null;
            }
            long shift = add ? amount : -(long) amount;
            LocalDateTime bound = value.getDatetime().minus(shift, UNITS.get(fn));
            return compare(column, op, bound);
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // left(s, n) = 'x', substr(s, 1, n) = 'x'
    // ------------------------------------------------------------------------------------------------------------

    private static final class PrefixPreimage {
        static Expression derive(CallOperator call, BinaryType op, ConstantOperator value, ColumnResolver resolver) {
            if (op != BinaryType.EQ) {
                return null;
            }
            Integer length;
            if (call.getFnName().equalsIgnoreCase(FunctionSet.LEFT)) {
                length = call.getChildren().size() == 2 ? intConstant(call.getChild(1)) : null;
            } else {
                Integer pos = call.getChildren().size() == 3 ? intConstant(call.getChild(1)) : null;
                length = pos != null && pos == 1 ? intConstant(call.getChild(2)) : null;
            }
            Column column = resolver.resolve(call.getChild(0));
            String text = stringConstant(value);
            if (length == null || length <= 0 || column == null || !column.isString() || text == null) {
                return null;
            }
            int chars = text.codePointCount(0, text.length());
            if (chars == length) {
                return Expressions.startsWith(column.name, text);
            } else if (chars < length) {
                // a shorter result means the whole string is shorter than n
                return Expressions.equal(column.name, text);
            }
            return null;
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // year(c) = Y and month(c) in (M...) / quarter(c) in (Q...)
    // ------------------------------------------------------------------------------------------------------------

    private static final class YearMonthPreimage {
        static List<Expression> derive(List<ScalarOperator> conjuncts, ColumnResolver resolver) {
            Map<String, Integer> years = new HashMap<>();
            Map<String, Column> columns = new HashMap<>();
            Map<String, List<int[]>> parts = new HashMap<>();
            for (ScalarOperator conjunct : conjuncts) {
                CallOperator call;
                List<ScalarOperator> values;
                if (conjunct instanceof BinaryPredicateOperator
                        && ((BinaryPredicateOperator) conjunct).getBinaryType() == BinaryType.EQ
                        && conjunct.getChild(0) instanceof CallOperator) {
                    call = (CallOperator) conjunct.getChild(0);
                    values = List.of(conjunct.getChild(1));
                } else if (conjunct instanceof InPredicateOperator && !((InPredicateOperator) conjunct).isNotIn()
                        && conjunct.getChild(0) instanceof CallOperator) {
                    call = (CallOperator) conjunct.getChild(0);
                    values = ((InPredicateOperator) conjunct).getListChildren();
                } else {
                    continue;
                }
                String fn = call.getFnName().toLowerCase(Locale.ROOT);
                int months = fn.equals(FunctionSet.MONTH) ? 1 : fn.equals(FunctionSet.QUARTER) ? 3 : 0;
                if ((months == 0 && !fn.equals(FunctionSet.YEAR)) || call.getChildren().size() != 1) {
                    continue;
                }
                Column column = resolver.resolve(unwrapDateCast(call.getChild(0)));
                if (column == null || !column.isTemporal()) {
                    continue;
                }
                if (values.size() > MAX_IN_VALUES) {
                    continue;
                }
                List<Integer> ints = new ArrayList<>();
                for (ScalarOperator v : values) {
                    Integer i = intConstant(v);
                    if (i == null) {
                        ints = null;
                        break;
                    }
                    ints.add(i);
                }
                if (ints == null) {
                    continue;
                }
                columns.put(column.name, column);
                if (months == 0) {
                    if (ints.size() == 1) {
                        years.put(column.name, ints.get(0));
                    }
                } else {
                    // [first month, months spanned] per value; an out-of-range value matches nothing, so skip it
                    List<int[]> ranges = new ArrayList<>();
                    for (int i : ints) {
                        if (i >= 1 && i <= 12 / months) {
                            ranges.add(new int[] {(i - 1) * months + 1, months});
                        }
                    }
                    if (ranges.size() == ints.size() && !parts.containsKey(column.name)) {
                        parts.put(column.name, ranges);
                    }
                }
            }
            List<Expression> result = new ArrayList<>();
            for (Map.Entry<String, List<int[]>> entry : parts.entrySet()) {
                Integer year = years.get(entry.getKey());
                if (year == null || year < 1 || year > 9998) {
                    continue;
                }
                Column column = columns.get(entry.getKey());
                Expression expr = null;
                for (int[] range : entry.getValue()) {
                    LocalDateTime floor = LocalDateTime.of(year, range[0], 1, 0, 0);
                    Expression one = truncRange(column, BinaryType.EQ, floor, true, floor.plusMonths(range[1]));
                    expr = expr == null ? one : Expressions.or(expr, one);
                }
                result.add(expr);
            }
            return result;
        }
    }

    // ------------------------------------------------------------------------------------------------------------
    // shared helpers
    // ------------------------------------------------------------------------------------------------------------

    /**
     * cast(date as datetime) is exact and order preserving, so the range can be put on the DATE column.
     */
    private static ScalarOperator unwrapDateCast(ScalarOperator operand) {
        if (operand instanceof CastOperator && operand.getType().isDatetime()
                && operand.getChild(0).getType().isDate()) {
            return operand.getChild(0);
        }
        return operand;
    }

    private static String stringConstant(ScalarOperator operator) {
        if (!(operator instanceof ConstantOperator) || ((ConstantOperator) operator).isNull()
                || !operator.getType().isStringType()) {
            return null;
        }
        return ((ConstantOperator) operator).getVarchar();
    }

    private static Integer intConstant(ScalarOperator operator) {
        if (!(operator instanceof ConstantOperator) || ((ConstantOperator) operator).isNull()
                || !operator.getType().isIntegerType()) {
            return null;
        }
        Optional<ConstantOperator> i = ((ConstantOperator) operator).castTo(IntegerType.INT);
        return i.isEmpty() || i.get().isNull() ? null : i.get().getInt();
    }

    private static LocalDateTime truncate(String unit, LocalDateTime d) {
        // reuse the FE constant-folding date_trunc so the boundaries agree with what the BE computes
        return ScalarOperatorFunctions.dateTrunc(ConstantOperator.createVarchar(unit),
                ConstantOperator.createDatetime(d)).getDatetime();
    }

    private static LocalDateTime plusUnit(String unit, LocalDateTime floor) {
        switch (unit) {
            case "year":
                return floor.plusYears(1);
            case "quarter":
                return floor.plusMonths(3);
            case "month":
                return floor.plusMonths(1);
            case "week":
                return floor.plusWeeks(1);
            case "day":
                return floor.plusDays(1);
            case "hour":
                return floor.plusHours(1);
            case "minute":
                return floor.plusMinutes(1);
            case "second":
                return floor.plusSeconds(1);
            default:
                throw new IllegalArgumentException(unit);
        }
    }

    /**
     * {@code f(c) op K} for a non-decreasing step function f whose step containing K is [floor, next).
     */
    private static Expression truncRange(Column column, BinaryType op, LocalDateTime floor, boolean aligned,
                                         LocalDateTime next) {
        if (next.getYear() > 9999) {
            return null;
        }
        switch (op) {
            case EQ:
                // a non-aligned constant never equals f(c); leave it unpruned rather than proving it empty
                return aligned ? Expressions.and(compare(column, BinaryType.GE, floor),
                        compare(column, BinaryType.LT, next)) : null;
            case GE:
                return compare(column, BinaryType.GE, aligned ? floor : next);
            case GT:
                return compare(column, BinaryType.GE, next);
            case LT:
                return compare(column, BinaryType.LT, aligned ? floor : next);
            case LE:
                return compare(column, BinaryType.LT, next);
            default:
                return null;
        }
    }

    /**
     * {@code c op bound} on a DATE or TIMESTAMP column; a superset when the column cannot hold {@code bound} exactly.
     */
    private static Expression compare(Column column, BinaryType op, LocalDateTime bound) {
        if (bound.getYear() < 1 || bound.getYear() > 9999) {
            return null;
        }
        String name = column.name;
        if (column.type.typeId() == Type.TypeID.DATE) {
            LocalDateTime day = bound.truncatedTo(ChronoUnit.DAYS);
            boolean midnight = day.equals(bound);
            long floorDay = day.toLocalDate().toEpochDay();
            long ceilDay = midnight ? floorDay : floorDay + 1;
            switch (op) {
                case EQ:
                    return midnight ? Expressions.equal(name, floorDay) : null;
                case GE:
                    return Expressions.greaterThanOrEqual(name, ceilDay);
                case GT:
                    return Expressions.greaterThanOrEqual(name, floorDay + 1);
                case LT:
                    return Expressions.lessThan(name, ceilDay);
                case LE:
                    return Expressions.lessThan(name, floorDay + 1);
                default:
                    return null;
            }
        }
        // A timestamptz is compared as a local time in the session time zone. At a DST overlap a local time names two
        // instants: lower bounds take the earlier one and upper bounds the later one, covering both occurrences.
        // A local time in a DST gap names no instant: every local time after it starts at the transition instant.
        boolean inGap = isInGap(column.type, bound);
        long earlier = toMicros(column.type, bound, true);
        long later = toMicros(column.type, bound, false);
        switch (op) {
            case EQ:
                return earlier == later ? Expressions.equal(name, earlier)
                        : Expressions.and(Expressions.greaterThanOrEqual(name, earlier),
                        Expressions.lessThanOrEqual(name, later));
            case GE:
                return Expressions.greaterThanOrEqual(name, earlier);
            case GT:
                // c > a gap time holds from the transition instant itself
                return inGap ? Expressions.greaterThanOrEqual(name, earlier) : Expressions.greaterThan(name, earlier);
            case LT:
                return Expressions.lessThan(name, later);
            case LE:
                return Expressions.lessThanOrEqual(name, later);
            default:
                return null;
        }
    }

    private static boolean isInGap(Type icebergType, LocalDateTime bound) {
        return Types.TimestampType.withZone().equals(icebergType)
                && TimeUtils.getTimeZone().toZoneId().getRules().getValidOffsets(bound).isEmpty();
    }

    private static long toMicros(Type icebergType, LocalDateTime bound, boolean earlier) {
        ZonedDateTime zoned;
        if (Types.TimestampType.withZone().equals(icebergType)) {
            ZoneId zone = TimeUtils.getTimeZone().toZoneId();
            ZoneOffsetTransition gap = zone.getRules().getTransition(bound);
            if (gap != null && gap.isGap()) {
                // atZone() would move the time forward by the gap length, past local times that follow the gap
                return toMicros(gap.getInstant());
            }
            zoned = bound.atZone(zone);
            zoned = earlier ? zoned.withEarlierOffsetAtOverlap() : zoned.withLaterOffsetAtOverlap();
        } else {
            zoned = bound.atZone(ZoneOffset.UTC);
        }
        return toMicros(zoned.toInstant());
    }

    // ChronoUnit.MICROS.between counts in nanoseconds and overflows before 1677 and after 2262
    private static long toMicros(Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1000);
    }
}
