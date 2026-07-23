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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LargeInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.rule.tree.VariantPathRewriteRule;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;

/**
 * Expressions currently supported in Iceberg, maps to StarRocks:
 * <p>
 * Supported predicate expressions are:
 * isNull ->IsNullPredicateOperator(..., false)
 * notNull ->IsNullPredicateOperator(..., true)
 * equal -> BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, ..., ...)
 * notEqual -> BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.NE, ..., ...)
 * lessThan -> BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LT, ..., ...)
 * lessThanOrEqual -> BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.LE, ..., ...)
 * greaterThan -> BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT, ..., ...)
 * greaterThanOrEqual -> BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GE, ..., ...)
 * in -> InPredicateOperator(..., ..., false)
 * notIn -> InPredicateOperator(..., ..., true)
 * startWith -> LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, ..., "prefix%")
 * <p>
 * Supported expression operations are:
 * and -> CompoundPredicateOperator(and, ..., ...)
 * or -> CompoundPredicateOperator(or, ..., ...)
 * not -> CompoundPredicateOperator(not, ...)
 * <p>
 */

public class ScalarOperatorToIcebergExpr {
    private static final Logger LOG = LogManager.getLogger(ScalarOperatorToIcebergExpr.class);

    public Expression convert(List<ScalarOperator> operators, IcebergContext context) {
        return convert(operators, context, false);
    }

    public Expression convertStrict(List<ScalarOperator> operators, IcebergContext context) {
        return convert(operators, context, true);
    }

    public Expression convert(List<ScalarOperator> operators, IcebergContext context, boolean strict) {
        IcebergExprVisitor visitor = new IcebergExprVisitor();
        IcebergContext effectiveContext = strict && !context.isStrict()
                ? new IcebergContext(context.getSchema(), context.isInsideNot(), true)
                : context;
        List<Expression> expressions = Lists.newArrayList();
        for (ScalarOperator operator : operators) {
            Expression filterExpr = operator.accept(visitor, effectiveContext);
            if (filterExpr == null) {
                if (strict) {
                    LOG.debug("Strict mode: cannot convert operator {}", operator.debugString());
                    return null;
                }
                continue;
            }

            try {
                Binder.bind(context.getSchema(), filterExpr, false);
                expressions.add(filterExpr);
            } catch (ValidationException e) {
                if (strict) {
                    LOG.debug("Strict mode: bind failed, {}", operator.debugString());
                    return null;
                }
                LOG.error("binding to the table schema failed, cannot be pushed down scanOperator: {}",
                        operator.debugString());
            }
        }

        LOG.debug("Number of predicates pushed down / Total number of predicates: {}/{}",
                expressions.size(), operators.size());
        return expressions.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
    }

    public static class IcebergContext {
        private final Types.StructType schema;
        private final boolean insideNot;
        private final boolean strict;

        public IcebergContext(Types.StructType schema) {
            this(schema, false, false);
        }

        public IcebergContext(Types.StructType schema, boolean insideNot, boolean strict) {
            this.schema = schema;
            this.insideNot = insideNot;
            this.strict = strict;
        }

        public Types.StructType getSchema() {
            return schema;
        }

        public boolean isInsideNot() {
            return insideNot;
        }

        public boolean isStrict() {
            return strict;
        }

        public IcebergContext withInsideNot() {
            return new IcebergContext(schema, true, strict);
        }
    }

    private static class IcebergExprVisitor extends ScalarOperatorVisitor<Expression, IcebergContext> {

        private static Type getResultType(String columnName, IcebergContext context) {
            Preconditions.checkNotNull(context);
            return getColumnType(columnName, context);
        }

        private static Type getColumnType(String qualifiedName, IcebergContext context) {
            Types.StructType structType = context.getSchema().asStructType();
            Type type = structType.fieldType(qualifiedName);
            if (null != type) {
                return type;
            }
            if (qualifiedName.contains(".")) {
                type = context.getSchema();
                String[] paths = qualifiedName.split("\\.");
                for (String path : paths) {
                    type = type.asStructType().fieldType(path);
                }
            }
            if (qualifiedName.equals(IcebergTable.ROW_ID)
                    || qualifiedName.equals(IcebergTable.LAST_UPDATED_SEQUENCE_NUMBER)) {
                type = new Types.LongType();
            }
            return type;
        }

        @Override
        public Expression visitCompoundPredicate(CompoundPredicateOperator operator, IcebergContext context) {
            CompoundPredicateOperator.CompoundType op = operator.getCompoundType();
            if (op == CompoundPredicateOperator.CompoundType.NOT) {
                if (operator.getChild(0) instanceof LikePredicateOperator) {
                    return null;
                }
                Expression expression = operator.getChild(0).accept(this, context.withInsideNot());

                if (expression != null) {
                    return not(expression);
                }
            } else {
                Expression left = operator.getChild(0).accept(this, context);
                Expression right = operator.getChild(1).accept(this, context);
                if (left != null && right != null) {
                    return (op == CompoundPredicateOperator.CompoundType.OR) ? or(left, right) : and(left, right);
                }
                // For AND predicates outside of NOT, allow partial pushdown.
                // If only one side converts successfully, push down that side alone.
                // This is safe because AND(a, b) is more restrictive than just a (or just b),
                // so pushing down one side still correctly filters data.
                // This must NOT be done inside NOT, because NOT(AND(a, b)) = OR(NOT(a), NOT(b)),
                // and pushing down NOT(a) alone would over-filter.
                if (!context.isStrict() && !context.isInsideNot()
                        && op == CompoundPredicateOperator.CompoundType.AND) {
                    if (left != null) {
                        return left;
                    }
                    if (right != null) {
                        return right;
                    }
                }
            }
            return null;
        }

        @Override
        public Expression visitIsNullPredicate(IsNullPredicateOperator operator, IcebergContext context) {
            String columnName = getColumnName(operator.getChild(0));
            if (columnName == null) {
                return null;
            }
            if (operator.isNotNull()) {
                return notNull(columnName);
            } else {
                return isNull(columnName);
            }
        }

        @Override
        public Expression visitBinaryPredicate(BinaryPredicateOperator operator, IcebergContext context) {
            String columnName = getColumnName(operator.getChild(0));
            if (columnName == null) {
                return null;
            }

            Type icebergType = getResultType(columnName, context);
            Type.TypeID typeID = icebergType.typeId();
            Object literalValue = getLiteralValue(operator.getChild(1), icebergType);

            if (literalValue == null) {
                return null;
            }
            if (typeID == Type.TypeID.BOOLEAN) {
                literalValue = convertBoolLiteralValue(literalValue);
            }
            switch (operator.getBinaryType()) {
                case LT:
                    return lessThan(columnName, literalValue);
                case LE:
                    return lessThanOrEqual(columnName, literalValue);
                case GT:
                    return greaterThan(columnName, literalValue);
                case GE:
                    return greaterThanOrEqual(columnName, literalValue);
                case EQ:
                    return equal(columnName, literalValue);
                case NE:
                    return notEqual(columnName, literalValue);
                default:
                    return null;
            }
        }

        @Override
        public Expression visitLargeInPredicate(LargeInPredicateOperator operator, IcebergContext context) {
            throw new UnsupportedOperationException("not support large in predicate in the ScalarOperatorToIcebergExpr");
        }

        @Override
        public Expression visitInPredicate(InPredicateOperator operator, IcebergContext context) {
            String columnName = getColumnName(operator.getChild(0));
            if (columnName == null) {
                return null;
            }

            List<Object> literalValues = operator.getListChildren().stream()
                    .map(childOperator -> {
                        Type icebergType = getResultType(columnName, context);
                        Type.TypeID typeID = icebergType.typeId();
                        Object literalValue = ScalarOperatorToIcebergExpr.getLiteralValue(childOperator, icebergType);
                        if (typeID == Type.TypeID.BOOLEAN) {
                            literalValue = convertBoolLiteralValue(literalValue);
                        }
                        return literalValue;
                    }).collect(Collectors.toList());

            // It should not be pushed down if there is an implicit cast
            // TODO: Some functions within ScalarOperatorFunctions could be computed on frontends.
            // Maybe we can obtain the result first and then convert it into an Iceberg expression.
            if (literalValues.stream().anyMatch(Objects::isNull)) {
                return null;
            }

            if (operator.isNotIn()) {
                return notIn(columnName, literalValues);
            } else {
                return in(columnName, literalValues);
            }
        }

        @Override
        public Expression visitLikePredicateOperator(LikePredicateOperator operator, IcebergContext context) {
            String columnName = getColumnName(operator.getChild(0));
            if (columnName == null) {
                return null;
            }

            if (operator.getLikeType() == LikePredicateOperator.LikeType.LIKE) {
                if (operator.getChild(1).getType().isStringType()) {
                    String literal = (String) getLiteralValue(operator.getChild(1), getResultType(columnName, context));
                    if (literal == null) {
                        return null;
                    }
                    if (literal.indexOf("%") == literal.length() - 1) {
                        return startsWith(columnName, literal.substring(0, literal.length() - 1));
                    }
                }
            }
            return null;
        }

        @Override
        public Expression visit(ScalarOperator scalarOperator, IcebergContext context) {
            return null;
        }
    }

    private static Object getLiteralValue(ScalarOperator operator, Type icebergType) {
        if (operator == null) {
            return null;
        }

        return operator.accept(new ExtractLiteralValue(), icebergType);
    }

    private static Object convertBoolLiteralValue(Object literalValue) {
        try {
            return new BoolLiteral(String.valueOf(literalValue)).getValue();
        } catch (Exception e) {
            throw new StarRocksConnectorException("Failed to convert %s to boolean type", literalValue);
        }
    }

    private static class ExtractLiteralValue extends ScalarOperatorVisitor<Object, Type> {
        private boolean needCast(PrimitiveType sourceType, Type.TypeID dstTypeID) {
            switch (sourceType) {
                case BOOLEAN:
                    return dstTypeID != Type.TypeID.BOOLEAN;
                case TINYINT:
                case SMALLINT:
                case INT:
                    return dstTypeID != Type.TypeID.INTEGER;
                case BIGINT:
                    return dstTypeID != Type.TypeID.LONG;
                case FLOAT:
                    return dstTypeID != Type.TypeID.FLOAT;
                case DOUBLE:
                    return dstTypeID != Type.TypeID.DOUBLE;
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    return dstTypeID != Type.TypeID.DECIMAL;
                case HLL:
                case VARCHAR:
                case CHAR:
                    return dstTypeID != Type.TypeID.STRING;
                case DATE:
                    return dstTypeID != Type.TypeID.DATE;
                case DATETIME:
                    return dstTypeID != Type.TypeID.TIMESTAMP;
                case VARBINARY:
                case BINARY:
                    return dstTypeID != Type.TypeID.BINARY;
                default:
                    return true;
            }
        }

        private ConstantOperator tryCastToResultType(ConstantOperator operator, Type.TypeID resultTypeID) {

            Optional<ConstantOperator> res = Optional.empty();
            switch (resultTypeID) {
                case BOOLEAN:
                    res = operator.castTo(BooleanType.BOOLEAN);
                    break;
                case DATE:
                    // Reject literal with non-zero time-of-day when the iceberg column is DATE.
                    // After visitCastOperator peels the column-side cast, two literal shapes
                    // can reach here: DATETIME literal (analyzer promoted DATE vs DATETIME to
                    // a common DATETIME type) or STRING literal (user wrote an explicit
                    // CAST(date_col AS STRING) that the analyzer kept intact).
                    // For both, ConstantOperator.castTo(DATE) silently truncates the
                    // time-of-day, so a predicate like
                    //     WHERE date_col >= TIMESTAMP '2024-01-01 12:00:00'
                    // would be pushed down as `date_col >= DATE '2024-01-01'` and return
                    // wrong rows. A pure-date / 00:00:00 literal is lossless and falls
                    // through to castTo(DATE).
                    if (operator.getType().isDatetime()) {
                        LocalDateTime dt = operator.getDatetime();
                        if (dt.getHour() != 0 || dt.getMinute() != 0
                                || dt.getSecond() != 0 || dt.getNano() != 0) {
                            return null;
                        }
                    } else if (operator.getType().getPrimitiveType().isStringType()) {
                        // Mirror ConstantOperator.castTo(DATE/DATETIME)'s parsing: strip the same
                        // 4 ASCII whitespace chars and call DateUtils.parseStrictDateTime, so the
                        // guard sees exactly the same LocalDateTime that castTo would see.
                        try {
                            String dateStr = StringUtils.strip(operator.getVarchar(), "\r\n\t ");
                            LocalDateTime dt = DateUtils.parseStrictDateTime(dateStr);
                            if (dt.getHour() != 0 || dt.getMinute() != 0
                                    || dt.getSecond() != 0 || dt.getNano() != 0) {
                                return null;
                            }
                        } catch (Exception ignored) {
                            // Unparseable string (e.g. '2024/01/01'): we cannot prove the
                            // time-of-day is zero, so conservatively refuse pushdown.
                            return null;
                        }
                    }
                    res = operator.castTo(DateType.DATE);
                    break;
                case TIMESTAMP:
                    // Reject DATE literal when the iceberg column is DATETIME.
                    // The DATE -> DATETIME literal cast itself is lossless (fills 00:00:00),
                    // but it is unsafe AFTER visitCastOperator unconditionally peels the outer
                    // cast on the column side. Concretely, consider:
                    //     WHERE CAST(datetime_col AS DATE) = DATE '2024-01-01'
                    // The original semantics is "truncate the column to day, then compare", so a
                    // row with datetime_col = '2024-01-01 12:00:00' MUST match. After peeling the
                    // column side becomes plain `datetime_col` (DATETIME) and the literal is the
                    // DATE '2024-01-01'; lifting the literal to DATETIME 00:00:00 would push down
                    //     datetime_col = TIMESTAMP '2024-01-01 00:00:00'
                    // which evaluates to FALSE on that row -> we silently drop matching rows.
                    // From the literal-side view, this dangerous case and the legitimate
                    //     WHERE datetime_col = DATE '2024-01-01'
                    // share the exact same (literal=DATE, columnType=TIMESTAMP) input, so we
                    // cannot tell them apart and must conservatively refuse pushdown for both.
                    // Non-DATE literals (e.g. STRING) fall through to castTo(DATETIME) as usual.
                    if (operator.getType().isDate()) {
                        return null;
                    }
                    res = operator.castTo(DateType.DATETIME);
                    break;
                case STRING:
                case UUID:
                    // Reject non-character literal when the iceberg column is STRING/UUID.
                    // After visitCastOperator peels the outer cast on the column side, the
                    // remaining comparison is `string_col <op> castTo(VARCHAR, literal)`, which
                    // compares by lexicographic order while the original predicate compared by
                    // the literal's native order. The two orders disagree for every non-string
                    // literal type, so pushdown may return a wrong row set:
                    //
                    // 1) NUMERIC: WHERE CAST(string_col AS INT) > 10
                    //    Pushed down as `string_col > '10'`. A row with string_col = '9' is
                    //    wrongly returned because '9' > '10' under string ordering, even though
                    //    9 < 10 numerically.
                    //
                    // 2) DATE: WHERE CAST(string_col AS DATE) = DATE '2024-01-01'
                    //    Pushed down as `string_col = '2024-01-01'`. A row with
                    //    string_col = '2024-1-1' is silently dropped, even though it parses to
                    //    the same DATE value as the literal.
                    //
                    // 3) DATETIME: WHERE CAST(string_col AS DATETIME) = TIMESTAMP '2024-01-01 00:00:00'
                    //    Pushed down as `string_col = '2024-01-01 00:00:00'`. A row with
                    //    string_col = '2024-01-01' is silently dropped, even though it parses to
                    //    the same DATETIME value (time-of-day defaults to 00:00:00).
                    //
                    // 4) BOOLEAN: WHERE CAST(string_col AS BOOLEAN) = FALSE
                    //    Note: `= TRUE` is normally folded away by the optimizer
                    //    (SimplifyCastRule etc.) into a bare `CAST(string_col AS BOOLEAN)`,
                    //    so in practice only `= FALSE` reaches this branch. We still guard
                    //    by type (not by value) for defence in depth, in case future
                    //    rule changes let a `= TRUE` slip through.
                    //    ConstantOperator.castTo(VARCHAR) on a BOOLEAN literal is hard-coded
                    //    to "1" / "0" (see ConstantOperator.castTo: `getBoolean() ? "1" : "0"`),
                    //    so without this guard the predicate would be pushed down as
                    //    `string_col = '0'`. But the BE-side CAST(string AS BOOLEAN)
                    //    (StringParser::string_to_bool_internal) recognizes a wider set of
                    //    inputs: "1"/"0" plus case-insensitive "true"/"false" (with optional
                    //    trailing whitespace). Rows like 'false' / 'FALSE' evaluate to FALSE
                    //    at the engine but do NOT equal the string literal "0", and would be
                    //    silently dropped once Iceberg file/row-group min-max pruning kicks in.
                    // Only true string-class literals fall through to castTo(VARCHAR).
                    PrimitiveType lt = operator.getType().getPrimitiveType();
                    if (lt.isNumericType()
                            || lt == PrimitiveType.DATE
                            || lt == PrimitiveType.DATETIME
                            || lt == PrimitiveType.BOOLEAN) {
                        return null;
                    }
                    res = operator.castTo(VarcharType.VARCHAR);
                    break;
                case BINARY:
                    res = operator.castTo(VarbinaryType.VARBINARY);
                    break;
                    // num usually don't need cast, and num and string has different comparator
                    // cast is dangerous.
                case INTEGER:
                case LONG:
                    // usually not used as partition column, don't do much work
                case DECIMAL:
                case FLOAT:
                case DOUBLE:
                case STRUCT:
                case LIST:
                case MAP:
                    // not supported
                case FIXED:
                case TIME:
                    return null;
            }

            return res.orElse(null);
        }

        @Override
        public Object visit(ScalarOperator scalarOperator, Type context) {
            return null;
        }

        @Override
        public Object visitConstant(ConstantOperator operator, Type context) {
            if (context != null && needCast(operator.getType().getPrimitiveType(), context.typeId())) {
                operator = tryCastToResultType(operator, context.typeId());
            }
            if (operator == null) {
                return null;
            }
            switch (operator.getType().getPrimitiveType()) {
                case BOOLEAN:
                    return operator.getBoolean();
                case TINYINT:
                    return operator.getTinyInt();
                case SMALLINT:
                    return operator.getSmallint();
                case INT:
                    return operator.getInt();
                case BIGINT:
                    return operator.getBigint();
                case FLOAT:
                    return operator.getFloat();
                case DOUBLE:
                    return operator.getDouble();
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    if (context != null) {
                        //In iceberg transform expr, the decimal's scale will influence the result, like truncate and bucket...
                        //For column value 123.40 and const value 123.4, column = value should be true
                        //But in iceberg transform, 123.40 and 123.4 are not the same, and the partition may be pruned incorretly.
                        return operator.getDecimal().setScale(((Types.DecimalType) context).scale(),
                                RoundingMode.HALF_UP);
                    } else {
                        return operator.getDecimal().setScale(((ScalarType) operator.getType()).getScalarScale(),
                                RoundingMode.HALF_UP);
                    }
                case HLL:
                case VARCHAR:
                case CHAR:
                    return operator.getVarchar();
                case DATE:
                    return operator.getDate().toLocalDate().toEpochDay();
                case DATETIME:
                    ZoneId zoneId;
                    if (Types.TimestampType.withZone().equals(context)) {
                        zoneId = TimeUtils.getTimeZone().toZoneId();
                    } else {
                        zoneId = ZoneOffset.UTC;
                    }

                    long value = operator.getDatetime().atZone(zoneId).toEpochSecond() * 1000
                            * 1000 * 1000 + operator.getDatetime().getNano();
                    return TimeUnit.MICROSECONDS.convert(value, TimeUnit.NANOSECONDS);
                case VARBINARY:
                case BINARY:
                    return operator.getBinary();
                default:
                    return null;
            }
        }
    }

    private static String getColumnName(ScalarOperator operator) {
        if (operator == null) {
            return null;
        }

        String columnName = operator.accept(new ExtractColumnName(), null);
        if (columnName == null || columnName.isEmpty()) {
            return null;
        }
        return columnName;
    }

    private static class ExtractColumnName extends ScalarOperatorVisitor<String, Void> {

        @Override
        public String visit(ScalarOperator scalarOperator, Void context) {
            return null;
        }

        @Override
        public String visitVariableReference(ColumnRefOperator operator, Void context) {
            if (operator.getHints().contains(VariantPathRewriteRule.COLUMN_REF_HINT)) {
                return null;
            }
            return operator.getName();
        }

        @Override
        public String visitCastOperator(CastOperator operator, Void context) {
            return operator.getChild(0).accept(this, context);
        }

        @Override
        public String visitSubfield(SubfieldOperator operator, Void context) {
            ScalarOperator child = operator.getChild(0);
            if (!(child instanceof ColumnRefOperator)) {
                return null;
            }
            ColumnRefOperator columnRefChild = ((ColumnRefOperator) child);
            if (columnRefChild.getHints().contains(VariantPathRewriteRule.COLUMN_REF_HINT)) {
                return null;
            }
            List<String> paths = new ImmutableList.Builder<String>()
                    .add(columnRefChild.getName()).addAll(operator.getFieldNames())
                    .build();
            return String.join(".", paths);
        }
    }
}
