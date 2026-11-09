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

package com.starrocks.connector.delta;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.LocalTimeBounds;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
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
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.VarcharType;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Or;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructField;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.delta.kernel.expressions.AlwaysTrue.ALWAYS_TRUE;

public class ScalarOperationToDeltaLakeExpr {
    // An IN list longer than this is not pushed down, to keep the kernel expression small.
    static final int MAX_IN_LIST_SIZE = 1000;

    public Predicate convert(List<ScalarOperator> operators, DeltaLakeContext context) {
        DeltaLakeExprVisitor visitor = new DeltaLakeExprVisitor();
        List<Predicate> predicates = Lists.newArrayList();

        for (ScalarOperator operator : operators) {
            Predicate predicate = operator.accept(visitor, context);
            if (predicate != null) {
                predicates.add(predicate);
            }
        }

        Optional<Predicate> result = predicates.stream().reduce(And::new);
        return result.orElse(ALWAYS_TRUE);
    }

    public static class DeltaLakeContext {
        private final io.delta.kernel.types.StructType schema;
        private final Set<String> partitionColumns;

        public DeltaLakeContext(io.delta.kernel.types.StructType schema, Set<String> partitionColumns) {
            this.schema = schema;
            this.partitionColumns = partitionColumns;
        }

        boolean isPartitionColumn(String column) {
            return partitionColumns.contains(column);
        }

        Column getColumn(String columnName) {
            if (schema == null) {
                return null;
            }
            int index = schema.indexOf(columnName);
            if (index < 0) {
                return null;
            }
            return schema.column(index);
        }

        public io.delta.kernel.types.StructType getSchema() {
            return schema;
        }
    }

    private static class DeltaLakeExprVisitor extends ScalarOperatorVisitor<Predicate, DeltaLakeContext> {
        // Under a NOT the converted predicate must be a subset of the original instead of a superset.
        private boolean insideNot = false;

        private static DeltaDataType getResultType(String columnName, DeltaLakeContext context) {
            Preconditions.checkNotNull(context);
            return getColumnType(columnName, context);
        }

        private static DeltaDataType getColumnType(String qualifiedName, DeltaLakeContext context) {
            //TODO: nested type
            StructField field = context.getSchema().get(qualifiedName);
            if (field != null && field.getDataType() != null) {
                return DeltaDataType.instanceFrom(field.getDataType().getClass());
            } else {
                return DeltaDataType.OTHER;
            }
        }

        @Override
        public Predicate visitCompoundPredicate(CompoundPredicateOperator operator, DeltaLakeContext context) {
            CompoundPredicateOperator.CompoundType op = operator.getCompoundType();
            if (op == CompoundPredicateOperator.CompoundType.NOT) {
                // TODO: implement
                if (operator.getChild(0) instanceof LikePredicateOperator) {
                    return null;
                }
                boolean savedInsideNot = insideNot;
                insideNot = true;
                Predicate predicate;
                try {
                    predicate = operator.getChild(0).accept(this, context);
                } finally {
                    insideNot = savedInsideNot;
                }

                if (predicate != null) {
                    return new Predicate("NOT", predicate);
                }
            } else {
                Predicate left = operator.getChild(0).accept(this, context);
                Predicate right = operator.getChild(1).accept(this, context);
                if (left != null && right != null) {
                    return (op == CompoundPredicateOperator.CompoundType.OR) ?
                            new Or(left, right) : new And(left, right);
                }
                // AND(a, b) implies a, so pushing down only the side that converts still prunes correctly.
                // Not under NOT: NOT(AND(a, b)) = OR(NOT(a), NOT(b)), so NOT(a) alone would over-prune.
                if (op == CompoundPredicateOperator.CompoundType.AND && !insideNot) {
                    return left != null ? left : right;
                }
            }
            return null;
        }

        @Override
        public Predicate visitInPredicate(InPredicateOperator operator, DeltaLakeContext context) {
            // NOT IN is not pushed down.
            if (operator.isNotIn() || operator.getListChildren().size() > MAX_IN_LIST_SIZE) {
                return null;
            }
            String columnName = getColumnName(operator.getChild(0));
            if (columnName == null) {
                return null;
            }
            Column column = context.getColumn(columnName);
            if (column == null) {
                return null;
            }
            DeltaDataType resultType = getResultType(columnName, context);
            boolean isPartitionColumn = context.isPartitionColumn(columnName);
            // The kernel's data skipping does not understand IN, so expand it to an OR of equalities.
            boolean dstAware = resultType == DeltaDataType.TIMESTAMP && !isPartitionColumn;
            List<Predicate> equalities = Lists.newArrayList();
            for (ScalarOperator child : operator.getListChildren()) {
                if (dstAware) {
                    LocalDateTime local = toLocalDateTime(child);
                    if (local != null && LocalTimeBounds.isAmbiguous(local)) {
                        Predicate range = convertAmbiguous(column, BinaryType.EQ, local);
                        if (range == null) {
                            return null;
                        }
                        equalities.add(range);
                        continue;
                    }
                }
                Literal literal = getLiteral(child, resultType, isPartitionColumn);
                if (literal == null) {
                    return null;
                }
                equalities.add(new Predicate("=", column, literal));
            }
            return equalities.isEmpty() ? null : balancedOr(equalities, 0, equalities.size());
        }

        @Override
        public Predicate visitLargeInPredicate(LargeInPredicateOperator operator, DeltaLakeContext context) {
            return null;
        }

        // A balanced tree keeps the kernel's recursive evaluation shallow for long IN lists.
        private static Predicate balancedOr(List<Predicate> predicates, int from, int to) {
            if (to - from == 1) {
                return predicates.get(from);
            }
            int mid = (from + to) >>> 1;
            return new Or(balancedOr(predicates, from, mid), balancedOr(predicates, mid, to));
        }

        @Override
        public Predicate visitLikePredicateOperator(LikePredicateOperator operator, DeltaLakeContext context) {
            if (operator.getLikeType() != LikePredicateOperator.LikeType.LIKE) {
                return null;
            }
            if (!(operator.getChild(1) instanceof ConstantOperator)) {
                return null;
            }
            ConstantOperator pattern = (ConstantOperator) operator.getChild(1);
            if (pattern.isNull() || !pattern.getType().isStringType()) {
                return null;
            }
            return startsWith(operator.getChild(0), getLikePrefix(pattern.getVarchar()), context);
        }

        // starts_with(s, 'abc'), which the function preimage derives from left(s, 3) = 'abc' and substr(s, 1, 3)
        @Override
        public Predicate visitCall(CallOperator call, DeltaLakeContext context) {
            if (!FunctionSet.STARTS_WITH.equalsIgnoreCase(call.getFnName()) || call.getChildren().size() != 2
                    || !(call.getChild(1) instanceof ConstantOperator prefix)
                    || prefix.isNull() || !prefix.getType().isStringType()) {
                return null;
            }
            return startsWith(call.getChild(0), prefix.getVarchar(), context);
        }

        private Predicate startsWith(ScalarOperator operand, String prefix, DeltaLakeContext context) {
            if (prefix == null) {
                return null;
            }
            String columnName = getColumnName(operand);
            if (columnName == null) {
                return null;
            }
            Column column = context.getColumn(columnName);
            // Only the kernel's partition pruning evaluates STARTS_WITH; its data skipping ignores it.
            if (column == null || !context.isPartitionColumn(columnName) ||
                    getResultType(columnName, context) != DeltaDataType.STRING) {
                return null;
            }
            return new Predicate("STARTS_WITH", column, Literal.ofString(prefix));
        }

        /**
         * Returns the prefix of a pattern of the form 'prefix%', where the prefix is non-empty and has no
         * wildcard or escape character, or null for any other pattern.
         */
        static String getLikePrefix(String pattern) {
            if (pattern == null || pattern.length() < 2 || pattern.charAt(pattern.length() - 1) != '%') {
                return null;
            }
            String prefix = pattern.substring(0, pattern.length() - 1);
            if (prefix.indexOf('%') >= 0 || prefix.indexOf('_') >= 0 || prefix.indexOf('\\') >= 0) {
                return null;
            }
            return prefix;
        }

        @Override
        public Predicate visitIsNullPredicate(IsNullPredicateOperator operator, DeltaLakeContext context) {
            String columnName = getColumnName(operator.getChild(0));
            if (columnName == null) {
                return null;
            }
            Column column = context.getColumn(columnName);
            // For struct subfield, cannot get the column for now, just return null
            if (column == null) {
                return null;
            }

            if (operator.isNotNull()) {
                return new Predicate("IS_NOT_NULL", column);
            } else {
                return new Predicate("IS_NULL", column);
            }
        }

        @Override
        public Predicate visitBinaryPredicate(BinaryPredicateOperator operator, DeltaLakeContext context) {
            String columnName = getColumnName(operator.getChild(0));
            if (columnName == null) {
                return null;
            }
            Column column = context.getColumn(columnName);
            // For struct subfield, cannot get the column for now, just return null
            if (column == null) {
                return null;
            }

            DeltaDataType resultType = getResultType(columnName, context);
            if (resultType == DeltaDataType.TIMESTAMP && !context.isPartitionColumn(columnName)) {
                LocalDateTime local = toLocalDateTime(operator.getChild(1));
                if (local != null && LocalTimeBounds.isAmbiguous(local)) {
                    return convertAmbiguous(column, operator.getBinaryType(), local);
                }
            }
            Literal literal = getLiteral(operator.getChild(1), resultType, context.isPartitionColumn(columnName));

            if (literal == null) {
                return null;
            }

            switch (operator.getBinaryType()) {
                case LT:
                    return new Predicate("<", column, literal);
                case LE:
                    return new Predicate("<=", column, literal);
                case GT:
                    return new Predicate(">", column, literal);
                case GE:
                    return new Predicate(">=", column, literal);
                case EQ:
                    return new Predicate("=", column, literal);
                case NE:
                    return new Predicate("NOT", new Predicate("=", column, literal));
                default:
                    return null;
            }
        }

        /**
         * A TIMESTAMP constant is a local time in the session time zone. In a DST overlap it names two instants and in
         * a gap none, so a single instant is neither a superset nor a subset of the original predicate; see
         * {@link LocalTimeBounds}. There is no subset to give under a NOT or for NE.
         */
        private Predicate convertAmbiguous(Column column, BinaryType op, LocalDateTime local) {
            List<LocalTimeBounds.Bound> bounds = insideNot ? null : LocalTimeBounds.superset(op, local);
            if (bounds == null) {
                return null;
            }
            Predicate result = null;
            for (LocalTimeBounds.Bound bound : bounds) {
                Predicate predicate = new Predicate(sqlOp(bound.op()), column, Literal.ofTimestamp(bound.micros()));
                result = result == null ? predicate : new And(result, predicate);
            }
            return result;
        }

        private static String sqlOp(BinaryType op) {
            switch (op) {
                case EQ:
                    return "=";
                case LT:
                    return "<";
                case LE:
                    return "<=";
                case GT:
                    return ">";
                case GE:
                    return ">=";
                default:
                    throw new IllegalArgumentException(op.toString());
            }
        }

        private static LocalDateTime toLocalDateTime(ScalarOperator operator) {
            if (!(operator instanceof ConstantOperator constant) || constant.isNull()) {
                return null;
            }
            if (constant.getType().isDatetime()) {
                return constant.getDatetime();
            }
            return constant.castTo(DateType.DATETIME).map(ConstantOperator::getDatetime).orElse(null);
        }

        @Override
        public Predicate visitVariableReference(ColumnRefOperator variable, DeltaLakeContext context) {
            // StarRocks will simplify predicate "column = true" to "column" in the optimizer,
            // so we need to handle this case here, transform it back to "column = true"
            String columnName = variable.getName();
            if (columnName == null) {
                return null;
            }
            Column column = context.getColumn(columnName);
            if (column == null) {
                return null;
            }
            return new Predicate("=", column, Literal.ofBoolean(true));
        }

        @Override
        public Predicate visit(ScalarOperator scalarOperator, DeltaLakeContext context) {
            return null;
        }

        private static Literal getLiteral(ScalarOperator operator, DeltaDataType deltaDataType,
                                          boolean isPartitionCol) {
            if (operator == null) {
                return null;
            }

            return operator.accept(new ExtractLiteralValue(isPartitionCol), deltaDataType);
        }
    }

    private static class ExtractLiteralValue extends ScalarOperatorVisitor<Literal, DeltaDataType> {
        private final boolean isPartitionColumn;

        public ExtractLiteralValue(boolean isPartitionColumn) {
            this.isPartitionColumn = isPartitionColumn;
        }

        private boolean needCast(PrimitiveType srcType, DeltaDataType destType) {
            switch (srcType) {
                case BOOLEAN:
                    return destType != DeltaDataType.BOOLEAN;
                case TINYINT:
                    return destType != DeltaDataType.BYTE;
                case SMALLINT:
                    return destType != DeltaDataType.SMALLINT;
                case INT:
                    return destType != DeltaDataType.INTEGER;
                case BIGINT:
                    return destType != DeltaDataType.LONG;
                case FLOAT:
                    return destType != DeltaDataType.FLOAT;
                case DOUBLE:
                    return destType != DeltaDataType.DOUBLE;
                case HLL:
                case VARCHAR:
                case CHAR:
                    return destType != DeltaDataType.STRING;
                case DATE:
                    return destType != DeltaDataType.DATE;
                case DATETIME:
                    return destType != DeltaDataType.TIMESTAMP && destType != DeltaDataType.TIMESTAMP_NTZ;
                default:
                    // INVALID_TYPE, NULL_TYPE, LARGEINT, DECIMAL_V2, TIME, BITMAP, PERCENTILE,
                    // JSON, FUNCTION, BINARY, VARBINARY, UNKNOWN_TYPE, DECIMAL32, DECIMAL64, DECIMAL128
                    return true;
            }
        }

        private ConstantOperator tryCastToResultType(ConstantOperator operator, DeltaDataType destType) {
            PrimitiveType srcType = operator.getType().getPrimitiveType();
            Optional<ConstantOperator> res = Optional.empty();

            switch (destType) {
                case BOOLEAN:
                    res = operator.castTo(BooleanType.BOOLEAN);
                    break;
                case BYTE:
                    if (srcType.isIntegerType()) {
                        res = operator.castTo(IntegerType.TINYINT);
                    }
                    break;
                case SMALLINT:
                    if (srcType.isIntegerType()) {
                        res = operator.castTo(IntegerType.SMALLINT);
                    }
                    break;
                case INTEGER:
                    if (srcType.isIntegerType()) {
                        res = operator.castTo(IntegerType.INT);
                    }
                    break;
                case LONG:
                    if (srcType.isIntegerType()) {
                        res = operator.castTo(IntegerType.BIGINT);
                    }
                    break;
                case DATE:
                    res = operator.castTo(DateType.DATE);
                    break;
                case TIMESTAMP:
                case TIMESTAMP_NTZ:
                    res = operator.castTo(DateType.DATETIME);
                    break;
                case STRING:
                    // num and string has different comparator
                    if (!operator.getType().isNumericType()) {
                        res = operator.castTo(VarcharType.VARCHAR);
                    }
                    break;
                default:
                    // Not supported: ARRAY, BYTE, DECIMAL, FLOAT, DOUBLE, MAP, STRUCT, OTHER, BINARY: not supported
                    return null;
            }

            return res.orElse(null);
        }

        @Override
        public Literal visit(ScalarOperator scalarOperator, DeltaDataType type) {
            return null;
        }

        @Override
        public Literal visitConstant(ConstantOperator operator, DeltaDataType type) {
            if (type == null) {
                return null;
            }

            if (needCast(operator.getType().getPrimitiveType(), type)) {
                operator = tryCastToResultType(operator, type);
            }
            if (operator == null) {
                return null;
            }

            switch (operator.getType().getPrimitiveType()) {
                case BOOLEAN:
                    return Literal.ofBoolean(operator.getBoolean());
                case TINYINT:
                    return Literal.ofShort(operator.getTinyInt());
                case SMALLINT:
                    return Literal.ofShort(operator.getSmallint());
                case INT:
                    return Literal.ofInt(operator.getInt());
                case BIGINT:
                    return Literal.ofLong(operator.getBigint());
                case FLOAT:
                    return Literal.ofFloat((float) operator.getFloat());
                case DOUBLE:
                    return Literal.ofDouble(operator.getDouble());
                case DATE:
                    return Literal.ofDate((int) operator.getDate().toLocalDate().toEpochDay());
                case DATETIME:
                    LocalDateTime localDateTime = operator.getDatetime();
                    ZoneId zoneOffset = ZoneOffset.UTC;
                    if (type == DeltaDataType.TIMESTAMP) {
                        if (!isPartitionColumn) {
                            // Note: A timestamp value in a partition value doesn't store
                            // the time zone due to historical reasons.
                            // It means its behavior looks similar to timestamp without time zone when it is used
                            // in a partition column.
                            // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
                            zoneOffset = TimeUtils.getTimeZone().toZoneId();
                        }
                        long value = localDateTime.atZone(zoneOffset).toEpochSecond() * 1000 * 1000
                                + localDateTime.getNano() / 1000;
                        return Literal.ofTimestamp(value);
                    } else {
                        long value = localDateTime.atZone(zoneOffset).toEpochSecond() * 1000 * 1000
                                + localDateTime.getNano() / 1000;
                        return Literal.ofTimestampNtz(value);
                    }
                case CHAR:
                case VARCHAR:
                case HLL:
                    return Literal.ofString(operator.getVarchar());
                default:
                    // not supported: INVALID_TYPE, NULL_TYPE, LARGEINT, DECIMALV2, TIME, BITMAP
                    //  PERCENTILE, JSON, FUNCTION, BINARY, VARBINARY, UNKNOWN_TYPE,
                    //  DECIMAL32, DECIMAL64, DECIMAL128
                    return null;
            }
        }
    }

    private static class ExtractColumnName extends ScalarOperatorVisitor<String, Void> {
        @Override
        public String visit(ScalarOperator scalarOperator, Void context) {
            return null;
        }

        @Override
        public String visitVariableReference(ColumnRefOperator operator, Void context) {
            return operator.getName();
        }

        @Override
        public String visitCastOperator(CastOperator operator, Void context) {
            // Stripping a non-identity cast can change predicate semantics and incorrectly prune files,
            // e.g. cast(date_col AS datetime) < '2020-01-01 10:00:00' must not become date_col < '2020-01-01'.
            // Any non-identity cast that still reaches this converter must remain a residual predicate.
            if (!operator.getType().equals(operator.getChild(0).getType())) {
                return null;
            }
            return operator.getChild(0).accept(this, context);
        }

        @Override
        public String visitSubfield(SubfieldOperator operator, Void context) {
            // The converter only binds top-level columns, so a joined path such as "s.a" would bind to a top-level
            // column literally named "s.a" instead of the field a of struct s. Keep subfield predicates residual.
            return null;
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
}
