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

package com.starrocks.connector.odps;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.table.optimizer.predicate.BinaryPredicate;
import com.aliyun.odps.table.optimizer.predicate.CompoundPredicate;
import com.aliyun.odps.table.optimizer.predicate.Constant;
import com.aliyun.odps.table.optimizer.predicate.InPredicate;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import com.aliyun.odps.table.optimizer.predicate.RawPredicate;
import com.aliyun.odps.table.optimizer.predicate.UnaryPredicate;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class EntityConvertUtils {

    public static Type convertType(TypeInfo typeInfo) {
        switch (typeInfo.getOdpsType()) {
            case BIGINT:
                return Type.BIGINT;
            case INT:
                return Type.INT;
            case SMALLINT:
                return Type.SMALLINT;
            case TINYINT:
                return Type.TINYINT;
            case FLOAT:
                return Type.FLOAT;
            case DECIMAL:
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                //In odps 2.0, the maximum length of decimal is 38, while in 1.0 it is 54. You need to convert it to String type for processing.
                //https://help.aliyun.com/zh/maxcompute/user-guide/maxcompute-v2-0-data-type-edition?spm=a2c4g.11186623.help-menu-27797.d_2_15_0_2.1c01123dDL8rEV
                if (decimalTypeInfo.getPrecision() > 38) {
                    return ScalarType.createDefaultCatalogString();
                }
                return ScalarType.createUnifiedDecimalType(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
            case DOUBLE:
                return Type.DOUBLE;
            case CHAR:
                CharTypeInfo charTypeInfo = (CharTypeInfo) typeInfo;
                return ScalarType.createCharType(charTypeInfo.getLength());
            case VARCHAR:
                VarcharTypeInfo varcharTypeInfo = (VarcharTypeInfo) typeInfo;
                return ScalarType.createVarcharType(varcharTypeInfo.getLength());
            case STRING:
            case JSON:
                return ScalarType.createDefaultCatalogString();
            case BINARY:
                return Type.VARBINARY;
            case BOOLEAN:
                return Type.BOOLEAN;
            case DATE:
                return Type.DATE;
            case TIMESTAMP:
            case DATETIME:
                return Type.DATETIME;
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                return new MapType(convertType(mapTypeInfo.getKeyTypeInfo()),
                        convertType(mapTypeInfo.getValueTypeInfo()));
            case ARRAY:
                ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) typeInfo;
                return new ArrayType(convertType(arrayTypeInfo.getElementTypeInfo()));
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                List<Type> fieldTypeList =
                        structTypeInfo.getFieldTypeInfos().stream().map(EntityConvertUtils::convertType)
                                .collect(Collectors.toList());
                return new StructType(fieldTypeList);
            default:
                return Type.VARCHAR;
        }
    }

    public static Column convertColumn(com.aliyun.odps.Column column) {
        return new Column(column.getName(), convertType(column.getTypeInfo()), true);
    }

    public static List<Column> getFullSchema(com.aliyun.odps.Table odpsTable) {
        TableSchema tableSchema = odpsTable.getSchema();
        List<com.aliyun.odps.Column> columns = new ArrayList<>(tableSchema.getColumns());
        columns.addAll(tableSchema.getPartitionColumns());
        return columns.stream().map(EntityConvertUtils::convertColumn).collect(
                Collectors.toList());
    }

    /**
     * Convert StarRocks predicate operator to MaxCompute predicate
     * Note that the predicates of the partition column have been filtered in advance at the optimizer layer
     * and should not be pushed down to the storage layer.
     * Therefore, this method will delete all the predicates of the partition column.
     *
     * @param predicate        StarRocks predicate operator
     * @param partitionColumns partition columns to be filtered
     * @return MaxCompute Predicate
     */
    public static Predicate convertPredicate(ScalarOperator predicate, Set<String> partitionColumns) {
        // First, filter out predicates that can be converted
        ScalarOperator convertiblePredicate = getConvertiblePredicate(predicate, partitionColumns);
        // Then convert the filtered predicate
        return doConvertPredicate(convertiblePredicate);
    }

    /**
     * Filter out predicates that can be converted to MaxCompute format
     * Implements partial pushdown logic similar to Spark's convertibleFilters
     */
    private static ScalarOperator getConvertiblePredicate(ScalarOperator predicate,
                                                          Set<String> partitionColumns) {
        if (predicate == null) {
            return null;
        }

        if (predicate instanceof ColumnRefOperator) {
            String columnName = ((ColumnRefOperator) predicate).getName();
            // Filter out partition columns
            if (partitionColumns.contains(columnName)) {
                return null;
            }
            return predicate;
        } else if (predicate instanceof ConstantOperator) {
            // Check if constant has a known type
            if (isKnownConstantType(((ConstantOperator) predicate).getValue())) {
                return predicate;
            }
            return null;
        } else if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) predicate;
            switch (binaryPredicate.getBinaryType()) {
                case EQ:
                case GE:
                case GT:
                case LE:
                case LT:
                case NE:
                    break;
                default:
                    return null;
            }
            ScalarOperator left = binaryPredicate.getChild(0);
            ScalarOperator right = binaryPredicate.getChild(1);

            ScalarOperator leftConvertible = getConvertiblePredicate(left, partitionColumns);
            ScalarOperator rightConvertible = getConvertiblePredicate(right, partitionColumns);

            if (leftConvertible != null && rightConvertible != null) {
                // Create a new binary predicate with the converted children
                return new BinaryPredicateOperator(
                        binaryPredicate.getBinaryType(),
                        leftConvertible,
                        rightConvertible
                );
            }
            return null;
        } else if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compoundPredicate = (CompoundPredicateOperator) predicate;
            CompoundPredicateOperator.CompoundType type = compoundPredicate.getCompoundType();

            List<ScalarOperator> children = compoundPredicate.getChildren();
            List<ScalarOperator> convertibleChildren = new ArrayList<>();

            for (ScalarOperator child : children) {
                ScalarOperator convertibleChild;
                // For NOT, we cannot do partial pushdown
                if (type == CompoundPredicateOperator.CompoundType.NOT) {
                    convertibleChild = getConvertiblePredicate(child, partitionColumns);
                } else {
                    convertibleChild = getConvertiblePredicate(child, partitionColumns);
                }

                if (convertibleChild != null) {
                    convertibleChildren.add(convertibleChild);
                }
            }

            if (type == CompoundPredicateOperator.CompoundType.AND) {
                if (convertibleChildren.isEmpty()) {
                    return null;
                }
                // If there's only one child, return it directly
                if (convertibleChildren.size() == 1) {
                    return convertibleChildren.get(0);
                }
                // Otherwise, create a new AND compound predicate
                return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, convertibleChildren);
            } else if (type == CompoundPredicateOperator.CompoundType.OR) {
                // For OR, all children must be convertible to maintain logical correctness
                if (convertibleChildren.size() != children.size()) {
                    return null;
                }
                if (convertibleChildren.isEmpty()) {
                    return null;
                }
                if (convertibleChildren.size() == 1) {
                    return convertibleChildren.get(0);
                }
                return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, convertibleChildren);
            } else if (type == CompoundPredicateOperator.CompoundType.NOT) {
                // For NOT, we need exactly one child to be convertible
                if (convertibleChildren.size() != 1) {
                    return null;
                }
                return new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, convertibleChildren);
            }

            return null;
        } else if (predicate instanceof IsNullPredicateOperator) {
            ScalarOperator operand = predicate.getChild(0);
            ScalarOperator convertibleOperand = getConvertiblePredicate(operand, partitionColumns);
            if (convertibleOperand != null) {
                return new IsNullPredicateOperator(
                        ((IsNullPredicateOperator) predicate).isNotNull(),
                        convertibleOperand
                );
            }
            return null;
        } else if (predicate instanceof InPredicateOperator) {
            InPredicateOperator inPredicate = (InPredicateOperator) predicate;
            ScalarOperator operand = inPredicate.getChild(0);
            ScalarOperator convertibleOperand = getConvertiblePredicate(operand, partitionColumns);

            if (convertibleOperand == null) {
                return null;
            }
            // Check if all values in the IN list are convertible
            boolean allValuesConvertible = true;
            List<ScalarOperator> convertibleValues = new ArrayList<>();
            convertibleValues.add(convertibleOperand);
            for (ScalarOperator value : inPredicate.getListChildren()) {
                ScalarOperator convertibleValue = getConvertiblePredicate(value, partitionColumns);
                if (convertibleValue == null) {
                    allValuesConvertible = false;
                    break;
                }
                convertibleValues.add(convertibleValue);
            }

            if (allValuesConvertible) {
                return new InPredicateOperator(
                        inPredicate.isNotIn(),
                        convertibleValues
                );
            }
            return null;
        }
        return null;
    }

    /**
     * Actually convert the filtered predicate to MaxCompute format
     */
    private static Predicate doConvertPredicate(ScalarOperator predicate) {
        if (predicate == null) {
            return Predicate.NO_PREDICATE;
        }

        if (predicate instanceof ColumnRefOperator) {
            String attributeName = quoteAttribute(((ColumnRefOperator) predicate).getName());
            return RawPredicate.of(attributeName);
        } else if (predicate instanceof ConstantOperator) {
            return Constant.of(((ConstantOperator) predicate).getValue());
        } else if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) predicate;
            Predicate left = doConvertPredicate(binaryPredicate.getChild(0));
            Predicate right = doConvertPredicate(binaryPredicate.getChild(1));

            BinaryType op = binaryPredicate.getBinaryType();
            switch (op) {
                case EQ:
                    return BinaryPredicate.equals(left, right);
                case GE:
                    return BinaryPredicate.greaterThanOrEqual(left, right);
                case GT:
                    return BinaryPredicate.greaterThan(left, right);
                case LE:
                    return BinaryPredicate.lessThanOrEqual(left, right);
                case LT:
                    return BinaryPredicate.lessThan(left, right);
                case NE:
                    return BinaryPredicate.notEquals(left, right);
                default:
                    throw new UnsupportedOperationException("Unsupported binary operator: " + op);
            }
        } else if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compoundPredicate = (CompoundPredicateOperator) predicate;
            CompoundPredicate.Operator op;

            switch (compoundPredicate.getCompoundType()) {
                case AND:
                    op = CompoundPredicate.Operator.AND;
                    break;
                case OR:
                    op = CompoundPredicate.Operator.OR;
                    break;
                case NOT:
                    op = CompoundPredicate.Operator.NOT;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported compound operator type");
            }

            CompoundPredicate result = new CompoundPredicate(op);
            for (ScalarOperator child : predicate.getChildren()) {
                Predicate childPredicate = doConvertPredicate(child);
                result.addPredicate(childPredicate);
            }
            return result;
        } else if (predicate instanceof IsNullPredicateOperator) {
            Predicate operand = doConvertPredicate(predicate.getChild(0));
            if (((IsNullPredicateOperator) predicate).isNotNull()) {
                return UnaryPredicate.notNull(operand);
            } else {
                return UnaryPredicate.isNull(operand);
            }
        } else if (predicate instanceof InPredicateOperator) {
            InPredicateOperator inPredicate = (InPredicateOperator) predicate;
            Predicate operand = doConvertPredicate(inPredicate.getChild(0));

            List<Serializable> values = new ArrayList<>();
            for (ScalarOperator value : inPredicate.getListChildren()) {
                values.add(doConvertPredicate(value));
            }
            if (inPredicate.isNotIn()) {
                return new InPredicate(InPredicate.Operator.NOT_IN, operand, values);
            } else {
                return new InPredicate(InPredicate.Operator.IN, operand, values);
            }
        }

        throw new UnsupportedOperationException("Unsupported predicate type: " + predicate.getClass().getSimpleName());
    }

    /**
     * Check if a constant value has a known type that can be converted
     */
    private static boolean isKnownConstantType(Object value) {
        if (value == null) {
            return false;
        }

        Class<?> clazz = value.getClass();
        return clazz == String.class ||
                Number.class.isAssignableFrom(clazz) ||
                clazz == Boolean.class ||
                clazz == java.math.BigDecimal.class;
    }

    /**
     * Quote attribute name properly
     */
    private static String quoteAttribute(String value) {
        if (value == null || value.isEmpty()) {
            return value;
        }

        if (value.startsWith("`") && value.endsWith("`") && value.length() > 1) {
            return value;
        } else {
            return String.format("`%s`", value.replace("`", "``"));
        }
    }
}
