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
import com.aliyun.odps.table.optimizer.predicate.Attribute;
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
import com.aliyun.odps.utils.StringUtils;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
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
     * convert StarRocks predicate operator to MaxCompute predicate
     * Note that the predicates of the partition column have been filtered in advance at the optimizer layer and should not be pushed down to the storage layer.
     * Therefore, this method will delete all the predicates of the partition column.
     *
     * @param predicate        StarRocks predicate operator
     * @param partitionColumns partition columns to be filtered
     * @return MaxCompute Predicate
     */
    public static Predicate convertPredicate(ScalarOperator predicate, Set<String> partitionColumns) {
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicateOperator = (BinaryPredicateOperator) predicate;

            Predicate leftChild = convertPredicate(binaryPredicateOperator.getChild(0), partitionColumns);
            Predicate rightChild = convertPredicate(binaryPredicateOperator.getChild(1), partitionColumns);

            if (Predicate.NO_PREDICATE.equals(leftChild) || Predicate.NO_PREDICATE.equals(rightChild)) {
                return Predicate.NO_PREDICATE;
            }

            return new RawPredicate(leftChild +
                    binaryPredicateOperator.getBinaryType().toString() +
                    rightChild);
        } else if (predicate instanceof ColumnRefOperator) {
            if (partitionColumns.contains(((ColumnRefOperator) predicate).getName())) {
                return Predicate.NO_PREDICATE;
            }
            return Attribute.of(((ColumnRefOperator) predicate).getName());
        } else if (predicate instanceof ConstantOperator) {
            return Constant.of(predicate.toString());
        } else if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicate compoundPredicate;
            switch (((CompoundPredicateOperator) predicate).getCompoundType()) {
                case AND:
                    compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.AND);
                    break;
                case OR:
                    compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.OR);
                    break;
                case NOT:
                    compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.NOT);
                    break;
                default:
                    return Predicate.NO_PREDICATE;
            }
            for (ScalarOperator child : predicate.getChildren()) {
                Predicate childPredicate = convertPredicate(child, partitionColumns);
                if (Predicate.NO_PREDICATE.equals(childPredicate)) {
                    continue;
                }
                compoundPredicate.addPredicate(childPredicate);
            }
            return compoundPredicate;
        } else if (predicate instanceof IsNullPredicateOperator) {
            Predicate operand = convertPredicate(predicate.getChild(0), partitionColumns);
            if (Predicate.NO_PREDICATE.equals(operand)) {
                return Predicate.NO_PREDICATE;
            }
            return new UnaryPredicate(
                    ((IsNullPredicateOperator) predicate).isNotNull() ? UnaryPredicate.Operator.NOT_NULL :
                            UnaryPredicate.Operator.IS_NULL, operand);
        } else if (predicate instanceof InPredicateOperator) {
            InPredicateOperator inPredicateOperator = (InPredicateOperator) predicate;
            Predicate operand = convertPredicate(inPredicateOperator.getChild(0), partitionColumns);
            if (Predicate.NO_PREDICATE.equals(operand)) {
                return Predicate.NO_PREDICATE;
            }
            List<Serializable> set =
                    inPredicateOperator.getListChildren().stream().map(p -> convertPredicate(p, partitionColumns))
                            .filter(
                                    s -> !StringUtils.isNullOrEmpty(s.toString()))
                            .collect(Collectors.toList());
            if (inPredicateOperator.isNotIn()) {
                return new InPredicate(InPredicate.Operator.NOT_IN, operand, set);
            } else {
                return new InPredicate(InPredicate.Operator.IN, operand, set);
            }
        } else {
            return Predicate.NO_PREDICATE;
        }
    }
}
