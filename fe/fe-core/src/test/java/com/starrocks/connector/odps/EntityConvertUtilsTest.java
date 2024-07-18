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

import com.aliyun.odps.table.optimizer.predicate.Attribute;
import com.aliyun.odps.table.optimizer.predicate.BinaryPredicate;
import com.aliyun.odps.table.optimizer.predicate.CompoundPredicate;
import com.aliyun.odps.table.optimizer.predicate.Constant;
import com.aliyun.odps.table.optimizer.predicate.InPredicate;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import com.aliyun.odps.table.optimizer.predicate.UnaryPredicate;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.type.VarcharTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EntityConvertUtilsTest {

    @Test
    public void testConvertTypeCaseBigint() {
        TypeInfo typeInfo = TypeInfoFactory.BIGINT;
        Type result = EntityConvertUtils.convertType(typeInfo);
        assertEquals(Type.BIGINT, result);
    }

    @Test
    public void testConvertTypeCaseInt() {
        TypeInfo typeInfo = TypeInfoFactory.INT;
        Type result = EntityConvertUtils.convertType(typeInfo);
        assertEquals(Type.INT, result);
    }

    @Test
    public void testConvertTypeCaseSmallint() {
        TypeInfo typeInfo = TypeInfoFactory.SMALLINT;
        Type result = EntityConvertUtils.convertType(typeInfo);
        assertEquals(Type.SMALLINT, result);
    }

    @Test
    public void testConvertTypeCaseTinyint() {
        TypeInfo typeInfo = TypeInfoFactory.TINYINT;
        Type result = EntityConvertUtils.convertType(typeInfo);
        assertEquals(Type.TINYINT, result);
    }

    @Test
    public void testConvertTypeCaseFloat() {
        TypeInfo typeInfo = TypeInfoFactory.FLOAT;
        Type result = EntityConvertUtils.convertType(typeInfo);
        assertEquals(Type.FLOAT, result);
    }

    @Test
    public void testConvertTypeCaseDecimalLessThanOrEqualMaxDecimal32Precision() {
        DecimalTypeInfo decimalTypeInfo = TypeInfoFactory.getDecimalTypeInfo(5, 2);
        Type result = EntityConvertUtils.convertType(decimalTypeInfo);
        Type expectedType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 5, 2);
        assertEquals(expectedType, result);
    }

    @Test
    public void testConvertTypeCaseDecimalLessThanOrEqualMaxDecimal64Precision() {
        DecimalTypeInfo decimalTypeInfo = TypeInfoFactory.getDecimalTypeInfo(12, 4);
        Type result = EntityConvertUtils.convertType(decimalTypeInfo);
        Type expectedType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 12, 4);
        assertEquals(expectedType, result);
    }

    @Test
    public void testConvertTypeCaseDecimalGreaterThanMaxDecimal64Precision() {
        DecimalTypeInfo decimalTypeInfo = TypeInfoFactory.getDecimalTypeInfo(20, 6);
        Type result = EntityConvertUtils.convertType(decimalTypeInfo);
        Type expectedType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 20, 6);
        assertEquals(expectedType, result);
    }

    @Test
    public void testConvertTypeCaseDouble() {
        TypeInfo typeInfo = TypeInfoFactory.DOUBLE;
        Type result = EntityConvertUtils.convertType(typeInfo);
        assertEquals(Type.DOUBLE, result);
    }

    @Test
    public void testConvertTypeCaseChar() {
        CharTypeInfo charTypeInfo = TypeInfoFactory.getCharTypeInfo(10);
        Type result = EntityConvertUtils.convertType(charTypeInfo);
        Type expectedType = ScalarType.createCharType(10);
        assertEquals(expectedType, result);
    }

    @Test
    public void testConvertTypeCaseVarchar() {
        VarcharTypeInfo varcharTypeInfo = TypeInfoFactory.getVarcharTypeInfo(20);
        Type result = EntityConvertUtils.convertType(varcharTypeInfo);
        Type expectedType = ScalarType.createVarcharType(20);
        assertEquals(expectedType, result);
    }

    @Test
    public void testConvertTypeCaseStringAndJson() {
        TypeInfo typeInfo = TypeInfoFactory.STRING;
        Type result = EntityConvertUtils.convertType(typeInfo);
        Type expectedType = ScalarType.createDefaultCatalogString();
        assertEquals(expectedType, result);
    }

    @Test
    public void testConvertTypeCaseBinary() {
        TypeInfo typeInfo = TypeInfoFactory.BINARY;
        Type result = EntityConvertUtils.convertType(typeInfo);
        assertEquals(Type.VARBINARY, result);
    }

    @Test
    public void testConvertTypeCaseBoolean() {
        TypeInfo typeInfo = TypeInfoFactory.BOOLEAN;
        Type result = EntityConvertUtils.convertType(typeInfo);
        assertEquals(Type.BOOLEAN, result);
    }

    @Test
    public void testConvertTypeCaseDate() {
        TypeInfo typeInfo = TypeInfoFactory.DATE;
        Type result = EntityConvertUtils.convertType(typeInfo);
        assertEquals(Type.DATE, result);
    }

    @Test
    public void testConvertTypeCaseTimestampAndDatetime() {
        TypeInfo typeInfo = TypeInfoFactory.TIMESTAMP;
        Type result = EntityConvertUtils.convertType(typeInfo);
        assertEquals(Type.DATETIME, result);
    }

    @Test
    public void testConvertTypeCaseMap() {
        TypeInfo keyTypeInfo = TypeInfoFactory.STRING;
        TypeInfo valueTypeInfo = TypeInfoFactory.INT;
        MapTypeInfo mapTypeInfo = TypeInfoFactory.getMapTypeInfo(keyTypeInfo, valueTypeInfo);
        Type result = EntityConvertUtils.convertType(mapTypeInfo);
        Type expectedType = new MapType(ScalarType.createDefaultCatalogString(), Type.INT);
        assertEquals(expectedType, result);
    }

    @Test
    public void testConvertTypeCaseArray() {
        TypeInfo elementTypeInfo = TypeInfoFactory.INT;
        ArrayTypeInfo arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(elementTypeInfo);
        Type result = EntityConvertUtils.convertType(arrayTypeInfo);
        Type expectedType = new ArrayType(Type.INT);
        assertEquals(expectedType, result);
    }

    @Test
    public void testConvertTypeCaseStruct() {
        TypeInfo fieldTypeInfo1 = TypeInfoFactory.STRING;
        TypeInfo fieldTypeInfo2 = TypeInfoFactory.INT;
        StructTypeInfo structTypeInfo =
                TypeInfoFactory.getStructTypeInfo(ImmutableList.of("fieldTypeInfo1", "fieldTypeInfo2"),
                        ImmutableList.of(fieldTypeInfo1, fieldTypeInfo2));
        Type result = EntityConvertUtils.convertType(structTypeInfo);
        Type expectedType1 = ScalarType.createDefaultCatalogString();
        Type expectedType2 = Type.INT;
        Type expectedType = new StructType(ImmutableList.of(expectedType1, expectedType2));
        assertEquals(expectedType, result);
    }

    @Test
    public void testConvertColumn() {
        com.aliyun.odps.Column column = mock(com.aliyun.odps.Column.class);
        when(column.getName()).thenReturn("test");
        TypeInfo typeInfo = TypeInfoFactory.INT;
        when(column.getTypeInfo()).thenReturn(typeInfo);
        Column result = EntityConvertUtils.convertColumn(column);
        Column expectedColumn = new Column("test", Type.INT, true);
        assertEquals(expectedColumn, result);
    }

    @Test
    public void testConvertPredicate_attributeAndConstantPredicate() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(0, Type.BIGINT, "col", false);
        Predicate result = EntityConvertUtils.convertPredicate(columnRefOperator, new HashSet<>());
        assertEquals(Attribute.of("col"), result);

        ConstantOperator constantOperator = new ConstantOperator(1L, Type.BIGINT);
        Predicate result2 = EntityConvertUtils.convertPredicate(constantOperator, new HashSet<>());
        assertEquals(Constant.of(1L), result2);
    }

    @Test
    public void testConvertPredicate_binaryPredicate() {
        BinaryPredicateOperator pred =
                new BinaryPredicateOperator(BinaryType.GE, new ColumnRefOperator(0, Type.BIGINT, "col", false),
                        new ConstantOperator(1L, Type.BIGINT));

        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());
        Predicate expect = BinaryPredicate.greaterThanOrEqual(Attribute.of("col"), Constant.of(1L));
        assertEquals(expect, result);

        BinaryPredicateOperator pred2 =
                new BinaryPredicateOperator(BinaryType.GE, new ColumnRefOperator(0, Type.BIGINT, "partitionCol", false),
                        new ConstantOperator(1L, Type.BIGINT));
        Predicate result2 = EntityConvertUtils.convertPredicate(pred2, ImmutableSet.of("partitionCol"));
        assertEquals(Predicate.NO_PREDICATE, result2);
    }

    @Test
    public void testConvertPredicate_isNullPredicate() {
        IsNullPredicateOperator notNullPred =
                new IsNullPredicateOperator(true, new ColumnRefOperator(0, Type.BIGINT, "col", false));

        Predicate result = EntityConvertUtils.convertPredicate(notNullPred, new HashSet<>());
        Predicate expect = UnaryPredicate.notNull(Attribute.of("col"));
        assertEquals(expect, result);

        IsNullPredicateOperator nullPred =
                new IsNullPredicateOperator(false, new ColumnRefOperator(0, Type.BIGINT, "col", false));
        Predicate result2 = EntityConvertUtils.convertPredicate(nullPred, new HashSet<>());
        Predicate expect2 = UnaryPredicate.isNull(Attribute.of("col"));
        assertEquals(result2, expect2);

        IsNullPredicateOperator predWithPartitionCol =
                new IsNullPredicateOperator(true, new ColumnRefOperator(0, Type.BIGINT, "partitionCol", false));
        Predicate result3 = EntityConvertUtils.convertPredicate(predWithPartitionCol, ImmutableSet.of("partitionCol"));
        assertEquals(Predicate.NO_PREDICATE, result3);
    }

    @Test
    public void testConvertPredicate_inPredicate() {
        InPredicateOperator notInPred = new InPredicateOperator(true,
                new ColumnRefOperator(0, Type.STRING, "col", false),
                new ConstantOperator("value1", Type.STRING),
                new ConstantOperator("value2", Type.STRING));
        Predicate result = EntityConvertUtils.convertPredicate(notInPred, new HashSet<>());
        Predicate expect = InPredicate.notIn(Attribute.of("col"),
                Arrays.asList(Constant.of("value1"), Constant.of("value2")));
        assertEquals(expect, result);

        InPredicateOperator inPred = new InPredicateOperator(false,
                new ColumnRefOperator(0, Type.STRING, "col", false),
                new ConstantOperator("value1", Type.STRING),
                new ConstantOperator("value2", Type.STRING));
        Predicate result2 = EntityConvertUtils.convertPredicate(inPred, new HashSet<>());
        Predicate expect2 = InPredicate.in(Attribute.of("col"),
                Arrays.asList(Constant.of("value1"), Constant.of("value2")));
        assertEquals(expect2, result2);

        InPredicateOperator predWithPartitionCol = new InPredicateOperator(true,
                new ColumnRefOperator(0, Type.STRING, "partitionCol", false),
                new ConstantOperator("value1", Type.STRING),
                new ConstantOperator("value2", Type.STRING));
        Predicate result3 = EntityConvertUtils.convertPredicate(predWithPartitionCol, ImmutableSet.of("partitionCol"));
        assertEquals(Predicate.NO_PREDICATE, result3);
    }

    @Test
    public void testConvertPredicate_compoundPredicate() {
        CompoundPredicateOperator pred = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                new BinaryPredicateOperator(BinaryType.GE, new ColumnRefOperator(0, Type.BIGINT, "col", false),
                        new ConstantOperator(1L, Type.BIGINT)),
                new BinaryPredicateOperator(BinaryType.LT, new ColumnRefOperator(0, Type.BIGINT, "col", false),
                        new ConstantOperator(2L, Type.BIGINT)));
        Predicate result = EntityConvertUtils.convertPredicate(pred, new HashSet<>());
        Predicate expect = CompoundPredicate.and(
                BinaryPredicate.greaterThanOrEqual(Attribute.of("col"), Constant.of(1L)),
                BinaryPredicate.lessThan(Attribute.of("col"), Constant.of(2L)));
        assertEquals(expect, result);

        CompoundPredicateOperator pred2 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                new BinaryPredicateOperator(BinaryType.GE, new ColumnRefOperator(0, Type.BIGINT, "col", false),
                        new ConstantOperator(1L, Type.BIGINT)),
                new BinaryPredicateOperator(BinaryType.LT, new ColumnRefOperator(0, Type.BIGINT, "col", false),
                        new ConstantOperator(2L, Type.BIGINT)));
        Predicate result2 = EntityConvertUtils.convertPredicate(pred2, new HashSet<>());
        Predicate expect2 = CompoundPredicate.or(
                BinaryPredicate.greaterThanOrEqual(Attribute.of("col"), Constant.of(1L)),
                BinaryPredicate.lessThan(Attribute.of("col"), Constant.of(2L)));
        assertEquals(expect2, result2);

        CompoundPredicateOperator pred3 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                new BinaryPredicateOperator(BinaryType.GE, new ColumnRefOperator(0, Type.BIGINT, "col", false),
                        new ConstantOperator(1L, Type.BIGINT)));
        Predicate result3 = EntityConvertUtils.convertPredicate(pred3, new HashSet<>());
        Predicate expect3 = CompoundPredicate.not(
                BinaryPredicate.greaterThanOrEqual(Attribute.of("col"), Constant.of(1L)));
        assertEquals(expect3, result3);

        CompoundPredicateOperator pred4 = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                new BinaryPredicateOperator(BinaryType.GE, new ColumnRefOperator(0, Type.BIGINT, "col", false),
                        new ConstantOperator(1L, Type.BIGINT)),
                new BinaryPredicateOperator(BinaryType.LT, new ColumnRefOperator(0, Type.BIGINT, "partitionCol", false),
                        new ConstantOperator(2L, Type.BIGINT)));
        Predicate result4 = EntityConvertUtils.convertPredicate(pred4, ImmutableSet.of("partitionCol"));
        Predicate expect4 =
                CompoundPredicate.and(BinaryPredicate.greaterThanOrEqual(Attribute.of("col"), Constant.of(1L)));
        assertEquals(expect4, result4);
    }
}
