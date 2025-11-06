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


package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ScalarTypeTest {

    @Test
    public void createUnifiedDecimalTypeWithoutPrecisionAndScale() throws AnalysisException {
        TypeFactory.createUnifiedDecimalType();
    }

    @Test
    public void testCreateUnifiedDecimalTypeWithoutScale() throws AnalysisException {
        TypeFactory.createUnifiedDecimalType(18);
    }

    @Test
    public void testCreateUnifiedDecimalType() {
        Config.enable_decimal_v3 = false;
        Assertions.assertEquals(
                TypeFactory.createUnifiedDecimalType(27, 3),
                TypeFactory.createDecimalV2Type(27, 3));
        Assertions.assertEquals(
                TypeFactory.createUnifiedDecimalType(28, 9),
                TypeFactory.createDecimalV2Type(28, 9));
        Assertions.assertEquals(
                TypeFactory.createUnifiedDecimalType(18, 10),
                TypeFactory.createUnifiedDecimalType(18, 10));

        Config.enable_decimal_v3 = true;
        Assertions.assertEquals(
                TypeFactory.createUnifiedDecimalType(9, 3),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 9, 3));

        Assertions.assertEquals(
                TypeFactory.createUnifiedDecimalType(18, 15),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 15));

        Assertions.assertEquals(
                TypeFactory.createUnifiedDecimalType(19, 15),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 19, 15));

        Assertions.assertEquals(
                TypeFactory.createUnifiedDecimalType(27, 15),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 27, 15));

        Assertions.assertEquals(
                TypeFactory.createUnifiedDecimalType(28, 28),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 28, 28));

        Assertions.assertEquals(
                TypeFactory.createUnifiedDecimalType(38, 0),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 0));

        Assertions.assertEquals(
                TypeFactory.createUnifiedDecimalType(38, 38),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 38));

        Assertions.assertThrows(Throwable.class, () -> TypeFactory.createUnifiedDecimalType(79, 38));
        Assertions.assertThrows(Throwable.class, () -> TypeFactory.createUnifiedDecimalType(10, 11));
    }

    @Test
    public void testGetCommonTypeForDecimalType() {

        ScalarType[][] testCases = {
                {
                        TypeFactory.createDecimalV3NarrowestType(9, 9),
                        TypeFactory.createDecimalV3NarrowestType(0, 0),
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 9),
                },
                {
                        TypeFactory.createDecimalV3NarrowestType(9, 9),
                        TypeFactory.createDecimalV3NarrowestType(3, 2),
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 9),
                },
                {
                        TypeFactory.createDecimalV3NarrowestType(9, 9),
                        TypeFactory.createDecimalV3NarrowestType(3, 3),
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 9),
                },
                {
                        TypeFactory.createDecimalV3NarrowestType(18, 9),
                        TypeFactory.createDecimalV3NarrowestType(11, 10),
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 19, 10),
                },
                {
                        TypeFactory.createDecimalV3NarrowestType(35, 4),
                        TypeFactory.createDecimalV3NarrowestType(18, 6),
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 37, 6),
                },
                {
                        TypeFactory.createDecimalV3NarrowestType(38, 4),
                        TypeFactory.createDecimalV3NarrowestType(18, 10),
                        ScalarType.DOUBLE,
                },
                {
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 7, 4),
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 3, 0),
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 7, 4),
                },
                {
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 15, 11),
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 11),
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 15, 11),
                },
                {
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 9, 4),
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 4),
                        TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 9, 4),
                },
        };

        for (ScalarType[] tc : testCases) {
            ScalarType lhs = tc[0];
            ScalarType rhs = tc[1];
            ScalarType expectResult = tc[2];
            ScalarType actualResult = TypeManager.getCommonTypeForDecimalV3(rhs, lhs);
            Assertions.assertEquals(expectResult, actualResult);
            actualResult = TypeManager.getCommonTypeForDecimalV3(rhs, lhs);
            Assertions.assertEquals(expectResult, actualResult);
        }
    }

    @Test
    public void testInvalidType() {
        // deserialize a not exist type
        String jsonStr = "{\"clazz\":\"ScalarType\",\"type\":\"NOT_EXIST\",\"len\":65530,\"precision\":0,\"scale\":0}";
        ScalarType type = GsonUtils.GSON.fromJson(jsonStr, ScalarType.class);
        Assertions.assertEquals(PrimitiveType.INVALID_TYPE, type.getPrimitiveType());

        // deserialize a null type
        jsonStr = "{\"clazz\":\"ScalarType\",\"type\":\"NOT_EXIST\",\"len\":65530,\"precision\":0,\"scale\":0}";
        type = GsonUtils.GSON.fromJson(jsonStr, ScalarType.class);
        Assertions.assertEquals(PrimitiveType.INVALID_TYPE, type.getPrimitiveType());
    }

    @Test
    public void testIsFullyCompatible() {
        List<ScalarType> integerTypes = Lists.newArrayList(
                ScalarType.TINYINT,
                ScalarType.SMALLINT,
                ScalarType.INT,
                ScalarType.BIGINT,
                ScalarType.LARGEINT
        );
        List<ScalarType> stringTypes = Lists.newArrayList(
                TypeFactory.createCharType(-1),
                TypeFactory.createVarcharType(-1)
        );
        List<ScalarType> decimalTypes = Lists.newArrayList(
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 3, 0),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 6, 2),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 4),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 12, 8),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 10),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 12),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 21, 14),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 24, 16)
        );

        // integer to integer
        for (int i = 0; i < integerTypes.size(); i++) {
            for (int j = 0; j < integerTypes.size(); j++) {
                Assertions.assertEquals(i <= j, integerTypes.get(i).isFullyCompatible(integerTypes.get(j)));
            }
        }
        // integer to string
        for (int i = 0; i < integerTypes.size(); i++) {
            for (int j = 0; j < stringTypes.size(); j++) {
                Assertions.assertTrue(integerTypes.get(i).isFullyCompatible(stringTypes.get(j)));
            }
        }
        // decimal to decimal
        for (int i = 0; i < decimalTypes.size(); i++) {
            for (int j = 0; j < decimalTypes.size(); j++) {
                Assertions.assertEquals(i <= j, decimalTypes.get(i).isFullyCompatible(decimalTypes.get(j)));
            }
        }
        // decimal to float
        for (int i = 0; i < decimalTypes.size(); i++) {
            Assertions.assertTrue(decimalTypes.get(i).isFullyCompatible(ScalarType.FLOAT));
            Assertions.assertTrue(decimalTypes.get(i).isFullyCompatible(ScalarType.DOUBLE));
        }
        // decimal to string
        for (int i = 0; i < decimalTypes.size(); i++) {
            for (int j = 0; j < stringTypes.size(); j++) {
                Assertions.assertTrue(decimalTypes.get(i).isFullyCompatible(stringTypes.get(j)));
            }
        }
        // string to string
        for (int i = 0; i < stringTypes.size(); i++) {
            for (int j = 0; j < stringTypes.size(); j++) {
                Assertions.assertTrue(stringTypes.get(i).isFullyCompatible(stringTypes.get(j)));
            }
        }

        // complex types
        Assertions.assertFalse(ScalarType.JSON.isFullyCompatible(ScalarType.INT));
        Assertions.assertFalse(ScalarType.JSON.isFullyCompatible(ScalarType.VARCHAR));
    }
}