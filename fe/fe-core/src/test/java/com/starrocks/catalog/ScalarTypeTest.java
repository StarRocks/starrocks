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
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ScalarTypeTest {

    @Test
    public void createUnifiedDecimalTypeWithoutPrecisionAndScale() throws AnalysisException {
        ScalarType.createUnifiedDecimalType();
    }

    @Test
    public void testCreateUnifiedDecimalTypeWithoutScale() throws AnalysisException {
        ScalarType.createUnifiedDecimalType(18);
    }

    @Test
    public void testCreateUnifiedDecimalType() {
        Config.enable_decimal_v3 = false;
        Assert.assertEquals(
                ScalarType.createUnifiedDecimalType(27, 3),
                ScalarType.createDecimalV2Type(27, 3));
        Assert.assertEquals(
                ScalarType.createUnifiedDecimalType(28, 9),
                ScalarType.createDecimalV2Type(28, 9));
        Assert.assertEquals(
                ScalarType.createUnifiedDecimalType(18, 10),
                ScalarType.createUnifiedDecimalType(18, 10));

        Config.enable_decimal_v3 = true;
        Assert.assertEquals(
                ScalarType.createUnifiedDecimalType(9, 3),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 9, 3));

        Assert.assertEquals(
                ScalarType.createUnifiedDecimalType(18, 15),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 15));

        Assert.assertEquals(
                ScalarType.createUnifiedDecimalType(19, 15),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 19, 15));

        Assert.assertEquals(
                ScalarType.createUnifiedDecimalType(27, 15),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 27, 15));

        Assert.assertEquals(
                ScalarType.createUnifiedDecimalType(28, 28),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 28, 28));

        Assert.assertEquals(
                ScalarType.createUnifiedDecimalType(38, 0),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 0));

        Assert.assertEquals(
                ScalarType.createUnifiedDecimalType(38, 38),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 38));

        Assert.assertThrows(Throwable.class, () -> ScalarType.createUnifiedDecimalType(39, 38));
        Assert.assertThrows(Throwable.class, () -> ScalarType.createUnifiedDecimalType(10, 11));
    }

    @Test
    public void testGetCommonTypeForDecimalType() {

        ScalarType[][] testCases = {
                {
                        ScalarType.createDecimalV3NarrowestType(9, 9),
                        ScalarType.createDecimalV3NarrowestType(0, 0),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 9),
                },
                {
                        ScalarType.createDecimalV3NarrowestType(9, 9),
                        ScalarType.createDecimalV3NarrowestType(3, 2),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 9),
                },
                {
                        ScalarType.createDecimalV3NarrowestType(9, 9),
                        ScalarType.createDecimalV3NarrowestType(3, 3),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 9),
                },
                {
                        ScalarType.createDecimalV3NarrowestType(18, 9),
                        ScalarType.createDecimalV3NarrowestType(11, 10),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 19, 10),
                },
                {
                        ScalarType.createDecimalV3NarrowestType(35, 4),
                        ScalarType.createDecimalV3NarrowestType(18, 6),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 37, 6),
                },
                {
                        ScalarType.createDecimalV3NarrowestType(38, 4),
                        ScalarType.createDecimalV3NarrowestType(18, 10),
                        ScalarType.DOUBLE,
                },
                {
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 7, 4),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 3, 0),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 7, 4),
                },
                {
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 15, 11),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 11),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 15, 11),
                },
                {
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 9, 4),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 4),
                        ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 9, 4),
                },
        };

        for (ScalarType[] tc : testCases) {
            ScalarType lhs = tc[0];
            ScalarType rhs = tc[1];
            ScalarType expectResult = tc[2];
            ScalarType actualResult = ScalarType.getCommonTypeForDecimalV3(rhs, lhs);
            Assert.assertEquals(expectResult, actualResult);
            actualResult = ScalarType.getCommonTypeForDecimalV3(rhs, lhs);
            Assert.assertEquals(expectResult, actualResult);
        }
    }

    @Test
    public void testInvalidType() {
        // deserialize a not exist type
        String jsonStr = "{\"clazz\":\"ScalarType\",\"type\":\"NOT_EXIST\",\"len\":65530,\"precision\":0,\"scale\":0}";
        ScalarType type = GsonUtils.GSON.fromJson(jsonStr, ScalarType.class);
        Assert.assertEquals(PrimitiveType.INVALID_TYPE, type.getPrimitiveType());

        // deserialize a null type
        jsonStr = "{\"clazz\":\"ScalarType\",\"type\":\"NOT_EXIST\",\"len\":65530,\"precision\":0,\"scale\":0}";
        type = GsonUtils.GSON.fromJson(jsonStr, ScalarType.class);
        Assert.assertEquals(PrimitiveType.INVALID_TYPE, type.getPrimitiveType());
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
                ScalarType.createCharType(-1),
                ScalarType.createVarcharType(-1)
        );
        List<ScalarType> decimalTypes = Lists.newArrayList(
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 3, 0),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 6, 2),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 4),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 12, 8),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 15, 10),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 12),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 21, 14),
                ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 24, 16)
        );

        // integer to integer
        for (int i = 0; i < integerTypes.size(); i++) {
            for (int j = 0; j < integerTypes.size(); j++) {
                Assert.assertEquals(i <= j, integerTypes.get(i).isFullyCompatible(integerTypes.get(j)));
            }
        }
        // integer to string
        for (int i = 0; i < integerTypes.size(); i++) {
            for (int j = 0; j < stringTypes.size(); j++) {
                Assert.assertTrue(integerTypes.get(i).isFullyCompatible(stringTypes.get(j)));
            }
        }
        // decimal to decimal
        for (int i = 0; i < decimalTypes.size(); i++) {
            for (int j = 0; j < decimalTypes.size(); j++) {
                Assert.assertEquals(i <= j, decimalTypes.get(i).isFullyCompatible(decimalTypes.get(j)));
            }
        }
        // decimal to float
        for (int i = 0; i < decimalTypes.size(); i++) {
            Assert.assertTrue(decimalTypes.get(i).isFullyCompatible(ScalarType.FLOAT));
            Assert.assertTrue(decimalTypes.get(i).isFullyCompatible(ScalarType.DOUBLE));
        }
        // decimal to string
        for (int i = 0; i < decimalTypes.size(); i++) {
            for (int j = 0; j < stringTypes.size(); j++) {
                Assert.assertTrue(decimalTypes.get(i).isFullyCompatible(stringTypes.get(j)));
            }
        }
        // string to string
        for (int i = 0; i < stringTypes.size(); i++) {
            for (int j = 0; j < stringTypes.size(); j++) {
                Assert.assertTrue(stringTypes.get(i).isFullyCompatible(stringTypes.get(j)));
            }
        }

        // complex types
        Assert.assertFalse(ScalarType.JSON.isFullyCompatible(ScalarType.INT));
        Assert.assertFalse(ScalarType.JSON.isFullyCompatible(ScalarType.VARCHAR));
    }
}