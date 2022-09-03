// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.Assert;
import org.junit.Test;

public class ScalarTypeTest {

    @Test(expected = AnalysisException.class)
    public void createUnifiedDecimalTypeWithoutPrecisionAndScale() throws AnalysisException {
        ScalarType.createUnifiedDecimalType();
        Assert.fail("should throw an exception");
    }

    @Test(expected = AnalysisException.class)
    public void testCreateUnifiedDecimalTypeWithoutScale() throws AnalysisException {
        ScalarType.createUnifiedDecimalType(18);
        Assert.fail("should throw an exception");
    }

    @Test
    public void testCreateUnifiedDecimalType() {
        Config.enable_decimal_v3 = false;
        Assert.assertEquals(
                ScalarType.createUnifiedDecimalType(27, 3),
                ScalarType.createDecimalV2Type(27, 3));
        try {
            ScalarType.createUnifiedDecimalType(28, 9);
            Assert.fail("should throw an exception");
        } catch (Error ex) {

        }

        try {
            ScalarType.createUnifiedDecimalType(18, 10);
            Assert.fail("should throw an exception");
        } catch (Error ex) {

        }
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

        try {
            ScalarType.createUnifiedDecimalType(39, 38);
            Assert.fail("should throw an error");
        } catch (Throwable ex) {

        }

        try {
            ScalarType.createUnifiedDecimalType(0, 0);
            Assert.fail("should throw an error");
        } catch (Throwable ex) {

        }

        try {
            ScalarType.createUnifiedDecimalType(10, 11);
            Assert.fail("should throw an error");
        } catch (Throwable ex) {

        }
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
}