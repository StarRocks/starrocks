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

import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Test;

import static com.starrocks.catalog.SchemaChangeTypeCompatibility.canReuseZonemapIndex;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaChangeTypeCompatibilityTest {

    @Test
    public void testZoneMapIndexReuse() {
        assertTrue(canReuseZonemapIndex(IntegerType.TINYINT, IntegerType.TINYINT));
        assertTrue(canReuseZonemapIndex(IntegerType.TINYINT, IntegerType.SMALLINT));
        assertTrue(canReuseZonemapIndex(IntegerType.TINYINT, IntegerType.INT));
        assertTrue(canReuseZonemapIndex(IntegerType.TINYINT, IntegerType.BIGINT));
        assertTrue(canReuseZonemapIndex(IntegerType.TINYINT, IntegerType.LARGEINT));
        assertTrue(canReuseZonemapIndex(IntegerType.TINYINT, FloatType.DOUBLE));

        assertTrue(canReuseZonemapIndex(IntegerType.SMALLINT, IntegerType.SMALLINT));
        assertTrue(canReuseZonemapIndex(IntegerType.SMALLINT, IntegerType.INT));
        assertTrue(canReuseZonemapIndex(IntegerType.SMALLINT, IntegerType.BIGINT));
        assertTrue(canReuseZonemapIndex(IntegerType.SMALLINT, IntegerType.LARGEINT));
        assertTrue(canReuseZonemapIndex(IntegerType.SMALLINT, FloatType.DOUBLE));

        assertTrue(canReuseZonemapIndex(IntegerType.INT, IntegerType.INT));
        assertTrue(canReuseZonemapIndex(IntegerType.INT, IntegerType.BIGINT));
        assertTrue(canReuseZonemapIndex(IntegerType.INT, IntegerType.LARGEINT));
        assertTrue(canReuseZonemapIndex(IntegerType.INT, FloatType.DOUBLE));

        assertTrue(canReuseZonemapIndex(IntegerType.BIGINT, IntegerType.BIGINT));
        assertTrue(canReuseZonemapIndex(IntegerType.BIGINT, IntegerType.LARGEINT));
        assertTrue(canReuseZonemapIndex(IntegerType.BIGINT, FloatType.DOUBLE));

        assertTrue(canReuseZonemapIndex(IntegerType.LARGEINT, IntegerType.LARGEINT));

        assertTrue(canReuseZonemapIndex(FloatType.FLOAT, FloatType.FLOAT));
        assertTrue(canReuseZonemapIndex(FloatType.FLOAT, FloatType.DOUBLE));

        assertTrue(canReuseZonemapIndex(FloatType.DOUBLE, FloatType.DOUBLE));

        assertTrue(canReuseZonemapIndex(DecimalType.DECIMALV2, DecimalType.DECIMALV2));
        assertTrue(canReuseZonemapIndex(DecimalType.DECIMALV2, DecimalType.DECIMAL128));

        assertTrue(canReuseZonemapIndex(DecimalType.DECIMAL32, DecimalType.DECIMAL32));
        assertTrue(canReuseZonemapIndex(DecimalType.DECIMAL32, DecimalType.DECIMAL64));
        assertTrue(canReuseZonemapIndex(DecimalType.DECIMAL32, DecimalType.DECIMAL128));
        assertTrue(canReuseZonemapIndex(DecimalType.DECIMAL32, DecimalType.DECIMAL256));

        assertTrue(canReuseZonemapIndex(DecimalType.DECIMAL64, DecimalType.DECIMAL64));
        assertTrue(canReuseZonemapIndex(DecimalType.DECIMAL64, DecimalType.DECIMAL128));
        assertTrue(canReuseZonemapIndex(DecimalType.DECIMAL64, DecimalType.DECIMAL256));

        assertTrue(canReuseZonemapIndex(DecimalType.DECIMAL128, DecimalType.DECIMAL128));
        assertTrue(canReuseZonemapIndex(DecimalType.DECIMAL128, DecimalType.DECIMAL256));

        assertTrue(canReuseZonemapIndex(DecimalType.DECIMAL256, DecimalType.DECIMAL256));

        assertTrue(canReuseZonemapIndex(DateType.DATE, DateType.DATE));
        assertTrue(canReuseZonemapIndex(DateType.DATE, DateType.DATETIME));

        assertTrue(canReuseZonemapIndex(DateType.DATETIME, DateType.DATETIME));

        ScalarType char10 = TypeFactory.createCharType(10);
        ScalarType varchar20 = TypeFactory.createVarcharType(20);
        assertTrue(canReuseZonemapIndex(char10, char10));
        assertTrue(canReuseZonemapIndex(char10, varchar20));

        ScalarType varchar30 = TypeFactory.createVarcharType(30);
        assertTrue(canReuseZonemapIndex(varchar20, varchar30));
    }

    @Test
    public void testZoneMapIndexNotReuse() {
        // decreasing width
        assertFalse(canReuseZonemapIndex(IntegerType.INT, IntegerType.SMALLINT));
        assertFalse(canReuseZonemapIndex(IntegerType.BIGINT, IntegerType.INT));

        // integer to float is not in allowed matrix (only float->double allowed)
        assertFalse(canReuseZonemapIndex(IntegerType.INT, FloatType.FLOAT));

        // double to float narrowing not allowed
        assertFalse(canReuseZonemapIndex(FloatType.DOUBLE, FloatType.FLOAT));

        // decimal narrowing
        assertFalse(canReuseZonemapIndex(DecimalType.DECIMAL128, DecimalType.DECIMAL64));

        assertFalse(canReuseZonemapIndex(DateType.DATETIME, DateType.DATE));

        // varchar to char not allowed by reuse matrix
        ScalarType varchar20 = TypeFactory.createVarcharType(20);
        ScalarType char10 = TypeFactory.createCharType(10);
        assertFalse(canReuseZonemapIndex(varchar20, char10));

        // string <-> int not allowed
        assertFalse(canReuseZonemapIndex(varchar20, IntegerType.INT));
        assertFalse(canReuseZonemapIndex(IntegerType.INT, varchar20));
    }
}
