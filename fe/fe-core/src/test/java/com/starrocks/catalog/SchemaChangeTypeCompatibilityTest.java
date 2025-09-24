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

import org.junit.jupiter.api.Test;

import static com.starrocks.catalog.SchemaChangeTypeCompatibility.canReuseZonemapIndex;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaChangeTypeCompatibilityTest {

    @Test
    public void testZoneMapIndexReuse() {
        assertTrue(canReuseZonemapIndex(Type.TINYINT, Type.TINYINT));
        assertTrue(canReuseZonemapIndex(Type.TINYINT, Type.SMALLINT));
        assertTrue(canReuseZonemapIndex(Type.TINYINT, Type.INT));
        assertTrue(canReuseZonemapIndex(Type.TINYINT, Type.BIGINT));
        assertTrue(canReuseZonemapIndex(Type.TINYINT, Type.LARGEINT));
        assertTrue(canReuseZonemapIndex(Type.TINYINT, Type.DOUBLE));

        assertTrue(canReuseZonemapIndex(Type.SMALLINT, Type.SMALLINT));
        assertTrue(canReuseZonemapIndex(Type.SMALLINT, Type.INT));
        assertTrue(canReuseZonemapIndex(Type.SMALLINT, Type.BIGINT));
        assertTrue(canReuseZonemapIndex(Type.SMALLINT, Type.LARGEINT));
        assertTrue(canReuseZonemapIndex(Type.SMALLINT, Type.DOUBLE));

        assertTrue(canReuseZonemapIndex(Type.INT, Type.INT));
        assertTrue(canReuseZonemapIndex(Type.INT, Type.BIGINT));
        assertTrue(canReuseZonemapIndex(Type.INT, Type.LARGEINT));
        assertTrue(canReuseZonemapIndex(Type.INT, Type.DOUBLE));

        assertTrue(canReuseZonemapIndex(Type.BIGINT, Type.BIGINT));
        assertTrue(canReuseZonemapIndex(Type.BIGINT, Type.LARGEINT));
        assertTrue(canReuseZonemapIndex(Type.BIGINT, Type.DOUBLE));

        assertTrue(canReuseZonemapIndex(Type.LARGEINT, Type.LARGEINT));

        assertTrue(canReuseZonemapIndex(Type.FLOAT, Type.FLOAT));
        assertTrue(canReuseZonemapIndex(Type.FLOAT, Type.DOUBLE));

        assertTrue(canReuseZonemapIndex(Type.DOUBLE, Type.DOUBLE));

        assertTrue(canReuseZonemapIndex(Type.DECIMALV2, Type.DECIMALV2));
        assertTrue(canReuseZonemapIndex(Type.DECIMALV2, Type.DECIMAL128));

        assertTrue(canReuseZonemapIndex(Type.DECIMAL32, Type.DECIMAL32));
        assertTrue(canReuseZonemapIndex(Type.DECIMAL32, Type.DECIMAL64));
        assertTrue(canReuseZonemapIndex(Type.DECIMAL32, Type.DECIMAL128));
        assertTrue(canReuseZonemapIndex(Type.DECIMAL32, Type.DECIMAL256));

        assertTrue(canReuseZonemapIndex(Type.DECIMAL64, Type.DECIMAL64));
        assertTrue(canReuseZonemapIndex(Type.DECIMAL64, Type.DECIMAL128));
        assertTrue(canReuseZonemapIndex(Type.DECIMAL64, Type.DECIMAL256));

        assertTrue(canReuseZonemapIndex(Type.DECIMAL128, Type.DECIMAL128));
        assertTrue(canReuseZonemapIndex(Type.DECIMAL128, Type.DECIMAL256));

        assertTrue(canReuseZonemapIndex(Type.DECIMAL256, Type.DECIMAL256));

        assertTrue(canReuseZonemapIndex(Type.DATE, Type.DATE));
        assertTrue(canReuseZonemapIndex(Type.DATE, Type.DATETIME));

        assertTrue(canReuseZonemapIndex(Type.DATETIME, Type.DATETIME));

        ScalarType char10 = ScalarType.createCharType(10);
        ScalarType varchar20 = ScalarType.createVarcharType(20);
        assertTrue(canReuseZonemapIndex(char10, char10));
        assertTrue(canReuseZonemapIndex(char10, varchar20));

        ScalarType varchar30 = ScalarType.createVarcharType(30);
        assertTrue(canReuseZonemapIndex(varchar20, varchar30));
    }

    @Test
    public void testZoneMapIndexNotReuse() {
        // decreasing width
        assertFalse(canReuseZonemapIndex(Type.INT, Type.SMALLINT));
        assertFalse(canReuseZonemapIndex(Type.BIGINT, Type.INT));

        // integer to float is not in allowed matrix (only float->double allowed)
        assertFalse(canReuseZonemapIndex(Type.INT, Type.FLOAT));

        // double to float narrowing not allowed
        assertFalse(canReuseZonemapIndex(Type.DOUBLE, Type.FLOAT));

        // decimal narrowing
        assertFalse(canReuseZonemapIndex(Type.DECIMAL128, Type.DECIMAL64));

        assertFalse(canReuseZonemapIndex(Type.DATETIME, Type.DATE));

        // varchar to char not allowed by reuse matrix
        ScalarType varchar20 = ScalarType.createVarcharType(20);
        ScalarType char10 = ScalarType.createCharType(10);
        assertFalse(canReuseZonemapIndex(varchar20, char10));

        // string <-> int not allowed
        assertFalse(canReuseZonemapIndex(varchar20, Type.INT));
        assertFalse(canReuseZonemapIndex(Type.INT, varchar20));
    }
}
