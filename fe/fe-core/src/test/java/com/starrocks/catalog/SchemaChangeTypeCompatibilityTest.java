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

import com.starrocks.type.ScalarType;
import com.starrocks.type.StandardTypes;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Test;

import static com.starrocks.catalog.SchemaChangeTypeCompatibility.canReuseZonemapIndex;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SchemaChangeTypeCompatibilityTest {

    @Test
    public void testZoneMapIndexReuse() {
        assertTrue(canReuseZonemapIndex(StandardTypes.TINYINT, StandardTypes.TINYINT));
        assertTrue(canReuseZonemapIndex(StandardTypes.TINYINT, StandardTypes.SMALLINT));
        assertTrue(canReuseZonemapIndex(StandardTypes.TINYINT, StandardTypes.INT));
        assertTrue(canReuseZonemapIndex(StandardTypes.TINYINT, StandardTypes.BIGINT));
        assertTrue(canReuseZonemapIndex(StandardTypes.TINYINT, StandardTypes.LARGEINT));
        assertTrue(canReuseZonemapIndex(StandardTypes.TINYINT, StandardTypes.DOUBLE));

        assertTrue(canReuseZonemapIndex(StandardTypes.SMALLINT, StandardTypes.SMALLINT));
        assertTrue(canReuseZonemapIndex(StandardTypes.SMALLINT, StandardTypes.INT));
        assertTrue(canReuseZonemapIndex(StandardTypes.SMALLINT, StandardTypes.BIGINT));
        assertTrue(canReuseZonemapIndex(StandardTypes.SMALLINT, StandardTypes.LARGEINT));
        assertTrue(canReuseZonemapIndex(StandardTypes.SMALLINT, StandardTypes.DOUBLE));

        assertTrue(canReuseZonemapIndex(StandardTypes.INT, StandardTypes.INT));
        assertTrue(canReuseZonemapIndex(StandardTypes.INT, StandardTypes.BIGINT));
        assertTrue(canReuseZonemapIndex(StandardTypes.INT, StandardTypes.LARGEINT));
        assertTrue(canReuseZonemapIndex(StandardTypes.INT, StandardTypes.DOUBLE));

        assertTrue(canReuseZonemapIndex(StandardTypes.BIGINT, StandardTypes.BIGINT));
        assertTrue(canReuseZonemapIndex(StandardTypes.BIGINT, StandardTypes.LARGEINT));
        assertTrue(canReuseZonemapIndex(StandardTypes.BIGINT, StandardTypes.DOUBLE));

        assertTrue(canReuseZonemapIndex(StandardTypes.LARGEINT, StandardTypes.LARGEINT));

        assertTrue(canReuseZonemapIndex(StandardTypes.FLOAT, StandardTypes.FLOAT));
        assertTrue(canReuseZonemapIndex(StandardTypes.FLOAT, StandardTypes.DOUBLE));

        assertTrue(canReuseZonemapIndex(StandardTypes.DOUBLE, StandardTypes.DOUBLE));

        assertTrue(canReuseZonemapIndex(StandardTypes.DECIMALV2, StandardTypes.DECIMALV2));
        assertTrue(canReuseZonemapIndex(StandardTypes.DECIMALV2, StandardTypes.DECIMAL128));

        assertTrue(canReuseZonemapIndex(StandardTypes.DECIMAL32, StandardTypes.DECIMAL32));
        assertTrue(canReuseZonemapIndex(StandardTypes.DECIMAL32, StandardTypes.DECIMAL64));
        assertTrue(canReuseZonemapIndex(StandardTypes.DECIMAL32, StandardTypes.DECIMAL128));
        assertTrue(canReuseZonemapIndex(StandardTypes.DECIMAL32, StandardTypes.DECIMAL256));

        assertTrue(canReuseZonemapIndex(StandardTypes.DECIMAL64, StandardTypes.DECIMAL64));
        assertTrue(canReuseZonemapIndex(StandardTypes.DECIMAL64, StandardTypes.DECIMAL128));
        assertTrue(canReuseZonemapIndex(StandardTypes.DECIMAL64, StandardTypes.DECIMAL256));

        assertTrue(canReuseZonemapIndex(StandardTypes.DECIMAL128, StandardTypes.DECIMAL128));
        assertTrue(canReuseZonemapIndex(StandardTypes.DECIMAL128, StandardTypes.DECIMAL256));

        assertTrue(canReuseZonemapIndex(StandardTypes.DECIMAL256, StandardTypes.DECIMAL256));

        assertTrue(canReuseZonemapIndex(StandardTypes.DATE, StandardTypes.DATE));
        assertTrue(canReuseZonemapIndex(StandardTypes.DATE, StandardTypes.DATETIME));

        assertTrue(canReuseZonemapIndex(StandardTypes.DATETIME, StandardTypes.DATETIME));

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
        assertFalse(canReuseZonemapIndex(StandardTypes.INT, StandardTypes.SMALLINT));
        assertFalse(canReuseZonemapIndex(StandardTypes.BIGINT, StandardTypes.INT));

        // integer to float is not in allowed matrix (only float->double allowed)
        assertFalse(canReuseZonemapIndex(StandardTypes.INT, StandardTypes.FLOAT));

        // double to float narrowing not allowed
        assertFalse(canReuseZonemapIndex(StandardTypes.DOUBLE, StandardTypes.FLOAT));

        // decimal narrowing
        assertFalse(canReuseZonemapIndex(StandardTypes.DECIMAL128, StandardTypes.DECIMAL64));

        assertFalse(canReuseZonemapIndex(StandardTypes.DATETIME, StandardTypes.DATE));

        // varchar to char not allowed by reuse matrix
        ScalarType varchar20 = TypeFactory.createVarcharType(20);
        ScalarType char10 = TypeFactory.createCharType(10);
        assertFalse(canReuseZonemapIndex(varchar20, char10));

        // string <-> int not allowed
        assertFalse(canReuseZonemapIndex(varchar20, StandardTypes.INT));
        assertFalse(canReuseZonemapIndex(StandardTypes.INT, varchar20));
    }
}
