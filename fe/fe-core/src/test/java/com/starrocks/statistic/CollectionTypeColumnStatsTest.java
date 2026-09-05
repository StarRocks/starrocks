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

package com.starrocks.statistic;

import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.base.CollectionTypeColumnStats;
import com.starrocks.type.ArrayType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.StringType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CollectionTypeColumnStatsTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testGetCollectionSizeArrayType() {
        Type itemType = IntegerType.INT;
        ArrayType arrayType = new ArrayType(itemType);
        CollectionTypeColumnStats stats = new CollectionTypeColumnStats("arr_col", arrayType, false);
        String result = stats.getCollectionSize();
        Assertions.assertEquals("IFNULL(AVG(ARRAY_LENGTH(`arr_col`)), -1) ", result);
    }

    @Test
    public void testGetCollectionSizeMapType() {
        Type keyType = StringType.STRING;
        Type valueType = IntegerType.INT;
        MapType mapType = new MapType(keyType, valueType);
        CollectionTypeColumnStats stats = new CollectionTypeColumnStats("map_col", mapType, false);
        String result = stats.getCollectionSize();
        Assertions.assertEquals("IFNULL(AVG(MAP_SIZE(`map_col`)), -1) ", result);
    }

    @Test
    public void testGetCollectionSizeWithNullHandling() {
        Type itemType = IntegerType.INT;
        ArrayType arrayType = new ArrayType(itemType);
        CollectionTypeColumnStats stats = new CollectionTypeColumnStats("arr_col", arrayType, false);
        String result = stats.getCollectionSize();
        Assertions.assertTrue(result.contains("IFNULL"), "Should use IFNULL to handle NULL AVG result");
        Assertions.assertTrue(result.contains("-1"), "Should return -1 as default when AVG returns NULL");
    }

    @Test
    public void testGetFullDataSize() {
        Type itemType = IntegerType.INT;
        ArrayType arrayType = new ArrayType(itemType);
        CollectionTypeColumnStats stats = new CollectionTypeColumnStats("arr_col", arrayType, false);
        String result = stats.getFullDataSize();
        Assertions.assertTrue(result.contains("COUNT(*) * " + itemType.getTypeSize() + " * "));
        Assertions.assertTrue(result.contains("IFNULL(AVG(ARRAY_LENGTH(`arr_col`)), -1)"));
    }
}

