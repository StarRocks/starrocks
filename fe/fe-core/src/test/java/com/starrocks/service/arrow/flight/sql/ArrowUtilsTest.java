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

package com.starrocks.service.arrow.flight.sql;

import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ArrowUtilsTest {
    private static final ArrowType NAIVE_TIMESTAMP = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);

    @Test
    public void testDatetimeIsTimestampWithoutTimeZone() {
        Field field = ArrowUtils.convertToArrowType(DateType.DATETIME, "dt", true);
        assertEquals(NAIVE_TIMESTAMP, field.getType());
    }

    @Test
    public void testNestedDatetimeIsTimestampWithoutTimeZone() {
        Field array = ArrowUtils.convertToArrowType(new ArrayType(DateType.DATETIME), "arr", true);
        assertEquals(NAIVE_TIMESTAMP, array.getChildren().get(0).getType());

        // Map -> entries struct -> (key, value)
        Field map = ArrowUtils.convertToArrowType(new MapType(IntegerType.INT, DateType.DATETIME), "m", true);
        assertEquals(NAIVE_TIMESTAMP, map.getChildren().get(0).getChildren().get(1).getType());

        List<Type> fieldTypes = List.of(DateType.DATETIME);
        Field struct = ArrowUtils.convertToArrowType(new StructType(fieldTypes), "s", true);
        assertEquals(NAIVE_TIMESTAMP, struct.getChildren().get(0).getType());
    }
}
