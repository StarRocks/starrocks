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

import com.starrocks.type.GeometryType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ArrowUtilsTest {
    @Test
    public void testGeometryUsesBinaryArrowType() {
        Field field = ArrowUtils.convertToArrowType(GeometryType.GEOMETRY, "geom", false);

        Assertions.assertEquals("geom", field.getName());
        Assertions.assertFalse(field.isNullable());
        Assertions.assertInstanceOf(ArrowType.Binary.class, field.getType());
    }
}
