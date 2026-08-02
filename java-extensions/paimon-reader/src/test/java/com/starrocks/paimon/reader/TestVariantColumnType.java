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

package com.starrocks.paimon.reader;

import com.starrocks.jni.connector.ColumnType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class TestVariantColumnType {
    @Test
    public void testParseVariant() {
        ColumnType type = new ColumnType("v", "variant");
        Assertions.assertEquals(ColumnType.TypeValue.VARIANT, type.getTypeValue());
        Assertions.assertTrue(type.isVariant());
        Assertions.assertEquals(Arrays.asList("metadata", "value"), type.getChildNames());
        Assertions.assertEquals(ColumnType.TypeValue.BINARY, type.getChildTypes().get(0).getTypeValue());
        Assertions.assertEquals(ColumnType.TypeValue.BINARY, type.getChildTypes().get(1).getTypeValue());
        Assertions.assertEquals(Arrays.asList(0, 1), type.getFieldIndex());
        // variant column meta: [null] + 2 binary children, each [null | offset | data] => 1 + 3 + 3
        Assertions.assertEquals(7, type.computeColumnSize());
        Assertions.assertEquals("variant", type.getTypeValueString());
    }
}
