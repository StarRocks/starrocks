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

package com.starrocks.odps.reader;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.starrocks.jni.connector.ColumnValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestOdpsColumnValue {

    @Test
    public void testUnpackArrayWithNullElement() {
        TypeInfo arrayType = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);
        OdpsColumnValue value = new OdpsColumnValue(Arrays.asList("a", null), arrayType, "UTC");
        List<ColumnValue> values = new ArrayList<>();
        value.unpackArray(values);
        Assertions.assertEquals(2, values.size());
        Assertions.assertEquals("a", values.get(0).getString(null));
        Assertions.assertNull(values.get(1));
    }

    @Test
    public void testUnpackStructWithNullField() {
        StructTypeInfo structType = TypeInfoFactory.getStructTypeInfo(Arrays.asList("id", "name"),
                Arrays.asList(TypeInfoFactory.BIGINT, TypeInfoFactory.STRING));
        OdpsColumnValue value = new OdpsColumnValue(new SimpleStruct(structType, Arrays.asList(1L, null)),
                structType, "UTC");
        List<ColumnValue> values = new ArrayList<>();
        value.unpackStruct(Arrays.asList(0, 1), values);
        Assertions.assertEquals(2, values.size());
        Assertions.assertEquals(1L, values.get(0).getLong());
        Assertions.assertNull(values.get(1));
    }
}
