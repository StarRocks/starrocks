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

import com.starrocks.jni.connector.ColumnValue;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestPaimonVariantColumnValue {
    @Test
    public void testTypeString() {
        Assertions.assertEquals("variant", PaimonTypeUtils.fromPaimonType(DataTypes.VARIANT()));
    }

    @Test
    public void testUnpackVariant() {
        GenericVariant variant = GenericVariant.fromJson("{\"a\":1,\"b\":\"x\"}");
        PaimonColumnValue cv = new PaimonColumnValue(variant, DataTypes.VARIANT(), "UTC");
        List<ColumnValue> values = new ArrayList<>();
        cv.unpackStruct(Arrays.asList(0, 1), values);
        Assertions.assertEquals(2, values.size());
        Assertions.assertArrayEquals(variant.metadata(), values.get(0).getBytes());
        Assertions.assertArrayEquals(variant.value(), values.get(1).getBytes());
    }
}
