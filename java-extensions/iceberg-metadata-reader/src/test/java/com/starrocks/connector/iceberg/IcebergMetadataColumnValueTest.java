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

package com.starrocks.connector.iceberg;

import com.starrocks.jni.connector.ColumnValue;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IcebergMetadataColumnValueTest {

    @Test
    public void testUnpackDecimalPartitionValues() {
        Types.StructType partitionType = Types.StructType.of(
                Types.NestedField.optional(1000, "d_trunc", Types.DecimalType.of(18, 4)),
                Types.NestedField.optional(1001, "d_null", Types.DecimalType.of(18, 4)));
        GenericRecord record = GenericRecord.create(partitionType);
        record.setField("d_trunc", new BigDecimal("123.4500"));
        record.setField("d_null", null);

        List<ColumnValue> values = new ArrayList<>();
        new IcebergMetadataColumnValue(record).unpackStruct(Arrays.asList(0, 1), values);

        Assertions.assertEquals(2, values.size());
        // A non-null decimal partition value must round-trip. Returning null here is what blows up
        // as an NPE in OffHeapColumnVector.putDecimal.
        Assertions.assertNotNull(values.get(0));
        Assertions.assertEquals(new BigDecimal("123.4500"), values.get(0).getDecimal());
        // A NULL partition value is represented by a null ColumnValue, which appendValue turns into appendNull.
        Assertions.assertNull(values.get(1));
    }
}
