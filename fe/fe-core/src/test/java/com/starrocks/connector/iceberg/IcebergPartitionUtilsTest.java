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

import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class IcebergPartitionUtilsTest {
    @Test
    public void testIcebergPartition() {
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Types.StructType structType1 = Types.StructType.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get())
        );
        PartitionData data1 = new PartitionData(structType1);
        IcebergPartitionUtils.IcebergPartition partition1 =
                new IcebergPartitionUtils.IcebergPartition(spec, data1, ChangelogOperation.INSERT);

        Types.StructType structType2 = Types.StructType.of(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "age", Types.IntegerType.get())
        );
        PartitionData data2 = new PartitionData(structType2);
        IcebergPartitionUtils.IcebergPartition partition2 =
                new IcebergPartitionUtils.IcebergPartition(spec, data2, ChangelogOperation.INSERT);

        IcebergPartitionUtils.IcebergPartition partition3 = partition1;
        Assert.assertEquals(partition3, partition1);
        Assert.assertNotEquals(partition1, partition2);

        Set<IcebergPartitionUtils.IcebergPartition> set = ImmutableSet.of(partition1, partition2, partition3);
        Assert.assertEquals(2, set.size());
    }
}
