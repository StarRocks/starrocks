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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.planner.PartitionIdGenerator;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergPartitionIdTest {
    @Test
    public void testFloatPartitionIdentity() {
        checkIdentity(Types.FloatType.get(), Float.intBitsToFloat(0x7fc00001),
                Float.intBitsToFloat(0xffc00002), true);
        checkIdentity(Types.FloatType.get(), 0.0f, -0.0f, false);
        checkIdentity(Types.FloatType.get(), Float.NaN, null, false);
        checkIdentity(Types.FloatType.get(), 1.0f, 1.0f, true);
    }

    @Test
    public void testDoublePartitionIdentity() {
        checkIdentity(Types.DoubleType.get(), Double.longBitsToDouble(0x7ff8000000000001L),
                Double.longBitsToDouble(0xfff8000000000002L), true);
        checkIdentity(Types.DoubleType.get(), 0.0d, -0.0d, false);
        checkIdentity(Types.DoubleType.get(), Double.NaN, null, false);
        checkIdentity(Types.DoubleType.get(), 1.0d, 1.0d, true);
    }

    private void checkIdentity(Type type, Object left, Object right, boolean equal) {
        Schema schema = new Schema(Types.NestedField.optional(1, "k", type));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("k").build();
        PartitionData a = new PartitionData(spec.partitionType());
        PartitionData b = new PartitionData(spec.partitionType());
        a.set(0, left);
        b.set(0, right);
        Assertions.assertEquals(equal, StructLikeWrapper.forType(spec.partitionType()).set(a)
                .equals(StructLikeWrapper.forType(spec.partitionType()).set(b)));
        Assertions.assertEquals(equal, Arrays.equals(partitionId(spec, a), partitionId(spec, b)));
    }

    private byte[] partitionId(PartitionSpec spec, PartitionData partition) {
        Table nativeTable = mock(Table.class);
        when(nativeTable.specs()).thenReturn(Map.of(spec.specId(), spec));
        IcebergTable table = mock(IcebergTable.class);
        when(table.getNativeTable()).thenReturn(nativeTable);
        DataFile file = mock(DataFile.class);
        when(file.specId()).thenReturn(spec.specId());
        when(file.partition()).thenReturn(partition);
        IcebergConnectorScanRangeSource source = new IcebergConnectorScanRangeSource(table,
                RemoteFileInfoDefaultSource.EMPTY, IcebergMORParams.EMPTY, null,
                Optional.empty(), PartitionIdGenerator.of(), false, false);
        return Deencapsulation.invoke(source, "buildPartitionId", file);
    }
}
