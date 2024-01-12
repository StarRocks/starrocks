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
import com.starrocks.catalog.Type;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Set;
import java.util.TimeZone;

public class IcebergPartitionUtilsTest extends TableTestBase {
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, temp.newFolder().toURI().toString());
    }

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

    @Test
    public void testNormalizeTimePartitionName() {
        new MockUp<TimeUtils>() {
            @Mock
            public  TimeZone getTimeZone() {
                return TimeZone.getTimeZone("GMT+6");
            }
        };
        // year
        // with time zone
        String partitionName = "2020";
        PartitionField partitionField = SPEC_D_2.fields().get(0);
        String result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATETIME);
        Assert.assertEquals("2020-01-01 06:00:00", result);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATE);
        Assert.assertEquals("2020-01-01", result);
        // without time zone
        partitionField = SPEC_E_2.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_E,
                Type.DATETIME);
        Assert.assertEquals("2020-01-01 00:00:00", result);

        // month
        // with time zone
        partitionName = "2020-02";
        partitionField = SPEC_D_3.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATETIME);
        Assert.assertEquals("2020-02-01 06:00:00", result);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATE);
        Assert.assertEquals("2020-02-01", result);
        // without time zone
        partitionField = SPEC_E_3.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_E,
                Type.DATETIME);
        Assert.assertEquals("2020-02-01 00:00:00", result);

        // day
        // with time zone
        partitionName = "2020-01-02";
        partitionField = SPEC_D_4.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATETIME);
        Assert.assertEquals("2020-01-02 06:00:00", result);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATE);
        Assert.assertEquals("2020-01-02", result);
        // without time zone
        partitionField = SPEC_E_4.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_E,
                Type.DATETIME);
        Assert.assertEquals("2020-01-02 00:00:00", result);

        // hour
        partitionName = "2020-01-02-12";
        partitionField = SPEC_D_5.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATETIME);
        Assert.assertEquals("2020-01-02 18:00:00", result);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_D,
                Type.DATE);
        Assert.assertEquals("2020-01-02", result);
        // without time zone
        partitionField = SPEC_E_5.fields().get(0);
        result = IcebergPartitionUtils.normalizeTimePartitionName(partitionName, partitionField, SCHEMA_E,
                Type.DATETIME);
        Assert.assertEquals("2020-01-02 12:00:00", result);
    }
}
