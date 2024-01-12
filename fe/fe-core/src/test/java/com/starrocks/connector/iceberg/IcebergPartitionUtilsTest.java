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
import com.starrocks.catalog.IcebergTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Set;

public class IcebergPartitionUtilsTest {
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
    public void testIcebergGetAllPartition() {
        MockIcebergMetadata mockIcebergMetadata =
                (MockIcebergMetadata) connectContext.getGlobalStateMgr().getMetadataMgr().
                        getOptionalMetadata(MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME).get();

        IcebergTable icebergTable = (IcebergTable) GlobalStateMgr.getCurrentState().getMetadataMgr().
                getTable("iceberg0", "partitioned_db", "t1");
        mockIcebergMetadata.addRowsToPartition("partitioned_db", "t1", 100, "date=2020-01-02");
        mockIcebergMetadata.addRowsToPartition("partitioned_db", "t1", 100, "date=2020-01-03");
        Set<IcebergPartitionUtils.IcebergPartition> partitions = IcebergPartitionUtils.
                getAllPartition(icebergTable.getNativeTable());
        Assert.assertEquals(2, partitions.size());
    }
}
