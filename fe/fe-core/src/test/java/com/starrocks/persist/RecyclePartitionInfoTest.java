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


package com.starrocks.persist;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.meta.MetaContext;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.thrift.TStorageMedium;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;

public class RecyclePartitionInfoTest {
    @Test
    public void testRangeSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_89);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File("./RecycleRangePartitionInfo");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        List<Column> columns = Lists.newArrayList(new Column("k1", Type.INT));
        Range<PartitionKey> range =
                Range.range(PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")), columns),
                        BoundType.CLOSED,
                        PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("3")), columns),
                        BoundType.CLOSED);
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        Partition partition = new Partition(1L, "p1", new MaterializedIndex(), null);

        CatalogRecycleBin.RecycleRangePartitionInfo info1 = new CatalogRecycleBin.RecycleRangePartitionInfo(11L, 22L,
                partition, range, dataProperty, (short) 1, false, null);
        info1.write(dos);

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        Assert.assertEquals(-1L, dis.readLong());
        CatalogRecycleBin.RecycleRangePartitionInfo rInfo1 =
                (CatalogRecycleBin.RecycleRangePartitionInfo) CatalogRecycleBin.RecyclePartitionInfoV2.read(dis);

        Assert.assertEquals(11L, rInfo1.getDbId());
        Assert.assertEquals(22L, rInfo1.getTableId());

        Assert.assertEquals(range, rInfo1.getRange());

        dos.flush();
        dos.close();
    }

    @Test
    public void testSerializationV1() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_89);
        metaContext.setThreadLocalInfo();

        // 1. Write objects to file
        File file = new File("./RecyclePartitionInfoV1");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        List<Column> columns = Lists.newArrayList(new Column("k1", Type.INT));
        Range<PartitionKey> range =
                Range.range(PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("1")), columns),
                        BoundType.CLOSED,
                        PartitionKey.createPartitionKey(Lists.newArrayList(new PartitionValue("3")), columns),
                        BoundType.CLOSED);
        DataProperty dataProperty = new DataProperty(TStorageMedium.HDD);
        DistributionInfo distributionInfo = new RandomDistributionInfo(32);
        Partition partition = new Partition(1L, "p1", new MaterializedIndex(), distributionInfo);

        CatalogRecycleBin.RecyclePartitionInfoV1 info1 = new CatalogRecycleBin.RecyclePartitionInfoV1(11L, 22L,
                partition, range, dataProperty, (short) 1, false);
        info1.write(dos);

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        Assert.assertEquals(11L, dis.readLong());

        CatalogRecycleBin.RecyclePartitionInfoV1 rinfo1 = new CatalogRecycleBin.RecyclePartitionInfoV1();
        rinfo1.readFields(dis);

        Assert.assertEquals(22L, rinfo1.getTableId());

        dos.flush();
        dos.close();
    }
}
