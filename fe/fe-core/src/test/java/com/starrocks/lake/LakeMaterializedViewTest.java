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

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedView.MvRefreshScheme;
import com.starrocks.catalog.MaterializedView.RefreshType;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.FastByteArrayOutputStream;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class LakeMaterializedViewTest {

    @Test
    public void testMaterializedView() throws IOException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            int getCurrentStateJournalVersion() {
                return FeConstants.META_VERSION;
            }
        };

        long dbId = 1L;
        long mvId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tablet1Id = 10L;
        long tablet2Id = 11L;

        // Schema
        List<Column> columns = Lists.newArrayList();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

        // Tablet
        Tablet tablet1 = new LakeTablet(tablet1Id);
        Tablet tablet2 = new LakeTablet(tablet2Id);

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, mvId, partitionId, indexId, 0, TStorageMedium.HDD, true);
        index.addTablet(tablet1, tabletMeta);
        index.addTablet(tablet2, tabletMeta);

        // Partition
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(partitionId, (short) 3);
        Partition partition = new Partition(partitionId, "p1", index, distributionInfo);

        // refresh scheme
        MvRefreshScheme mvRefreshScheme = new MvRefreshScheme();
        mvRefreshScheme.setType(RefreshType.SYNC);

        // Lake mv
        LakeMaterializedView mv = new LakeMaterializedView(mvId, dbId, "mv1", columns, KeysType.AGG_KEYS,
                partitionInfo, distributionInfo, mvRefreshScheme);
        Deencapsulation.setField(mv, "baseIndexId", indexId);
        mv.addPartition(partition);
        mv.setIndexMeta(indexId, "mv1", columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.AGG_KEYS);

        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();

        S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
        s3FsBuilder.setBucket("test-bucket");
        s3FsBuilder.setRegion("test-region");
        S3FileStoreInfo s3FsInfo = s3FsBuilder.build();

        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("test-bucket");
        fsBuilder.setS3FsInfo(s3FsInfo);
        FileStoreInfo fsInfo = fsBuilder.build();

        builder.setFsInfo(fsInfo);
        builder.setFullPath("s3://test-bucket/1/");
        FilePathInfo pathInfo = builder.build();
        mv.setStorageInfo(pathInfo, true, 3600, true);

        // Test serialize and deserialize
        FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(byteArrayOutputStream)) {
            mv.write(out);
            out.flush();
        }

        Table newTable = null;
        try (DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream())) {
            newTable = Table.read(in);
        }
        byteArrayOutputStream.close();

        // Check lake mv and lake tablet
        Assert.assertTrue(newTable.isLakeMaterializedView());
        Assert.assertTrue(newTable.isCloudNativeTable());
        LakeMaterializedView newMv = (LakeMaterializedView) newTable;

        Assert.assertEquals("s3://test-bucket/1/", newMv.getStorageGroup());
        FileCacheInfo cacheInfo = newMv.getPartitionFileCacheInfo(partitionId);
        Assert.assertTrue(cacheInfo.getEnableCache());
        Assert.assertEquals(3600, cacheInfo.getTtlSeconds());
        Assert.assertTrue(cacheInfo.getAsyncWriteBack());

        Partition p1 = newMv.getPartition(partitionId);
        MaterializedIndex newIndex = p1.getBaseIndex();
        long expectedTabletId = 10L;
        for (Tablet tablet : newIndex.getTablets()) {
            Assert.assertTrue(tablet instanceof LakeTablet);
            LakeTablet lakeTablet = (LakeTablet) tablet;
            Assert.assertEquals(expectedTabletId, lakeTablet.getId());
            Assert.assertEquals(expectedTabletId, lakeTablet.getShardId());
            ++expectedTabletId;
        }

        // Test selectiveCopy
        MaterializedView newMv2 = mv.selectiveCopy(Lists.newArrayList("p1"), true, IndexExtState.ALL);
        Assert.assertTrue(newMv2.isLakeMaterializedView());
        Assert.assertEquals("s3://test-bucket/1/", newMv.getStorageGroup());
        cacheInfo = newMv.getPartitionFileCacheInfo(partitionId);
        Assert.assertTrue(cacheInfo.getEnableCache());
        Assert.assertEquals(3600, cacheInfo.getTtlSeconds());
        Assert.assertTrue(cacheInfo.getAsyncWriteBack());

        Assert.assertNull(mv.delete(true));
        Assert.assertNotNull(mv.delete(false));
    }
}
