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


package com.starrocks.server;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.catalog.system.sys.SysDb;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class LocalMetaStoreTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.alter_scheduler_interval_millisecond = 1000;
        FeConstants.runningUnitTest = true;

        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable(
                        "CREATE TABLE test.t1(k1 int, k2 int, k3 int)" +
                                " distributed by hash(k1) buckets 3 properties('replication_num' = '1');");

        UtFrameUtils.PseudoImage.setUpImageVersion();
    }

    @Test
    public void testGetNewPartitionsFromPartitions() throws DdlException {
        Database db = connectContext.getGlobalStateMgr().getDb("test");
        Table table = db.getTable("t1");
        Assert.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        Partition sourcePartition = olapTable.getPartition("t1");
        List<Long> sourcePartitionIds = Lists.newArrayList(sourcePartition.getId());
        List<Long> tmpPartitionIds = Lists.newArrayList(connectContext.getGlobalStateMgr().getNextId());
        LocalMetastore localMetastore = connectContext.getGlobalStateMgr().getLocalMetastore();
        Map<Long, String> origPartitions = Maps.newHashMap();
        OlapTable copiedTable = localMetastore.getCopiedTable(db, olapTable, sourcePartitionIds, origPartitions);
        Assert.assertEquals(olapTable.getName(), copiedTable.getName());
        Set<Long> tabletIdSet = Sets.newHashSet();
        List<Partition> newPartitions = localMetastore.getNewPartitionsFromPartitions(db,
                olapTable, sourcePartitionIds, origPartitions, copiedTable, "_100", tabletIdSet, tmpPartitionIds);
        Assert.assertEquals(sourcePartitionIds.size(), newPartitions.size());
        Assert.assertEquals(1, newPartitions.size());
        Partition newPartition = newPartitions.get(0);
        Assert.assertEquals("t1_100", newPartition.getName());
        olapTable.addTempPartition(newPartition);

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        partitionInfo.addPartition(newPartition.getId(), partitionInfo.getDataProperty(sourcePartition.getId()),
                partitionInfo.getReplicationNum(sourcePartition.getId()),
                partitionInfo.getIsInMemory(sourcePartition.getId()));
        olapTable.replacePartition("t1", "t1_100");

        Assert.assertEquals(newPartition.getId(), olapTable.getPartition("t1").getId());
    }

    @Test
    public void testGetPartitionIdToStorageMediumMap() throws Exception {
        starRocksAssert.withMaterializedView(
                "CREATE MATERIALIZED VIEW test.mv1\n" +
                        "distributed by hash(k1) buckets 3\n" +
                        "refresh async\n" +
                        "properties(\n" +
                        "'replication_num' = '1'\n" +
                        ")\n" +
                        "as\n" +
                        "select k1,k2 from test.t1;");
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        new MockUp<PartitionInfo>() {
            @Mock
            public DataProperty getDataProperty(long partitionId) {
                return new DataProperty(TStorageMedium.SSD, 0);
            }
        };
        new MockUp<EditLog>() {
            @Mock
            public void logModifyPartition(ModifyPartitionInfo info) {
                Assert.assertNotNull(info);
                Assert.assertTrue(db.getTable(info.getTableId()).isOlapTableOrMaterializedView());
                Assert.assertEquals(TStorageMedium.HDD, info.getDataProperty().getStorageMedium());
                Assert.assertEquals(DataProperty.MAX_COOLDOWN_TIME_MS, info.getDataProperty().getCooldownTimeMs());
            }
        };

        LocalMetastore localMetastore = connectContext.getGlobalStateMgr().getLocalMetastore();
        localMetastore.getPartitionIdToStorageMediumMap();
    }

    @Test
    public void testLoadClusterV2() throws Exception {
        LocalMetastore localMetaStore = new LocalMetastore(GlobalStateMgr.getCurrentState(),
                GlobalStateMgr.getCurrentRecycleBin(),
                GlobalStateMgr.getCurrentColocateIndex());

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        localMetaStore.save(image.getDataOutputStream());

        SRMetaBlockReader reader = new SRMetaBlockReader(image.getDataInputStream());
        localMetaStore.load(reader);
        reader.close();

        Assert.assertNotNull(localMetaStore.getDb(SystemId.INFORMATION_SCHEMA_DB_ID));
        Assert.assertNotNull(localMetaStore.getDb(InfoSchemaDb.DATABASE_NAME));
        Assert.assertNotNull(localMetaStore.getDb(SystemId.SYS_DB_ID));
        Assert.assertNotNull(localMetaStore.getDb(SysDb.DATABASE_NAME));
    }
}
