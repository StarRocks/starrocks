// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.server;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class LocalMetaStoreTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1000;
        FeConstants.runningUnitTest = true;

        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable(
                        "CREATE TABLE test.t1(k1 int, k2 int, k3 int)" +
                                " distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
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
        Optional<ConnectorMetadata> metadataOpt = connectContext.getGlobalStateMgr()
                .getMetadataMgr().getOptionalMetadata(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        Assert.assertTrue(metadataOpt.isPresent());
        ConnectorMetadata metadata = metadataOpt.get();
        Assert.assertTrue(metadata instanceof LocalMetastore);
        LocalMetastore localMetastore = (LocalMetastore) metadata;
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
}
