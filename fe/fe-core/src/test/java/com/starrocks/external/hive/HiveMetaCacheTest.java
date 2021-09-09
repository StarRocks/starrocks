// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class HiveMetaCacheTest {
    private List<Column> partColumns = Lists.newArrayList(new Column("k1", Type.INT),
            new Column("k2", Type.INT),
            new Column("k3", Type.INT));
    private List<String> partColumnNames = Lists.newArrayList("k1", "k2", "k3");

    private int clientMethodGetPartitionKeysCalledTimes = 0;
    private int clientMethodGetPartitionCalledTimes = 0;
    private int clientMethodGetTableStatsCalledTimes = 0;
    private int clientMethodGetPartitionStatsCalledTimes = 0;
    private String partitionPath = "hdfs://nameservice1/hive/db/tbl/k1=1/k2=1/k3=3";

    @Test
    public void testGetPartitionKeys() throws Exception {
        HiveMetaClient metaClient = new MockedHiveMetaClient();
        HiveMetaCache metaCache = new HiveMetaCache(metaClient, Executors.newFixedThreadPool(10));
        ImmutableMap<PartitionKey, Long> partitionKeys = metaCache.getPartitionKeys("db", "tbl", partColumns);

        Assert.assertEquals(3, partitionKeys.size());
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns)));
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "4"), partColumns)));
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "5"), partColumns)));

        partitionKeys = metaCache.getPartitionKeys("db", "tbl", partColumns);
        Assert.assertEquals(3, partitionKeys.size());
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns)));
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "4"), partColumns)));
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "5"), partColumns)));

        Assert.assertEquals(1, clientMethodGetPartitionKeysCalledTimes);
    }

    @Test
    public void testGetPartition() throws Exception {
        HiveMetaClient metaClient = new MockedHiveMetaClient();
        HiveMetaCache metaCache = new HiveMetaCache(metaClient, Executors.newFixedThreadPool(10));

        HivePartition partition = metaCache.getPartition("db", "tbl",
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        Assert.assertEquals(1, partition.getFiles().size());
        Assert.assertEquals(HdfsFileFormat.PARQUET, partition.getFormat());
        Assert.assertEquals(partitionPath, partition.getFullPath());

        partition = metaCache.getPartition("db", "tbl",
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        Assert.assertEquals(1, partition.getFiles().size());
        Assert.assertEquals(HdfsFileFormat.PARQUET, partition.getFormat());
        Assert.assertEquals(partitionPath, partition.getFullPath());

        Assert.assertEquals(1, clientMethodGetPartitionCalledTimes);
    }

    @Test
    public void testGetTableStats() throws Exception {
        HiveMetaClient metaClient = new MockedHiveMetaClient();
        HiveMetaCache metaCache = new HiveMetaCache(metaClient, Executors.newFixedThreadPool(10));

        HiveTableStats tableStats = metaCache.getTableStats("db", "tbl");
        Assert.assertEquals(100L, tableStats.getNumRows());
        Assert.assertEquals(10000L, tableStats.getTotalFileBytes());

        tableStats = metaCache.getTableStats("db", "tbl");
        Assert.assertEquals(100L, tableStats.getNumRows());
        Assert.assertEquals(10000L, tableStats.getTotalFileBytes());

        Assert.assertEquals(1, clientMethodGetTableStatsCalledTimes);
    }

    @Test
    public void testGetPartitionStats() throws Exception {
        HiveMetaClient metaClient = new MockedHiveMetaClient();
        HiveMetaCache metaCache = new HiveMetaCache(metaClient, Executors.newFixedThreadPool(10));

        HivePartitionStats partitionStats = metaCache.getPartitionStats("db", "tbl",
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        Assert.assertEquals(10000L, partitionStats.getNumRows());
        Assert.assertEquals(10000L, partitionStats.getTotalFileBytes());

        partitionStats = metaCache.getPartitionStats("db", "tbl",
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        Assert.assertEquals(10000L, partitionStats.getNumRows());
        Assert.assertEquals(10000L, partitionStats.getTotalFileBytes());

        Assert.assertEquals(1, clientMethodGetPartitionStatsCalledTimes);
    }

    @Test
    public void clearCache() throws Exception {
        HiveMetaClient metaClient = new MockedHiveMetaClient();
        HiveMetaCache metaCache = new HiveMetaCache(metaClient, Executors.newFixedThreadPool(10));

        metaCache.getPartitionKeys("db", "tbl", partColumns);
        metaCache.getPartition("db", "tbl",
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        metaCache.getTableStats("db", "tbl");

        metaCache.getPartitionStats("db", "tbl",
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));

        Assert.assertEquals(1, clientMethodGetPartitionKeysCalledTimes);
        Assert.assertEquals(1, clientMethodGetPartitionCalledTimes);
        Assert.assertEquals(1, clientMethodGetTableStatsCalledTimes);
        Assert.assertEquals(1, clientMethodGetPartitionStatsCalledTimes);

        metaCache.clearCache("db", "tbl");

        metaCache.getPartitionKeys("db", "tbl", partColumns);
        metaCache.getPartition("db", "tbl",
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        metaCache.getTableStats("db", "tbl");

        metaCache.getPartitionStats("db", "tbl",
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));

        Assert.assertEquals(2, clientMethodGetPartitionKeysCalledTimes);
        Assert.assertEquals(2, clientMethodGetPartitionCalledTimes);
        Assert.assertEquals(2, clientMethodGetTableStatsCalledTimes);
        Assert.assertEquals(2, clientMethodGetPartitionStatsCalledTimes);
    }

    public class MockedHiveMetaClient extends HiveMetaClient {
        public MockedHiveMetaClient() throws DdlException {
            super("");
        }

        public CurrentNotificationEventId getCurrentNotificationEventId() throws DdlException {
            return new CurrentNotificationEventId(1L);
        }

        @Override
        public Map<PartitionKey, Long> getPartitionKeys(String dbName, String tableName, List<Column> partColumns)
                throws DdlException {
            clientMethodGetPartitionKeysCalledTimes++;
            try {
                Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();
                partitionKeys.put(Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns), 0L);
                partitionKeys.put(Utils.createPartitionKey(Lists.newArrayList("1", "2", "4"), partColumns), 1L);
                partitionKeys.put(Utils.createPartitionKey(Lists.newArrayList("1", "2", "5"), partColumns), 2L);
                return partitionKeys;
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
        }

        @Override
        public HivePartition getPartition(String dbName, String tableName, List<String> partValues)
                throws DdlException {
            clientMethodGetPartitionCalledTimes++;
            return new HivePartition(HdfsFileFormat.PARQUET,
                    ImmutableList.of(new HdfsFileDesc("file1",
                            "",
                            10000L,
                            ImmutableList.of())),
                    partitionPath);
        }

        @Override
        public HiveTableStats getTableStats(String dbName, String tableName) throws DdlException {
            clientMethodGetTableStatsCalledTimes++;
            return new HiveTableStats(100L, 10000L);
        }

        @Override
        public HivePartitionStats getPartitionStats(String dbName, String tableName, List<String> partValues)
                throws DdlException {
            clientMethodGetPartitionStatsCalledTimes++;
            return new HivePartitionStats(10000L);
        }
    }
}



