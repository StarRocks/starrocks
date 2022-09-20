// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveMetaStoreTableInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
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

    HiveMetaStoreTableInfo hmsTable = new HiveMetaStoreTableInfo("resource", "db", "tbl",
            partColumnNames, null, constructNameToColumn(), Table.TableType.HIVE);

    public Map<String, Column> constructNameToColumn() {
        Map<String, Column> nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Column col : partColumns) {
            nameToColumn.put(col.getName(), col);
        }
        return nameToColumn;
    }

    @Test
    public void testGetPartitionKeys() throws Exception {
        HiveMetaClient metaClient = new MockedHiveMetaClient();
        HiveMetaCache metaCache = new HiveMetaCache(metaClient, Executors.newFixedThreadPool(10));
        ImmutableMap<PartitionKey, Long> partitionKeys = metaCache.getPartitionKeys(hmsTable);

        Assert.assertEquals(3, partitionKeys.size());
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns)));
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "4"), partColumns)));
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "5"), partColumns)));

        partitionKeys = metaCache.getPartitionKeys(hmsTable);
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

        HivePartition partition = metaCache.getPartition(hmsTable,
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        Assert.assertEquals(1, partition.getFiles().size());
        Assert.assertEquals(RemoteFileInputFormat.PARQUET, partition.getFormat());
        Assert.assertEquals(partitionPath, partition.getFullPath());

        partition = metaCache.getPartition(hmsTable,
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        Assert.assertEquals(1, partition.getFiles().size());
        Assert.assertEquals(RemoteFileInputFormat.PARQUET, partition.getFormat());
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

        HivePartitionStats partitionStats = metaCache.getPartitionStats(hmsTable,
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        Assert.assertEquals(10000L, partitionStats.getNumRows());
        Assert.assertEquals(10000L, partitionStats.getTotalFileBytes());

        partitionStats = metaCache.getPartitionStats(hmsTable,
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        Assert.assertEquals(10000L, partitionStats.getNumRows());
        Assert.assertEquals(10000L, partitionStats.getTotalFileBytes());

        Assert.assertEquals(1, clientMethodGetPartitionStatsCalledTimes);
    }

    @Test
    public void testAddPartitionByEvent() throws Exception {
        HiveMetaClient metaClient = new MockedHiveMetaClient();
        HiveMetaCache metaCache = new HiveMetaCache(metaClient, Executors.newFixedThreadPool(10), "resource");
        ImmutableMap<PartitionKey, Long> partitionKeys = metaCache.getPartitionKeys(hmsTable);
        Assert.assertEquals(3, partitionKeys.size());
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns)));
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "4"), partColumns)));
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "5"), partColumns)));

        HivePartitionKeysKey newPartitionKeysKey =
                new HivePartitionKeysKey("db", "tbl", Table.TableType.HIVE, partColumns);
        List<String> partValues = Lists.newArrayList("11", "22", "33");
        PartitionKey newPartitionKey = Utils.createPartitionKey(partValues, partColumns);
        HivePartitionName newHivePartitionKey = new HivePartitionName("db", "tbl", Table.TableType.HIVE, partValues);
        metaCache.addPartitionKeyByEvent(newPartitionKeysKey, newPartitionKey, newHivePartitionKey);
        partitionKeys = metaCache.getPartitionKeys(hmsTable);
        Assert.assertEquals(4, partitionKeys.size());
        Assert.assertTrue(partitionKeys.containsKey(newPartitionKey));
        Assert.assertFalse(metaCache.partitionExistInCache(newHivePartitionKey));
    }

    @Test
    public void testAlterPartitionByEvent() throws Exception {
        HiveMetaClient metaClient = new MockedHiveMetaClient();
        HiveMetaCache metaCache = new HiveMetaCache(metaClient, Executors.newFixedThreadPool(10), "resource");
        HivePartitionStats partitionStats = metaCache.getPartitionStats(hmsTable,
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        Assert.assertEquals(10000L, partitionStats.getNumRows());
        Assert.assertEquals(10000L, partitionStats.getTotalFileBytes());

        long ts = System.currentTimeMillis();
        String path = "/tmp/" + ts;
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdir();
        }
        File file = new File(dir + "/test_event");
        file.createNewFile();
        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            outputStream.write(1111);
        }

        Map<String, String> params = Maps.newHashMap();
        params.put("numRows", "5");
        List<String> partValues = Lists.newArrayList("1", "2", "3");
        HivePartitionName partitionKey = new HivePartitionName("db", "tbl", Table.TableType.HIVE, partValues);
        StorageDescriptor sd = new StorageDescriptor();
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setParameters(Maps.newHashMap());
        sd.setSerdeInfo(serDeInfo);
        sd.setLocation("file:/tmp/" + ts);
        metaCache.alterPartitionByEvent(partitionKey, sd, params);

        partitionStats = metaCache.getPartitionStats(hmsTable,
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        HivePartition partition = metaCache.getPartition(hmsTable,
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        Assert.assertTrue(metaCache.partitionExistInCache(partitionKey));
        Assert.assertEquals(5, partitionStats.getNumRows());
        Assert.assertSame(partition.getFormat(), RemoteFileInputFormat.TEXT);
        long totalSize = partition.getFiles().stream().mapToLong(HdfsFileDesc::getLength).sum();
        Assert.assertEquals(partitionStats.getTotalFileBytes(), totalSize);
        file.delete();
        dir.delete();
    }

    @Test
    public void testDropPartitionByEvent() throws Exception {
        HiveMetaClient metaClient = new MockedHiveMetaClient();
        HiveMetaCache metaCache = new HiveMetaCache(metaClient, Executors.newFixedThreadPool(10), "resource");
        ImmutableMap<PartitionKey, Long> partitionKeys = metaCache.getPartitionKeys(hmsTable);
        Assert.assertEquals(3, partitionKeys.size());
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns)));
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "4"), partColumns)));
        Assert.assertTrue(
                partitionKeys.containsKey(Utils.createPartitionKey(Lists.newArrayList("1", "2", "5"), partColumns)));

        HivePartitionKeysKey dropPartitionKeysKey =
                new HivePartitionKeysKey("db", "tbl", Table.TableType.HIVE, partColumns);
        List<String> partValues = Lists.newArrayList("1", "2", "3");
        PartitionKey dropPartitionKey = Utils.createPartitionKey(partValues, partColumns);
        HivePartitionName dropHivePartitionKey = new HivePartitionName("db", "tbl", Table.TableType.HIVE, partValues);
        metaCache.dropPartitionKeyByEvent(dropPartitionKeysKey, dropPartitionKey, dropHivePartitionKey);
        partitionKeys = metaCache.getPartitionKeys(hmsTable);
        Assert.assertEquals(2, partitionKeys.size());
        Assert.assertFalse(partitionKeys.containsKey(dropPartitionKey));
        Assert.assertFalse(metaCache.partitionExistInCache(dropHivePartitionKey));
    }

    @Test
    public void clearCache() throws Exception {
        HiveMetaClient metaClient = new MockedHiveMetaClient();
        HiveMetaCache metaCache = new HiveMetaCache(metaClient, Executors.newFixedThreadPool(10), "resource");

        metaCache.getPartitionKeys(hmsTable);
        metaCache.getPartition(hmsTable,
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        metaCache.getTableStats("db", "tbl");

        metaCache.getPartitionStats(hmsTable,
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));

        Assert.assertEquals(1, clientMethodGetPartitionKeysCalledTimes);
        Assert.assertEquals(1, clientMethodGetPartitionCalledTimes);
        Assert.assertEquals(1, clientMethodGetTableStatsCalledTimes);
        Assert.assertEquals(1, clientMethodGetPartitionStatsCalledTimes);

        metaCache.clearCache(hmsTable);

        metaCache.getPartitionKeys(hmsTable);
        metaCache.getPartition(hmsTable,
                Utils.createPartitionKey(Lists.newArrayList("1", "2", "3"), partColumns));
        metaCache.getTableStats("db", "tbl");

        metaCache.getPartitionStats(hmsTable,
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

        public CurrentNotificationEventId getCurrentNotificationEventId() {
            return new CurrentNotificationEventId(1L);
        }

        @Override
        public Map<PartitionKey, Long> getPartitionKeys(String dbName, String tableName, List<Column> partColumns,
                                                        boolean isHudiTable)
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
            return new HivePartition(RemoteFileInputFormat.PARQUET,
                    ImmutableList.of(new HdfsFileDesc("file1",
                            "",
                            10000L,
                            ImmutableList.of(),
                            ImmutableList.of(),
                            false,
                            null)),
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



