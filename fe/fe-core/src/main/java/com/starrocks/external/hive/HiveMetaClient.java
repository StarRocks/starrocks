// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.external.ObejctStorageUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class HiveMetaClient {
    private static final Logger LOG = LogManager.getLogger(HiveMetaClient.class);
    public static final String PARTITION_NULL_VALUE = "__HIVE_DEFAULT_PARTITION__";
    // Maximum number of idle metastore connections in the connection pool at any point.
    private static final int MAX_HMS_CONNECTION_POOL_SIZE = 32;

    private final LinkedList<AutoCloseClient> clientPool = new LinkedList<>();
    private final Object clientPoolLock = new Object();

    private final HiveConf conf;

    Map<String, FileSystem> fileSystems = Maps.newHashMap();
    ReadWriteLock fileSystemsLock = new ReentrantReadWriteLock();
    // blockHost is ip:port
    private final Map<String, Long> blockHostToId = new ConcurrentHashMap<>();
    private final Map<Long, String> idToBlockHost = new ConcurrentHashMap<>();
    private long hostId = 0;
    private final Map<Integer, Long> storageHashToId = new ConcurrentHashMap<>();
    private long storageId = 0;
    private static final int UNKNOWN_STORAGE_ID = -1;
    private final AtomicLong partitionIdGen = new AtomicLong(0L);

    // Required for creating an instance of RetryingMetaStoreClient.
    private static final HiveMetaHookLoader dummyHookLoader = tbl -> null;

    public HiveMetaClient(String uris) {
        HiveConf conf = new HiveConf();
        conf.set("hive.metastore.uris", uris);
        conf.set(MetastoreConf.ConfVars.CLIENT_SOCKET_TIMEOUT.getHiveName(),
                String.valueOf(Config.hive_meta_store_timeout_s));
        this.conf = conf;
    }

    public class AutoCloseClient implements AutoCloseable {
        private final IMetaStoreClient hiveClient;

        private AutoCloseClient(HiveConf conf) throws MetaException {
            hiveClient = RetryingMetaStoreClient.getProxy(conf, dummyHookLoader,
                    HiveMetaStoreThriftClient.class.getName());
        }

        @Override
        public void close() {
            synchronized (clientPoolLock) {
                if (clientPool.size() >= MAX_HMS_CONNECTION_POOL_SIZE) {
                    hiveClient.close();
                } else {
                    clientPool.offer(this);
                }
            }
        }
    }

    private AutoCloseClient getClient() throws MetaException {
        // The MetaStoreClient c'tor relies on knowing the Hadoop version by asking
        // org.apache.hadoop.util.VersionInfo. The VersionInfo class relies on opening
        // the 'common-version-info.properties' file as a resource from hadoop-common*.jar
        // using the Thread's context classloader. If necessary, set the Thread's context
        // classloader, otherwise VersionInfo will fail in it's c'tor.
        if (Thread.currentThread().getContextClassLoader() == null) {
            Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
        }

        synchronized (clientPoolLock) {
            AutoCloseClient client = clientPool.poll();
            // The pool was empty so create a new client and return that.
            // Serialize client creation to defend against possible race conditions accessing
            // local Kerberos state
            if (client == null) {
                return new AutoCloseClient(conf);
            } else {
                return client;
            }
        }
    }

    public Table getTable(String dbName, String tableName) throws DdlException {
        try (AutoCloseClient client = getClient()) {
            return client.hiveClient.getTable(dbName, tableName);
        } catch (Exception e) {
            LOG.warn("get hive table failed", e);
            throw new DdlException("get hive table from meta store failed: " + e.getMessage());
        }
    }

    public Map<PartitionKey, Long> getPartitionKeys(String dbName, String tableName, List<Column> partColumns)
            throws DdlException {
        try (AutoCloseClient client = getClient()) {
            Table table = client.hiveClient.getTable(dbName, tableName);
            // partitionKeysSize > 0 means table is a partition table
            if (table.getPartitionKeysSize() > 0) {
                List<String> partNames = client.hiveClient.listPartitionNames(dbName, tableName, (short) -1);
                Map<PartitionKey, Long> partitionKeys = Maps.newHashMapWithExpectedSize(partNames.size());
                for (String partName : partNames) {
                    List<String> values = client.hiveClient.partitionNameToVals(partName);
                    PartitionKey partitionKey = Utils.createPartitionKey(values, partColumns);
                    partitionKeys.put(partitionKey, nextPartitionId());
                }
                return partitionKeys;
            } else {
                Map<PartitionKey, Long> partitionKeys = Maps.newHashMapWithExpectedSize(1);
                partitionKeys.put(new PartitionKey(), nextPartitionId());
                return partitionKeys;
            }
        } catch (Exception e) {
            LOG.warn("get partition keys failed", e);
            throw new DdlException("get hive partition keys from meta store failed: " + e.getMessage());
        }
    }

    public List<String> partitionNameToVals(String partName) throws DdlException {
        try (AutoCloseClient client = getClient()) {
            return client.hiveClient.partitionNameToVals(partName);
        } catch (Exception e) {
            LOG.warn("convert partitionName to vals failed", e);
            throw new DdlException("convert partition name to vals failed: " + e.getMessage());
        }
    }

    public HivePartition getPartition(String dbName, String tableName, List<String> partValues) throws DdlException {
        try (AutoCloseClient client = getClient()) {
            StorageDescriptor sd;
            if (partValues.size() > 0) {
                Partition partition = client.hiveClient.getPartition(dbName, tableName, partValues);
                sd = partition.getSd();
            } else {
                Table table = client.hiveClient.getTable(dbName, tableName);
                sd = table.getSd();
            }
            HdfsFileFormat format = HdfsFileFormat.fromHdfsInputFormatClass(sd.getInputFormat());
            if (format == null) {
                throw new DdlException("unsupported file format [" + sd.getInputFormat() + "]");
            }

            String path = ObejctStorageUtils.formatObjectStoragePath(sd.getLocation());
            List<HdfsFileDesc> fileDescs = getHdfsFileDescs(path,
                    ObejctStorageUtils.isObjectStorage(path) || HdfsFileFormat.isSplittable(sd.getInputFormat()));
            return new HivePartition(format, ImmutableList.copyOf(fileDescs), path);
        } catch (NoSuchObjectException e) {
            throw new DdlException("get hive partition meta data failed: "
                    + "partition not exists, partValues: "
                    + String.join(",", partValues));
        } catch (Exception e) {
            LOG.warn("get partition failed", e);
            throw new DdlException("get hive partition meta data failed: " + e.getMessage());
        }
    }

    public HiveTableStats getTableStats(String dbName, String tableName) throws DdlException {
        try (AutoCloseClient client = getClient()) {
            Table table = client.hiveClient.getTable(dbName, tableName);
            Map<String, String> parameters = table.getParameters();
            return new HiveTableStats(Utils.getRowCount(parameters), Utils.getTotalSize(parameters));
        } catch (Exception e) {
            LOG.warn("get table stats failed", e);
            throw new DdlException("get hive table stats from meta store failed: " + e.getMessage());
        }
    }

    public HivePartitionStats getPartitionStats(String dbName, String tableName, List<String> partValues)
            throws DdlException {
        try (AutoCloseClient client = getClient()) {
            Map<String, String> parameters;
            if (partValues.size() > 0) {
                Partition partition = client.hiveClient.getPartition(dbName, tableName, partValues);
                parameters = partition.getParameters();
            } else {
                Table table = client.hiveClient.getTable(dbName, tableName);
                parameters = table.getParameters();
            }
            return new HivePartitionStats(Utils.getRowCount(parameters));
        } catch (Exception e) {
            LOG.warn("get partition stats failed", e);
            throw new DdlException("get hive partition stats from hive metastore failed: " + e.getMessage());
        }
    }

    // columnName -> HiveColumnStats
    public Map<String, HiveColumnStats> getTableLevelColumnStatsForUnpartTable(String dbName, String tableName,
                                                                               List<String> columnNames)
            throws DdlException {
        try (AutoCloseClient client = getClient()) {
            List<ColumnStatisticsObj> stats =
                    client.hiveClient.getTableColumnStatistics(dbName, tableName, columnNames);
            Map<String, HiveColumnStats> statsMap = Maps.newHashMapWithExpectedSize(stats.size());
            for (ColumnStatisticsObj statsObj : stats) {
                HiveColumnStats hiveColumnStats = new HiveColumnStats();
                if (!hiveColumnStats.init(statsObj.getColType(), statsObj.getStatsData())) {
                    LOG.warn("hive column get statistics failed, column name {}, column type {}",
                            statsObj.getColName(), statsObj.getColType());
                }
                statsMap.put(statsObj.getColName(), hiveColumnStats);
            }
            for (String columnName : columnNames) {
                if (!statsMap.containsKey(columnName)) {
                    statsMap.put(columnName, new HiveColumnStats());
                }
            }
            return statsMap;
        } catch (Exception e) {
            LOG.warn("get table level column stats for unpartition table failed", e);
            throw new DdlException("get table column statistics from hive metastore failed: " + e.getMessage());
        }
    }

    // columnName -> HiveColumnStats
    public Map<String, HiveColumnStats> getTableLevelColumnStatsForPartTable(String dbName, String tableName,
                                                                             List<PartitionKey> partitionKeys,
                                                                             List<Column> partitionColumns,
                                                                             List<String> columnNames)
            throws DdlException {
        // calculate partition names
        List<String> partNames = Lists.newArrayListWithCapacity(partitionKeys.size());
        List<String> partColumnNames = partitionColumns.stream().map(Column::getName).collect(Collectors.toList());
        for (PartitionKey partitionKey : partitionKeys) {
            partNames.add(FileUtils.makePartName(partColumnNames, Utils.getPartitionValues(partitionKey)));
        }

        // get partition row number from metastore
        // partName => row number
        Map<String, Long> partRowNumbers = Maps.newHashMapWithExpectedSize(partNames.size());
        long tableRowNumber = 0L;
        List<Partition> partitions;
        try (AutoCloseClient client = getClient()) {
            partitions = client.hiveClient.getPartitionsByNames(dbName, tableName, partNames);
        } catch (Exception e) {
            LOG.warn("get table level column stats for partition table failed", e);
            throw new DdlException("get partitions from hive metastore failed: " + e.getMessage());
        }
        for (Partition partition : partitions) {
            String partName = FileUtils.makePartName(partColumnNames, partition.getValues());
            long rowNumber = Utils.getRowCount(partition.getParameters());
            partRowNumbers.put(partName, rowNumber);
            if (rowNumber != -1L) {
                tableRowNumber += rowNumber;
            }
        }
        // set to zero row number for not exists partition
        if (partRowNumbers.size() < partNames.size()) {
            for (String partName : partNames) {
                if (!partRowNumbers.containsKey(partName)) {
                    partRowNumbers.put(partName, 0L);
                }
            }
        }

        Map<String, HiveColumnStats> result = Maps.newHashMapWithExpectedSize(columnNames.size());

        // calculate non-partition-key column stats
        Map<String, List<ColumnStatisticsObj>> partitionColumnStats;
        try (AutoCloseClient client = getClient()) {
            // there is only non-partition-key column stats in hive metastore
            partitionColumnStats =
                    client.hiveClient.getPartitionColumnStatistics(dbName, tableName, partNames, columnNames);
        } catch (Exception e) {
            throw new DdlException("get partition column statistics from hive metastore failed: " + e.getMessage());
        }
        Map<String, Double> columnLengthSum = Maps.newHashMap();
        for (Map.Entry<String, List<ColumnStatisticsObj>> entry : partitionColumnStats.entrySet()) {
            String partName = entry.getKey();
            long partRowNumber = partRowNumbers.get(partName);
            for (ColumnStatisticsObj statisticsObj : entry.getValue()) {
                String colName = statisticsObj.getColName();
                String colType = statisticsObj.getColType();
                HiveColumnStats pStats = new HiveColumnStats();
                if (!pStats.init(colType, statisticsObj.getStatsData())) {
                    LOG.warn("init column statistics failed, columnName: {}, columnType: {}", colName, colType);
                }
                if (isStringType(colType)) {
                    columnLengthSum.compute(colName, (columnName, lengthSum) -> {
                        long notNullRowNumber;
                        if (pStats.getNumNulls() > 0) {
                            notNullRowNumber = Math.max(partRowNumber - pStats.getNumNulls(), 0);
                        } else {
                            notNullRowNumber = partRowNumber;
                        }

                        if (lengthSum == null) {
                            return pStats.getAvgSize() * notNullRowNumber;
                        } else {
                            return lengthSum + pStats.getAvgSize() * notNullRowNumber;
                        }
                    });
                }
                result.compute(colName, (columnName, tStats) -> {
                    if (tStats == null) {
                        return pStats;
                    } else {
                        tStats.addNumNulls(pStats.getNumNulls());
                        tStats.updateMinValue(pStats.getMinValue());
                        tStats.updateMaxValue(pStats.getMaxValue());
                        tStats.updateNumDistinctValues(pStats.getNumDistinctValues());
                        return tStats;
                    }
                });
            }
        }
        // set avgSize
        for (Map.Entry<String, Double> entry : columnLengthSum.entrySet()) {
            String columnName = entry.getKey();
            double lengthSum = entry.getValue();
            HiveColumnStats stats = result.get(columnName);
            long notNullRowNumber;
            if (stats.getNumNulls() > 0) {
                notNullRowNumber = tableRowNumber - stats.getNumNulls();
            } else {
                notNullRowNumber = tableRowNumber;
            }
            stats.setAvgSize(lengthSum / Math.max(notNullRowNumber, 1));
        }

        // calculate partition-key column stats
        Set<String> columnNamesSet = new HashSet<>(columnNames);
        for (int colIndex = 0; colIndex < partitionColumns.size(); colIndex++) {
            Column column = partitionColumns.get(colIndex);
            if (!columnNamesSet.contains(column.getName())) {
                continue;
            }

            HiveColumnStats stats = new HiveColumnStats();
            Set<String> distinctCnt = Sets.newHashSet();
            long numNulls = 0;
            double vLength = 0.0f;
            for (PartitionKey partitionKey : partitionKeys) {
                LiteralExpr literalExpr = partitionKey.getKeys().get(colIndex);
                String partName = FileUtils.makePartName(partColumnNames, Utils.getPartitionValues(partitionKey));
                Long partRowNumber = partRowNumbers.get(partName);
                if (literalExpr instanceof NullLiteral) {
                    distinctCnt.add(PARTITION_NULL_VALUE);
                    numNulls += partRowNumber;
                    continue;
                } else {
                    distinctCnt.add(literalExpr.getStringValue());
                }

                double value = getValueFromLiteral(literalExpr, column.getType());
                stats.updateMaxValue(value);
                stats.updateMinValue(value);
                if (column.getType().isStringType()) {
                    vLength += getLengthFromLiteral(literalExpr, column.getType()) * partRowNumber;
                }
            }
            if (column.getType().isStringType()) {
                stats.setAvgSize(vLength / Math.max(tableRowNumber - numNulls, 1));
            }
            stats.setNumDistinctValues(distinctCnt.size());
            stats.setNumNulls(numNulls);
            result.put(column.getName(), stats);
        }

        // set not exits column stats to default
        for (String columnName : columnNames) {
            if (!result.containsKey(columnName)) {
                result.put(columnName, new HiveColumnStats());
            }
        }

        return result;
    }

    private boolean isStringType(String hiveType) {
        hiveType = Utils.getTypeKeyword(hiveType);
        return hiveType.equalsIgnoreCase("string")
                || hiveType.equalsIgnoreCase("char")
                || hiveType.equalsIgnoreCase("varchar");
    }

    private double getValueFromLiteral(LiteralExpr literalExpr, Type type) {
        switch (type.getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return literalExpr.getLongValue();
            case LARGEINT:
            case FLOAT:
            case DOUBLE:
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return literalExpr.getDoubleValue();
            case DATE:
            case DATETIME:
                return (((DateLiteral) literalExpr).unixTimestamp(TimeZone.getDefault())) / 1000L;
            default:
                return Double.NaN;
        }
    }

    private double getLengthFromLiteral(LiteralExpr literalExpr, Type type) {
        switch (type.getPrimitiveType()) {
            case CHAR:
            case VARCHAR:
                return literalExpr.getStringValue().length();
            default:
                return type.getPrimitiveType().getSlotSize();
        }
    }

    private List<HdfsFileDesc> getHdfsFileDescs(String dirPath, boolean isSplittable) throws Exception {
        URI uri = new URI(dirPath);
        FileSystem fileSystem = getFileSystem(uri);
        List<HdfsFileDesc> fileDescs = Lists.newArrayList();
        // fileSystem.listLocatedStatus is an api to list all statuses and
        // block locations of the files in the given path in one operation.
        // The performance is better than getting status and block location one by one.
        try {
            RemoteIterator<LocatedFileStatus> blockIterator = fileSystem.listLocatedStatus(new Path(uri.getPath()));
            while (blockIterator.hasNext()) {
                LocatedFileStatus locatedFileStatus = blockIterator.next();
                if (!isValidDataFile(locatedFileStatus)) {
                    continue;
                }
                String fileName = Utils.getSuffixName(dirPath, locatedFileStatus.getPath().toString());
                BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
                List<HdfsFileBlockDesc> fileBlockDescs = getHdfsFileBlockDescs(blockLocations);
                fileDescs.add(new HdfsFileDesc(fileName, "", locatedFileStatus.getLen(),
                        ImmutableList.copyOf(fileBlockDescs), isSplittable));
            }
        } catch (FileNotFoundException ignored) {
            // hive empty partition may not create directory
        }
        return fileDescs;
    }

    private boolean isValidDataFile(FileStatus fileStatus) {
        if (fileStatus.isDirectory()) {
            return false;
        }

        String lcFileName = fileStatus.getPath().getName().toLowerCase();
        return !(lcFileName.startsWith(".") || lcFileName.startsWith("_") ||
                lcFileName.endsWith(".copying") || lcFileName.endsWith(".tmp"));
    }

    private List<HdfsFileBlockDesc> getHdfsFileBlockDescs(BlockLocation[] blockLocations) throws IOException {
        List<HdfsFileBlockDesc> fileBlockDescs = Lists.newArrayList();
        for (BlockLocation blockLocation : blockLocations) {
            fileBlockDescs.add(buildHdfsFileBlockDesc(
                    blockLocation.getOffset(),
                    blockLocation.getLength(),
                    getReplicaHostIds(blockLocation.getNames()))
            );
        }
        return fileBlockDescs;
    }

    private long[] getReplicaHostIds(String[] hostNames) {
        long[] replicaHostIds = new long[hostNames.length];
        for (int j = 0; j < hostNames.length; j++) {
            String name = hostNames[j];
            replicaHostIds[j] = getHostId(name);
        }
        return replicaHostIds;
    }

    private HdfsFileBlockDesc buildHdfsFileBlockDesc(long offset, long length, long[] replicaHostIds) {
        return new HdfsFileBlockDesc(offset,
                length,
                replicaHostIds,
                // TODO get storageId through blockStorageLocation.getVolumeIds()
                // because this function is a rpc call, we give a fake value now.
                // Set it to real value, when planner needs this param.
                new long[] {UNKNOWN_STORAGE_ID},
                this);
    }

    private FileSystem getFileSystem(URI uri) throws IOException {
        String key = String.format("%s:%d", uri.getHost(), uri.getPort());
        fileSystemsLock.readLock().lock();
        FileSystem fileSystem = fileSystems.get(key);
        fileSystemsLock.readLock().unlock();
        if (fileSystem != null) {
            return fileSystem;
        }

        fileSystemsLock.writeLock().lock();
        fileSystem = fileSystems.get(key);
        if (fileSystem != null) {
            fileSystemsLock.writeLock().unlock();
            return fileSystem;
        }
        try {
            Configuration conf = new Configuration();
            fileSystem = FileSystem.get(uri, conf);
            fileSystems.put(key, fileSystem);
            return fileSystem;
        } finally {
            fileSystemsLock.writeLock().unlock();
        }
    }

    private long getHostId(String hostName) {
        return blockHostToId.computeIfAbsent(hostName, k -> {
            long newId = hostId++;
            idToBlockHost.put(newId, hostName);
            return newId;
        });
    }

    private long getStorageId(Integer storageHash) {
        return storageHashToId.computeIfAbsent(storageHash, k -> (storageId++));
    }

    public String getHdfsDataNodeIp(long hostId) {
        String hostPort = idToBlockHost.get(hostId);
        return hostPort.split(":")[0];
    }

    public long nextPartitionId() {
        return partitionIdGen.getAndIncrement();
    }
}
