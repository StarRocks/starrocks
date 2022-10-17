// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.PlannerProfile;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HiveMetaClient {
    private static final Logger LOG = LogManager.getLogger(HiveMetaClient.class);
    public static final String PARTITION_NULL_VALUE = "__HIVE_DEFAULT_PARTITION__";
    public static final String HUDI_PARTITION_NULL_VALUE = "default";
    public static final String DLF_HIVE_METASTORE = "dlf";
    public static final String HIVE_METASTORE_TYPE = "hive.metastore.type";
    // Maximum number of idle metastore connections in the connection pool at any point.
    private static final int MAX_HMS_CONNECTION_POOL_SIZE = 32;

    private final LinkedList<AutoCloseClient> clientPool = new LinkedList<>();
    private final Object clientPoolLock = new Object();

    private final HiveConf conf;

    private long baseHmsEventId;

    // Required for creating an instance of RetryingMetaStoreClient.
    private static final HiveMetaHookLoader DUMMY_HOOK_LOADER = tbl -> null;

    public HiveMetaClient(HiveConf conf) {
        this.conf = conf;
        if (Config.enable_hms_events_incremental_sync) {
            init();
        }
    }

    private void init() {
        CurrentNotificationEventId currentNotificationEventId = getCurrentNotificationEventId();
        this.baseHmsEventId = currentNotificationEventId.getEventId();
    }

    public class AutoCloseClient implements AutoCloseable {
        private final IMetaStoreClient hiveClient;

        private AutoCloseClient(HiveConf conf) throws MetaException {
            if (!DLF_HIVE_METASTORE.equalsIgnoreCase(conf.get(HIVE_METASTORE_TYPE))) {
                hiveClient = RetryingMetaStoreClient.getProxy(conf, DUMMY_HOOK_LOADER,
                        HiveMetaStoreThriftClient.class.getName());
            } else {
                hiveClient = RetryingMetaStoreClient.getProxy(conf, DUMMY_HOOK_LOADER,
                        DLFProxyMetaStoreClient.class.getName());
            }
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

    public List<String> getAllDatabaseNames() {
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("HMS.getAllDatabases");
                AutoCloseClient client = getClient()) {
            return client.hiveClient.getAllDatabases();
        } catch (Exception e) {
            LOG.error("Failed to get all database names", e);
            throw new StarRocksConnectorException("Failed to get all database names from meta store: " + e.getMessage());
        }
    }

    public List<String> getAllTableNames(String dbName) {
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("HMS.getAllTables");
                AutoCloseClient client = getClient()) {
            return client.hiveClient.getAllTables(dbName);
        } catch (Exception e) {
            LOG.error("Failed to get all table names on database: " + dbName, e);
            throw new StarRocksConnectorException("Failed to get all table names on database [%s] from meta store: %s",
                    dbName, e.getMessage());
        }
    }

    public List<String> getPartitionKeys(String dbName, String tableName) {
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("HMS.listPartitionNames");
                AutoCloseClient client = getClient()) {
            return client.hiveClient.listPartitionNames(dbName, tableName, (short) -1);
        } catch (Exception e) {
            LOG.error("Failed to get partitionKeys on {}.{}", dbName, tableName, e);
            throw new StarRocksConnectorException("Failed to get partition keys on [%s.%s] from meta store: %s",
                    dbName, tableName, e.getMessage());
        }
    }

    public Database getDb(String dbName) {
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("HMS.getDatabase");
                AutoCloseClient client = getClient()) {
            return client.hiveClient.getDatabase(dbName);
        } catch (Exception e) {
            LOG.error("Failed to get database {}", dbName, e);
            throw new StarRocksConnectorException("Failed to get database [%s] from meta store: %s",
                    dbName, e.getMessage());
        }
    }

    public Table getTable(String dbName, String tableName) {
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("HMS.getTable");
                AutoCloseClient client = getClient()) {
            return client.hiveClient.getTable(dbName, tableName);
        } catch (Exception e) {
            LOG.error("Failed to get table [{}.{}]", dbName, tableName, e);
            throw new StarRocksConnectorException("get hive table [%s.%s] from meta store failed: %s",
                    dbName, tableName, e.getMessage());
        }
    }

    public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("HMS.getPartition");
                AutoCloseClient client = getClient()) {
            return client.hiveClient.getPartition(dbName, tableName, partitionValues);
        } catch (Exception e) {
            LOG.error("Failed to get partition on {}.{}", dbName, tableName, e);
            throw new StarRocksConnectorException("Failed to get partition on [%s.%s] from meta store: %s",
                    dbName, tableName, e.getMessage());
        }
    }

    /**
     * Both 'getPartitionByNames' and 'getPartitionColumnStatistics' could throw exception or no response
     * when querying too many partitions at present. Due to statistics don't affect accuracy, user could adjust
     * session variable 'hive_partition_stats_sample_size' to ensure 'getPartitionColumnStat' normal return.
     * But "getPartitionByNames" interface must return the full contents due to the need to get partition file information.
     * So we resend request "getPartitionByNames" when an exception occurs.
     */
    public List<Partition> getPartitionsByNames(String dbName, String tblName, List<String> partitionNames) {
        int size = partitionNames.size();
        List<Partition> partitions;
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("HMS.getPartitionsByNames", size);
                AutoCloseClient client = getClient()) {
            partitions = client.hiveClient.getPartitionsByNames(dbName, tblName, partitionNames);
            if (partitions.size() != partitionNames.size()) {
                LOG.warn("Expect to fetch {} partition on [{}.{}], but actually fetched {} partition",
                        partitionNames.size(), dbName, tblName, partitions.size());
            }
        } catch (TTransportException te) {
            partitions = getPartitionsWithRetry(dbName, tblName, partitionNames, 1);
        } catch (Exception e) {
            LOG.error("Failed to get partitions on {}.{}", dbName, tblName, e);
            throw new StarRocksConnectorException("Failed to get partitions on [%s.%s] from meta store: %s",
                    dbName, tblName, e.getMessage());
        }
        return partitions;
    }

    public List<ColumnStatisticsObj> getTableColumnStats(String dbName, String tableName, List<String> columns) {
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("HMS.getTableColumnStatistics");
                AutoCloseClient client = getClient()) {
            return client.hiveClient.getTableColumnStatistics(dbName, tableName, columns);
        } catch (Exception e) {
            LOG.error("Failed to get table column statistics on [{}.{}]", dbName, tableName, e);
            throw new StarRocksConnectorException("Failed to get table column statistics on [%s.%s], msg: %s",
                    dbName, tableName, e.getMessage());
        }
    }

    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStats(String dbName,
                                                                          String tableName,
                                                                          List<String> columns,
                                                                          List<String> partitionNames) {
        int size = partitionNames.size();
        try (PlannerProfile.ScopedTimer ignored = PlannerProfile.getScopedTimer("HMS.getPartitionColumnStatistics", size);
                AutoCloseClient client = getClient()) {
            return client.hiveClient.getPartitionColumnStatistics(dbName, tableName, columns, partitionNames);
        } catch (Exception e) {
            LOG.error("Failed to get partitions column statistics on [{}.{}]. partition size: {}, columns size: {}",
                    dbName, tableName, partitionNames.size(), columns.size(), e);
            throw new StarRocksConnectorException("Failed to get partitions column statistics on [%s.%s]." +
                    " partition size: %d, columns size: %d. msg: %s", dbName, tableName, partitionNames.size(),
                    columns.size(), e.getMessage());
        }
    }

    /**
     * When the query scans many partitions in the table or the 'hive.metastore.try.direct.sql' in
     * hive metastore is false. The hive metastore will throw StackOverFlow exception.
     * We solve this problem by get partitions information multiple times.
     * Each retry reduces the number of partitions fetched by half until only one partition is fetched at a time.
     * @return Hive table partitions
     * @throws StarRocksConnectorException If there is an exception with only one partition at a time when get partition,
     * then we determine that there is a bug with the user's hive metastore.
     */
    private List<Partition> getPartitionsWithRetry(String dbName, String tableName,
                                                   List<String> partNames, int retryNum) throws StarRocksConnectorException {
        int subListSize = (int) Math.pow(2, retryNum);
        int subListNum = partNames.size() / subListSize;
        List<List<String>> partNamesList = Lists.partition(partNames, subListNum);
        List<Partition> partitions = Lists.newArrayList();

        LOG.warn("Execute getPartitionsByNames on [{}.{}] with {} times retry, slice size is {}, partName size is {}",
                dbName, tableName, retryNum, subListSize, partNames.size());

        try (AutoCloseClient client = getClient()) {
            for (List<String> parts : partNamesList) {
                partitions.addAll(client.hiveClient.getPartitionsByNames(dbName, tableName, parts));
            }
            LOG.info("Succeed to getPartitionByName on [{}.{}] with {} times retry, slice size is {}, partName size is {}",
                    dbName, tableName, retryNum, subListSize, partNames.size());
            return partitions;
        } catch (TTransportException te) {
            if (subListNum > 1) {
                return getPartitionsWithRetry(dbName, tableName, partNames, retryNum + 1);
            } else {
                throw new StarRocksConnectorException("Failed to getPartitionsByNames on [%s.%s] with slice size is %d",
                        dbName, tableName, subListNum);
            }
        } catch (Exception e) {
            throw new StarRocksConnectorException("Failed to getPartitionsNames on [%s.%s], msg: %s",
                    dbName, tableName, e.getMessage());
        }
    }

    public CurrentNotificationEventId getCurrentNotificationEventId() {
        try (AutoCloseClient client = getClient()) {
            return client.hiveClient.getCurrentNotificationEventId();
        } catch (Exception e) {
            LOG.error("Failed to fetch current notification event id", e);
            throw new StarRocksConnectorException("Failed to get current notification event id. msg: " + e.getMessage());
        }
    }

    public NotificationEventResponse getNextNotification(long lastEventId,
                                                         int maxEvents,
                                                         IMetaStoreClient.NotificationFilter filter)
            throws DdlException {
        try (AutoCloseClient client = getClient()) {
            return client.hiveClient.getNextNotification(lastEventId, maxEvents, filter);
        } catch (Exception e) {
            throw new DdlException("Failed to get next notification. msg: " + e.getMessage());
        }
    }

    public long getBaseHmsEventId() {
        return baseHmsEventId;
    }
}
