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


package com.starrocks.connector.hive.glue.metastore;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.connector.hive.glue.converters.CatalogToHiveConverter;
import com.starrocks.connector.hive.glue.converters.GlueInputConverter;
import com.starrocks.connector.hive.glue.converters.HiveToCatalogConverter;
import com.starrocks.connector.hive.glue.util.BatchCreatePartitionsHelper;
import com.starrocks.connector.hive.glue.util.ExpressionHelper;
import com.starrocks.connector.hive.glue.util.MetastoreClientUtils;
import com.starrocks.connector.hive.glue.util.PartitionKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.starrocks.connector.hive.glue.util.AWSGlueConfig.CUSTOM_EXECUTOR_FACTORY_CONF;
import static org.apache.hadoop.hive.metastore.HiveMetaStore.PUBLIC;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;

/***
 * Delegate Class to provide all common functionality
 * between Spark-hive version, Hive and Presto clients
 *
 */
public class GlueMetastoreClientDelegate {

    private static final Logger LOGGER = LogManager.getLogger(GlueMetastoreClientDelegate.class);

    private static final List<Role> IMPLICIT_ROLES = Lists.newArrayList(new Role(PUBLIC, 0, PUBLIC));
    public static final int MILLISECOND_TO_SECOND_FACTOR = 1000;
    public static final Long NO_MAX = -1L;
    public static final String MATCH_ALL = ".*";

    private static final int BATCH_CREATE_PARTITIONS_MAX_REQUEST_SIZE = 100;

    static final String GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT = "glue-metastore-delegate-%d";

    private final ExecutorService executorService;
    private final AWSGlueMetastore glueMetastore;
    private final HiveConf conf;
    private final Warehouse wh;
    private final String catalogId;

    public static final String CATALOG_ID_CONF = "hive.metastore.glue.catalogid";

    protected ExecutorService getExecutorService() {
        Class<? extends ExecutorServiceFactory> executorFactoryClass = this.conf
                .getClass(CUSTOM_EXECUTOR_FACTORY_CONF,
                        DefaultExecutorServiceFactory.class).asSubclass(
                        ExecutorServiceFactory.class);
        ExecutorServiceFactory factory = ReflectionUtils.newInstance(
                executorFactoryClass, conf);
        return factory.getExecutorService(conf);
    }

    public GlueMetastoreClientDelegate(HiveConf conf, AWSGlueMetastore glueMetastore,
                                       Warehouse wh) throws MetaException {
        checkNotNull(conf, "Hive Config cannot be null");
        checkNotNull(glueMetastore, "glueMetastore cannot be null");
        checkNotNull(wh, "Warehouse cannot be null");

        this.conf = conf;
        this.glueMetastore = glueMetastore;
        this.wh = wh;
        this.executorService = getExecutorService();

        // TODO - May be validate catalogId confirms to AWS AccountId too.
        catalogId = MetastoreClientUtils.getCatalogId(conf);
    }

    // ======================= Database =======================

    public void createDatabase(org.apache.hadoop.hive.metastore.api.Database database) throws TException {
        checkNotNull(database, "database cannot be null");

        if (StringUtils.isEmpty(database.getLocationUri())) {
            database.setLocationUri(wh.getDefaultDatabasePath(database.getName()).toString());
        } else {
            database.setLocationUri(wh.getDnsPath(new Path(database.getLocationUri())).toString());
        }
        Path dbPath = new Path(database.getLocationUri());
        boolean madeDir = MetastoreClientUtils.makeDirs(wh, dbPath);

        try {
            DatabaseInput catalogDatabase = GlueInputConverter.convertToDatabaseInput(database);
            glueMetastore.createDatabase(catalogDatabase);
        } catch (AmazonServiceException e) {
            if (madeDir) {
                wh.deleteDir(dbPath, true, database);
            }
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to create database: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    public org.apache.hadoop.hive.metastore.api.Database getDatabase(String name) throws TException {
        checkArgument(StringUtils.isNotEmpty(name), "name cannot be null or empty");

        try {
            Database catalogDatabase = glueMetastore.getDatabase(name);
            return CatalogToHiveConverter.convertDatabase(catalogDatabase);
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to get database object: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    public List<String> getDatabases(String pattern) throws TException {
        // Special handling for compatibility with Hue that passes "*" instead of ".*"
        if (pattern == null || pattern.equals("*")) {
            pattern = MATCH_ALL;
        }

        try {
            List<String> ret = new ArrayList<>();

            List<Database> allDatabases = glueMetastore.getAllDatabases();

            //filter by pattern
            for (Database db : allDatabases) {
                String name = db.getName();
                if (Pattern.matches(pattern, name)) {
                    ret.add(name);
                }
            }
            return ret;
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to get databases: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    public void alterDatabase(String databaseName, org.apache.hadoop.hive.metastore.api.Database database)
            throws TException {
        checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
        checkNotNull(database, "database cannot be null");

        try {
            DatabaseInput catalogDatabase = GlueInputConverter.convertToDatabaseInput(database);
            glueMetastore.updateDatabase(databaseName, catalogDatabase);
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to alter database: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
            throws TException {
        checkArgument(StringUtils.isNotEmpty(name), "name cannot be null or empty");

        String dbLocation;
        org.apache.hadoop.hive.metastore.api.Database db = getDatabase(name);
        try {
            List<String> tables = getTables(name, MATCH_ALL);
            boolean isEmptyDatabase = tables.isEmpty();

            dbLocation = db.getLocationUri();

            // TODO: handle cascade
            if (isEmptyDatabase || cascade) {
                glueMetastore.deleteDatabase(name);
            } else {
                throw new InvalidOperationException("Database " + name + " is not empty.");
            }
        } catch (NoSuchObjectException e) {
            if (ignoreUnknownDb) {
                return;
            } else {
                throw e;
            }
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to drop database: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }

        if (deleteData) {
            try {
                wh.deleteDir(new Path(dbLocation), true, db);
            } catch (Exception e) {
                LOGGER.error("Unable to remove database directory " + dbLocation, e);
            }
        }
    }

    public boolean databaseExists(String dbName) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");

        try {
            getDatabase(dbName);
        } catch (NoSuchObjectException e) {
            return false;
        } catch (AmazonServiceException e) {
            throw new TException(e);
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        }
        return true;
    }

    // ======================== Table ========================

    public void createTable(org.apache.hadoop.hive.metastore.api.Table tbl) throws TException {
        checkNotNull(tbl, "tbl cannot be null");
        boolean dirCreated = validateNewTableAndCreateDirectory(tbl);
        try {
            // Glue Server side does not set DDL_TIME. Set it here for the time being.
            // TODO: Set DDL_TIME parameter in Glue service
            tbl.setParameters(MetastoreClientUtils.deepCopyMap(tbl.getParameters()));
            tbl.getParameters().put(hive_metastoreConstants.DDL_TIME,
                    Long.toString(System.currentTimeMillis() / MILLISECOND_TO_SECOND_FACTOR));

            TableInput tableInput = GlueInputConverter.convertToTableInput(tbl);
            glueMetastore.createTable(tbl.getDbName(), tableInput);
        } catch (AmazonServiceException e) {
            if (dirCreated) {
                Path tblPath = new Path(tbl.getSd().getLocation());
                wh.deleteDir(tblPath, true, getDatabase(tbl.getDbName()));
            }
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to create table: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    public boolean tableExists(String databaseName, String tableName) throws TException {
        checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");

        if (!databaseExists(databaseName)) {
            throw new UnknownDBException("Database: " + databaseName + " does not exist.");
        }
        try {
            glueMetastore.getTable(databaseName, tableName);
            return true;
        } catch (EntityNotFoundException e) {
            return false;
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to check table exist: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    public org.apache.hadoop.hive.metastore.api.Table getTable(String dbName, String tableName) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");

        try {
            Table table = glueMetastore.getTable(dbName, tableName);
            MetastoreClientUtils.validateGlueTable(table);
            return CatalogToHiveConverter.convertTable(table, dbName);
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to get table: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    public List<String> getTables(String dbname, String tablePattern) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbname), "dbName cannot be null or empty");

        List<String> names = Lists.newArrayList();
        try {
            List<Table> tables = glueMetastore.getTables(dbname, tablePattern);
            for (Table catalogTable : tables) {
                names.add(catalogTable.getName());
            }
            return names;
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to get tables: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    public List<TableMeta> getTableMeta(
            String dbPatterns,
            String tablePatterns,
            List<String> tableTypes
    ) throws TException {
        List<TableMeta> tables = new ArrayList<>();
        List<String> databases = getDatabases(dbPatterns);
        for (String dbName : databases) {
            String nextToken = null;
            List<Table> dbTables = glueMetastore.getTables(dbName, tablePatterns);
            for (Table catalogTable : dbTables) {
                if (tableTypes == null ||
                        tableTypes.isEmpty() ||
                        tableTypes.contains(catalogTable.getTableType())) {
                    tables.add(CatalogToHiveConverter.convertTableMeta(catalogTable, dbName));
                }
            }
        }
        return tables;
    }

    /*
     * Hive reference: https://github.com/apache/hive/blob/rel/release-2.3.0/metastore/src/java/org/apache/hadoop/hive/metastore/HiveAlterHandler.java#L88
     */
    public void alterTable(
            String dbName,
            String oldTableName,
            org.apache.hadoop.hive.metastore.api.Table newTable,
            EnvironmentContext environmentContext
    ) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(oldTableName), "oldTableName cannot be null or empty");
        checkNotNull(newTable, "newTable cannot be null");

        if (isCascade(environmentContext)) {
            throw new UnsupportedOperationException("Cascade for alter_table is not supported");
        }

        if (!oldTableName.equals(newTable.getTableName())) {
            throw new UnsupportedOperationException("Table rename is not supported");
        }

        MetastoreClientUtils.validateTableObject(newTable, conf);
        if (!tableExists(dbName, oldTableName)) {
            throw new UnknownTableException("Table: " + oldTableName + " does not exists");
        }

        // If table properties has EXTERNAL set, update table type accordinly
        // mimics Hive's ObjectStore#convertToMTable, added in HIVE-1329
        boolean isExternal = Boolean.parseBoolean(newTable.getParameters().get("EXTERNAL"));
        if (MANAGED_TABLE.toString().equals(newTable.getTableType()) && isExternal) {
            newTable.setTableType(EXTERNAL_TABLE.toString());
        } else if (EXTERNAL_TABLE.toString().equals(newTable.getTableType()) && !isExternal) {
            newTable.setTableType(MANAGED_TABLE.toString());
        }

        try {
            TableInput newTableInput = GlueInputConverter.convertToTableInput(newTable);
            glueMetastore.updateTable(dbName, newTableInput);
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to alter table: " + oldTableName;
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    private boolean isCascade(EnvironmentContext environmentContext) {
        return environmentContext != null &&
                environmentContext.isSetProperties() &&
                StatsSetupConst.TRUE.equals(environmentContext.getProperties().get(StatsSetupConst.CASCADE));
    }

    public void dropTable(
            String dbName,
            String tableName,
            boolean deleteData,
            boolean ignoreUnknownTbl,
            boolean ifPurge
    ) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");

        if (!tableExists(dbName, tableName)) {
            if (!ignoreUnknownTbl) {
                throw new UnknownTableException("Cannot find table: " + dbName + "." + tableName);
            } else {
                return;
            }
        }

        org.apache.hadoop.hive.metastore.api.Table tbl = getTable(dbName, tableName);
        String tblLocation = tbl.getSd().getLocation();
        boolean isExternal = MetastoreClientUtils.isExternalTable(tbl);
        dropPartitionsForTable(dbName, tableName, deleteData && !isExternal);

        try {
            glueMetastore.deleteTable(dbName, tableName);
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to drop table: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }

        if (StringUtils.isNotEmpty(tblLocation) && deleteData && !isExternal) {
            Path tblPath = new Path(tblLocation);
            try {
                wh.deleteDir(tblPath, true, ifPurge, getDatabase(dbName));
            } catch (Exception e) {
                LOGGER.error("Unable to remove table directory " + tblPath, e);
            }
        }
    }

    private void dropPartitionsForTable(String dbName, String tableName, boolean deleteData) throws TException {
        List<org.apache.hadoop.hive.metastore.api.Partition> partitionsToDelete =
                getPartitions(dbName, tableName, null, NO_MAX);
        for (org.apache.hadoop.hive.metastore.api.Partition part : partitionsToDelete) {
            dropPartition(dbName, tableName, part.getValues(), true, deleteData, false);
        }
    }

    public List<String> getTables(String dbname, String tablePattern, TableType tableType) throws TException {
        throw new UnsupportedOperationException("getTables with TableType is not supported");
    }

    public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables) throws TException {
        throw new UnsupportedOperationException("listTableNamesByFilter is not supported");
    }

    /**
     * @return boolean
     * true  -> directory created
     * false -> directory not created
     */
    public boolean validateNewTableAndCreateDirectory(org.apache.hadoop.hive.metastore.api.Table tbl)
            throws TException {
        checkNotNull(tbl, "tbl cannot be null");
        if (tableExists(tbl.getDbName(), tbl.getTableName())) {
            throw new AlreadyExistsException("Table " + tbl.getTableName() + " already exists.");
        }
        MetastoreClientUtils.validateTableObject(tbl, conf);

        if (TableType.VIRTUAL_VIEW.toString().equals(tbl.getTableType())) {
            // we don't need to create directory for virtual views
            return false;
        }

        tbl.getSd().setLocation(wh.getDnsPath(new Path(tbl.getSd().getLocation())).toString());
        Path tblPath = new Path(tbl.getSd().getLocation());
        return MetastoreClientUtils.makeDirs(wh, tblPath);
    }

    // =========================== Partition ===========================

    public org.apache.hadoop.hive.metastore.api.Partition appendPartition(
            String dbName,
            String tblName,
            List<String> values
    ) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
        checkNotNull(values, "partition values cannot be null");
        org.apache.hadoop.hive.metastore.api.Table table = getTable(dbName, tblName);
        checkNotNull(table.getSd(), "StorageDescriptor cannot be null for Table " + tblName);
        org.apache.hadoop.hive.metastore.api.Partition partition = buildPartitionFromValues(table, values);
        addPartitions(Lists.newArrayList(partition), false, true);
        return partition;
    }

    /**
     * Taken from HiveMetaStore#append_partition_common
     */
    private org.apache.hadoop.hive.metastore.api.Partition buildPartitionFromValues(
            org.apache.hadoop.hive.metastore.api.Table table, List<String> values) throws MetaException {
        org.apache.hadoop.hive.metastore.api.Partition partition = new org.apache.hadoop.hive.metastore.api.Partition();
        partition.setDbName(table.getDbName());
        partition.setTableName(table.getTableName());
        partition.setValues(values);
        partition.setSd(table.getSd().deepCopy());

        Path partLocation =
                new Path(table.getSd().getLocation(), Warehouse.makePartName(table.getPartitionKeys(), values));
        partition.getSd().setLocation(partLocation.toString());

        long timeInSecond = System.currentTimeMillis() / MILLISECOND_TO_SECOND_FACTOR;
        partition.setCreateTime((int) timeInSecond);
        partition.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(timeInSecond));
        return partition;
    }

    public List<org.apache.hadoop.hive.metastore.api.Partition> addPartitions(
            List<org.apache.hadoop.hive.metastore.api.Partition> partitions,
            boolean ifNotExists,
            boolean needResult
    ) throws TException {
        checkNotNull(partitions, "partitions cannot be null");
        List<Partition> partitionsCreated =
                batchCreatePartitions(partitions, ifNotExists);
        if (!needResult) {
            return null;
        }
        return CatalogToHiveConverter.convertPartitions(partitionsCreated);
    }

    private List<Partition> batchCreatePartitions(
            final List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions,
            final boolean ifNotExists
    ) throws TException {
        if (hivePartitions.isEmpty()) {
            return Lists.newArrayList();
        }

        final String dbName = hivePartitions.get(0).getDbName();
        final String tableName = hivePartitions.get(0).getTableName();
        org.apache.hadoop.hive.metastore.api.Table tbl = getTable(dbName, tableName);
        validateInputForBatchCreatePartitions(tbl, hivePartitions);

        List<Partition> catalogPartitions = Lists.newArrayList();
        Map<PartitionKey, Path> addedPath = Maps.newHashMap();
        try {
            for (org.apache.hadoop.hive.metastore.api.Partition partition : hivePartitions) {
                Path location = getPartitionLocation(tbl, partition);
                boolean partDirCreated = false;
                if (location != null) {
                    partition.getSd().setLocation(location.toString());
                    partDirCreated = MetastoreClientUtils.makeDirs(wh, location);
                }
                Partition catalogPartition = HiveToCatalogConverter.convertPartition(partition);
                catalogPartitions.add(catalogPartition);
                if (partDirCreated) {
                    addedPath.put(new PartitionKey(catalogPartition), new Path(partition.getSd().getLocation()));
                }
            }
        } catch (MetaException e) {
            for (Path path : addedPath.values()) {
                deletePath(path);
            }
            throw e;
        }

        List<Future<BatchCreatePartitionsHelper>> batchCreatePartitionsFutures = Lists.newArrayList();
        for (int i = 0; i < catalogPartitions.size(); i += BATCH_CREATE_PARTITIONS_MAX_REQUEST_SIZE) {
            int j = Math.min(i + BATCH_CREATE_PARTITIONS_MAX_REQUEST_SIZE, catalogPartitions.size());
            final List<Partition> partitionsOnePage = catalogPartitions.subList(i, j);

            batchCreatePartitionsFutures.add(this.executorService.submit(new Callable<BatchCreatePartitionsHelper>() {
                @Override
                public BatchCreatePartitionsHelper call() throws Exception {
                    return new BatchCreatePartitionsHelper(glueMetastore, dbName, tableName, catalogId,
                            partitionsOnePage, ifNotExists)
                            .createPartitions();
                }
            }));
        }

        TException tException = null;
        List<Partition> partitionsCreated = Lists.newArrayList();
        for (Future<BatchCreatePartitionsHelper> future : batchCreatePartitionsFutures) {
            try {
                BatchCreatePartitionsHelper batchCreatePartitionsHelper = future.get();
                partitionsCreated.addAll(batchCreatePartitionsHelper.getPartitionsCreated());
                tException = tException == null ? batchCreatePartitionsHelper.getFirstTException() : tException;
                deletePathForPartitions(batchCreatePartitionsHelper.getPartitionsFailed(), addedPath);
            } catch (Exception e) {
                LOGGER.error("Exception thrown by BatchCreatePartitions thread pool. ", e);
            }
        }

        if (tException != null) {
            throw tException;
        }
        return partitionsCreated;
    }

    private void validateInputForBatchCreatePartitions(
            org.apache.hadoop.hive.metastore.api.Table tbl,
            List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions) {
        checkNotNull(tbl.getPartitionKeys(), "Partition keys cannot be null");
        for (org.apache.hadoop.hive.metastore.api.Partition partition : hivePartitions) {
            checkArgument(tbl.getDbName().equals(partition.getDbName()), "Partitions must be in the same DB");
            checkArgument(tbl.getTableName().equals(partition.getTableName()), "Partitions must be in the same table");
            checkNotNull(partition.getValues(), "Partition values cannot be null");
            checkArgument(tbl.getPartitionKeys().size() == partition.getValues().size(),
                    "Number of table partition keys must match number of partition values");
        }
    }

    private void deletePathForPartitions(List<Partition> partitions, Map<PartitionKey, Path> addedPath) {
        for (Partition partition : partitions) {
            Path path = addedPath.get(new PartitionKey(partition));
            if (path != null) {
                deletePath(path);
            }
        }
    }

    private void deletePath(Path path) {
        try {
            wh.deleteDir(path, true, false, false);
        } catch (MetaException e) {
            LOGGER.error("Warehouse delete directory failed. ", e);
        }
    }

    /**
     * Taken from HiveMetastore#createLocationForAddedPartition
     */
    private Path getPartitionLocation(
            org.apache.hadoop.hive.metastore.api.Table tbl,
            org.apache.hadoop.hive.metastore.api.Partition part) throws MetaException {
        Path partLocation = null;
        String partLocationStr = null;
        if (part.getSd() != null) {
            partLocationStr = part.getSd().getLocation();
        }

        if (StringUtils.isEmpty(partLocationStr)) {
            // set default location if not specified and this is
            // a physical table partition (not a view)
            if (tbl.getSd().getLocation() != null) {
                partLocation = new Path(tbl.getSd().getLocation(),
                        Warehouse.makePartName(tbl.getPartitionKeys(), part.getValues()));
            }
        } else {
            if (tbl.getSd().getLocation() == null) {
                throw new MetaException("Cannot specify location for a view partition");
            }
            partLocation = wh.getDnsPath(new Path(partLocationStr));
        }
        return partLocation;
    }

    public List<String> listPartitionNames(
            String databaseName,
            String tableName,
            List<String> values,
            short max
    ) throws TException {
        String expression = null;
        org.apache.hadoop.hive.metastore.api.Table table = getTable(databaseName, tableName);
        if (values != null) {
            expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, values);
        }

        List<String> names = Lists.newArrayList();
        List<org.apache.hadoop.hive.metastore.api.Partition> partitions =
                getPartitions(databaseName, tableName, expression, max);
        for (org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
            names.add(Warehouse.makePartName(table.getPartitionKeys(), p.getValues()));
        }
        return names;
    }

    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(
            String databaseName,
            String tableName,
            List<String> partitionNames
    ) throws TException {
        checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");
        checkNotNull(partitionNames, "partitionNames cannot be null");

        List<PartitionValueList> partitionsToGet = Lists.newArrayList();
        for (String partitionName : partitionNames) {
            partitionsToGet.add(new PartitionValueList().withValues(partitionNameToVals(partitionName)));
        }
        try {
            List<Partition> partitions =
                    glueMetastore.getPartitionsByNames(databaseName, tableName, partitionsToGet);

            return CatalogToHiveConverter.convertPartitions(partitions);
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to get partition by names: " + StringUtils.join(partitionNames, "/");
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName,
                                                                       String partitionName)
            throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(partitionName), "partitionName cannot be null or empty");
        List<String> values = partitionNameToVals(partitionName);
        return getPartition(dbName, tblName, values);
    }

    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName,
                                                                       List<String> values) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
        checkNotNull(values, "values cannot be null");

        Partition partition;
        try {
            partition = glueMetastore.getPartition(dbName, tblName, values);
            if (partition == null) {
                LOGGER.debug(
                        "No partitions were return for dbName = " + dbName + ", tblName = " + tblName + ", values = " +
                                values);
                return null;
            }
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to get partition with values: " + StringUtils.join(values, "/");
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
        return CatalogToHiveConverter.convertPartition(partition);
    }

    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitions(
            String databaseName,
            String tableName,
            String filter,
            long max
    ) throws TException {
        checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");
        List<Partition> partitions = getCatalogPartitions(databaseName, tableName, filter, max);
        return CatalogToHiveConverter.convertPartitions(partitions);
    }

    public List<Partition> getCatalogPartitions(
            final String databaseName,
            final String tableName,
            final String expression,
            final long max
    ) throws TException {
        checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");
        try {
            return glueMetastore.getPartitions(databaseName, tableName, expression, max);
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to get partitions with expression: " + expression;
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    public boolean dropPartition(
            String dbName,
            String tblName,
            List<String> values,
            boolean ifExist,
            boolean deleteData,
            boolean purgeData
    ) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
        checkNotNull(values, "values cannot be null");

        org.apache.hadoop.hive.metastore.api.Partition partition = null;
        try {
            partition = getPartition(dbName, tblName, values);
        } catch (NoSuchObjectException e) {
            if (ifExist) {
                return true;
            }
        }

        try {
            glueMetastore.deletePartition(dbName, tblName, partition.getValues());
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to drop partition with values: " + StringUtils.join(values, "/");
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }

        performDropPartitionPostProcessing(dbName, tblName, partition, deleteData, purgeData);
        return true;
    }

    private void performDropPartitionPostProcessing(
            String dbName,
            String tblName,
            org.apache.hadoop.hive.metastore.api.Partition partition,
            boolean deleteData,
            boolean ifPurge
    ) throws TException {
        if (deleteData && partition.getSd() != null && partition.getSd().getLocation() != null) {
            Path partPath = new Path(partition.getSd().getLocation());
            org.apache.hadoop.hive.metastore.api.Table table = getTable(dbName, tblName);
            if (MetastoreClientUtils.isExternalTable(table)) {
                //Don't delete external table data
                return;
            }
            boolean mustPurge = isMustPurge(table, ifPurge);
            wh.deleteDir(partPath, true, mustPurge, getDatabase(dbName));
            try {
                List<String> values = partition.getValues();
                deleteParentRecursive(partPath.getParent(), values.size() - 1, mustPurge);
            } catch (IOException e) {
                throw new MetaException(e.getMessage());
            }
        }
    }

    /**
     * Taken from HiveMetaStore#isMustPurge
     */
    private boolean isMustPurge(org.apache.hadoop.hive.metastore.api.Table table, boolean ifPurge) {
        return (ifPurge || "true".equalsIgnoreCase(table.getParameters().get("auto.purge")));
    }

    /**
     * Taken from HiveMetaStore#deleteParentRecursive
     */
    private void deleteParentRecursive(Path parent, int depth, boolean mustPurge) throws IOException, MetaException {
        if (depth > 0 && parent != null && wh.isWritable(parent) && wh.isEmpty(parent)) {
            wh.deleteDir(parent, true, mustPurge, false);
            deleteParentRecursive(parent.getParent(), depth - 1, mustPurge);
        }
    }

    public void alterPartitions(
            String dbName,
            String tblName,
            List<org.apache.hadoop.hive.metastore.api.Partition> partitions
    ) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
        checkNotNull(partitions, "partitions cannot be null");

        for (org.apache.hadoop.hive.metastore.api.Partition part : partitions) {
            part.setParameters(MetastoreClientUtils.deepCopyMap(part.getParameters()));
            if (part.getParameters().get(hive_metastoreConstants.DDL_TIME) == null ||
                    Integer.parseInt(part.getParameters().get(hive_metastoreConstants.DDL_TIME)) == 0) {
                part.putToParameters(hive_metastoreConstants.DDL_TIME,
                        Long.toString(System.currentTimeMillis() / MILLISECOND_TO_SECOND_FACTOR));
            }

            PartitionInput partitionInput = GlueInputConverter.convertToPartitionInput(part);

            try {
                glueMetastore.updatePartition(dbName, tblName, part.getValues(), partitionInput);
            } catch (AmazonServiceException e) {
                throw CatalogToHiveConverter.wrapInHiveException(e);
            } catch (Exception e) {
                String msg = "Unable to alter partition: ";
                LOGGER.error(msg, e);
                throw new MetaException(msg + e);
            }
        }
    }

    /**
     * Taken from HiveMetaStore#partition_name_to_vals
     */
    public List<String> partitionNameToVals(String name) throws TException {
        checkNotNull(name, "name cannot be null");
        if (name.isEmpty()) {
            return Lists.newArrayList();
        }
        LinkedHashMap<String, String> map = Warehouse.makeSpecFromName(name);
        List<String> vals = Lists.newArrayList();
        vals.addAll(map.values());
        return vals;
    }

    // ======================= Roles & Privilege =======================

    public boolean createRole(Role role) throws TException {
        throw new UnsupportedOperationException("createRole is not supported");
    }

    public boolean dropRole(String roleName) throws TException {
        throw new UnsupportedOperationException("dropRole is not supported");
    }

    public List<Role> listRoles(
            String principalName,
            PrincipalType principalType
    ) throws TException {
        // All users belong to public role implicitly, add that role
        // Bring logic from Hive's ObjectStore
        // https://code.amazon.com/packages/Aws157Hive/blobs/48f6e30080df475ffe54c39f70dd134268e30358/
        // --/metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L4208
        if (principalType == PrincipalType.USER) {
            return IMPLICIT_ROLES;
        } else {
            throw new UnsupportedOperationException(
                    "listRoles is only supported for " + PrincipalType.USER + " Principal type");
        }
    }

    public List<String> listRoleNames() throws TException {
        // return PUBLIC role as implicit role to prevent unnecessary failure,
        // even though Glue doesn't support Role API yet
        // https://code.amazon.com/packages/Aws157Hive/blobs/48f6e30080df475ffe54c39f70dd134268e30358/
        // --/metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L4325
        return Lists.newArrayList(PUBLIC);
    }

    public org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse getPrincipalsInRole(
            org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest request
    ) throws TException {
        throw new UnsupportedOperationException("getPrincipalsInRole is not supported");
    }

    public GetRoleGrantsForPrincipalResponse getRoleGrantsForPrincipal(
            GetRoleGrantsForPrincipalRequest request
    ) throws TException {
        throw new UnsupportedOperationException("getRoleGrantsForPrincipal is not supported");
    }

    public boolean grantRole(
            String roleName,
            String userName,
            PrincipalType principalType,
            String grantor, PrincipalType grantorType,
            boolean grantOption
    ) throws TException {
        throw new UnsupportedOperationException("grantRole is not supported");
    }

    public boolean revokeRole(
            String roleName,
            String userName,
            PrincipalType principalType,
            boolean grantOption
    ) throws TException {
        throw new UnsupportedOperationException("revokeRole is not supported");
    }

    public boolean revokePrivileges(
            org.apache.hadoop.hive.metastore.api.PrivilegeBag privileges,
            boolean grantOption
    ) throws TException {
        throw new UnsupportedOperationException("revokePrivileges is not supported");
    }

    public boolean grantPrivileges(org.apache.hadoop.hive.metastore.api.PrivilegeBag privileges)
            throws TException {
        throw new UnsupportedOperationException("grantPrivileges is not supported");
    }

    public org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet getPrivilegeSet(
            HiveObjectRef objectRef,
            String user, List<String> groups
    ) throws TException {
        // getPrivilegeSet is NOT yet supported.
        // return null not to break due to optional info
        // Hive return null when every condition fail
        // https://code.amazon.com/packages/Aws157Hive/blobs/c1ced60e67765d27086b3621255cd843947c151e/
        // --/metastore/src/java/org/apache/hadoop/hive/metastore/HiveMetaStore.java#L5237
        return null;
    }

    public List<HiveObjectPrivilege> listPrivileges(
            String principal,
            PrincipalType principalType,
            HiveObjectRef objectRef
    ) throws TException {
        throw new UnsupportedOperationException("listPrivileges is not supported");
    }

    // ========================== Statistics ==========================

    public boolean deletePartitionColumnStatistics(
            String dbName,
            String tableName,
            String partName,
            String colName
    ) throws TException {
        throw new UnsupportedOperationException("deletePartitionColumnStatistics is not supported");
    }

    public boolean deleteTableColumnStatistics(
            String dbName,
            String tableName,
            String colName
    ) throws TException {
        throw new UnsupportedOperationException("deleteTableColumnStatistics is not supported");
    }

    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName,
            String tableName,
            List<String> partitionNames, List<String> colNames
    ) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "databaseName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");
        checkNotNull(partitionNames, "colNames cannot be null");
        checkNotNull(colNames, "colNames cannot be null");
        try {
            return glueMetastore.getPartitionColumnStatistics(dbName, tableName, partitionNames, colNames);
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = String.format("Unable to get table column statistics, %s.%s", dbName, tableName);
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    public List<ColumnStatisticsObj> getTableColumnStatistics(
            String dbName,
            String tableName,
            List<String> colNames
    ) throws TException {
        checkArgument(StringUtils.isNotEmpty(dbName), "databaseName cannot be null or empty");
        checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");
        checkNotNull(colNames, "colNames cannot be null");
        try {
            return glueMetastore.getTableColumnStatistics(dbName, tableName, colNames);
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = String.format("Unable to get table column statistics, %s.%s", dbName, tableName);
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    public boolean updatePartitionColumnStatistics(
            org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics
    ) throws TException {
        throw new UnsupportedOperationException("updatePartitionColumnStatistics is not supported");
    }

    public boolean updateTableColumnStatistics(
            org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics
    ) throws TException {
        throw new UnsupportedOperationException("updateTableColumnStatistics is not supported");
    }

    public AggrStats getAggrColStatsFor(
            String dbName,
            String tblName,
            List<String> colNames,
            List<String> partName
    ) throws TException {
        throw new UnsupportedOperationException("getAggrColStatsFor is not supported");
    }

    public void cancelDelegationToken(String tokenStrForm) throws TException {
        throw new UnsupportedOperationException("cancelDelegationToken is not supported");
    }

    public String getTokenStrForm() throws IOException {
        throw new UnsupportedOperationException("getTokenStrForm is not supported");
    }

    public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
        throw new UnsupportedOperationException("addToken is not supported");
    }

    public boolean removeToken(String tokenIdentifier) throws TException {
        throw new UnsupportedOperationException("removeToken is not supported");
    }

    public String getToken(String tokenIdentifier) throws TException {
        throw new UnsupportedOperationException("getToken is not supported");
    }

    public List<String> getAllTokenIdentifiers() throws TException {
        throw new UnsupportedOperationException("getAllTokenIdentifiers is not supported");
    }

    public int addMasterKey(String key) throws TException {
        throw new UnsupportedOperationException("addMasterKey is not supported");
    }

    public void updateMasterKey(Integer seqNo, String key) throws TException {
        throw new UnsupportedOperationException("updateMasterKey is not supported");
    }

    public boolean removeMasterKey(Integer keySeq) throws TException {
        throw new UnsupportedOperationException("removeMasterKey is not supported");
    }

    public String[] getMasterKeys() throws TException {
        throw new UnsupportedOperationException("getMasterKeys is not supported");
    }

    public LockResponse checkLock(long lockId) throws TException {
        throw new UnsupportedOperationException("checkLock is not supported");
    }

    public void commitTxn(long txnId) throws TException {
        throw new UnsupportedOperationException("commitTxn is not supported");
    }

    public void abortTxns(List<Long> txnIds) throws TException {
        throw new UnsupportedOperationException("abortTxns is not supported");
    }

    public void compact(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType
    ) throws TException {
        throw new UnsupportedOperationException("compact is not supported");
    }

    public void compact(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType,
            Map<String, String> tblProperties
    ) throws TException {
        throw new UnsupportedOperationException("compact is not supported");
    }

    public CompactionResponse compact2(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType,
            Map<String, String> tblProperties
    ) throws TException {
        throw new UnsupportedOperationException("compact2 is not supported");
    }

    public ValidTxnList getValidTxns() throws TException {
        throw new UnsupportedOperationException("getValidTxns is not supported");
    }

    public ValidTxnList getValidTxns(long currentTxn) throws TException {
        throw new UnsupportedOperationException("getValidTxns is not supported");
    }

    public org.apache.hadoop.hive.metastore.api.Partition exchangePartition(
            Map<String, String> partitionSpecs,
            String srcDb,
            String srcTbl,
            String dstDb,
            String dstTbl
    ) throws TException {
        throw new UnsupportedOperationException("exchangePartition not yet supported.");
    }

    public List<org.apache.hadoop.hive.metastore.api.Partition> exchangePartitions(
            Map<String, String> partitionSpecs,
            String sourceDb,
            String sourceTbl,
            String destDb,
            String destTbl
    ) throws TException {
        throw new UnsupportedOperationException("exchangePartitions is not yet supported");
    }

    public String getDelegationToken(
            String owner,
            String renewerKerberosPrincipalName
    ) throws TException {
        throw new UnsupportedOperationException("getDelegationToken is not supported");
    }

    public void heartbeat(long txnId, long lockId) throws TException {
        throw new UnsupportedOperationException("heartbeat is not supported");
    }

    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
        throw new UnsupportedOperationException("heartbeatTxnRange is not supported");
    }

    public boolean isPartitionMarkedForEvent(
            String dbName,
            String tblName,
            Map<String, String> partKVs,
            PartitionEventType eventType
    ) throws TException {
        throw new UnsupportedOperationException("isPartitionMarkedForEvent is not supported");
    }

    public int getNumPartitionsByFilter(
            String dbName,
            String tableName,
            String filter
    ) throws TException {
        throw new UnsupportedOperationException("getNumPartitionsByFilter is not supported.");
    }

    public PartitionSpecProxy listPartitionSpecs(
            String dbName,
            String tblName,
            int max
    ) throws TException {
        throw new UnsupportedOperationException("listPartitionSpecs is not supported.");
    }

    public PartitionSpecProxy listPartitionSpecsByFilter(
            String dbName,
            String tblName,
            String filter,
            int max
    ) throws TException {
        throw new UnsupportedOperationException("listPartitionSpecsByFilter is not supported");
    }

    public LockResponse lock(LockRequest lockRequest) throws TException {
        throw new UnsupportedOperationException("lock is not supported");
    }

    public void markPartitionForEvent(
            String dbName,
            String tblName,
            Map<String, String> partKeyValues,
            PartitionEventType eventType
    ) throws TException {
        throw new UnsupportedOperationException("markPartitionForEvent is not supported");
    }

    public long openTxn(String user) throws TException {
        throw new UnsupportedOperationException("openTxn is not supported");
    }

    public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
        throw new UnsupportedOperationException("openTxns is not supported");
    }

    public long renewDelegationToken(String tokenStrForm) throws TException {
        throw new UnsupportedOperationException("renewDelegationToken is not supported");
    }

    public void rollbackTxn(long txnId) throws TException {
        throw new UnsupportedOperationException("rollbackTxn is not supported");
    }

    public void createTableWithConstraints(
            org.apache.hadoop.hive.metastore.api.Table table,
            List<SQLPrimaryKey> primaryKeys,
            List<SQLForeignKey> foreignKeys
    ) throws AlreadyExistsException, TException {
        throw new UnsupportedOperationException("createTableWithConstraints is not supported");
    }

    public void dropConstraint(
            String dbName,
            String tblName,
            String constraintName
    ) throws TException {
        throw new UnsupportedOperationException("dropConstraint is not supported");
    }

    public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols) throws TException {
        throw new UnsupportedOperationException("addPrimaryKey is not supported");
    }

    public void addForeignKey(List<SQLForeignKey> foreignKeyCols) throws TException {
        throw new UnsupportedOperationException("addForeignKey is not supported");
    }

    public ShowCompactResponse showCompactions() throws TException {
        throw new UnsupportedOperationException("showCompactions is not supported");
    }

    public void insertTable(org.apache.hadoop.hive.metastore.api.Table table, boolean overwrite) throws MetaException {
        throw new UnsupportedOperationException("insertTable is not supported");
    }

    public NotificationEventResponse getNextNotification(
            long lastEventId,
            int maxEvents,
            IMetaStoreClient.NotificationFilter notificationFilter
    ) throws TException {
        throw new UnsupportedOperationException("getNextNotification is not supported");
    }

    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
        throw new UnsupportedOperationException("getCurrentNotificationEventId is not supported");
    }

    public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
        throw new UnsupportedOperationException("fireListenerEvent is not supported");
    }

    public ShowLocksResponse showLocks() throws TException {
        throw new UnsupportedOperationException("showLocks is not supported");
    }

    public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
        throw new UnsupportedOperationException("showLocks is not supported");
    }

    public GetOpenTxnsInfoResponse showTxns() throws TException {
        throw new UnsupportedOperationException("showTxns is not supported");
    }

    public void unlock(long lockId) throws TException {
        throw new UnsupportedOperationException("unlock is not supported");
    }

    public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
        throw new UnsupportedOperationException("getFileMetadata is not supported");
    }

    public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
            List<Long> fileIds,
            ByteBuffer sarg,
            boolean doGetFooters
    ) throws TException {
        throw new UnsupportedOperationException("getFileMetadataBySarg is not supported");
    }

    public void clearFileMetadata(List<Long> fileIds) throws TException {
        throw new UnsupportedOperationException("clearFileMetadata is not supported");
    }

    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
        throw new UnsupportedOperationException("putFileMetadata is not supported");
    }

    public boolean setPartitionColumnStatistics(
            org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest request
    ) throws TException {
        throw new UnsupportedOperationException("setPartitionColumnStatistics is not supported");
    }

    public boolean cacheFileMetadata(
            String dbName,
            String tblName,
            String partName,
            boolean allParts
    ) throws TException {
        throw new UnsupportedOperationException("cacheFileMetadata is not supported");
    }

    public int addPartitionsSpecProxy(PartitionSpecProxy pSpec) throws TException {
        throw new UnsupportedOperationException("addPartitionsSpecProxy is unsupported");
    }

    public void setUGI(String username) throws TException {
        throw new UnsupportedOperationException("setUGI is unsupported");
    }

    /**
     * Gets the user defined function in a database stored in metastore and
     * converts back to Hive function.
     *
     * @param dbName
     * @param functionName
     * @return
     * @throws MetaException
     * @throws TException
     */
    public org.apache.hadoop.hive.metastore.api.Function getFunction(String dbName, String functionName)
            throws TException {
        try {
            UserDefinedFunction userDefinedFunction = glueMetastore.getUserDefinedFunction(dbName, functionName);
            return CatalogToHiveConverter.convertFunction(dbName, userDefinedFunction);
        } catch (AmazonServiceException e) {
            LOGGER.error(e);
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to get Function: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    /**
     * Gets user defined functions that match a pattern in database stored in
     * metastore and converts back to Hive function.
     *
     * @param dbName
     * @param pattern
     * @return
     * @throws MetaException
     * @throws TException
     */
    public List<String> getFunctions(String dbName, String pattern) throws TException {
        try {
            List<String> functionNames = Lists.newArrayList();
            List<UserDefinedFunction> functions =
                    glueMetastore.getUserDefinedFunctions(dbName, pattern);
            for (UserDefinedFunction catalogFunction : functions) {
                functionNames.add(catalogFunction.getFunctionName());
            }
            return functionNames;
        } catch (AmazonServiceException e) {
            LOGGER.error(e);
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to get Functions: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    /**
     * Creates a new user defined function in the metastore.
     *
     * @param function
     * @throws InvalidObjectException
     * @throws MetaException
     * @throws TException
     */
    public void createFunction(org.apache.hadoop.hive.metastore.api.Function function) throws InvalidObjectException,
            TException {
        try {
            UserDefinedFunctionInput functionInput = GlueInputConverter.convertToUserDefinedFunctionInput(function);
            glueMetastore.createUserDefinedFunction(function.getDbName(), functionInput);
        } catch (AmazonServiceException e) {
            LOGGER.error(e);
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to create Function: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    /**
     * Drops a user defined function in the database stored in metastore.
     *
     * @param dbName
     * @param functionName
     * @throws MetaException
     * @throws NoSuchObjectException
     * @throws InvalidObjectException
     * @throws org.apache.hadoop.hive.metastore.api.InvalidInputException
     * @throws TException
     */
    public void dropFunction(String dbName, String functionName) throws NoSuchObjectException,
            InvalidObjectException, org.apache.hadoop.hive.metastore.api.InvalidInputException, TException {
        try {
            glueMetastore.deleteUserDefinedFunction(dbName, functionName);
        } catch (AmazonServiceException e) {
            LOGGER.error(e);
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to drop Function: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    /**
     * Updates a user defined function in a database stored in the metastore.
     *
     * @param dbName
     * @param functionName
     * @param newFunction
     * @throws InvalidObjectException
     * @throws MetaException
     * @throws TException
     */
    public void alterFunction(String dbName, String functionName,
                              org.apache.hadoop.hive.metastore.api.Function newFunction)
            throws InvalidObjectException, MetaException,
            TException {
        try {
            UserDefinedFunctionInput functionInput = GlueInputConverter.convertToUserDefinedFunctionInput(newFunction);
            glueMetastore.updateUserDefinedFunction(dbName, functionName, functionInput);
        } catch (AmazonServiceException e) {
            LOGGER.error(e);
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to alter Function: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    /**
     * Fetches the fields for a table in a database.
     *
     * @param db
     * @param tableName
     * @return
     * @throws MetaException
     * @throws TException
     * @throws UnknownTableException
     * @throws UnknownDBException
     */
    public List<FieldSchema> getFields(String db, String tableName) throws MetaException, TException,
            UnknownTableException, UnknownDBException {
        try {
            Table table = glueMetastore.getTable(db, tableName);
            return CatalogToHiveConverter.convertFieldSchemaList(table.getStorageDescriptor().getColumns());
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to get field from table: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    /**
     * Fetches the schema for a table in a database.
     *
     * @param db
     * @param tableName
     * @return
     * @throws MetaException
     * @throws TException
     * @throws UnknownTableException
     * @throws UnknownDBException
     */
    public List<FieldSchema> getSchema(String db, String tableName) throws TException,
            UnknownTableException, UnknownDBException {
        try {
            Table table = glueMetastore.getTable(db, tableName);
            List<Column> schemas = table.getStorageDescriptor().getColumns();
            if (table.getPartitionKeys() != null && !table.getPartitionKeys().isEmpty()) {
                schemas.addAll(table.getPartitionKeys());
            }
            return CatalogToHiveConverter.convertFieldSchemaList(schemas);
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        } catch (Exception e) {
            String msg = "Unable to get field from table: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e);
        }
    }

    /**
     * Updates the partition values for a table in database stored in metastore.
     *
     * @param databaseName
     * @param tableName
     * @param partitionValues
     * @param newPartition
     * @throws InvalidOperationException
     * @throws MetaException
     * @throws TException
     */
    public void renamePartitionInCatalog(String databaseName, String tableName, List<String> partitionValues,
                                         org.apache.hadoop.hive.metastore.api.Partition newPartition)
            throws InvalidOperationException,
            TException {
        try {
            PartitionInput partitionInput = GlueInputConverter.convertToPartitionInput(newPartition);
            glueMetastore.updatePartition(databaseName, tableName, partitionValues, partitionInput);
        } catch (AmazonServiceException e) {
            throw CatalogToHiveConverter.wrapInHiveException(e);
        }
    }
}
