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


package com.starrocks.connector.hive.glue;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.connector.hive.glue.metastore.AWSGlueClientFactory;
import com.starrocks.connector.hive.glue.metastore.AWSGlueMetastore;
import com.starrocks.connector.hive.glue.metastore.AWSGlueMetastoreFactory;
import com.starrocks.connector.hive.glue.metastore.GlueMetastoreClientDelegate;
import com.starrocks.connector.hive.glue.util.ExpressionHelper;
import com.starrocks.connector.hive.glue.util.LoggingHelper;
import com.starrocks.connector.hive.glue.util.MetastoreClientUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_COMMENT;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_DATABASE_NAME;

/**
 * Modified from https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore
 */

public class AWSCatalogMetastoreClient implements IMetaStoreClient {
    // TODO "hook" into Hive logging (hive or hive.metastore)
    private static final Logger LOGGER = Logger.getLogger(AWSCatalogMetastoreClient.class);

    private final HiveConf conf;
    private final AWSGlue glueClient;
    private final Warehouse wh;
    private final GlueMetastoreClientDelegate glueMetastoreClientDelegate;
    private final String catalogId;

    private static final int BATCH_DELETE_PARTITIONS_THREADS_COUNT = 5;
    static final String BATCH_DELETE_PARTITIONS_THREAD_POOL_NAME_FORMAT = "batch-delete-partitions-%d";
    private static final ExecutorService BATCH_DELETE_PARTITIONS_THREAD_POOL = Executors.newFixedThreadPool(
            BATCH_DELETE_PARTITIONS_THREADS_COUNT,
            new ThreadFactoryBuilder()
                    .setNameFormat(BATCH_DELETE_PARTITIONS_THREAD_POOL_NAME_FORMAT)
                    .setDaemon(true).build()
    );

    private Map<String, String> currentMetaVars;

    public AWSCatalogMetastoreClient(Configuration conf, HiveMetaHookLoader hook, Boolean allowEmbedded) throws MetaException {
        this.conf = new HiveConf(conf, AWSCatalogMetastoreClient.class);
        glueClient = new AWSGlueClientFactory(this.conf).newClient();

        // TODO preserve existing functionality for HiveMetaHook
        wh = new Warehouse(this.conf);

        AWSGlueMetastore glueMetastore = new AWSGlueMetastoreFactory().newMetastore(this.conf);
        glueMetastoreClientDelegate = new GlueMetastoreClientDelegate(this.conf, glueMetastore, wh);

        snapshotActiveConf();
        catalogId = MetastoreClientUtils.getCatalogId(conf);
        if (!doesDefaultDBExist()) {
            createDefaultDatabase();
        }
    }

    private boolean doesDefaultDBExist() throws MetaException {
        try {
            GetDatabaseRequest getDatabaseRequest =
                    new GetDatabaseRequest().withName(DEFAULT_DATABASE_NAME).withCatalogId(
                            catalogId);
            glueClient.getDatabase(getDatabaseRequest);
        } catch (EntityNotFoundException e) {
            return false;
        } catch (AmazonServiceException e) {
            String msg = "Unable to verify existence of default database: ";
            LOGGER.error(msg, e);
            throw new MetaException(msg + e.getErrorMessage());
        }
        return true;
    }

    private void createDefaultDatabase() throws MetaException {
        Database defaultDB = new Database();
        defaultDB.setName(DEFAULT_DATABASE_NAME);
        defaultDB.setDescription(DEFAULT_DATABASE_COMMENT);
        defaultDB.setLocationUri(wh.getDefaultDatabasePath(DEFAULT_DATABASE_NAME).toString());

        org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet principalPrivilegeSet
                = new org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet();
        principalPrivilegeSet.setRolePrivileges(
                Maps.<String, List<org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo>>newHashMap());

        defaultDB.setPrivileges(principalPrivilegeSet);

        try {
            createDatabase(defaultDB);
        } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
            LOGGER.warn("database - default already exists. Ignoring..");
        } catch (Exception e) {
            LOGGER.error("Unable to create default database", e);
        }
    }

    @Override
    public void createDatabase(Database database) throws InvalidObjectException,
            org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
        glueMetastoreClientDelegate.createDatabase(database);
    }

    @Override
    public Database getDatabase(String name) throws NoSuchObjectException, MetaException, TException {
        return glueMetastoreClientDelegate.getDatabase(name);
    }

    @Override
    public Database getDatabase(String catalogName, String databaseName)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<String> getDatabases(String pattern) throws MetaException, TException {
        return glueMetastoreClientDelegate.getDatabases(pattern);
    }

    @Override
    public List<String> getDatabases(String catName, String databasePattern) throws MetaException, TException {
        return null;
    }

    @Override
    public List<String> getAllDatabases() throws MetaException, TException {
        return getDatabases(".*");
    }

    @Override
    public List<String> getAllDatabases(String catName) throws MetaException, TException {
        return null;
    }

    @Override
    public void alterDatabase(String databaseName, Database database) throws NoSuchObjectException, MetaException,
            TException {
        glueMetastoreClientDelegate.alterDatabase(databaseName, database);
    }

    @Override
    public void alterDatabase(String catName, String dbName, Database newDb)
            throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public void dropDatabase(String name) throws NoSuchObjectException, InvalidOperationException, MetaException,
            TException {
        dropDatabase(name, true, false, false);
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb) throws NoSuchObjectException,
            InvalidOperationException, MetaException, TException {
        dropDatabase(name, deleteData, ignoreUnknownDb, false);
    }

    @Override
    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        glueMetastoreClientDelegate.dropDatabase(name, deleteData, ignoreUnknownDb, cascade);
    }

    @Override
    public void dropDatabase(String catName, String dbName, boolean deleteData, boolean ignoreUnknownDb,
                             boolean cascade)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition add_partition(
            org.apache.hadoop.hive.metastore.api.Partition partition)
            throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException,
            TException {
        glueMetastoreClientDelegate.addPartitions(Lists.newArrayList(partition), false, true);
        return partition;
    }

    @Override
    public int add_partitions(List<org.apache.hadoop.hive.metastore.api.Partition> partitions)
            throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException,
            TException {
        return glueMetastoreClientDelegate.addPartitions(partitions, false, true).size();
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> add_partitions(
            List<org.apache.hadoop.hive.metastore.api.Partition> partitions,
            boolean ifNotExists,
            boolean needResult
    ) throws TException {
        return glueMetastoreClientDelegate.addPartitions(partitions, ifNotExists, needResult);
    }

    @Override
    public int add_partitions_pspec(
            PartitionSpecProxy pSpec
    ) throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException,
            MetaException, TException {
        return glueMetastoreClientDelegate.addPartitionsSpecProxy(pSpec);
    }

    @Override
    public void alterFunction(String dbName, String functionName,
                              org.apache.hadoop.hive.metastore.api.Function newFunction) throws InvalidObjectException,
            MetaException, TException {
        glueMetastoreClientDelegate.alterFunction(dbName, functionName, newFunction);
    }

    @Override
    public void alterFunction(String catName, String dbName, String funcName, Function newFunction)
            throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alter_partition(
            String dbName,
            String tblName,
            org.apache.hadoop.hive.metastore.api.Partition partition
    ) throws InvalidOperationException, MetaException, TException {
        glueMetastoreClientDelegate.alterPartitions(dbName, tblName, Lists.newArrayList(partition));
    }

    @Override
    public void alter_partition(
            String dbName,
            String tblName,
            org.apache.hadoop.hive.metastore.api.Partition partition,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
        glueMetastoreClientDelegate.alterPartitions(dbName, tblName, Lists.newArrayList(partition));
    }

    @Override
    public void alter_partition(String catName, String dbName, String tblName,
                                org.apache.hadoop.hive.metastore.api.Partition newPart,
                                EnvironmentContext environmentContext)
            throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_partitions(
            String dbName,
            String tblName,
            List<org.apache.hadoop.hive.metastore.api.Partition> partitions
    ) throws InvalidOperationException, MetaException, TException {
        glueMetastoreClientDelegate.alterPartitions(dbName, tblName, partitions);
    }

    @Override
    public void alter_partitions(
            String dbName,
            String tblName,
            List<org.apache.hadoop.hive.metastore.api.Partition> partitions,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
        glueMetastoreClientDelegate.alterPartitions(dbName, tblName, partitions);
    }

    @Override
    public void alter_partitions(String catName, String dbName, String tblName,
                                 List<org.apache.hadoop.hive.metastore.api.Partition> newParts,
                                 EnvironmentContext environmentContext)
            throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_table(String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Table table)
            throws InvalidOperationException, MetaException, TException {
        glueMetastoreClientDelegate.alterTable(dbName, tblName, table, null);
    }

    @Override
    public void alter_table(String catName, String dbName, String tblName, Table newTable,
                            EnvironmentContext envContext) throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public void alter_table(String dbName, String tblName, org.apache.hadoop.hive.metastore.api.Table table,
                            boolean cascade)
            throws InvalidOperationException, MetaException, TException {
        glueMetastoreClientDelegate.alterTable(dbName, tblName, table, null);
    }

    @Override
    public void alter_table_with_environmentContext(
            String dbName,
            String tblName,
            org.apache.hadoop.hive.metastore.api.Table table,
            EnvironmentContext environmentContext
    ) throws InvalidOperationException, MetaException, TException {
        glueMetastoreClientDelegate.alterTable(dbName, tblName, table, environmentContext);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String dbName, String tblName,
                                                                          List<String> values)
            throws InvalidObjectException, org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException,
            TException {
        return glueMetastoreClientDelegate.appendPartition(dbName, tblName, values);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String catName, String dbName,
                                                                          String tableName, List<String> partVals)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String dbName, String tblName,
                                                                          String partitionName)
            throws InvalidObjectException,
            org.apache.hadoop.hive.metastore.api.AlreadyExistsException, MetaException, TException {
        List<String> partVals = partitionNameToVals(partitionName);
        return glueMetastoreClientDelegate.appendPartition(dbName, tblName, partVals);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition appendPartition(String catName, String dbName,
                                                                          String tableName, String name)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        return null;
    }

    @Override
    public boolean create_role(org.apache.hadoop.hive.metastore.api.Role role) throws MetaException, TException {
        return glueMetastoreClientDelegate.createRole(role);
    }

    @Override
    public boolean drop_role(String roleName) throws MetaException, TException {
        return glueMetastoreClientDelegate.dropRole(roleName);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Role> list_roles(
            String principalName, org.apache.hadoop.hive.metastore.api.PrincipalType principalType
    ) throws MetaException, TException {
        return glueMetastoreClientDelegate.listRoles(principalName, principalType);
    }

    @Override
    public List<String> listRoleNames() throws MetaException, TException {
        return glueMetastoreClientDelegate.listRoleNames();
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse get_principals_in_role(
            org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest request) throws MetaException, TException {
        return glueMetastoreClientDelegate.getPrincipalsInRole(request);
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
            GetRoleGrantsForPrincipalRequest request) throws MetaException, TException {
        return glueMetastoreClientDelegate.getRoleGrantsForPrincipal(request);
    }

    @Override
    public boolean grant_role(
            String roleName,
            String userName,
            org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
            String grantor, org.apache.hadoop.hive.metastore.api.PrincipalType grantorType,
            boolean grantOption
    ) throws MetaException, TException {
        return glueMetastoreClientDelegate
                .grantRole(roleName, userName, principalType, grantor, grantorType, grantOption);
    }

    @Override
    public boolean revoke_role(
            String roleName,
            String userName,
            org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
            boolean grantOption
    ) throws MetaException, TException {
        return glueMetastoreClientDelegate.revokeRole(roleName, userName, principalType, grantOption);
    }

    @Override
    public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
        glueMetastoreClientDelegate.cancelDelegationToken(tokenStrForm);
    }

    @Override
    public String getTokenStrForm() throws IOException {
        return glueMetastoreClientDelegate.getTokenStrForm();
    }

    @Override
    public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
        return glueMetastoreClientDelegate.addToken(tokenIdentifier, delegationToken);
    }

    @Override
    public boolean removeToken(String tokenIdentifier) throws TException {
        return glueMetastoreClientDelegate.removeToken(tokenIdentifier);
    }

    @Override
    public String getToken(String tokenIdentifier) throws TException {
        return glueMetastoreClientDelegate.getToken(tokenIdentifier);
    }

    @Override
    public List<String> getAllTokenIdentifiers() throws TException {
        return glueMetastoreClientDelegate.getAllTokenIdentifiers();
    }

    @Override
    public int addMasterKey(String key) throws MetaException, TException {
        return glueMetastoreClientDelegate.addMasterKey(key);
    }

    @Override
    public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException, TException {
        glueMetastoreClientDelegate.updateMasterKey(seqNo, key);
    }

    @Override
    public boolean removeMasterKey(Integer keySeq) throws TException {
        return glueMetastoreClientDelegate.removeMasterKey(keySeq);
    }

    @Override
    public String[] getMasterKeys() throws TException {
        return glueMetastoreClientDelegate.getMasterKeys();
    }

    @Override
    public LockResponse checkLock(long lockId)
            throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
        return glueMetastoreClientDelegate.checkLock(lockId);
    }

    @Override
    public void close() {
        currentMetaVars = null;
    }

    @Override
    public void commitTxn(long txnId) throws NoSuchTxnException, TxnAbortedException, TException {
        glueMetastoreClientDelegate.commitTxn(txnId);
    }

    @Override
    public void replCommitTxn(long srcTxnid, String replPolicy)
            throws NoSuchTxnException, TxnAbortedException, TException {

    }

    @Override
    public void abortTxns(List<Long> txnIds) throws TException {
        glueMetastoreClientDelegate.abortTxns(txnIds);
    }

    @Override
    public long allocateTableWriteId(long txnId, String dbName, String tableName) throws TException {
        return 0;
    }

    @Override
    public void replTableWriteIdState(String validWriteIdList, String dbName, String tableName, List<String> partNames)
            throws TException {

    }

    @Override
    public List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName)
            throws TException {
        return null;
    }

    @Override
    public List<TxnToWriteId> replAllocateTableWriteIdsBatch(String dbName, String tableName, String replPolicy,
                                                             List<TxnToWriteId> srcTxnToWriteIdList) throws TException {
        return null;
    }

    @Deprecated
    public void compact(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType
    ) throws TException {
        glueMetastoreClientDelegate.compact(dbName, tblName, partitionName, compactionType);
    }

    @Deprecated
    public void compact(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType,
            Map<String, String> tblProperties
    ) throws TException {
        glueMetastoreClientDelegate.compact(dbName, tblName, partitionName, compactionType, tblProperties);
    }

    @Override
    public CompactionResponse compact2(
            String dbName,
            String tblName,
            String partitionName,
            CompactionType compactionType,
            Map<String, String> tblProperties
    ) throws TException {
        return glueMetastoreClientDelegate.compact2(dbName, tblName, partitionName, compactionType, tblProperties);
    }

    @Override
    public void createFunction(org.apache.hadoop.hive.metastore.api.Function function)
            throws InvalidObjectException, MetaException, TException {
        glueMetastoreClientDelegate.createFunction(function);
    }

    @Override
    public void createTable(org.apache.hadoop.hive.metastore.api.Table tbl)
            throws org.apache.hadoop.hive.metastore.api.AlreadyExistsException, InvalidObjectException, MetaException,
            NoSuchObjectException, TException {
        glueMetastoreClientDelegate.createTable(tbl);
    }

    @Override
    public boolean deletePartitionColumnStatistics(
            String dbName, String tableName, String partName, String colName
    ) throws NoSuchObjectException, MetaException, InvalidObjectException,
            TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
        return glueMetastoreClientDelegate.deletePartitionColumnStatistics(dbName, tableName, partName, colName);
    }

    @Override
    public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName, String partName,
                                                   String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return false;
    }

    @Override
    public boolean deleteTableColumnStatistics(
            String dbName, String tableName, String colName
    ) throws NoSuchObjectException, MetaException, InvalidObjectException,
            TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
        return glueMetastoreClientDelegate.deleteTableColumnStatistics(dbName, tableName, colName);
    }

    @Override
    public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName, String colName)
            throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
        return false;
    }

    @Override
    public void dropFunction(String dbName, String functionName) throws MetaException, NoSuchObjectException,
            InvalidObjectException, org.apache.hadoop.hive.metastore.api.InvalidInputException, TException {
        glueMetastoreClientDelegate.dropFunction(dbName, functionName);
    }

    @Override
    public void dropFunction(String catName, String dbName, String funcName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {

    }

    @Override
    public boolean dropPartition(String dbName, String tblName, List<String> values, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        return glueMetastoreClientDelegate.dropPartition(dbName, tblName, values, false, deleteData, false);
    }

    @Override
    public boolean dropPartition(String catName, String dbName, String tblName, List<String> partVals,
                                 boolean deleteData) throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, List<String> values, PartitionDropOptions options)
            throws TException {
        return glueMetastoreClientDelegate
                .dropPartition(dbName, tblName, values, options.ifExists, options.deleteData, options.purgeData);
    }

    @Override
    public boolean dropPartition(String catName, String dbName, String tblName, List<String> partVals,
                                 PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(String dbName, String tblName,
                List<org.apache.hadoop.hive.metastore.utils.ObjectPair<Integer, byte[]>> partExprs,
                                                                               boolean deleteData, boolean ifExists)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(String dbName, String tblName,
            List<org.apache.hadoop.hive.metastore.utils.ObjectPair<Integer, byte[]>> partExprs,
            boolean deleteData, boolean ifExists,
            boolean needResults)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(String dbName, String tblName,
            List<org.apache.hadoop.hive.metastore.utils.ObjectPair<Integer, byte[]>> partExprs,
            PartitionDropOptions options)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> dropPartitions(String catName, String dbName,
            String tblName,
            List<org.apache.hadoop.hive.metastore.utils.ObjectPair<Integer, byte[]>> partExprs,
            PartitionDropOptions options)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public boolean dropPartition(String dbName, String tblName, String partitionName, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        List<String> values = partitionNameToVals(partitionName);
        return glueMetastoreClientDelegate.dropPartition(dbName, tblName, values, false, deleteData, false);
    }

    @Override
    public boolean dropPartition(String catName, String dbName, String tblName, String name, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        return false;
    }

    @Deprecated
    public void dropTable(String tableName, boolean deleteData) throws MetaException, UnknownTableException, TException,
            NoSuchObjectException {
        dropTable(DEFAULT_DATABASE_NAME, tableName, deleteData, false);
    }

    @Override
    public void dropTable(String dbname, String tableName) throws MetaException, TException, NoSuchObjectException {
        dropTable(dbname, tableName, true, true, false);
    }

    @Override
    public void dropTable(String catName, String dbName, String tableName, boolean deleteData,
                          boolean ignoreUnknownTable, boolean ifPurge)
            throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void truncateTable(String dbName, String tableName, List<String> partNames)
            throws MetaException, TException {

    }

    @Override
    public void truncateTable(String catName, String dbName, String tableName, List<String> partNames)
            throws MetaException, TException {

    }

    @Override
    public CmRecycleResponse recycleDirToCmPath(CmRecycleRequest request) throws MetaException, TException {
        return null;
    }

    @Override
    public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab)
            throws MetaException, TException, NoSuchObjectException {
        dropTable(dbname, tableName, deleteData, ignoreUnknownTab, false);
    }

    @Override
    public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab,
                          boolean ifPurge)
            throws MetaException, TException, NoSuchObjectException {
        glueMetastoreClientDelegate.dropTable(dbname, tableName, deleteData, ignoreUnknownTab, ifPurge);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition exchange_partition(
            Map<String, String> partitionSpecs,
            String srcDb,
            String srcTbl,
            String dstDb,
            String dstTbl
    ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return glueMetastoreClientDelegate.exchangePartition(partitionSpecs, srcDb, srcTbl, dstDb, dstTbl);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition exchange_partition(Map<String, String> partitionSpecs,
                                                                             String sourceCat, String sourceDb,
                                                                             String sourceTable, String destCat,
                                                                             String destdb, String destTableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> exchange_partitions(
            Map<String, String> partitionSpecs,
            String sourceDb,
            String sourceTbl,
            String destDb,
            String destTbl
    ) throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return glueMetastoreClientDelegate.exchangePartitions(partitionSpecs, sourceDb, sourceTbl, destDb, destTbl);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> exchange_partitions(Map<String, String> partitionSpecs,
                                                                                    String sourceCat, String sourceDb,
                                                                                    String sourceTable, String destCat,
                                                                                    String destdb, String destTableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
        return null;
    }

    @Override
    public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames, List<String> partName)
            throws NoSuchObjectException, MetaException, TException {
        return glueMetastoreClientDelegate.getAggrColStatsFor(dbName, tblName, colNames, partName);
    }

    @Override
    public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName, List<String> colNames,
                                        List<String> partNames)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<String> getAllTables(String dbname) throws MetaException, TException, UnknownDBException {
        return getTables(dbname, ".*");
    }

    @Override
    public List<String> getAllTables(String catName, String dbName)
            throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public String getConfigValue(String name, String defaultValue) throws TException, ConfigValSecurityException {
        if (!Pattern.matches("(hive|hdfs|mapred).*", name)) {
            throw new ConfigValSecurityException(
                    "For security reasons, the config key " + name + " cannot be accessed");
        }

        return conf.get(name, defaultValue);
    }

    @Override
    public String getDelegationToken(
            String owner, String renewerKerberosPrincipalName
    ) throws MetaException, TException {
        return glueMetastoreClientDelegate.getDelegationToken(owner, renewerKerberosPrincipalName);
    }

    @Override
    public List<FieldSchema> getFields(String db, String tableName) throws MetaException, TException,
            UnknownTableException, UnknownDBException {
        return glueMetastoreClientDelegate.getFields(db, tableName);
    }

    @Override
    public List<FieldSchema> getFields(String catName, String db, String tableName)
            throws MetaException, TException, UnknownTableException, UnknownDBException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Function getFunction(String dbName, String functionName)
            throws MetaException, TException {
        return glueMetastoreClientDelegate.getFunction(dbName, functionName);
    }

    @Override
    public Function getFunction(String catName, String dbName, String funcName) throws MetaException, TException {
        return null;
    }

    @Override
    public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
        return glueMetastoreClientDelegate.getFunctions(dbName, pattern);
    }

    @Override
    public List<String> getFunctions(String catName, String dbName, String pattern) throws MetaException, TException {
        return null;
    }

    @Override
    public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public String getMetaConf(String key) throws MetaException, TException {
        ConfVars metaConfVar = HiveConf.getMetaConf(key);
        if (metaConfVar == null) {
            throw new MetaException("Invalid configuration key " + key);
        }
        return conf.get(key, metaConfVar.getDefaultValue());
    }

    @Override
    public void createCatalog(Catalog catalog)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterCatalog(String catalogName, Catalog newCatalog)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public Catalog getCatalog(String catName) throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<String> getCatalogs() throws MetaException, TException {
        return null;
    }

    @Override
    public void dropCatalog(String catName)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName,
                                                                       List<String> values)
            throws NoSuchObjectException, MetaException, TException {
        return glueMetastoreClientDelegate.getPartition(dbName, tblName, values);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String catName, String dbName, String tblName,
                                                                       List<String> partVals)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String dbName, String tblName,
                                                                       String partitionName)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return glueMetastoreClientDelegate.getPartition(dbName, tblName, partitionName);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartition(String catName, String dbName, String tblName,
                                                                       String name)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            String dbName,
            String tableName,
            List<String> partitionNames,
            List<String> columnNames
    ) throws NoSuchObjectException, MetaException, TException {
        return glueMetastoreClientDelegate.getPartitionColumnStatistics(dbName, tableName, partitionNames, columnNames);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String catName, String dbName,
                                                                               String tableName, List<String> partNames,
                                                                               List<String> colNames)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartitionWithAuthInfo(
            String databaseName, String tableName, List<String> values,
            String userName, List<String> groupNames)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {

        // TODO move this into the service
        org.apache.hadoop.hive.metastore.api.Partition partition = getPartition(databaseName, tableName, values);
        org.apache.hadoop.hive.metastore.api.Table table = getTable(databaseName, tableName);
        if ("TRUE".equalsIgnoreCase(table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
            String partName = Warehouse.makePartName(table.getPartitionKeys(), values);
            HiveObjectRef obj = new HiveObjectRef();
            obj.setObjectType(HiveObjectType.PARTITION);
            obj.setDbName(databaseName);
            obj.setObjectName(tableName);
            obj.setPartValues(values);
            org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet privilegeSet =
                    this.get_privilege_set(obj, userName, groupNames);
            partition.setPrivileges(privilegeSet);
        }

        return partition;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Partition getPartitionWithAuthInfo(String catName, String dbName,
                                                                                   String tableName, List<String> pvals,
                                                                                   String userName,
                                                                                   List<String> groupNames)
            throws MetaException, UnknownTableException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(
            String databaseName, String tableName, List<String> partitionNames)
            throws NoSuchObjectException, MetaException, TException {
        return glueMetastoreClientDelegate.getPartitionsByNames(databaseName, tableName, partitionNames);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitionsByNames(String catName, String dbName,
                                                                                     String tblName,
                                                                                     List<String> partNames)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<FieldSchema> getSchema(String db, String tableName)
            throws MetaException, TException, UnknownTableException,
            UnknownDBException {
        return glueMetastoreClientDelegate.getSchema(db, tableName);
    }

    @Override
    public List<FieldSchema> getSchema(String catName, String db, String tableName)
            throws MetaException, TException, UnknownTableException, UnknownDBException {
        return null;
    }

    @Deprecated
    public org.apache.hadoop.hive.metastore.api.Table getTable(String tableName)
            throws MetaException, TException, NoSuchObjectException {
        //this has been deprecated
        return getTable(DEFAULT_DATABASE_NAME, tableName);
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.Table getTable(String dbName, String tableName)
            throws MetaException, TException, NoSuchObjectException {
        return glueMetastoreClientDelegate.getTable(dbName, tableName);
    }

    @Override
    public Table getTable(String catName, String dbName, String tableName) throws MetaException, TException {
        return null;
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames)
            throws NoSuchObjectException, MetaException, TException {
        return glueMetastoreClientDelegate.getTableColumnStatistics(dbName, tableName, colNames);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName, String tableName,
                                                              List<String> colNames)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Table> getTableObjectsByName(String dbName,
                                                                                  List<String> tableNames)
            throws MetaException,
            InvalidOperationException, UnknownDBException, TException {
        List<org.apache.hadoop.hive.metastore.api.Table> hiveTables = Lists.newArrayList();
        for (String tableName : tableNames) {
            hiveTables.add(getTable(dbName, tableName));
        }

        return hiveTables;
    }

    @Override
    public List<Table> getTableObjectsByName(String catName, String dbName, List<String> tableNames)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return null;
    }

    @Override
    public Materialization getMaterializationInvalidationInfo(CreationMetadata cm, String validTxnList)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        return null;
    }

    @Override
    public void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm)
            throws MetaException, TException {

    }

    @Override
    public void updateCreationMetadata(String catName, String dbName, String tableName, CreationMetadata cm)
            throws MetaException, TException {

    }

    @Override
    public List<String> getTables(String dbname, String tablePattern)
            throws MetaException, TException, UnknownDBException {
        return glueMetastoreClientDelegate.getTables(dbname, tablePattern);
    }

    @Override
    public List<String> getTables(String catName, String dbName, String tablePattern)
            throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public List<String> getTables(String dbname, String tablePattern, TableType tableType)
            throws MetaException, TException, UnknownDBException {
        return glueMetastoreClientDelegate.getTables(dbname, tablePattern, tableType);
    }

    @Override
    public List<String> getTables(String catName, String dbName, String tablePattern, TableType tableType)
            throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String dbName)
            throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
            throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
            throws MetaException, TException, UnknownDBException {
        return glueMetastoreClientDelegate.getTableMeta(dbPatterns, tablePatterns, tableTypes);
    }

    @Override
    public List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns,
                                        List<String> tableTypes) throws MetaException, TException, UnknownDBException {
        return null;
    }

    @Override
    public ValidTxnList getValidTxns() throws TException {
        return glueMetastoreClientDelegate.getValidTxns();
    }

    @Override
    public ValidTxnList getValidTxns(long currentTxn) throws TException {
        return glueMetastoreClientDelegate.getValidTxns(currentTxn);
    }

    @Override
    public ValidWriteIdList getValidWriteIds(String fullTableName) throws TException {
        return null;
    }

    @Override
    public List<TableValidWriteIds> getValidWriteIds(List<String> tablesList, String validTxnList) throws TException {
        return null;
    }

    @Override
    public org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet get_privilege_set(
            HiveObjectRef obj,
            String user, List<String> groups
    ) throws MetaException, TException {
        return glueMetastoreClientDelegate.getPrivilegeSet(obj, user, groups);
    }

    @Override
    public boolean grant_privileges(org.apache.hadoop.hive.metastore.api.PrivilegeBag privileges)
            throws MetaException, TException {
        return glueMetastoreClientDelegate.grantPrivileges(privileges);
    }

    @Override
    public boolean revoke_privileges(
            org.apache.hadoop.hive.metastore.api.PrivilegeBag privileges,
            boolean grantOption
    ) throws MetaException, TException {
        return glueMetastoreClientDelegate.revokePrivileges(privileges, grantOption);
    }

    @Override
    public boolean refresh_privileges(HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges)
            throws MetaException, TException {
        return false;
    }

    @Override
    public void heartbeat(long txnId, long lockId)
            throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
        glueMetastoreClientDelegate.heartbeat(txnId, lockId);
    }

    @Override
    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
        return glueMetastoreClientDelegate.heartbeatTxnRange(min, max);
    }

    @Override
    public boolean isCompatibleWith(Configuration conf) {
        return false;
    }

    @Override
    public void setHiveAddedJars(String addedJars) {
        //taken from HiveMetaStoreClient
        HiveConf.setVar(conf, ConfVars.HIVEADDEDJARS, addedJars);
    }

    @Override
    public boolean isLocalMetaStore() {
        return false;
    }

    private void snapshotActiveConf() {
        currentMetaVars = new HashMap<String, String>(HiveConf.metaVars.length);
        for (ConfVars oneVar : HiveConf.metaVars) {
            currentMetaVars.put(oneVar.varname, conf.get(oneVar.varname, ""));
        }
    }

    @Override
    public boolean isPartitionMarkedForEvent(String dbName, String tblName, Map<String, String> partKVs,
                                             PartitionEventType eventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        return glueMetastoreClientDelegate.isPartitionMarkedForEvent(dbName, tblName, partKVs, eventType);
    }

    @Override
    public boolean isPartitionMarkedForEvent(String catName, String dbName, String tblName,
                                             Map<String, String> partKVs, PartitionEventType eventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        return false;
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tblName, short max)
            throws MetaException, TException {
        try {
            return listPartitionNames(dbName, tblName, Collections.emptyList(), max);
        } catch (NoSuchObjectException e) {
            // For compatibility with Hive 1.0.0
            return Collections.emptyList();
        }
    }

    @Override
    public List<String> listPartitionNames(String catName, String dbName, String tblName, int maxParts)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName,
                                           List<String> values, short max)
            throws MetaException, TException, NoSuchObjectException {
        return glueMetastoreClientDelegate.listPartitionNames(databaseName, tableName, values, max);
    }

    @Override
    public List<String> listPartitionNames(String catName, String dbName, String tblName, List<String> partVals,
                                           int maxParts) throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request)
            throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public int getNumPartitionsByFilter(String dbName, String tableName, String filter)
            throws MetaException, NoSuchObjectException, TException {
        return glueMetastoreClientDelegate.getNumPartitionsByFilter(dbName, tableName, filter);
    }

    @Override
    public int getNumPartitionsByFilter(String catName, String dbName, String tableName, String filter)
            throws MetaException, NoSuchObjectException, TException {
        return 0;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String dbName, String tblName, int max) throws TException {
        return glueMetastoreClientDelegate.listPartitionSpecs(dbName, tblName, max);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName, int maxParts)
            throws TException {
        return null;
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String dbName, String tblName, String filter, int max)
            throws MetaException, NoSuchObjectException, TException {
        return glueMetastoreClientDelegate.listPartitionSpecsByFilter(dbName, tblName, filter, max);
    }

    @Override
    public PartitionSpecProxy listPartitionSpecsByFilter(String catName, String dbName, String tblName, String filter,
                                                         int maxParts)
            throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(String dbName, String tblName, short max)
            throws NoSuchObjectException, MetaException, TException {
        return listPartitions(dbName, tblName, Collections.emptyList(), max);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(String catName, String dbName,
                                                                               String tblName, int maxParts)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(
            String databaseName,
            String tableName,
            List<String> values,
            short max
    ) throws NoSuchObjectException, MetaException, TException {
        String expression = null;
        if (values != null) {
            org.apache.hadoop.hive.metastore.api.Table table = getTable(databaseName, tableName);
            expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, values);
        }
        return glueMetastoreClientDelegate.getPartitions(databaseName, tableName, expression, (long) max);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitions(String catName, String dbName,
                                                                               String tblName, List<String> partVals,
                                                                               int maxParts)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public boolean listPartitionsByExpr(
            String databaseName,
            String tableName,
            byte[] expr,
            String defaultPartitionName,
            short max,
            List<org.apache.hadoop.hive.metastore.api.Partition> result
    ) throws TException {
        checkNotNull(result, "The result argument cannot be null.");

        String catalogExpression = ExpressionHelper.convertHiveExpressionToCatalogExpression(expr);
        List<org.apache.hadoop.hive.metastore.api.Partition> partitions =
                glueMetastoreClientDelegate.getPartitions(databaseName, tableName, catalogExpression, (long) max);
        result.addAll(partitions);

        return false;
    }

    @Override
    public boolean listPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr,
                                        String defaultPartitionName, int maxParts,
                                        List<org.apache.hadoop.hive.metastore.api.Partition> result) throws TException {
        return false;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsByFilter(
            String databaseName,
            String tableName,
            String filter,
            short max
    ) throws MetaException, NoSuchObjectException, TException {
        // we need to replace double quotes with single quotes in the filter expression
        // since server side does not accept double quote expressions.
        if (StringUtils.isNotBlank(filter)) {
            filter = ExpressionHelper.replaceDoubleQuoteWithSingleQuotes(filter);
        }
        return glueMetastoreClientDelegate.getPartitions(databaseName, tableName, filter, (long) max);
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsByFilter(String catName, String dbName,
                                                                                       String tblName, String filter,
                                                                                       int maxParts)
            throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String database,
                                                                                           String table, short maxParts,
                                                                                           String user,
                                                                                           List<String> groups)
            throws MetaException, TException, NoSuchObjectException {
        List<org.apache.hadoop.hive.metastore.api.Partition> partitions = listPartitions(database, table, maxParts);

        for (org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
            HiveObjectRef obj = new HiveObjectRef();
            obj.setObjectType(HiveObjectType.PARTITION);
            obj.setDbName(database);
            obj.setObjectName(table);
            obj.setPartValues(p.getValues());
            org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet set = this.get_privilege_set(obj, user, groups);
            p.setPrivileges(set);
        }

        return partitions;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String catName,
                                                                                           String dbName,
                                                                                           String tableName,
                                                                                           int maxParts,
                                                                                           String userName,
                                                                                           List<String> groupNames)
            throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String database,
                                                                                           String table,
                                                                                           List<String> partVals,
                                                                                           short maxParts,
                                                                                           String user,
                                                                                           List<String> groups)
            throws MetaException, TException, NoSuchObjectException {
        List<org.apache.hadoop.hive.metastore.api.Partition> partitions =
                listPartitions(database, table, partVals, maxParts);

        for (org.apache.hadoop.hive.metastore.api.Partition p : partitions) {
            HiveObjectRef obj = new HiveObjectRef();
            obj.setObjectType(HiveObjectType.PARTITION);
            obj.setDbName(database);
            obj.setObjectName(table);
            obj.setPartValues(p.getValues());
            org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet set;
            try {
                set = get_privilege_set(obj, user, groups);
            } catch (MetaException e) {
                LOGGER.info(String.format("No privileges found for user: %s, "
                        + "groups: [%s]", user, LoggingHelper.concatCollectionToStringForLogging(groups, ",")));
                set = new org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet();
            }
            p.setPrivileges(set);
        }

        return partitions;
    }

    @Override
    public List<org.apache.hadoop.hive.metastore.api.Partition> listPartitionsWithAuthInfo(String catName,
                                                                                           String dbName,
                                                                                           String tableName,
                                                                                           List<String> partialPvals,
                                                                                           int maxParts,
                                                                                           String userName,
                                                                                           List<String> groupNames)
            throws MetaException, TException, NoSuchObjectException {
        return null;
    }

    @Override
    public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables) throws MetaException,
            TException, InvalidOperationException, UnknownDBException {
        return glueMetastoreClientDelegate.listTableNamesByFilter(dbName, filter, maxTables);
    }

    @Override
    public List<String> listTableNamesByFilter(String catName, String dbName, String filter, int maxTables)
            throws TException, InvalidOperationException, UnknownDBException {
        return null;
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(
            String principal,
            org.apache.hadoop.hive.metastore.api.PrincipalType principalType,
            HiveObjectRef objectRef
    ) throws MetaException, TException {
        return glueMetastoreClientDelegate.listPrivileges(principal, principalType, objectRef);
    }

    @Override
    public LockResponse lock(LockRequest lockRequest) throws NoSuchTxnException, TxnAbortedException, TException {
        return glueMetastoreClientDelegate.lock(lockRequest);
    }

    @Override
    public void markPartitionForEvent(
            String dbName,
            String tblName,
            Map<String, String> partKVs,
            PartitionEventType eventType
    ) throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {
        glueMetastoreClientDelegate.markPartitionForEvent(dbName, tblName, partKVs, eventType);
    }

    @Override
    public void markPartitionForEvent(String catName, String dbName, String tblName, Map<String, String> partKVs,
                                      PartitionEventType eventType)
            throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
            UnknownPartitionException, InvalidPartitionException {

    }

    @Override
    public long openTxn(String user) throws TException {
        return glueMetastoreClientDelegate.openTxn(user);
    }

    @Override
    public List<Long> replOpenTxn(String replPolicy, List<Long> srcTxnIds, String user) throws TException {
        return null;
    }

    @Override
    public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
        return glueMetastoreClientDelegate.openTxns(user, numTxns);
    }

    @Override
    public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
        // Lifted from HiveMetaStore
        if (name.length() == 0) {
            return new HashMap<String, String>();
        }
        return Warehouse.makeSpecFromName(name);
    }

    @Override
    public List<String> partitionNameToVals(String name) throws MetaException, TException {
        return glueMetastoreClientDelegate.partitionNameToVals(name);
    }

    @Override
    public void reconnect() throws MetaException {
        // TODO reset active Hive confs for metastore glueClient
        LOGGER.debug("reconnect() was called.");
    }

    @Override
    public void renamePartition(String dbName, String tblName, List<String> partitionValues,
                                org.apache.hadoop.hive.metastore.api.Partition newPartition)
            throws InvalidOperationException, MetaException, TException {
        throw new TException("method not implemented");
    }

    @Override
    public void renamePartition(String catName, String dbname, String tableName, List<String> partVals,
                                org.apache.hadoop.hive.metastore.api.Partition newPart)
            throws InvalidOperationException, MetaException, TException {

    }

    @Override
    public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
        return glueMetastoreClientDelegate.renewDelegationToken(tokenStrForm);
    }

    @Override
    public void rollbackTxn(long txnId) throws NoSuchTxnException, TException {
        glueMetastoreClientDelegate.rollbackTxn(txnId);
    }

    @Override
    public void replRollbackTxn(long srcTxnid, String replPolicy) throws NoSuchTxnException, TException {

    }

    @Override
    public void setMetaConf(String key, String value) throws MetaException, TException {
        ConfVars confVar = HiveConf.getMetaConf(key);
        if (confVar == null) {
            throw new MetaException("Invalid configuration key " + key);
        }
        String validate = confVar.validate(value);
        if (validate != null) {
            throw new MetaException("Invalid configuration value " + value + " for key " + key + " by " + validate);
        }
        conf.set(key, value);
    }

    @Override
    public boolean setPartitionColumnStatistics(org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest request)
            throws NoSuchObjectException, InvalidObjectException,
            MetaException, TException, org.apache.hadoop.hive.metastore.api.InvalidInputException {
        return glueMetastoreClientDelegate.setPartitionColumnStatistics(request);
    }

    @Override
    public void flushCache() {
        //no op
    }

    @Override
    public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
        return glueMetastoreClientDelegate.getFileMetadata(fileIds);
    }

    @Override
    public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
            List<Long> fileIds,
            ByteBuffer sarg,
            boolean doGetFooters
    ) throws TException {
        return glueMetastoreClientDelegate.getFileMetadataBySarg(fileIds, sarg, doGetFooters);
    }

    @Override
    public void clearFileMetadata(List<Long> fileIds) throws TException {
        glueMetastoreClientDelegate.clearFileMetadata(fileIds);
    }

    @Override
    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
        glueMetastoreClientDelegate.putFileMetadata(fileIds, metadata);
    }

    @Override
    public boolean isSameConfObj(Configuration c) {
        return false;
    }

    @Override
    public boolean cacheFileMetadata(String dbName, String tblName, String partName, boolean allParts)
            throws TException {
        return glueMetastoreClientDelegate.cacheFileMetadata(dbName, tblName, partName, allParts);
    }

    @Override
    public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest primaryKeysRequest)
            throws MetaException, NoSuchObjectException, TException {
        // PrimaryKeys are currently unsupported
        //return null to allow DESCRIBE (FORMATTED | EXTENDED)
        return null;
    }

    @Override
    public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest foreignKeysRequest)
            throws MetaException, NoSuchObjectException, TException {
        // PrimaryKeys are currently unsupported
        //return null to allow DESCRIBE (FORMATTED | EXTENDED)
        return null;
    }

    @Override
    public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest request)
            throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest request)
            throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest request)
            throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest request)
            throws MetaException, NoSuchObjectException, TException {
        return null;
    }

    @Override
    public void createTableWithConstraints(Table tTbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
                                           List<SQLUniqueConstraint> uniqueConstraints,
                                           List<SQLNotNullConstraint> notNullConstraints,
                                           List<SQLDefaultConstraint> defaultConstraints,
                                           List<SQLCheckConstraint> checkConstraints)
            throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void dropConstraint(
            String dbName,
            String tblName,
            String constraintName
    ) throws MetaException, NoSuchObjectException, TException {
        glueMetastoreClientDelegate.dropConstraint(dbName, tblName, constraintName);
    }

    @Override
    public void dropConstraint(String catName, String dbName, String tableName, String constraintName)
            throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols)
            throws MetaException, NoSuchObjectException, TException {
        glueMetastoreClientDelegate.addPrimaryKey(primaryKeyCols);
    }

    @Override
    public void addForeignKey(List<SQLForeignKey> foreignKeyCols)
            throws MetaException, NoSuchObjectException, TException {
        glueMetastoreClientDelegate.addForeignKey(foreignKeyCols);
    }

    @Override
    public void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols)
            throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols)
            throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints)
            throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public void addCheckConstraint(List<SQLCheckConstraint> checkConstraints)
            throws MetaException, NoSuchObjectException, TException {

    }

    @Override
    public String getMetastoreDbUuid() throws MetaException, TException {
        return null;
    }

    @Override
    public void createResourcePlan(WMResourcePlan resourcePlan, String copyFromName)
            throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public WMFullResourcePlan getResourcePlan(String resourcePlanName)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public List<WMResourcePlan> getAllResourcePlans() throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public void dropResourcePlan(String resourcePlanName) throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public WMFullResourcePlan alterResourcePlan(String resourcePlanName, WMNullableResourcePlan resourcePlan,
                                                boolean canActivateDisabled, boolean isForceDeactivate,
                                                boolean isReplace)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        return null;
    }

    @Override
    public WMFullResourcePlan getActiveResourcePlan() throws MetaException, TException {
        return null;
    }

    @Override
    public WMValidateResourcePlanResponse validateResourcePlan(String resourcePlanName)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        return null;
    }

    @Override
    public void createWMTrigger(WMTrigger trigger) throws InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterWMTrigger(WMTrigger trigger)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void dropWMTrigger(String resourcePlanName, String triggerName)
            throws NoSuchObjectException, MetaException, TException {

    }

    @Override
    public List<WMTrigger> getTriggersForResourcePlan(String resourcePlan)
            throws NoSuchObjectException, MetaException, TException {
        return null;
    }

    @Override
    public void createWMPool(WMPool pool)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void alterWMPool(WMNullablePool pool, String poolPath)
            throws NoSuchObjectException, InvalidObjectException, TException {

    }

    @Override
    public void dropWMPool(String resourcePlanName, String poolPath) throws TException {

    }

    @Override
    public void createOrUpdateWMMapping(WMMapping mapping, boolean isUpdate) throws TException {

    }

    @Override
    public void dropWMMapping(WMMapping mapping) throws TException {

    }

    @Override
    public void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath,
                                                 boolean shouldDrop)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {

    }

    @Override
    public void createISchema(ISchema schema) throws TException {

    }

    @Override
    public void alterISchema(String catName, String dbName, String schemaName, ISchema newSchema) throws TException {

    }

    @Override
    public ISchema getISchema(String catName, String dbName, String name) throws TException {
        return null;
    }

    @Override
    public void dropISchema(String catName, String dbName, String name) throws TException {

    }

    @Override
    public void addSchemaVersion(SchemaVersion schemaVersion) throws TException {

    }

    @Override
    public SchemaVersion getSchemaVersion(String catName, String dbName, String schemaName, int version)
            throws TException {
        return null;
    }

    @Override
    public SchemaVersion getSchemaLatestVersion(String catName, String dbName, String schemaName) throws TException {
        return null;
    }

    @Override
    public List<SchemaVersion> getSchemaAllVersions(String catName, String dbName, String schemaName)
            throws TException {
        return null;
    }

    @Override
    public void dropSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException {

    }

    @Override
    public FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst rqst) throws TException {
        return null;
    }

    @Override
    public void mapSchemaVersionToSerde(String catName, String dbName, String schemaName, int version, String serdeName)
            throws TException {

    }

    @Override
    public void setSchemaVersionState(String catName, String dbName, String schemaName, int version,
                                      SchemaVersionState state) throws TException {

    }

    @Override
    public void addSerDe(SerDeInfo serDeInfo) throws TException {

    }

    @Override
    public SerDeInfo getSerDe(String serDeName) throws TException {
        return null;
    }

    @Override
    public LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
        return null;
    }

    @Override
    public boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
        return false;
    }

    @Override
    public void addRuntimeStat(RuntimeStat stat) throws TException {

    }

    @Override
    public List<RuntimeStat> getRuntimeStats(int maxWeight, int maxCreateTime) throws TException {
        return null;
    }

    @Override
    public ShowCompactResponse showCompactions() throws TException {
        return glueMetastoreClientDelegate.showCompactions();
    }

    @Override
    public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames)
            throws TException {

    }

    @Override
    public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames,
                                     DataOperationType operationType) throws TException {

    }

    @Override
    public void insertTable(org.apache.hadoop.hive.metastore.api.Table table, boolean overwrite) throws MetaException {
        glueMetastoreClientDelegate.insertTable(table, overwrite);
    }

    @Override
    public NotificationEventResponse getNextNotification(
            long lastEventId, int maxEvents, NotificationFilter notificationFilter) throws TException {
        return glueMetastoreClientDelegate.getNextNotification(lastEventId, maxEvents, notificationFilter);
    }

    @Override
    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
        return glueMetastoreClientDelegate.getCurrentNotificationEventId();
    }

    @Override
    public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst)
            throws TException {
        return null;
    }

    @Override
    public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
        return glueMetastoreClientDelegate.fireListenerEvent(fireEventRequest);
    }

    @Override
    public ShowLocksResponse showLocks() throws TException {
        return glueMetastoreClientDelegate.showLocks();
    }

    @Override
    public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
        return glueMetastoreClientDelegate.showLocks(showLocksRequest);
    }

    @Override
    public GetOpenTxnsInfoResponse showTxns() throws TException {
        return glueMetastoreClientDelegate.showTxns();
    }

    @Deprecated
    public boolean tableExists(String tableName) throws MetaException, TException, UnknownDBException {
        //this method has been deprecated;
        return tableExists(DEFAULT_DATABASE_NAME, tableName);
    }

    @Override
    public boolean tableExists(String databaseName, String tableName) throws MetaException, TException,
            UnknownDBException {
        return glueMetastoreClientDelegate.tableExists(databaseName, tableName);
    }

    @Override
    public boolean tableExists(String catName, String dbName, String tableName)
            throws MetaException, TException, UnknownDBException {
        return false;
    }

    @Override
    public void unlock(long lockId) throws NoSuchLockException, TxnOpenException, TException {
        glueMetastoreClientDelegate.unlock(lockId);
    }

    @Override
    public boolean updatePartitionColumnStatistics(
            org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
            org.apache.hadoop.hive.metastore.api.InvalidInputException {
        return glueMetastoreClientDelegate.updatePartitionColumnStatistics(columnStatistics);
    }

    @Override
    public boolean updateTableColumnStatistics(org.apache.hadoop.hive.metastore.api.ColumnStatistics columnStatistics)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
            org.apache.hadoop.hive.metastore.api.InvalidInputException {
        return glueMetastoreClientDelegate.updateTableColumnStatistics(columnStatistics);
    }

    @Override
    public void validatePartitionNameCharacters(List<String> partVals) throws TException, MetaException {
        try {
            String partitionValidationRegex = conf.getVar(ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN);
            Pattern partitionValidationPattern = Strings.isNullOrEmpty(partitionValidationRegex) ? null
                    : Pattern.compile(partitionValidationRegex);
            MetaStoreUtils.validatePartitionNameCharacters(partVals, partitionValidationPattern);
        } catch (Exception e) {
            throw new MetaException(e.getMessage());
        }
    }

    private Path constructRenamedPath(Path defaultNewPath, Path currentPath) {
        URI currentUri = currentPath.toUri();

        return new Path(currentUri.getScheme(), currentUri.getAuthority(),
                defaultNewPath.toUri().getPath());
    }

}
