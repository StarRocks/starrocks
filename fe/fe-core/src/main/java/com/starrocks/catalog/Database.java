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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Database.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.catalog.system.sys.SysDb;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.concurrent.LockUtils.SlowLockLogStats;
import com.starrocks.common.util.concurrent.QueryableReentrantReadWriteLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.DropInfo;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.zip.Adler32;

/**
 * Internal representation of db-related metadata. Owned by GlobalStateMgr instance.
 * Not thread safe.
 * <p/>
 * The static initialization method loadDb is the only way to construct a Db
 * object.
 * <p/>
 * Tables are stored in a map from the table name to the table object. They may
 * be loaded 'eagerly' at construction or 'lazily' on first reference. Tables
 * are accessed via getTable which may trigger a metadata read in two cases: *
 * if the table has never been loaded * if the table loading failed on the
 * previous attempt
 */
public class Database extends MetaObject implements Writable {
    private static final Logger LOG = LogManager.getLogger(Database.class);
    // empirical value.
    // assume that the time a lock is held by thread is less than 100ms
    public static final long TRY_LOCK_TIMEOUT_MS = 100L;

    @SerializedName(value = "i")
    private long id;
    @SerializedName(value = "n")
    private String fullQualifiedName;
    // user define function
    @SerializedName(value = "f")
    private ConcurrentMap<String, List<Function>> name2Function = Maps.newConcurrentMap();
    @SerializedName(value = "d")
    private volatile long dataQuotaBytes;
    @SerializedName(value = "r")
    private volatile long replicaQuotaSize;

    private final Map<String, Table> nameToTable;
    private final Map<Long, Table> idToTable;

    // catalogName is set if the database comes from an external catalog
    private String catalogName;

    private final QueryableReentrantReadWriteLock rwLock;
    private SlowLockLogStats slowLockLogStats = new SlowLockLogStats();

    // This param is used to make sure db not dropped when leader node writes wal,
    // so this param does not need to be persisted,
    // and this param maybe not right when the db is dropped and the catalog has done a checkpoint,
    // but that's ok to meet our needs.
    private volatile boolean exist = true;

    // For external database location like hdfs://name_node:9000/user/hive/warehouse/test.db/
    private String location;

    public Database() {
        this(0, null);
    }

    public Database(long id, String name) {
        this(id, name, "");
    }

    public Database(long id, String name, String location) {
        this.id = id;
        this.fullQualifiedName = name;
        if (this.fullQualifiedName == null) {
            this.fullQualifiedName = "";
        }
        this.rwLock = new QueryableReentrantReadWriteLock(true);
        this.idToTable = new ConcurrentHashMap<>();
        this.nameToTable = new ConcurrentHashMap<>();
        this.dataQuotaBytes = FeConstants.DEFAULT_DB_DATA_QUOTA_BYTES;
        this.replicaQuotaSize = FeConstants.DEFAULT_DB_REPLICA_QUOTA_SIZE;
        this.location = location;
    }

    /**
     * Database rwLock will be deleted later, please do not use this interface directly.
     * Use Locker.lockDatabase to obtain db lock
     */
    @Deprecated
    public QueryableReentrantReadWriteLock getRwLock() {
        return rwLock;
    }

    public SlowLockLogStats getSlowLockLogStats() {
        return slowLockLogStats;
    }

    public long getId() {
        return id;
    }

    /**
     * Get the unique id of database in string format, since we already ensure
     * the uniqueness of id for internal database, we just convert it to string
     * and return.
     * Note: for external database, we use database name as the privilege entry
     * id, not the uuid returned by this interface.
     *
     * @return unique id of database in string format
     */
    public String getUUID() {
        if (CatalogMgr.isExternalCatalog(catalogName)) {
            return catalogName + "." + fullQualifiedName;
        }
        return Long.toString(id);
    }

    public String getOriginName() {
        return fullQualifiedName;
    }

    public String getFullName() {
        return fullQualifiedName;
    }

    public String getLocation() {
        return location;
    }

    public Database setLocation(String location) {
        this.location = location;
        return this;
    }

    public void setDataQuota(long newQuota) {
        Preconditions.checkArgument(newQuota >= 0L);
        LOG.info("database[{}] set quota from {} to {}", fullQualifiedName, dataQuotaBytes, newQuota);
        this.dataQuotaBytes = newQuota;
    }

    public void setReplicaQuota(long newQuota) {
        Preconditions.checkArgument(newQuota >= 0L);
        LOG.info("database[{}] set replica quota from {} to {}", fullQualifiedName, replicaQuotaSize, newQuota);
        this.replicaQuotaSize = newQuota;
    }

    public long getDataQuota() {
        return dataQuotaBytes;
    }

    public long getReplicaQuota() {
        return replicaQuotaSize;
    }

    public long getUsedDataQuotaWithLock() {
        long usedDataQuota = 0;
        Locker locker = new Locker();
        locker.lockDatabase(id, LockType.READ);
        try {
            for (Table table : this.idToTable.values()) {
                if (!table.isOlapTableOrMaterializedView()) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                usedDataQuota = usedDataQuota + olapTable.getDataSize();
            }
            return usedDataQuota;
        } finally {
            locker.unLockDatabase(id, LockType.READ);
        }
    }

    public void checkDataSizeQuota() throws DdlException {
        Pair<Double, String> quotaUnitPair = DebugUtil.getByteUint(dataQuotaBytes);
        String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaUnitPair.first) + " "
                + quotaUnitPair.second;
        long usedDataQuota = getUsedDataQuotaWithLock();
        long leftDataQuota = Math.max(dataQuotaBytes - usedDataQuota, 0);

        Pair<Double, String> leftQuotaUnitPair = DebugUtil.getByteUint(leftDataQuota);
        String readableLeftQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(leftQuotaUnitPair.first) + " "
                + leftQuotaUnitPair.second;

        LOG.info("database[{}] data quota: left bytes: {} / total: {}",
                fullQualifiedName, readableLeftQuota, readableQuota);

        if (leftDataQuota == 0L) {
            throw new DdlException("Database[" + fullQualifiedName
                    + "] data size exceeds quota[" + readableQuota + "]");
        }
    }

    public void checkReplicaQuota() throws DdlException {
        long usedReplicaQuota = 0;
        Locker locker = new Locker();
        locker.lockDatabase(id, LockType.READ);
        try {
            for (Table table : this.idToTable.values()) {
                if (!table.isOlapTableOrMaterializedView()) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                usedReplicaQuota = usedReplicaQuota + olapTable.getReplicaCount();
            }
        } finally {
            locker.unLockDatabase(id, LockType.READ);
        }

        long leftReplicaQuota = Math.max(replicaQuotaSize - usedReplicaQuota, 0L);
        LOG.info("database[{}] replica quota: left number: {} / total: {}",
                fullQualifiedName, leftReplicaQuota, replicaQuotaSize);

        if (leftReplicaQuota == 0L) {
            throw new DdlException("Database[" + fullQualifiedName
                    + "] replica number exceeds quota[" + replicaQuotaSize + "]");
        }
    }

    public void checkQuota() throws DdlException {
        checkDataSizeQuota();
        checkReplicaQuota();
    }

    public boolean registerTableUnlocked(Table table) {
        if (table == null) {
            return false;
        }
        if (table.isTemporaryTable()) {
            long tableId = table.getId();
            if (idToTable.containsKey(tableId)) {
                return false;
            }
            idToTable.put(tableId, table);
        } else {
            String tableName = table.getName();
            if (nameToTable.containsKey(tableName)) {
                return false;
            }
            idToTable.put(table.getId(), table);
            nameToTable.put(table.getName(), table);
        }
        return true;
    }

    public void dropTable(String tableName, boolean isSetIfExists, boolean isForce) throws DdlException {
        Table table;
        Locker locker = new Locker();
        locker.lockDatabase(id, LockType.WRITE);
        try {
            table = nameToTable.get(tableName);
            if (table == null && isSetIfExists) {
                return;
            }
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                return;
            }
            if (!isForce &&
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().existCommittedTxns(id, table.getId(), null)) {
                throw new DdlException("There are still some transactions in the COMMITTED state waiting to be completed. " +
                        "The table [" + table.getName() +
                        "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                        " please use \"DROP TABLE <table> FORCE\".");
            }
            unprotectDropTable(table.getId(), isForce, false);
            DropInfo info = new DropInfo(id, table.getId(), -1L, isForce);
            GlobalStateMgr.getCurrentState().getEditLog().logDropTable(info);
        } finally {
            locker.unLockDatabase(id, LockType.WRITE);
        }

        if (isForce) {
            table.delete(getId(), false);
        }

        LOG.info("Finished log drop table '{}' from database '{}'. tableId: {} tableType: {} force: {}",
                tableName, fullQualifiedName, table.getId(), table.getType(), isForce);
    }

    public void dropTemporaryTable(long tableId, String tableName, boolean isSetIfExists, boolean isForce) throws DdlException {
        Table table;
        Locker locker = new Locker();
        locker.lockDatabase(id, LockType.WRITE);
        try {
            table = idToTable.get(tableId);
            if (table == null) {
                if (isSetIfExists) {
                    return;
                }
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }
            unprotectDropTemporaryTable(tableId, isForce, false);
            DropInfo info = new DropInfo(id, table.getId(), -1L, isForce);
            GlobalStateMgr.getCurrentState().getEditLog().logDropTable(info);
        } finally {
            locker.unLockDatabase(id, LockType.WRITE);
        }
    }


    /**
     * Drop a table from this database.
     *
     * <p>
     * Note: Prefer to modify {@link Table#onDrop(Database, boolean, boolean)} and
     * {@link Table#delete(long, boolean)} rather than this function.
     *
     * @param tableId the id of the table to be dropped
     * @param isForceDrop is this a force drop
     * @param isReplay is this a log replay operation
     * @return The dropped table
     */
    public Table unprotectDropTable(long tableId, boolean isForceDrop, boolean isReplay) {
        Table table = dropTable(tableId);
        if (table != null) {
            table.onDrop(this, isForceDrop, isReplay);
            if (!isForceDrop) {
                GlobalStateMgr.getCurrentState().getRecycleBin().recycleTable(id, table, true);
            }
            LOG.info("Finished drop table '{}' from database '{}'. tableId: {} force: {} replay: {}",
                    table.getName(), getOriginName(), tableId, isForceDrop, isReplay);
        }
        return table;
    }

    public Table unprotectDropTemporaryTable(long tableId, boolean isForceDrop, boolean isReplay) {
        Table table = dropTemporaryTable(tableId);
        if (table != null) {
            table.onDrop(this, isForceDrop, isReplay);
            LOG.info("Finished drop temporary table '{}' from database '{}', tableId: {}, sessionId: {}",
                    table.getName(), getOriginName(), tableId, ((OlapTable) table).getSessionId());
        }
        return table;
    }

    public Table dropTable(long tableId) {
        Table table = this.idToTable.get(tableId);
        if (table != null) {
            this.nameToTable.remove(table.getName());
            this.idToTable.remove(tableId);
        }
        return table;
    }

    public Table dropTable(String tableName) {
        Table table = this.nameToTable.get(tableName);
        if (table != null) {
            this.nameToTable.remove(tableName);
            this.idToTable.remove(table.getId());
        }
        return table;
    }

    public Table dropTemporaryTable(long tableId) {
        Table table = this.idToTable.get(tableId);
        if (table != null) {
            Preconditions.checkArgument(table.isTemporaryTable(), "table should be temporary table");
            this.idToTable.remove(tableId);
        }
        return table;
    }

    public List<Table> getTables() {
        return new ArrayList<>(idToTable.values());
    }

    public int getTableNumber() {
        return idToTable.size();
    }

    public List<Table> getTemporaryTables() {
        return idToTable.values().stream().filter(t -> t.isTemporaryTable()).collect(Collectors.toList());
    }

    public List<Table> getViews() {
        List<Table> views = new ArrayList<>();
        for (Table table : idToTable.values()) {
            if (TableType.VIEW == table.getType()) {
                views.add(table);
            }
        }
        return views;
    }

    public List<MaterializedView> getMaterializedViews() {
        return idToTable.values().stream()
                .filter(Table::isMaterializedView)
                .map(x -> (MaterializedView) x)
                .collect(Collectors.toList());
    }

    public Set<String> getTableNamesViewWithLock() {
        Locker locker = new Locker();
        locker.lockDatabase(id, LockType.READ);
        try {
            return Collections.unmodifiableSet(this.nameToTable.keySet());
        } finally {
            locker.unLockDatabase(id, LockType.READ);
        }
    }

    /**
     * This is a thread-safe method when idToTable is a concurrent hash map
     */
    public Table getTable(long tableId) {
        return idToTable.get(tableId);
    }

    public Table getTable(String tableName) {
        return nameToTable.get(tableName);
    }

    public Pair<Table, MaterializedIndexMeta> getMaterializedViewIndex(String mvName) {
        // TODO: add an index to speed it up.
        for (Table table : idToTable.values()) {
            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                for (MaterializedIndexMeta mvMeta : olapTable.getVisibleIndexMetas()) {
                    String indexName = olapTable.getIndexNameById(mvMeta.getIndexId());
                    if (indexName == null) {
                        continue;
                    }
                    if (indexName.equals(mvName)) {
                        return Pair.create(table, mvMeta);
                    }
                }
            }
        }
        return null;
    }

    @Override
    public int getSignature(int signatureVersion) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);
        adler32.update(this.fullQualifiedName.getBytes(StandardCharsets.UTF_8));
        return Math.abs((int) adler32.getValue());
    }

    public boolean isExist() {
        return exist;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeLong(id);
        // compatible with old version
        if (fullQualifiedName.isEmpty()) {
            Text.writeString(out, fullQualifiedName);
        } else {
            Text.writeString(out, ClusterNamespace.getFullName(fullQualifiedName));
        }
        // write tables
        int numTables = nameToTable.size();
        out.writeInt(numTables);
        for (Map.Entry<String, Table> entry : nameToTable.entrySet()) {
            entry.getValue().write(out);
        }

        out.writeLong(dataQuotaBytes);
        Text.writeString(out, SystemInfoService.DEFAULT_CLUSTER);
        // compatible for dbState
        Text.writeString(out, "NORMAL");
        // NOTE: compatible attachDbName
        Text.writeString(out, "");

        // write functions
        out.writeInt(name2Function.size());
        for (Entry<String, List<Function>> entry : name2Function.entrySet()) {
            Text.writeString(out, entry.getKey());
            out.writeInt(entry.getValue().size());
            for (Function function : entry.getValue()) {
                function.write(out);
            }
        }

        out.writeLong(replicaQuotaSize);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Database)) {
            return false;
        }

        Database database = (Database) obj;

        if (idToTable != database.idToTable) {
            if (idToTable.size() != database.idToTable.size()) {
                return false;
            }
            for (Entry<Long, Table> entry : idToTable.entrySet()) {
                long key = entry.getKey();
                if (!database.idToTable.containsKey(key)) {
                    return false;
                }
                if (!entry.getValue().equals(database.idToTable.get(key))) {
                    return false;
                }
            }
        }

        return (id == database.id) && (fullQualifiedName.equals(database.fullQualifiedName)
                && dataQuotaBytes == database.dataQuotaBytes);
    }

    public void setName(String name) {
        this.fullQualifiedName = name;
    }

    public void setCatalogName(String name) {
        this.catalogName = name;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public synchronized void addFunction(Function function) throws UserException {
        addFunction(function, false, false);
    }

    public synchronized void addFunction(Function function, boolean allowExists, boolean createIfNotExists) throws UserException {
        addFunctionImpl(function, false, allowExists, createIfNotExists);
        GlobalStateMgr.getCurrentState().getEditLog().logAddFunction(function);
    }

    public synchronized void replayAddFunction(Function function) {
        try {
            addFunctionImpl(function, true, false, false);
        } catch (UserException e) {
            Preconditions.checkArgument(false);
        }
    }

    public static void replayCreateFunctionLog(Function function) {
        String dbName = function.getFunctionName().getDb();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new Error("unknown database when replay log, db=" + dbName);
        }
        db.replayAddFunction(function);
    }

    private void addFunctionImpl(Function function, boolean isReplay, boolean allowExists, boolean createIfNotExists)
            throws UserException {
        String functionName = function.getFunctionName().getFunction();
        List<Function> existFuncs = name2Function.getOrDefault(functionName, ImmutableList.of());
        if (allowExists && createIfNotExists) {
            // In most DB system (like MySQL, Oracle, Snowflake etc.), these two conditions are now allowed to use together
            throw new UserException(
                    "\"IF NOT EXISTS\" and \"OR REPLACE\" cannot be used together in the same CREATE statement");
        }
        if (!isReplay) {
            for (Function existFunc : existFuncs) {
                if (function.compare(existFunc, Function.CompareMode.IS_IDENTICAL)) {
                    if (createIfNotExists) {
                        LOG.info("create function [{}] which already exists", functionName);
                        return;
                    } else if (!allowExists) {
                        throw new UserException("function already exists");
                    }
                }
            }
            GlobalFunctionMgr.assignIdToUserDefinedFunction(function);
        }
        name2Function.put(functionName, GlobalFunctionMgr.addOrReplaceFunction(function, existFuncs));
    }

    public synchronized void dropFunction(FunctionSearchDesc function, boolean dropIfExists) throws UserException {
        dropFunctionImpl(function, dropIfExists);
        GlobalStateMgr.getCurrentState().getEditLog().logDropFunction(function);
    }

    public synchronized void replayDropFunction(FunctionSearchDesc functionSearchDesc) {
        try {
            dropFunctionImpl(functionSearchDesc, false);
        } catch (UserException e) {
            Preconditions.checkArgument(false);
        }
    }

    public static void replayDropFunctionLog(FunctionSearchDesc functionSearchDesc) {
        String dbName = functionSearchDesc.getName().getDb();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new Error("unknown database when replay log, db=" + dbName);
        }
        db.replayDropFunction(functionSearchDesc);
    }

    public Function getFunction(FunctionSearchDesc function) {
        String functionName = function.getName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (existFuncs == null) {
            return null;
        }
        Function func = null;
        for (Function existFunc : existFuncs) {
            if (function.isIdentical(existFunc)) {
                func = existFunc;
                break;
            }
        }
        return func;
    }

    private void dropFunctionImpl(FunctionSearchDesc function, boolean dropIfExists) throws UserException {
        String functionName = function.getName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (existFuncs == null) {
            if (dropIfExists) {
                LOG.info("drop function [{}] which does not exist", functionName);
                return;
            }
            throw new UserException("Unknown function, function=" + function.toString());
        }
        boolean isFound = false;
        List<Function> newFunctions = new ArrayList<>();
        for (Function existFunc : existFuncs) {
            if (function.isIdentical(existFunc)) {
                isFound = true;
            } else {
                newFunctions.add(existFunc);
            }
        }
        if (!isFound) {
            if (dropIfExists) {
                LOG.info("drop function [{}] which does not exist", functionName);
                return;
            }
            throw new UserException("Unknown function, function=" + function.toString());
        }
        if (newFunctions.isEmpty()) {
            name2Function.remove(functionName);
        } else {
            name2Function.put(functionName, newFunctions);
        }
    }

    public synchronized Function getFunction(Function desc, Function.CompareMode mode) {
        List<Function> fns = name2Function.get(desc.getFunctionName().getFunction());
        if (fns == null) {
            return null;
        }
        return Function.getFunction(fns, desc, mode);
    }

    public synchronized List<Function> getFunctions() {
        List<Function> functions = Lists.newArrayList();
        for (Map.Entry<String, List<Function>> entry : name2Function.entrySet()) {
            functions.addAll(entry.getValue());
        }
        return functions;
    }

    public boolean isSystemDatabase() {
        return fullQualifiedName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME) ||
                fullQualifiedName.equalsIgnoreCase(SysDb.DATABASE_NAME);
    }

    public boolean isStatisticsDatabase() {
        return fullQualifiedName.equalsIgnoreCase(StatsConstants.STATISTICS_DB_NAME);
    }

    // the invoker should hold db's writeLock
    public void setExist(boolean exist) {
        this.exist = exist;
    }

    public boolean getExist() {
        return exist;
    }

    public List<PhysicalPartition> getPartitionSamples() {
        return this.idToTable.values()
                .stream()
                .filter(table -> table instanceof OlapTable)
                .map(table -> ((OlapTable) table).getPartitionSample())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public int getOlapPartitionsCount() {
        return this.idToTable.values()
                .stream()
                .filter(table -> table instanceof OlapTable)
                .mapToInt(table -> ((OlapTable) table).getPartitionsCount())
                .sum();
    }
}
