// This file is made available under Elastic License 2.0.
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
import com.starrocks.catalog.Table.TableType;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.LogUtil;
import com.starrocks.common.util.QueryableReentrantReadWriteLock;
import com.starrocks.common.util.Util;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.DropInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
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

    private long id;
    private String fullQualifiedName;
    private QueryableReentrantReadWriteLock rwLock;

    // table family group map
    private Map<Long, Table> idToTable;
    private Map<String, Table> nameToTable;

    // user define function
    private ConcurrentMap<String, ImmutableList<Function>> name2Function = Maps.newConcurrentMap();

    private volatile long dataQuotaBytes;

    private volatile long replicaQuotaSize;

    private long lastSlowLockLogTime = 0;

    // This param is used to make sure db not dropped when leader node writes wal,
    // so this param dose not need to be persistent,
    // and this param maybe not right when the db is dropped and the catalog has done a checkpoint,
    // but that'ok to meet our needs.
    private volatile boolean exist = true;

    public Database() {
        this(0, null);
    }

    public Database(long id, String name) {
        this.id = id;
        this.fullQualifiedName = name;
        if (this.fullQualifiedName == null) {
            this.fullQualifiedName = "";
        }
        this.rwLock = new QueryableReentrantReadWriteLock(true);
        this.idToTable = new ConcurrentHashMap<>();
        this.nameToTable = new ConcurrentHashMap<>();
        this.dataQuotaBytes = FeConstants.default_db_data_quota_bytes;
        this.replicaQuotaSize = FeConstants.default_db_replica_quota_size;
    }

    private String getOwnerInfo(Thread owner) {
        if (owner == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("owner id: ").append(owner.getId()).append(", owner name: ")
                .append(owner.getName()).append(", owner stack: ").append(Util.dumpThread(owner, 50));
        return sb.toString();
    }

    private void logSlowLockEventIfNeeded(long startMs, String type, String threadDump) {
        long endMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
        if (endMs - startMs > Config.slow_lock_threshold_ms &&
                endMs > lastSlowLockLogTime + Config.slow_lock_log_every_ms) {
            lastSlowLockLogTime = endMs;
            LOG.warn("slow db lock. type: {}, db id: {}, db name: {}, wait time: {}ms, " +
                            "former {}, current stack trace: {}", type, id, fullQualifiedName, endMs - startMs,
                    threadDump, LogUtil.getCurrentStackTrace());
        }
    }

    private void logTryLockFailureEvent(String type, String threadDump) {
        LOG.warn("try db lock failed. type: {}, current {}", type, threadDump);
    }

    public void readLock() {
        long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
        String threadDump = getOwnerInfo(rwLock.getOwner());
        this.rwLock.readLock().lock();
        logSlowLockEventIfNeeded(startMs, "readLock", threadDump);
    }

    // this function make sure lock can only be obtained if the db has not been dropped
    public boolean readLockAndCheckExist() {
        long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
        String threadDump = getOwnerInfo(rwLock.getOwner());
        this.rwLock.readLock().lock();
        logSlowLockEventIfNeeded(startMs, "readLock", threadDump);
        if (exist) {
            return true;
        } else {
            this.rwLock.readLock().unlock();
            return false;
        }
    }

    public boolean tryReadLock(long timeout, TimeUnit unit) {
        try {
            long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
            String threadDump = getOwnerInfo(rwLock.getOwner());
            if (!this.rwLock.readLock().tryLock(timeout, unit)) {
                logTryLockFailureEvent("readLock", threadDump);
                return false;
            }
            logSlowLockEventIfNeeded(startMs, "tryReadLock", threadDump);
            return true;
        } catch (InterruptedException e) {
            LOG.warn("failed to try read lock at db[" + id + "]", e);
            return false;
        }
    }

    // this function make sure lock can only be obtained if the db has not been dropped
    public boolean tryReadLockAndCheckExist(long timeout, TimeUnit unit) {
        try {
            long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
            String threadDump = getOwnerInfo(rwLock.getOwner());
            if (!this.rwLock.readLock().tryLock(timeout, unit)) {
                logTryLockFailureEvent("readLock", threadDump);
                return false;
            }
            logSlowLockEventIfNeeded(startMs, "tryReadLock", threadDump);
            if (exist) {
                return true;
            } else {
                this.rwLock.readLock().unlock();
                return false;
            }
        } catch (InterruptedException e) {
            LOG.warn("failed to try read lock at db[" + id + "]", e);
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public void readUnlock() {
        this.rwLock.readLock().unlock();
    }

    public boolean isReadLockHeldByCurrentThread() {
        return this.rwLock.getReadHoldCount() > 0;
    }

    public void writeLock() {
        long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
        String threadDump = getOwnerInfo(rwLock.getOwner());
        this.rwLock.writeLock().lock();
        logSlowLockEventIfNeeded(startMs, "writeLock", threadDump);
    }

    // this function make sure lock can only be obtained if the db has not been dropped
    public boolean writeLockAndCheckExist() {
        long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
        String threadDump = getOwnerInfo(rwLock.getOwner());
        this.rwLock.writeLock().lock();
        logSlowLockEventIfNeeded(startMs, "writeLock", threadDump);
        if (exist) {
            return true;
        } else {
            this.rwLock.writeLock().unlock();
            return false;
        }
    }

    public boolean tryWriteLock(long timeout, TimeUnit unit) {
        try {
            long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
            String threadDump = getOwnerInfo(rwLock.getOwner());
            if (!this.rwLock.writeLock().tryLock(timeout, unit)) {
                logTryLockFailureEvent("writeLock", threadDump);
                return false;
            }
            logSlowLockEventIfNeeded(startMs, "tryWriteLock", threadDump);
            return true;
        } catch (InterruptedException e) {
            LOG.warn("failed to try write lock at db[" + id + "]", e);
            return false;
        }
    }

    // this function make sure lock can only be obtained if the db has not been dropped
    public boolean tryWriteLockAndCheckExist(long timeout, TimeUnit unit) {
        try {
            long startMs = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
            String threadDump = getOwnerInfo(rwLock.getOwner());
            if (!this.rwLock.writeLock().tryLock(timeout, unit)) {
                logTryLockFailureEvent("tryWriteLock", threadDump);
                return false;
            }
            logSlowLockEventIfNeeded(startMs, "tryWriteLock", threadDump);
            if (exist) {
                return true;
            } else {
                this.rwLock.writeLock().unlock();
                return false;
            }
        } catch (InterruptedException e) {
            LOG.warn("failed to try write lock at db[" + id + "]", e);
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public void writeUnlock() {
        this.rwLock.writeLock().unlock();
    }

    public boolean isWriteLockHeldByCurrentThread() {
        return this.rwLock.writeLock().isHeldByCurrentThread();
    }

    public QueryableReentrantReadWriteLock getLock() {
        return this.rwLock;
    }

    public long getId() {
        return id;
    }

    public String getOriginName() {
        return fullQualifiedName;
    }

    public String getFullName() {
        return fullQualifiedName;
    }

    public void setNameWithLock(String newName) {
        writeLock();
        try {
            this.fullQualifiedName = newName;
        } finally {
            writeUnlock();
        }
    }

    public void setDataQuotaWithLock(long newQuota) {
        Preconditions.checkArgument(newQuota >= 0L);
        LOG.info("database[{}] set quota from {} to {}", fullQualifiedName, dataQuotaBytes, newQuota);
        writeLock();
        try {
            this.dataQuotaBytes = newQuota;
        } finally {
            writeUnlock();
        }
    }

    public void setReplicaQuotaWithLock(long newQuota) {
        Preconditions.checkArgument(newQuota >= 0L);
        LOG.info("database[{}] set replica quota from {} to {}", fullQualifiedName, replicaQuotaSize, newQuota);
        writeLock();
        try {
            this.replicaQuotaSize = newQuota;
        } finally {
            writeUnlock();
        }
    }

    public long getDataQuota() {
        return dataQuotaBytes;
    }

    public long getReplicaQuota() {
        return replicaQuotaSize;
    }

    public long getUsedDataQuotaWithLock() {
        long usedDataQuota = 0;
        readLock();
        try {
            for (Table table : this.idToTable.values()) {
                if (!table.isLocalTable()) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                usedDataQuota = usedDataQuota + olapTable.getDataSize();
            }
            return usedDataQuota;
        } finally {
            readUnlock();
        }
    }

    public long getReplicaQuotaLeftWithLock() {
        long usedReplicaQuota = 0;
        readLock();
        try {
            for (Table table : this.idToTable.values()) {
                if (!table.isLocalTable()) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                usedReplicaQuota = usedReplicaQuota + olapTable.getReplicaCount();
            }

            long leftReplicaQuota = replicaQuotaSize - usedReplicaQuota;
            return Math.max(leftReplicaQuota, 0L);
        } finally {
            readUnlock();
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

        if (leftDataQuota <= 0L) {
            throw new DdlException("Database[" + fullQualifiedName
                    + "] data size exceeds quota[" + readableQuota + "]");
        }
    }

    public void checkReplicaQuota() throws DdlException {
        long leftReplicaQuota = getReplicaQuotaLeftWithLock();
        LOG.info("database[{}] replica quota: left number: {} / total: {}",
                fullQualifiedName, leftReplicaQuota, replicaQuotaSize);

        if (leftReplicaQuota <= 0L) {
            throw new DdlException("Database[" + fullQualifiedName
                    + "] replica number exceeds quota[" + replicaQuotaSize + "]");
        }
    }

    public void checkQuota() throws DdlException {
        checkDataSizeQuota();
        checkReplicaQuota();
    }

    // return false if table already exists
    public boolean createTableWithLock(Table table, boolean isReplay) {
        writeLock();
        try {
            String tableName = table.getName();
            if (nameToTable.containsKey(tableName)) {
                return false;
            } else {
                idToTable.put(table.getId(), table);
                nameToTable.put(table.getName(), table);
                table.onCreate();
                if (!isReplay) {
                    // Write edit log
                    CreateTableInfo info = new CreateTableInfo(fullQualifiedName, table);
                    GlobalStateMgr.getCurrentState().getEditLog().logCreateTable(info);
                }
            }
            return true;
        } finally {
            writeUnlock();
        }
    }

    public boolean createTable(Table table) {
        boolean result = true;
        String tableName = table.getName();
        if (nameToTable.containsKey(tableName)) {
            result = false;
        } else {
            idToTable.put(table.getId(), table);
            nameToTable.put(table.getName(), table);
        }
        return result;
    }

    public void dropTable(String tableName, boolean isSetIfExists, boolean isForce) throws DdlException {
        Table table;
        Runnable runnable;
        writeLock();
        try {
            table = getTable(tableName);
            if (table == null && isSetIfExists) {
                return;
            }
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                return;
            }
            if (!isForce && GlobalStateMgr.getCurrentGlobalTransactionMgr().existCommittedTxns(id, table.getId(), null)) {
                throw new DdlException("There are still some transactions in the COMMITTED state waiting to be completed. " +
                        "The table [" + table.getName() +
                        "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                        " please use \"DROP TABLE <table> FORCE\".");
            }
            runnable = unprotectDropTable(table.getId(), isForce, false);
            DropInfo info = new DropInfo(id, table.getId(), -1L, isForce);
            GlobalStateMgr.getCurrentState().getEditLog().logDropTable(info);
        } finally {
            writeUnlock();
        }

        if (runnable != null) {
            runnable.run();
        }
        LOG.info("finished dropping table: {}, type:{} from db: {}, is force: {}", tableName, table.getType(), fullQualifiedName,
                isForce);
    }

    public Runnable unprotectDropTable(long tableId, boolean isForceDrop, boolean isReplay) {
        Runnable runnable;
        Table table = getTable(tableId);
        // delete from db meta
        if (table == null) {
            return null;
        }

        table.onDrop(this, isForceDrop, isReplay);

        dropTable(table.getName());

        if (!isForceDrop) {
            Table oldTable = GlobalStateMgr.getCurrentState().getRecycleBin().recycleTable(id, table);
            runnable = (oldTable != null) ? oldTable.delete(isReplay) : null;
        } else {
            runnable = table.delete(isReplay);
        }

        LOG.info("finished dropping table[{}] in db[{}], tableId: {}", table.getName(), getOriginName(), tableId);
        return runnable;
    }

    public void dropTableWithLock(String tableName) {
        writeLock();
        try {
            Table table = this.nameToTable.get(tableName);
            if (table != null) {
                this.nameToTable.remove(tableName);
                this.idToTable.remove(table.getId());
            }
        } finally {
            writeUnlock();
        }
    }

    public void dropTable(String tableName) {
        Table table = this.nameToTable.get(tableName);
        if (table != null) {
            this.nameToTable.remove(tableName);
            this.idToTable.remove(table.getId());
        }
    }

    public boolean createMaterializedWithLock(MaterializedView materializedView, boolean isReplay) {
        writeLock();
        try {
            String mvName = materializedView.getName();
            if (nameToTable.containsKey(mvName)) {
                return false;
            } else {
                idToTable.put(materializedView.getId(), materializedView);
                nameToTable.put(materializedView.getName(), materializedView);
                materializedView.onCreate();
                if (!isReplay) {
                    CreateTableInfo info = new CreateTableInfo(fullQualifiedName, materializedView);
                    GlobalStateMgr.getCurrentState().getEditLog().logCreateMaterializedView(info);
                }
            }
            return true;
        } finally {
            writeUnlock();
        }
    }

    public List<Table> getTables() {
        return new ArrayList<Table>(idToTable.values());
    }

    public int getTableNumber() {
        return idToTable.size();
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
        List<MaterializedView> materializedViews = new ArrayList<>();
        for (Table table : idToTable.values()) {
            if (TableType.MATERIALIZED_VIEW == table.getType()) {
                materializedViews.add((MaterializedView) table);
            }
        }
        return materializedViews;
    }

    public Set<String> getTableNamesViewWithLock() {
        readLock();
        try {
            return Collections.unmodifiableSet(this.nameToTable.keySet());
        } finally {
            readUnlock();
        }
    }

    public Optional<Table> tryGetTable(String tableName) {
        return Optional.ofNullable(nameToTable.get(tableName));
    }

    public Optional<Table> tryGetTable(long tableId) {
        return Optional.ofNullable(idToTable.get(tableId));
    }

    public Table getTable(String tableName) {
        if (nameToTable.containsKey(tableName)) {
            return nameToTable.get(tableName);
        }
        return null;
    }

    /**
     * This is a thread-safe method when idToTable is a concurrent hash map
     *
     * @param tableId
     * @return
     */
    public Table getTable(long tableId) {
        return idToTable.get(tableId);
    }

    public static Database read(DataInput in) throws IOException {
        Database db = new Database();
        db.readFields(in);
        return db;
    }

    @Override
    public int getSignature(int signatureVersion) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);
        adler32.update(this.fullQualifiedName.getBytes(StandardCharsets.UTF_8));
        return Math.abs((int) adler32.getValue());
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
        for (Entry<String, ImmutableList<Function>> entry : name2Function.entrySet()) {
            Text.writeString(out, entry.getKey());
            out.writeInt(entry.getValue().size());
            for (Function function : entry.getValue()) {
                function.write(out);
            }
        }

        out.writeLong(replicaQuotaSize);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        id = in.readLong();
        if (GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_30) {
            fullQualifiedName = Text.readString(in);
        } else {
            fullQualifiedName = ClusterNamespace.getNameFromFullName(Text.readString(in));
        }
        // read groups
        int numTables = in.readInt();
        for (int i = 0; i < numTables; ++i) {
            Table table = Table.read(in);
            nameToTable.put(table.getName(), table);
            idToTable.put(table.getId(), table);
        }

        // read quota
        dataQuotaBytes = in.readLong();
        // Compatible for Cluster
        Text.readString(in);
        // Compatible for dbState
        Text.readString(in);
        // Compatible for attachDbName
        Text.readString(in);

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_47) {
            int numEntries = in.readInt();
            for (int i = 0; i < numEntries; ++i) {
                String name = Text.readString(in);
                ImmutableList.Builder<Function> builder = ImmutableList.builder();
                int numFunctions = in.readInt();
                for (int j = 0; j < numFunctions; ++j) {
                    builder.add(Function.read(in));
                }

                name2Function.put(name, builder.build());
            }
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_81) {
            replicaQuotaSize = in.readLong();
        } else {
            replicaQuotaSize = FeConstants.default_db_replica_quota_size;
        }
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

    public synchronized void addFunction(Function function) throws UserException {
        addFunctionImpl(function, false);
        GlobalStateMgr.getCurrentState().getEditLog().logAddFunction(function);
    }

    public synchronized void replayAddFunction(Function function) {
        try {
            addFunctionImpl(function, true);
        } catch (UserException e) {
            Preconditions.checkArgument(false);
        }
    }

    public static void replayCreateFunctionLog(Function function) {
        String dbName = function.getFunctionName().getDb();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new Error("unknown database when replay log, db=" + dbName);
        }
        db.replayAddFunction(function);
    }

    // return true if add success, false
    private void addFunctionImpl(Function function, boolean isReplay) throws UserException {
        String functionName = function.getFunctionName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (!isReplay) {
            if (existFuncs != null) {
                for (Function existFunc : existFuncs) {
                    if (function.compare(existFunc, Function.CompareMode.IS_IDENTICAL)) {
                        throw new UserException("function already exists");
                    }
                }
            }
            // Get function id for this UDF, use CatalogIdGenerator. Only get function id
            // when isReplay is false
            long functionId = GlobalStateMgr.getCurrentState().getNextId();
            // all user-defined functions id are negative to avoid conflicts with the builtin function
            function.setFunctionId(-functionId);
        }

        ImmutableList.Builder<Function> builder = ImmutableList.builder();
        if (existFuncs != null) {
            builder.addAll(existFuncs);
        }
        builder.add(function);
        name2Function.put(functionName, builder.build());
    }

    public synchronized void dropFunction(FunctionSearchDesc function) throws UserException {
        dropFunctionImpl(function);
        GlobalStateMgr.getCurrentState().getEditLog().logDropFunction(function);
    }

    public synchronized void replayDropFunction(FunctionSearchDesc functionSearchDesc) {
        try {
            dropFunctionImpl(functionSearchDesc);
        } catch (UserException e) {
            Preconditions.checkArgument(false);
        }
    }

    public static void replayDropFunctionLog(FunctionSearchDesc functionSearchDesc) {
        String dbName = functionSearchDesc.getName().getDb();
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new Error("unknown database when replay log, db=" + dbName);
        }
        db.replayDropFunction(functionSearchDesc);
    }

    private void dropFunctionImpl(FunctionSearchDesc function) throws UserException {
        String functionName = function.getName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (existFuncs == null) {
            throw new UserException("Unknown function, function=" + function.toString());
        }
        boolean isFound = false;
        ImmutableList.Builder<Function> builder = ImmutableList.builder();
        for (Function existFunc : existFuncs) {
            if (function.isIdentical(existFunc)) {
                isFound = true;
            } else {
                builder.add(existFunc);
            }
        }
        if (!isFound) {
            throw new UserException("Unknown function, function=" + function.toString());
        }
        ImmutableList<Function> newFunctions = builder.build();
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
        for (Map.Entry<String, ImmutableList<Function>> entry : name2Function.entrySet()) {
            functions.addAll(entry.getValue());
        }
        return functions;
    }

    public boolean isInfoSchemaDb() {
        return fullQualifiedName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME);
    }

    // the invoker should hold db's writeLock
    public void setExist(boolean exist) {
        this.exist = exist;
    }

    public List<Table> getHiveTables() {
        return idToTable.values().stream().filter(Table::isHiveTable).collect(Collectors.toList());
    }
}
