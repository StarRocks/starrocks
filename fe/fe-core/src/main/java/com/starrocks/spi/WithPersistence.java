package com.starrocks.spi;

import com.starrocks.analysis.AlterDatabaseQuotaStmt;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropLinkDbAndUpdateDbInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.PartitionPersistInfo;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TruncateTableInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface WithPersistence {
    long loadDb(DataInputStream dis, long checksum) throws IOException, DdlException;

    long saveDb(DataOutputStream dos, long checksum) throws IOException;

    void replayCreateDb(Database db);

    void replayDropLinkDb(DropLinkDbAndUpdateDbInfo info);

    void replayDropDb(String dbName, boolean isForceDrop) throws DdlException;

    void replayUpdateDb(DatabaseInfo info);

    void replayRecoverDatabase(RecoverInfo info);

    void replayAlterDatabaseQuota(String dbName, long quota, AlterDatabaseQuotaStmt.QuotaType quotaType);

    void replayRenameDatabase(String dbName, String newDbName);

    void replayCreateTable(String dbName, Table table);

    void replayRenameTable(TableInfo tableInfo) throws DdlException;

    void replayDropTable(Database db, long tableId, boolean isForceDrop);

    void replayConvertDistributionType(TableInfo tableInfo);

    void replayTruncateTable(TruncateTableInfo info);

    void replayRecoverTable(RecoverInfo info);

    void replayAddPartition(PartitionPersistInfo info) throws DdlException;

    void replayDropPartition(DropPartitionInfo info);

    void replayRenamePartition(TableInfo tableInfo) throws DdlException;

    void replayReplaceTempPartition(ReplacePartitionOperationLog replaceTempPartitionLog);

    void replayBackendTabletsInfo(BackendTabletsInfo backendTabletsInfo);

    void replayAddReplica(ReplicaPersistInfo info);

    void replayUpdateReplica(ReplicaPersistInfo info);

    void replayDeleteReplica(ReplicaPersistInfo info);

    void replayModifyTableProperty(short opCode, ModifyTablePropertyOperationLog info);

    public void replayRenameRollup(TableInfo tableInfo) throws DdlException;
}
