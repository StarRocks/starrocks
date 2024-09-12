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

package com.starrocks.meta;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.DdlException;
import com.starrocks.persist.AddPartitionsInfoV2;
import com.starrocks.persist.AddSubPartitionsInfoV2;
import com.starrocks.persist.AlterMaterializedViewBaseTableInfosLog;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.BatchDeleteReplicaInfo;
import com.starrocks.persist.BatchDropInfo;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.ColumnRenameInfo;
import com.starrocks.persist.ConsistencyCheckInfo;
import com.starrocks.persist.CreateDbInfo;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropDbInfo;
import com.starrocks.persist.DropInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.DropPartitionsInfo;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.PartitionVersionRecoveryInfo;
import com.starrocks.persist.RenameMaterializedViewLog;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.SetReplicaStatusOperationLog;
import com.starrocks.persist.SwapTableOperationLog;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.thrift.TTabletMetaType;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public interface MetaStore {
    void createDb(CreateDbInfo createDbInfo);

    void dropDb(DropDbInfo dropDbInfo);

    void recoverDatabase(Database db);

    void alterDatabaseQuota(DatabaseInfo dbInfo);

    void renameDatabase(String dbName, String newDbName);

    List<String> listDbNames();

    ConcurrentHashMap<Long, Database> getIdToDb();

    List<Long> getDbIds();

    ConcurrentHashMap<String, Database> getFullNameToDb();

    Database getDb(String name);

    Database getDb(long dbId);

    void createTable(CreateTableInfo createTableInfo);

    void dropTable(DropInfo dropInfo);

    void renameTable(TableInfo tableInfo);

    void truncateTable(TruncateTableInfo info);

    void swapTable(SwapTableOperationLog log);

    void updateTableMeta(Database db, String tableName, Map<String, String> properties, TTabletMetaType metaType)
            throws DdlException;

    void alterTable(ModifyTablePropertyOperationLog log);

    void renameColumn(ColumnRenameInfo columnRenameInfo);

    List<String> listTableNames(String dbName);

    List<Table> getTables(Long dbId);

    Table getTable(String dbName, String tblName);

    Table getTable(Long dbId, Long tableId);

    void modifyViewDef(AlterViewInfo alterViewInfo);

    void renameMaterializedView(RenameMaterializedViewLog log);

    void alterMvBaseTableInfos(AlterMaterializedViewBaseTableInfosLog log);

    void alterMvStatus(AlterMaterializedViewStatusLog log);

    void alterMaterializedViewProperties(ModifyTablePropertyOperationLog log);

    void changeMaterializedRefreshScheme(ChangeMaterializedViewRefreshSchemeLog log);

    void addPartitionLog(Database db, OlapTable olapTable, List<PartitionDesc> partitionDescs,
                         boolean isTempPartition, PartitionInfo partitionInfo,
                         List<Partition> partitionList, Set<String> existPartitionNameSet) throws DdlException;

    void addPartition(AddPartitionsInfoV2 addPartitionsInfo);

    void dropPartition(DropPartitionInfo dropPartitionInfo);

    void dropPartitions(DropPartitionsInfo dropPartitionsInfo);

    void renamePartition(TableInfo tableInfo);

    void replaceTempPartition(ReplacePartitionOperationLog info);

    void modifyPartition(ModifyPartitionInfo info);

    void setPartitionVersion(PartitionVersionRecoveryInfo info);

    void addSubPartitionLog(AddSubPartitionsInfoV2 addSubPartitionsInfo);

    List<PhysicalPartition> getAllPhysicalPartition(OlapTable olapTable);

    List<PhysicalPartition> getAllPhysicalPartition(Partition partition);

    PhysicalPartition getPhysicalPartition(Partition partition, Long physicalPartitionId);

    void addPhysicalPartition(Partition partition, PhysicalPartition physicalPartition);

    void dropPhysicalPartition(Partition partition, Long physicalPartitionId);

    List<MaterializedIndex> getMaterializedIndices(PhysicalPartition physicalPartition,
                                                   MaterializedIndex.IndexExtState indexExtState);

    MaterializedIndex getMaterializedIndex(PhysicalPartition physicalPartition, Long mIndexId);

    void addMaterializedIndex(PhysicalPartition physicalPartition, MaterializedIndex materializedIndex);

    void dropMaterializedIndex(PhysicalPartition physicalPartition, Long mIndexId);

    void dropRollup(DropInfo dropInfo);

    void batchDropRollup(BatchDropInfo batchDropInfo);

    void renameRollup(TableInfo tableInfo);

    List<Tablet> getAllTablets(MaterializedIndex materializedIndex);

    List<Long> getAllTabletIDs(MaterializedIndex materializedIndex);

    Tablet getTablet(MaterializedIndex materializedIndex, Long tabletId);

    //void addTablet(MaterializedIndex materializedIndex, Tablet tablet, TabletMeta tabletMeta);

    List<Replica> getAllReplicas(Tablet tablet);

    Replica getReplica(LocalTablet tablet, Long replicaId);

    void addReplica(ReplicaPersistInfo replicaPersistInfo);

    void deleteReplica(ReplicaPersistInfo replicaPersistInfo);

    void batchDeleteReplicaInfo(BatchDeleteReplicaInfo replicaPersistInfo);

    void updateReplica(ReplicaPersistInfo replicaPersistInfo);

    void setReplicaStatus(SetReplicaStatusOperationLog log);

    void backendTabletsInfo(BackendTabletsInfo backendTabletsInfo);

    void finishConsistencyCheck(ConsistencyCheckInfo info);
}

