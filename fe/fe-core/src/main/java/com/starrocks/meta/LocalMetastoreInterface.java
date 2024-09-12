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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.DropPartitionsInfo;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.PartitionVersionRecoveryInfo;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.sql.ast.PartitionDesc;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public interface LocalMetastoreInterface {
    void createDb(Database db, String storageVolumeId);

    void dropDb(Database db, boolean isForceDrop);

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

    void alterTable(ModifyTablePropertyOperationLog log);

    void modifyTableProperty(Database db, OlapTable table, Map<String, String> properties, short operationType);

    List<String> listTableNames(String dbName);

    List<Table> getTables(Long dbId);

    Table getTable(String dbName, String tblName);

    Table getTable(Long dbId, Long tableId);

    void addPartitionLog(Database db, OlapTable olapTable, List<PartitionDesc> partitionDescs,
                         boolean isTempPartition, PartitionInfo partitionInfo,
                         List<Partition> partitionList, Set<String> existPartitionNameSet) throws DdlException;

    void dropPartition(DropPartitionInfo dropPartitionInfo);

    void dropPartitions(DropPartitionsInfo dropPartitionsInfo);

    void renamePartition(TableInfo tableInfo);

    void replaceTempPartition(ReplacePartitionOperationLog info);

    void setPartitionVersion(PartitionVersionRecoveryInfo info);

    void addSubPartitionLog(Database db, OlapTable olapTable, Partition partition, List<PhysicalPartition> subPartitions);
}

