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

package com.starrocks.persist.image;

import java.util.Objects;

/**
 * Identifies one section of a format-v3 image. The numeric value is what goes into
 * {@code IndexEntryPB.type}; the name only serves logging.
 *
 * <p>Values are assigned from per-module ranges and are never reused. The open-source
 * range is [1, 20000); enterprise sections extend this class and use [20001, ...), the
 * same convention as {@code SRMetaBlockID}. The value space is independent of
 * {@code SRMetaBlockID}: a v2 block may map to several v3 sections.
 */
public class SectionId {
    public static final SectionId INVALID = new SectionId(0, "INVALID");
    // Global state: 1-9.
    public static final SectionId GLOBAL_STATE = new SectionId(1, "GLOBAL_STATE");

    // Reserve ten IDs per manager, in v2 save order. Add list sections within that manager's range.
    // NodeMgr: 10-19.
    public static final SectionId NODE_MGR = new SectionId(10, "NODE_MGR");

    // LocalMetastore: 20-29.
    public static final SectionId LOCAL_METASTORE_DB = new SectionId(20, "LOCAL_METASTORE_DB");
    public static final SectionId LOCAL_METASTORE_TABLE = new SectionId(21, "LOCAL_METASTORE_TABLE");
    public static final SectionId LOCAL_METASTORE_PARTITION = new SectionId(22, "LOCAL_METASTORE_PARTITION");

    // AlterJobMgr: 30-39.
    public static final SectionId ALTER_JOB_MGR = new SectionId(30, "ALTER_JOB_MGR");

    // CatalogRecycleBin: 40-49.
    public static final SectionId CATALOG_RECYCLE_BIN_DB = new SectionId(40, "CATALOG_RECYCLE_BIN_DB");
    public static final SectionId CATALOG_RECYCLE_BIN_TABLE = new SectionId(41, "CATALOG_RECYCLE_BIN_TABLE");
    public static final SectionId CATALOG_RECYCLE_BIN_PARTITION = new SectionId(42, "CATALOG_RECYCLE_BIN_PARTITION");
    public static final SectionId CATALOG_RECYCLE_BIN_INDEX = new SectionId(43, "CATALOG_RECYCLE_BIN_INDEX");

    // VariableMgr: 50-59.
    public static final SectionId VARIABLE_MGR = new SectionId(50, "VARIABLE_MGR");

    // ResourceMgr: 60-69.
    public static final SectionId RESOURCE_MGR = new SectionId(60, "RESOURCE_MGR");

    // ExportMgr: 70-79.
    public static final SectionId EXPORT_MGR = new SectionId(70, "EXPORT_MGR");

    // BackupHandler: 80-89.
    public static final SectionId BACKUP_MGR = new SectionId(80, "BACKUP_MGR");

    // GlobalTransactionMgr: 90-99.
    public static final SectionId GLOBAL_TRANSACTION_MGR = new SectionId(90, "GLOBAL_TRANSACTION_MGR");

    // ColocateTableIndex: 100-109.
    public static final SectionId COLOCATE_TABLE_INDEX = new SectionId(100, "COLOCATE_TABLE_INDEX");

    // RoutineLoadMgr: 110-119.
    public static final SectionId ROUTINE_LOAD_MGR = new SectionId(110, "ROUTINE_LOAD_MGR");

    // LoadMgr: 120-129.
    public static final SectionId LOAD_MGR = new SectionId(120, "LOAD_MGR");

    // SmallFileMgr: 130-139.
    public static final SectionId SMALL_FILE_MGR = new SectionId(130, "SMALL_FILE_MGR");

    // PluginMgr: 140-149.
    public static final SectionId PLUGIN_MGR = new SectionId(140, "PLUGIN_MGR");

    // DeleteMgr: 150-159.
    public static final SectionId DELETE_MGR = new SectionId(150, "DELETE_MGR");

    // AnalyzeMgr: 160-169.
    public static final SectionId ANALYZE_JOB = new SectionId(160, "ANALYZE_JOB");
    public static final SectionId ANALYZE_STATUS = new SectionId(161, "ANALYZE_STATUS");
    public static final SectionId ANALYZE_BASIC_STATS = new SectionId(162, "ANALYZE_BASIC_STATS");
    public static final SectionId ANALYZE_HISTOGRAM_STATS = new SectionId(163, "ANALYZE_HISTOGRAM_STATS");
    public static final SectionId ANALYZE_EXTERNAL_BASIC_STATS = new SectionId(164, "ANALYZE_EXTERNAL_BASIC_STATS");
    public static final SectionId ANALYZE_EXTERNAL_HISTOGRAM_STATS = new SectionId(165, "ANALYZE_EXTERNAL_HISTOGRAM_STATS");
    public static final SectionId ANALYZE_MULTI_COLUMN_STATS = new SectionId(166, "ANALYZE_MULTI_COLUMN_STATS");

    // ResourceGroupMgr: 170-179.
    public static final SectionId RESOURCE_GROUP_MGR = new SectionId(170, "RESOURCE_GROUP_MGR");

    // AuthenticationMgr: 180-189.
    public static final SectionId AUTHENTICATION_USER_PROPERTY = new SectionId(180, "AUTHENTICATION_USER_PROPERTY");
    public static final SectionId AUTHENTICATION_GROUP_PROVIDER = new SectionId(181, "AUTHENTICATION_GROUP_PROVIDER");
    public static final SectionId AUTHENTICATION_SECURITY_INTEGRATION = new SectionId(182, "AUTHENTICATION_SECURITY_INTEGRATION");
    public static final SectionId AUTHENTICATION_USER = new SectionId(183, "AUTHENTICATION_USER");

    // AuthorizationMgr: 190-199.
    public static final SectionId AUTHORIZATION_ROLE_NAME = new SectionId(190, "AUTHORIZATION_ROLE_NAME");
    public static final SectionId AUTHORIZATION_GROUP_ROLES = new SectionId(191, "AUTHORIZATION_GROUP_ROLES");
    public static final SectionId AUTHORIZATION_USER_HEADER = new SectionId(192, "AUTHORIZATION_USER_HEADER");
    public static final SectionId AUTHORIZATION_USER_PRIVILEGE = new SectionId(193, "AUTHORIZATION_USER_PRIVILEGE");
    public static final SectionId AUTHORIZATION_ROLE_HEADER = new SectionId(194, "AUTHORIZATION_ROLE_HEADER");
    public static final SectionId AUTHORIZATION_ROLE_PRIVILEGE = new SectionId(195, "AUTHORIZATION_ROLE_PRIVILEGE");

    // TaskManager: 200-209.
    public static final SectionId TASK = new SectionId(200, "TASK");
    public static final SectionId TASK_RUN_STATUS = new SectionId(201, "TASK_RUN_STATUS");

    // CatalogMgr: 210-219.
    public static final SectionId CATALOG_MGR = new SectionId(210, "CATALOG_MGR");

    // InsertOverwriteJobMgr: 220-229.
    public static final SectionId INSERT_OVERWRITE_JOB = new SectionId(220, "INSERT_OVERWRITE_JOB");
    public static final SectionId INSERT_OVERWRITE_TABLE_JOBS = new SectionId(221, "INSERT_OVERWRITE_TABLE_JOBS");

    // CompactionMgr: 230-239.
    public static final SectionId COMPACTION_MGR = new SectionId(230, "COMPACTION_MGR");

    // StreamLoadMgr: 240-249.
    public static final SectionId STREAM_LOAD_TASK = new SectionId(240, "STREAM_LOAD_TASK");
    public static final SectionId STREAM_LOAD_MULTI_STMT_TASK = new SectionId(241, "STREAM_LOAD_MULTI_STMT_TASK");

    // MaterializedViewMgr: 250-259.
    public static final SectionId MATERIALIZED_VIEW_MGR = new SectionId(250, "MATERIALIZED_VIEW_MGR");

    // GlobalFunctionMgr: 260-269.
    public static final SectionId GLOBAL_FUNCTION_MGR = new SectionId(260, "GLOBAL_FUNCTION_MGR");

    // StorageVolumeMgr: 270-279.
    public static final SectionId STORAGE_VOLUME_MGR = new SectionId(270, "STORAGE_VOLUME_MGR");

    // DictionaryMgr: 280-289.
    public static final SectionId DICTIONARY_MGR = new SectionId(280, "DICTIONARY_MGR");
    public static final SectionId DICTIONARY_NAME = new SectionId(281, "DICTIONARY_NAME");

    // ReplicationMgr: 290-299.
    public static final SectionId REPLICATION_MGR = new SectionId(290, "REPLICATION_MGR");

    // KeyMgr: 300-309.
    public static final SectionId KEY_MGR = new SectionId(300, "KEY_MGR");

    // PipeRepo: 310-319.
    public static final SectionId PIPE_MGR = new SectionId(310, "PIPE_MGR");

    // WarehouseManager: 320-329 (CE has no persisted data).

    // SqlBlackList: 330-339.
    public static final SectionId SQL_BLACKLIST = new SectionId(330, "SQL_BLACKLIST");

    // ClusterSnapshotMgr: 340-349.
    public static final SectionId CLUSTER_SNAPSHOT_MGR = new SectionId(340, "CLUSTER_SNAPSHOT_MGR");

    // HistoricalNodeMgr: 350-359.
    public static final SectionId HISTORICAL_NODE_MGR = new SectionId(350, "HISTORICAL_NODE_MGR");

    // TabletReshardJobMgr: 360-369.
    public static final SectionId TABLET_RESHARD_JOB_MGR = new SectionId(360, "TABLET_RESHARD_JOB_MGR");

    // SqlDigestBlackList: 370-379.
    public static final SectionId SQL_DIGEST_BLACKLIST = new SectionId(370, "SQL_DIGEST_BLACKLIST");

    // AIProviderMgr: 380-389.
    public static final SectionId AI_PROVIDER_MGR = new SectionId(380, "AI_PROVIDER_MGR");

    // GtidGenerator: 400-409.
    public static final SectionId GTID_GENERATOR = new SectionId(400, "GTID_GENERATOR");

    private final int value;
    private final String name;

    protected SectionId(int value, String name) {
        this.value = value;
        this.name = Objects.requireNonNull(name, "name");
    }

    public int getValue() {
        return value;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SectionId && ((SectionId) o).value == value;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public String toString() {
        return name + "(" + value + ")";
    }
}
