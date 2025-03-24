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

package com.starrocks.persist;

import org.junit.Assert;
import org.junit.Test;

public class OperationTypeTest {

    @Test
    public void testRecoverableOperations() {
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ERASE_DB));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_PARTITION));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ERASE_PARTITION));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_VIEW_DEF));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_BATCH_MODIFY_PARTITION));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_BATCH_DROP_ROLLUP));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_REMOVE_ALTER_JOB_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_GLOBAL_VARIABLE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_ALL_BROKER));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_REPOSITORY));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DELETE_AUTO_INCREMENT_ID));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DYNAMIC_PARTITION));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_REPLICATION_NUM));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_IN_MEMORY));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_SET_FORBIDDEN_GLOBAL_DICT));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_SET_HAS_DELETE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_INSTALL_PLUGIN));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_UNINSTALL_PLUGIN));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_RESOURCE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_RESOURCE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_SWAP_TABLE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_FINISH_MULTI_DELETE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ERASE_MULTI_TABLES));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_ENABLE_PERSISTENT_INDEX));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_WRITE_QUORUM));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_REPLICATED_STORAGE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_BINLOG_CONFIG));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_BINLOG_AVAILABLE_VERSION));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_ANALYZER_JOB));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_REMOVE_ANALYZER_JOB));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_ANALYZE_STATUS));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_BASIC_STATS_META));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_HISTOGRAM_STATS_META));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_REMOVE_BASIC_STATS_META));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_REMOVE_HISTOGRAM_STATS_META));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_REMOVE_ANALYZE_STATUS));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_RESOURCE_GROUP));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_HIVE_TABLE_COLUMN));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_HEARTBEAT_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_CATALOG));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_CATALOG));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_TASK));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_TASKS));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_TASK_RUN));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_UPDATE_TASK_RUN));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_UPDATE_TASK_RUN_STATE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ALTER_TASK));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_RENAME_MATERIALIZED_VIEW));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(
                OperationType.OP_CHANGE_MATERIALIZED_VIEW_REFRESH_SCHEME));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ALTER_MATERIALIZED_VIEW_STATUS));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_COMPUTE_NODE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_COMPUTE_NODE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_PARTITION_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_UPDATE_USER_PRIVILEGE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ALTER_USER_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_ROLE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MV_EPOCH_UPDATE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MV_JOB_STATE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ALTER_LOAD_JOB));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ALTER_TABLE_PROPERTIES));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_BUCKET_SIZE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_MUTABLE_BUCKET_NUM));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_ENABLE_LOAD_PROFILE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(
                OperationType.OP_MODIFY_BASE_COMPACTION_FORBIDDEN_TIME_RANGES));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_EXTERNAL_ANALYZE_STATUS));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_REMOVE_EXTERNAL_ANALYZE_STATUS));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_EXTERNAL_ANALYZER_JOB));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_REMOVE_EXTERNAL_ANALYZER_JOB));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_EXTERNAL_BASIC_STATS_META));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_REMOVE_EXTERNAL_BASIC_STATS_META));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ALTER_DB_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_RENAME_DB_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_LOAD_JOB_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_END_LOAD_JOB_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CHANGE_ROUTINE_LOAD_JOB_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_COLOCATE_ADD_TABLE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_COLOCATE_MARK_UNSTABLE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_COLOCATE_MARK_STABLE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_TABLE_COLOCATE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_EXPORT_CREATE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_EXPORT_UPDATE_INFO_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_SMALL_FILE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_SMALL_FILE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_BACKUP_JOB_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_REPOSITORY_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_TABLE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_RENAME_TABLE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_RENAME_PARTITION_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_RENAME_COLUMN_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_ROLLUP_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_FINISH_CONSISTENCY_CHECK_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_RENAME_ROLLUP_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_DISTRIBUTION_TYPE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_BATCH_ADD_ROLLUP_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DELETE_REPLICA_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_BACKEND_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_BACKEND_STATE_CHANGE_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_REMOVE_FRONTEND_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_BROKER_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_BROKER_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DECOMMISSION_DISK));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CANCEL_DECOMMISSION_DISK));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DISABLE_DISK));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_TIMESTAMP_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_FUNCTION_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_FUNCTION_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_UPDATE_USER_PROP_V3));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_STREAM_LOAD_TASK_V2));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_SET_DEFAULT_STORAGE_VOLUME));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_STORAGE_VOLUME));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_UPDATE_STORAGE_VOLUME));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_STORAGE_VOLUME));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_UPDATE_TABLE_STORAGE_INFOS));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_PIPE));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(
                OperationType.OP_MODIFY_PRIMARY_INDEX_CACHE_EXPIRE_SEC));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ALTER_CATALOG));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_DICTIONARY));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_DICTIONARY));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_DICTIONARY_MGR));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_REPLICATION_JOB));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DELETE_REPLICATION_JOB));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_RESET_FRONTENDS));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CLUSTER_SNAPSHOT_LOG));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_SQL_QUERY_BLACK_LIST));
        Assert.assertTrue(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DELETE_SQL_QUERY_BLACK_LIST));
    }

    @Test
    public void testUnRecoverableOperations() {
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_BACKEND_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_FIRST_FRONTEND_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_FRONTEND_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_PARTITIONS_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_PARTITION_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_REPLICA_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ADD_SUB_PARTITIONS_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_ALTER_JOB_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_BACKEND_TABLETS_INFO_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_BATCH_DELETE_REPLICA));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_DB_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_INSERT_OVERWRITE));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_TABLE_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_CREATE_USER_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_DROP_USER_V3));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_INSERT_OVERWRITE_STATE_CHANGE));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_INVALID));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_LEADER_INFO_CHANGE_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_META_VERSION_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_MODIFY_TABLE_ADD_OR_DROP_COLUMNS));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_RECOVER_DB_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_RECOVER_PARTITION_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_RECOVER_TABLE_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_REPLACE_TEMP_PARTITION));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_RESTORE_JOB_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_SAVE_AUTO_INCREMENT_ID));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_SAVE_NEXTID));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_SAVE_TRANSACTION_ID_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_SET_REPLICA_STATUS));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_STARMGR));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_TRUNCATE_TABLE));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_TYPE_EOF));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_UPDATE_FRONTEND_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_UPDATE_REPLICA_V2));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_UPSERT_TRANSACTION_STATE_BATCH));
        Assert.assertFalse(OperationType.IGNORABLE_OPERATIONS.contains(OperationType.OP_UPSERT_TRANSACTION_STATE_V2));
    }
}
