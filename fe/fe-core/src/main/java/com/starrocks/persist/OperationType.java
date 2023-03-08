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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/OperationType.java

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

package com.starrocks.persist;

//NOTICE new added type starts from 10000, to avoid conflicting with community added type
public class OperationType {
    public static final short OP_INVALID = -1;
    public static final short OP_SAVE_NEXTID = 0;
    public static final short OP_CREATE_DB = 1;
    public static final short OP_DROP_DB = 2;
    public static final short OP_ALTER_DB = 3;
    public static final short OP_ERASE_DB = 4;
    public static final short OP_RECOVER_DB = 5;
    public static final short OP_RENAME_DB = 6;

    // 10~19 110~119 210~219 ...
    public static final short OP_CREATE_TABLE = 10;
    public static final short OP_DROP_TABLE = 11;
    public static final short OP_ADD_PARTITION = 12;
    public static final short OP_DROP_PARTITION = 13;
    public static final short OP_MODIFY_PARTITION = 14;
    @Deprecated
    public static final short OP_ERASE_TABLE = 15;
    public static final short OP_ERASE_PARTITION = 16;
    public static final short OP_RECOVER_TABLE = 17;
    public static final short OP_RECOVER_PARTITION = 18;
    public static final short OP_RENAME_TABLE = 19;
    public static final short OP_RENAME_PARTITION = 110;
    public static final short OP_BACKUP_JOB = 116;
    public static final short OP_RESTORE_JOB = 117;
    public static final short OP_TRUNCATE_TABLE = 118;
    public static final short OP_MODIFY_VIEW_DEF = 119;
    public static final short OP_REPLACE_TEMP_PARTITION = 210;
    public static final short OP_BATCH_MODIFY_PARTITION = 211;

    // 20~29 120~129 220~229 ...
    @Deprecated
    public static final short OP_START_ROLLUP = 20;
    @Deprecated
    public static final short OP_FINISH_ROLLUP = 21;
    @Deprecated
    public static final short OP_CANCEL_ROLLUP = 23;
    public static final short OP_DROP_ROLLUP = 24;
    @Deprecated
    public static final short OP_START_SCHEMA_CHANGE = 25;
    @Deprecated
    public static final short OP_FINISH_SCHEMA_CHANGE = 26;
    @Deprecated
    public static final short OP_CANCEL_SCHEMA_CHANGE = 27;
    public static final short OP_CLEAR_ROLLUP_INFO = 28;
    public static final short OP_FINISH_CONSISTENCY_CHECK = 29;
    public static final short OP_RENAME_ROLLUP = 120;
    public static final short OP_ALTER_JOB_V2 = 121;
    public static final short OP_MODIFY_DISTRIBUTION_TYPE = 122;
    public static final short OP_BATCH_ADD_ROLLUP = 123;
    public static final short OP_BATCH_DROP_ROLLUP = 124;
    public static final short OP_REMOVE_ALTER_JOB_V2 = 125;

    // 30~39 130~139 230~239 ...
    // load job for only hadoop load
    public static final short OP_EXPORT_CREATE = 36;
    public static final short OP_EXPORT_UPDATE_STATE = 37;
    public static final short OP_EXPORT_UPDATE_INFO = 38;

    @Deprecated
    public static final short OP_FINISH_SYNC_DELETE = 40;
    public static final short OP_FINISH_DELETE = 41;
    public static final short OP_ADD_REPLICA = 42;
    public static final short OP_DELETE_REPLICA = 43;
    @Deprecated
    public static final short OP_FINISH_ASYNC_DELETE = 44;
    public static final short OP_UPDATE_REPLICA = 45;
    public static final short OP_BACKEND_TABLETS_INFO = 46;
    public static final short OP_SET_REPLICA_STATUS = 47;

    public static final short OP_ADD_BACKEND = 50;
    public static final short OP_DROP_BACKEND = 51;
    public static final short OP_BACKEND_STATE_CHANGE = 52;
    @Deprecated
    public static final short OP_START_DECOMMISSION_BACKEND = 53;
    @Deprecated
    public static final short OP_FINISH_DECOMMISSION_BACKEND = 54;
    public static final short OP_ADD_FRONTEND = 55;
    public static final short OP_ADD_FIRST_FRONTEND = 56;
    public static final short OP_REMOVE_FRONTEND = 57;
    public static final short OP_SET_LOAD_ERROR_HUB = 58;
    public static final short OP_HEARTBEAT = 59;
    public static final short OP_CREATE_USER = 62;
    public static final short OP_NEW_DROP_USER = 63;
    public static final short OP_GRANT_PRIV = 64;
    public static final short OP_REVOKE_PRIV = 65;
    public static final short OP_SET_PASSWORD = 66;
    public static final short OP_CREATE_ROLE = 67;
    public static final short OP_DROP_ROLE = 68;
    public static final short OP_UPDATE_USER_PROPERTY = 69;

    public static final short OP_TIMESTAMP = 70;
    public static final short OP_LEADER_INFO_CHANGE = 71;
    public static final short OP_META_VERSION = 72;
    @Deprecated
    // replaced by OP_GLOBAL_VARIABLE_V2
    public static final short OP_GLOBAL_VARIABLE = 73;

    public static final short OP_CREATE_CLUSTER = 74;
    public static final short OP_GLOBAL_VARIABLE_V2 = 84;

    public static final short OP_ADD_BROKER = 85;
    public static final short OP_DROP_BROKER = 86;
    public static final short OP_DROP_ALL_BROKER = 87;
    public static final short OP_UPDATE_CLUSTER_AND_BACKENDS = 88;
    public static final short OP_CREATE_REPOSITORY = 89;
    public static final short OP_DROP_REPOSITORY = 90;

    //colocate table
    public static final short OP_COLOCATE_ADD_TABLE = 94;
    public static final short OP_COLOCATE_REMOVE_TABLE = 95;
    public static final short OP_COLOCATE_BACKENDS_PER_BUCKETSEQ = 96;
    public static final short OP_COLOCATE_MARK_UNSTABLE = 97;
    public static final short OP_COLOCATE_MARK_STABLE = 98;
    public static final short OP_MODIFY_TABLE_COLOCATE = 99;

    //real time load 100 -108
    public static final short OP_UPSERT_TRANSACTION_STATE = 100;
    public static final short OP_DELETE_TRANSACTION_STATE = 101;
    @Deprecated
    public static final short OP_FINISHING_ROLLUP = 102;
    @Deprecated
    public static final short OP_FINISHING_SCHEMA_CHANGE = 103;
    public static final short OP_SAVE_TRANSACTION_ID = 104;
    public static final short OP_SAVE_AUTO_INCREMENT_ID = 105;
    public static final short OP_DELETE_AUTO_INCREMENT_ID = 106;

    // routine load 110~120
    public static final short OP_ROUTINE_LOAD_JOB = 110;
    public static final short OP_ALTER_ROUTINE_LOAD_JOB = 111;

    // UDF 130-140
    public static final short OP_ADD_FUNCTION = 130;
    public static final short OP_DROP_FUNCTION = 131;

    // routine load 200
    public static final short OP_CREATE_ROUTINE_LOAD_JOB = 200;
    public static final short OP_CHANGE_ROUTINE_LOAD_JOB = 201;
    public static final short OP_REMOVE_ROUTINE_LOAD_JOB = 202;

    // load job v2 for broker load 230~250
    public static final short OP_CREATE_LOAD_JOB = 230;
    // this finish op include finished and cancelled
    public static final short OP_END_LOAD_JOB = 231;
    // update job info, used by spark load
    public static final short OP_UPDATE_LOAD_JOB = 232;

    // small files 251~260
    public static final short OP_CREATE_SMALL_FILE = 251;
    public static final short OP_DROP_SMALL_FILE = 252;

    // dynamic partition 261~265
    public static final short OP_DYNAMIC_PARTITION = 261;

    // set table replication_num config 266
    public static final short OP_MODIFY_REPLICATION_NUM = 266;
    // set table in memory
    public static final short OP_MODIFY_IN_MEMORY = 267;

    // global dict
    public static final short OP_SET_FORBIT_GLOBAL_DICT = 268;

    // plugin 270~275
    public static final short OP_INSTALL_PLUGIN = 270;

    public static final short OP_UNINSTALL_PLUGIN = 271;

    // resource 276~290
    public static final short OP_CREATE_RESOURCE = 276;
    public static final short OP_DROP_RESOURCE = 277;

    // NOTICE newly added type starts from 10000, to avoid conflicting with community added type

    public static final short OP_META_VERSION_V2 = 10000;
    public static final short OP_SWAP_TABLE = 10001;
    public static final short OP_ADD_PARTITIONS = 10002;
    public static final short OP_FINISH_MULTI_DELETE = 10003;
    public static final short OP_ERASE_MULTI_TABLES = 10004;
    public static final short OP_MODIFY_ENABLE_PERSISTENT_INDEX = 10005;
    public static final short OP_MODIFY_WRITE_QUORUM = 10006;
    public static final short OP_MODIFY_REPLICATED_STORAGE = 10007;
    public static final short OP_MODIFY_BINLOG_CONFIG = 10008;
    public static final short OP_MODIFY_BINLOG_AVAILABLE_VERSION = 10009;

    // statistic 10010 ~ 10020
    public static final short OP_ADD_ANALYZER_JOB = 10010;
    public static final short OP_REMOVE_ANALYZER_JOB = 10011;
    public static final short OP_ADD_ANALYZE_STATUS = 10012;
    public static final short OP_ADD_BASIC_STATS_META = 10013;
    public static final short OP_ADD_HISTOGRAM_STATS_META = 10014;
    public static final short OP_REMOVE_BASIC_STATS_META = 10015;
    public static final short OP_REMOVE_HISTOGRAM_STATS_META = 10016;
    public static final short OP_REMOVE_ANALYZE_STATUS = 10017;

    // workgroup 10021 ~ 10030
    public static final short OP_RESOURCE_GROUP = 10021;

    // external hive table column change
    public static final short OP_MODIFY_HIVE_TABLE_COLUMN = 10031;

    // New version of heartbeat
    public static final short OP_HEARTBEAT_V2 = 10041; // V2 version of heartbeat

    // create external catalog
    public static final short OP_CREATE_CATALOG = 10051;

    // drop catalog
    public static final short OP_DROP_CATALOG = 10061;
    // grant & revoke impersonate
    public static final short OP_GRANT_IMPERSONATE = 10062;
    public static final short OP_REVOKE_IMPERSONATE = 10063;
    public static final short OP_GRANT_ROLE = 10064;
    public static final short OP_REVOKE_ROLE = 10065;

    // task 10071 ~ 10090
    public static final short OP_CREATE_TASK = 10071;
    public static final short OP_DROP_TASKS = 10072;
    public static final short OP_CREATE_TASK_RUN = 10081;
    public static final short OP_UPDATE_TASK_RUN = 10082;
    public static final short OP_DROP_TASK_RUNS = 10083;
    public static final short OP_UPDATE_TASK_RUN_STATE = 10084;

    // materialized view 10091 ~ 10100
    public static final short OP_RENAME_MATERIALIZED_VIEW = 10091;
    public static final short OP_CHANGE_MATERIALIZED_VIEW_REFRESH_SCHEME = 10092;
    public static final short OP_ALTER_MATERIALIZED_VIEW_PROPERTIES = 10093;
    public static final short OP_CREATE_MATERIALIZED_VIEW = 10094;
    public static final short OP_CREATE_INSERT_OVERWRITE = 10095;
    public static final short OP_INSERT_OVERWRITE_STATE_CHANGE = 10096;

    // manage system node info 10101 ~ 10120
    public static final short OP_UPDATE_FRONTEND = 10101;

    // manage compute node 10201 ~ 10220
    public static final short OP_ADD_COMPUTE_NODE = 10201;
    public static final short OP_DROP_COMPUTE_NODE = 10202;

    // shard operate 10221 ~ 10240. Deprecated
    @Deprecated
    public static final short OP_ADD_UNUSED_SHARD = 10221;
    @Deprecated
    public static final short OP_DELETE_UNUSED_SHARD = 10222;

    // new operator for add partition 10241 ~ 10260
    // only used in list partition currently
    public static final short OP_ADD_PARTITION_V2 = 10241;

    // only used in lake table currently
    public static final short OP_ADD_PARTITIONS_V2 = 10242;

    // new privilege, all ends with V2
    public static final short OP_CREATE_USER_V2 = 10261;
    public static final short OP_UPDATE_USER_PRIVILEGE_V2 = 10262;
    public static final short OP_ALTER_USER_V2 = 10263;
    public static final short OP_DROP_USER_V2 = 10264;
    public static final short OP_UPDATE_ROLE_PRIVILEGE_V2 = 10265;
    public static final short OP_DROP_ROLE_V2 = 10266;
    public static final short OP_AUTH_UPGRADE_V2 = 10267;
    public static final short OP_UPDATE_USER_PROP_V2 = 10268;

    // integrate with starmgr
    public static final short OP_STARMGR = 11000;

    // stream load
    public static final short OP_CREATE_STREAM_LOAD_TASK = 11020;

    // MaterializedView Maintenance
    public static final short OP_MV_EPOCH_UPDATE = 11030;
    public static final short OP_MV_JOB_STATE = 11031;

    // alter load
    public static final short OP_ALTER_LOAD_JOB = 11100;
    public static final short OP_ALTER_TABLE_PROPERTIES = 11101;

    // warehouse
    public static final short OP_CREATE_WH = 11110;
    public static final short OP_DROP_WH = 11111;
    public static final short OP_ALTER_WH_ADD_CLUSTER = 11112;
    public static final short OP_ALTER_WH_REMOVE_CLUSTER = 11113;
    public static final short OP_ALTER_WH_MOD_PROP = 11114;
    public static final short OP_SUSPEND_WH = 11115;
    public static final short OP_RESUME_WH = 11116;

    // constraint properties
    public static final short OP_MODIFY_TABLE_CONSTRAINT_PROPERTY = 11130;
}
