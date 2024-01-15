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

import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

//NOTICE new added type starts from 10000, to avoid conflicting with community added type
public class OperationType {
    private static final Logger LOG = LogManager.getLogger(OperationType.class);

    public static final short OP_INVALID = -1;

    public static final short OP_SAVE_NEXTID = 0;

    @Deprecated
    //Added OP_CREATE_DB_V2 in version 3.1, can be removed in version 3.2
    public static final short OP_CREATE_DB = 1;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_DB = 2;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_ALTER_DB = 3;

    @RecoverableOnReplayFailed
    public static final short OP_ERASE_DB = 4;

    @Deprecated
    public static final short OP_RECOVER_DB = 5;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_RENAME_DB = 6;

    // 10~19 110~119 210~219 ...

    @Deprecated
    //Added OP_CREATE_TABLE_V2 in version 3.1, can be removed in version 3.2
    public static final short OP_CREATE_TABLE = 10;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_DROP_TABLE = 11;

    @Deprecated
    public static final short OP_ADD_PARTITION = 12;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_PARTITION = 13;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_PARTITION = 14;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_ERASE_TABLE = 15;

    @RecoverableOnReplayFailed
    public static final short OP_ERASE_PARTITION = 16;

    @Deprecated
    public static final short OP_RECOVER_TABLE = 17;

    @Deprecated
    public static final short OP_RECOVER_PARTITION = 18;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_RENAME_TABLE = 19;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_RENAME_PARTITION = 110;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_BACKUP_JOB = 116;

    @Deprecated
    public static final short OP_RESTORE_JOB = 117;

    public static final short OP_TRUNCATE_TABLE = 118;

    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_VIEW_DEF = 119;

    public static final short OP_REPLACE_TEMP_PARTITION = 210;

    @RecoverableOnReplayFailed
    public static final short OP_BATCH_MODIFY_PARTITION = 211;

    // 20~29 120~129 220~229 ...
    @Deprecated
    public static final short OP_START_ROLLUP = 20;

    @Deprecated
    public static final short OP_FINISH_ROLLUP = 21;

    @Deprecated
    public static final short OP_CANCEL_ROLLUP = 23;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_DROP_ROLLUP = 24;

    @Deprecated
    public static final short OP_START_SCHEMA_CHANGE = 25;

    @Deprecated
    public static final short OP_FINISH_SCHEMA_CHANGE = 26;

    @Deprecated
    public static final short OP_CANCEL_SCHEMA_CHANGE = 27;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_CLEAR_ROLLUP_INFO = 28;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_FINISH_CONSISTENCY_CHECK = 29;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_RENAME_ROLLUP = 120;

    public static final short OP_ALTER_JOB_V2 = 121;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_DISTRIBUTION_TYPE = 122;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_BATCH_ADD_ROLLUP = 123;

    @RecoverableOnReplayFailed
    public static final short OP_BATCH_DROP_ROLLUP = 124;

    @RecoverableOnReplayFailed
    public static final short OP_REMOVE_ALTER_JOB_V2 = 125;

    // 30~39 130~139 230~239 ...
    // load job for only hadoop load
    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_EXPORT_CREATE = 36;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_EXPORT_UPDATE_STATE = 37;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_EXPORT_UPDATE_INFO = 38;

    @Deprecated
    public static final short OP_FINISH_SYNC_DELETE = 40;

    @Deprecated
    public static final short OP_FINISH_DELETE = 41;

    @Deprecated
    public static final short OP_ADD_REPLICA = 42;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_DELETE_REPLICA = 43;

    @Deprecated
    public static final short OP_FINISH_ASYNC_DELETE = 44;

    @Deprecated
    public static final short OP_UPDATE_REPLICA = 45;

    @Deprecated
    public static final short OP_BACKEND_TABLETS_INFO = 46;

    public static final short OP_SET_REPLICA_STATUS = 47;

    @Deprecated
    public static final short OP_ADD_BACKEND = 50;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_DROP_BACKEND = 51;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_BACKEND_STATE_CHANGE = 52;

    @Deprecated
    public static final short OP_START_DECOMMISSION_BACKEND = 53;

    @Deprecated
    public static final short OP_FINISH_DECOMMISSION_BACKEND = 54;

    @Deprecated
    public static final short OP_ADD_FRONTEND = 55;

    @Deprecated
    public static final short OP_ADD_FIRST_FRONTEND = 56;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_REMOVE_FRONTEND = 57;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_SET_LOAD_ERROR_HUB = 58;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_HEARTBEAT = 59;

    @Deprecated
    public static final short OP_CREATE_USER = 62;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_NEW_DROP_USER = 63;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_GRANT_PRIV = 64;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_REVOKE_PRIV = 65;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_SET_PASSWORD = 66;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_CREATE_ROLE = 67;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_DROP_ROLE = 68;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_UPDATE_USER_PROPERTY = 69;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_TIMESTAMP = 70;

    @Deprecated
    public static final short OP_LEADER_INFO_CHANGE = 71;
    @Deprecated

    public static final short OP_META_VERSION = 72;

    @Deprecated
    // replaced by OP_GLOBAL_VARIABLE_V2
    @RecoverableOnReplayFailed
    public static final short OP_GLOBAL_VARIABLE = 73;

    @Deprecated
    public static final short OP_CREATE_CLUSTER = 74;

    @RecoverableOnReplayFailed
    public static final short OP_GLOBAL_VARIABLE_V2 = 84;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_ADD_BROKER = 85;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_DROP_BROKER = 86;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_ALL_BROKER = 87;

    @Deprecated
    public static final short OP_UPDATE_CLUSTER_AND_BACKENDS = 88;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_CREATE_REPOSITORY = 89;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_REPOSITORY = 90;

    //colocate table
    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_COLOCATE_ADD_TABLE = 94;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_COLOCATE_REMOVE_TABLE = 95;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_COLOCATE_BACKENDS_PER_BUCKETSEQ = 96;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_COLOCATE_MARK_UNSTABLE = 97;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_COLOCATE_MARK_STABLE = 98;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_TABLE_COLOCATE = 99;

    //real time load 100 -108
    @Deprecated
    public static final short OP_UPSERT_TRANSACTION_STATE = 100;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_DELETE_TRANSACTION_STATE = 101;

    @Deprecated
    public static final short OP_FINISHING_ROLLUP = 102;

    @Deprecated
    public static final short OP_FINISHING_SCHEMA_CHANGE = 103;

    @Deprecated
    public static final short OP_SAVE_TRANSACTION_ID = 104;

    public static final short OP_SAVE_AUTO_INCREMENT_ID = 105;

    @RecoverableOnReplayFailed
    public static final short OP_DELETE_AUTO_INCREMENT_ID = 106;

    // light schema change for add and drop columns
    public static final short OP_MODIFY_TABLE_ADD_OR_DROP_COLUMNS = 107;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_ALTER_ROUTINE_LOAD_JOB = 111;

    // UDF 130-140
    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_ADD_FUNCTION = 130;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_DROP_FUNCTION = 131;

    // routine load 200
    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_CREATE_ROUTINE_LOAD_JOB = 200;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_CHANGE_ROUTINE_LOAD_JOB = 201;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_REMOVE_ROUTINE_LOAD_JOB = 202;

    // load job v2 for broker load 230~250
    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_CREATE_LOAD_JOB = 230;

    // this finish op include finished and cancelled
    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_END_LOAD_JOB = 231;

    // update job info, used by spark load
    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_UPDATE_LOAD_JOB = 232;

    // small files 251~260
    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_CREATE_SMALL_FILE = 251;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_DROP_SMALL_FILE = 252;

    // dynamic partition 261~265
    @RecoverableOnReplayFailed
    public static final short OP_DYNAMIC_PARTITION = 261;

    // set table replication_num config 266
    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_REPLICATION_NUM = 266;

    // set table in memory
    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_IN_MEMORY = 267;

    // global dict
    @RecoverableOnReplayFailed
    public static final short OP_SET_FORBIDDEN_GLOBAL_DICT = 268;

    // plugin 270~275
    @RecoverableOnReplayFailed
    public static final short OP_INSTALL_PLUGIN = 270;

    @RecoverableOnReplayFailed
    public static final short OP_UNINSTALL_PLUGIN = 271;

    // resource 276~290
    @RecoverableOnReplayFailed
    public static final short OP_CREATE_RESOURCE = 276;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_RESOURCE = 277;

    // NOTICE newly added type starts from 10000, to avoid conflicting with community added type

    public static final short OP_META_VERSION_V2 = 10000;

    @RecoverableOnReplayFailed
    public static final short OP_SWAP_TABLE = 10001;

    @Deprecated
    public static final short OP_ADD_PARTITIONS = 10002;

    @RecoverableOnReplayFailed
    public static final short OP_FINISH_MULTI_DELETE = 10003;

    @RecoverableOnReplayFailed
    public static final short OP_ERASE_MULTI_TABLES = 10004;

    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_ENABLE_PERSISTENT_INDEX = 10005;

    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_WRITE_QUORUM = 10006;

    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_REPLICATED_STORAGE = 10007;

    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_BINLOG_CONFIG = 10008;

    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_BINLOG_AVAILABLE_VERSION = 10009;

    // statistic 10010 ~ 10020
    @RecoverableOnReplayFailed
    public static final short OP_ADD_ANALYZER_JOB = 10010;

    @RecoverableOnReplayFailed
    public static final short OP_REMOVE_ANALYZER_JOB = 10011;

    @RecoverableOnReplayFailed
    public static final short OP_ADD_ANALYZE_STATUS = 10012;

    @RecoverableOnReplayFailed
    public static final short OP_ADD_BASIC_STATS_META = 10013;

    @RecoverableOnReplayFailed
    public static final short OP_ADD_HISTOGRAM_STATS_META = 10014;

    @RecoverableOnReplayFailed
    public static final short OP_REMOVE_BASIC_STATS_META = 10015;

    @RecoverableOnReplayFailed
    public static final short OP_REMOVE_HISTOGRAM_STATS_META = 10016;

    @RecoverableOnReplayFailed
    public static final short OP_REMOVE_ANALYZE_STATUS = 10017;

    // workgroup 10021 ~ 10030
    @RecoverableOnReplayFailed
    public static final short OP_RESOURCE_GROUP = 10021;

    // external hive table column change
    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_HIVE_TABLE_COLUMN = 10031;

    // New version of heartbeat
    @RecoverableOnReplayFailed
    public static final short OP_HEARTBEAT_V2 = 10041; // V2 version of heartbeat

    // create external catalog
    @RecoverableOnReplayFailed
    public static final short OP_CREATE_CATALOG = 10051;

    // drop catalog
    @RecoverableOnReplayFailed
    public static final short OP_DROP_CATALOG = 10061;

    // grant & revoke impersonate
    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_GRANT_IMPERSONATE = 10062;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_REVOKE_IMPERSONATE = 10063;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_GRANT_ROLE = 10064;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_REVOKE_ROLE = 10065;

    // task 10071 ~ 10090
    @RecoverableOnReplayFailed
    public static final short OP_CREATE_TASK = 10071;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_TASKS = 10072;

    @RecoverableOnReplayFailed
    public static final short OP_CREATE_TASK_RUN = 10081;

    @RecoverableOnReplayFailed
    public static final short OP_UPDATE_TASK_RUN = 10082;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_DROP_TASK_RUNS = 10083;

    @RecoverableOnReplayFailed
    public static final short OP_UPDATE_TASK_RUN_STATE = 10084;

    @RecoverableOnReplayFailed
    public static final short OP_ALTER_TASK = 10085;

    // materialized view 10091 ~ 10100
    @RecoverableOnReplayFailed
    public static final short OP_RENAME_MATERIALIZED_VIEW = 10091;

    @RecoverableOnReplayFailed
    public static final short OP_CHANGE_MATERIALIZED_VIEW_REFRESH_SCHEME = 10092;

    @RecoverableOnReplayFailed
    public static final short OP_ALTER_MATERIALIZED_VIEW_PROPERTIES = 10093;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_CREATE_MATERIALIZED_VIEW = 10094;

    public static final short OP_CREATE_INSERT_OVERWRITE = 10095;

    public static final short OP_INSERT_OVERWRITE_STATE_CHANGE = 10096;

    @RecoverableOnReplayFailed
    public static final short OP_ALTER_MATERIALIZED_VIEW_STATUS = 10097;
    public static final short OP_ALTER_MATERIALIZED_VIEW_BASE_TABLE_INFOS = 10098;

    // manage system node info 10101 ~ 10120
    @Deprecated
    public static final short OP_UPDATE_FRONTEND = 10101;

    // manage compute node 10201 ~ 10220

    @RecoverableOnReplayFailed
    public static final short OP_ADD_COMPUTE_NODE = 10201;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_COMPUTE_NODE = 10202;

    // shard operate 10221 ~ 10240. Deprecated
    @Deprecated
    public static final short OP_ADD_UNUSED_SHARD = 10221;
    @Deprecated
    public static final short OP_DELETE_UNUSED_SHARD = 10222;

    // new operator for add partition 10241 ~ 10260
    public static final short OP_ADD_PARTITION_V2 = 10241;
    public static final short OP_ADD_PARTITIONS_V2 = 10242;
    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_PARTITION_V2 = 10243;

    public static final short OP_ADD_SUB_PARTITIONS_V2 = 10244;

    // new privilege, all ends with V2

    public static final short OP_CREATE_USER_V2 = 10261;

    @RecoverableOnReplayFailed
    public static final short OP_UPDATE_USER_PRIVILEGE_V2 = 10262;

    @RecoverableOnReplayFailed
    public static final short OP_ALTER_USER_V2 = 10263;

    @Deprecated
    public static final short OP_DROP_USER_V2 = 10264;

    @RecoverableOnReplayFailed
    public static final short OP_UPDATE_ROLE_PRIVILEGE_V2 = 10265;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_ROLE_V2 = 10266;

    public static final short OP_AUTH_UPGRADE_V2 = 10267;

    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_UPDATE_USER_PROP_V2 = 10268;

    @RecoverableOnReplayFailed
    public static final short OP_CREATE_SECURITY_INTEGRATION = 10269;

    // integrate with starmgr
    public static final short OP_STARMGR = 11000;

    // stream load
    @Deprecated
    @RecoverableOnReplayFailed
    public static final short OP_CREATE_STREAM_LOAD_TASK = 11020;

    // MaterializedView Maintenance

    @RecoverableOnReplayFailed
    public static final short OP_MV_EPOCH_UPDATE = 11030;

    @RecoverableOnReplayFailed
    public static final short OP_MV_JOB_STATE = 11031;

    // alter load

    @RecoverableOnReplayFailed
    public static final short OP_ALTER_LOAD_JOB = 11100;

    @RecoverableOnReplayFailed
    public static final short OP_ALTER_TABLE_PROPERTIES = 11101;


    // constraint properties

    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_TABLE_CONSTRAINT_PROPERTY = 11130;

    // modify table property bucket size
    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_BUCKET_SIZE = 11140;

    // external table analyze

    @RecoverableOnReplayFailed
    public static final short OP_ADD_EXTERNAL_ANALYZE_STATUS = 11200;

    @RecoverableOnReplayFailed
    public static final short OP_REMOVE_EXTERNAL_ANALYZE_STATUS = 11201;

    @RecoverableOnReplayFailed
    public static final short OP_ADD_EXTERNAL_ANALYZER_JOB = 11202;

    @RecoverableOnReplayFailed
    public static final short OP_REMOVE_EXTERNAL_ANALYZER_JOB = 11203;

    @RecoverableOnReplayFailed
    public static final short OP_ADD_EXTERNAL_BASIC_STATS_META = 11204;

    @RecoverableOnReplayFailed
    public static final short OP_REMOVE_EXTERNAL_BASIC_STATS_META = 11205;

    //Database json format log
    public static final short OP_CREATE_DB_V2 = 12001;
    @RecoverableOnReplayFailed
    public static final short OP_ALTER_DB_V2 = 12002;
    public static final short OP_RECOVER_DB_V2 = 12003;
    @RecoverableOnReplayFailed
    public static final short OP_RENAME_DB_V2 = 12004;

    //Load Job json format log

    @RecoverableOnReplayFailed
    public static final short OP_CREATE_LOAD_JOB_V2 = 12100;

    @RecoverableOnReplayFailed
    public static final short OP_END_LOAD_JOB_V2 = 12101;

    @RecoverableOnReplayFailed
    public static final short OP_CREATE_ROUTINE_LOAD_JOB_V2 = 12102;

    @RecoverableOnReplayFailed
    public static final short OP_CHANGE_ROUTINE_LOAD_JOB_V2 = 12103;

    //Txn json format log
    public static final short OP_UPSERT_TRANSACTION_STATE_V2 = 12110;
    public static final short OP_SAVE_TRANSACTION_ID_V2 = 12111;
    public static final short OP_UPSERT_TRANSACTION_STATE_BATCH = 12112;

    //colocate table json format log
    @RecoverableOnReplayFailed
    public static final short OP_COLOCATE_ADD_TABLE_V2 = 12130;

    @RecoverableOnReplayFailed
    public static final short OP_COLOCATE_BACKENDS_PER_BUCKETSEQ_V2 = 12131;

    @RecoverableOnReplayFailed
    public static final short OP_COLOCATE_MARK_UNSTABLE_V2 = 12132;

    @RecoverableOnReplayFailed
    public static final short OP_COLOCATE_MARK_STABLE_V2 = 12133;

    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_TABLE_COLOCATE_V2 = 12134;

    //Export json format log

    @RecoverableOnReplayFailed
    public static final short OP_EXPORT_CREATE_V2 = 12120;

    @RecoverableOnReplayFailed
    public static final short OP_EXPORT_UPDATE_INFO_V2 = 12121;

    // small files json format log

    @RecoverableOnReplayFailed
    public static final short OP_CREATE_SMALL_FILE_V2 = 12140;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_SMALL_FILE_V2 = 12141;

    //Backup/Restore json format log

    @RecoverableOnReplayFailed
    public static final short OP_BACKUP_JOB_V2 = 12150;

    public static final short OP_RESTORE_JOB_V2 = 12151;

    @RecoverableOnReplayFailed
    public static final short OP_CREATE_REPOSITORY_V2 = 12152;

    //Table/Partition json format log

    public static final short OP_CREATE_TABLE_V2 = 13000;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_TABLE_V2 = 13001;

    public static final short OP_RECOVER_TABLE_V2 = 13002;

    public static final short OP_RECOVER_PARTITION_V2 = 13003;

    @RecoverableOnReplayFailed
    public static final short OP_RENAME_TABLE_V2 = 13004;

    @RecoverableOnReplayFailed
    public static final short OP_RENAME_PARTITION_V2 = 13005;

    @RecoverableOnReplayFailed
    public static final short OP_RENAME_COLUMN_V2 = 13006;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_ROLLUP_V2 = 13010;

    @RecoverableOnReplayFailed
    public static final short OP_FINISH_CONSISTENCY_CHECK_V2 = 13011;

    @RecoverableOnReplayFailed
    public static final short OP_RENAME_ROLLUP_V2 = 13012;

    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_DISTRIBUTION_TYPE_V2 = 13014;

    @RecoverableOnReplayFailed
    public static final short OP_BATCH_ADD_ROLLUP_V2 = 13015;

    public static final short OP_ADD_REPLICA_V2 = 13020;

    @RecoverableOnReplayFailed
    public static final short OP_DELETE_REPLICA_V2 = 13021;

    public static final short OP_UPDATE_REPLICA_V2 = 13022;

    public static final short OP_BACKEND_TABLETS_INFO_V2 = 13023;

    public static final short OP_ADD_BACKEND_V2 = 13024;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_BACKEND_V2 = 13025;

    @RecoverableOnReplayFailed
    public static final short OP_BACKEND_STATE_CHANGE_V2 = 13026;

    public static final short OP_ADD_FRONTEND_V2 = 13027;

    public static final short OP_ADD_FIRST_FRONTEND_V2 = 13028;

    @RecoverableOnReplayFailed
    public static final short OP_REMOVE_FRONTEND_V2 = 13029;

    public static final short OP_UPDATE_FRONTEND_V2 = 13030;

    @RecoverableOnReplayFailed
    public static final short OP_ADD_BROKER_V2 = 13031;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_BROKER_V2 = 13032;

    @RecoverableOnReplayFailed
    public static final short OP_DECOMMISSION_DISK = 13033;

    @RecoverableOnReplayFailed
    public static final short OP_CANCEL_DECOMMISSION_DISK = 13034;

    @RecoverableOnReplayFailed
    public static final short OP_DISABLE_DISK = 13035;

    @RecoverableOnReplayFailed
    public static final short OP_CANCEL_DISABLE_DISK = 13036;

    public static final short OP_BATCH_DELETE_REPLICA = 13037;

    @RecoverableOnReplayFailed
    public static final short OP_TIMESTAMP_V2 = 13040;

    public static final short OP_LEADER_INFO_CHANGE_V2 = 13041;

    @RecoverableOnReplayFailed
    public static final short OP_ADD_FUNCTION_V2 = 13050;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_FUNCTION_V2 = 13051;

    public static final short OP_DROP_USER_V3 = 13060;

    @RecoverableOnReplayFailed
    public static final short OP_UPDATE_USER_PROP_V3 = 13061;

    @RecoverableOnReplayFailed
    public static final short OP_CREATE_STREAM_LOAD_TASK_V2 = 13070;

    // storage volume

    @RecoverableOnReplayFailed
    public static final short OP_SET_DEFAULT_STORAGE_VOLUME = 13100;

    @RecoverableOnReplayFailed
    public static final short OP_CREATE_STORAGE_VOLUME = 13101;

    @RecoverableOnReplayFailed
    public static final short OP_UPDATE_STORAGE_VOLUME = 13102;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_STORAGE_VOLUME = 13103;

    // Pipe operations log
    @RecoverableOnReplayFailed
    public static final short OP_PIPE = 12200;

    // Primary key
    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_PRIMARY_INDEX_CACHE_EXPIRE_SEC = 13200;

    // alter catalog
    @RecoverableOnReplayFailed
    public static final short OP_ALTER_CATALOG = 13300;

    // Dictionary

    @RecoverableOnReplayFailed
    public static final short OP_CREATE_DICTIONARY = 13400;

    @RecoverableOnReplayFailed
    public static final short OP_DROP_DICTIONARY = 13401;

    @RecoverableOnReplayFailed
    public static final short OP_MODIFY_DICTIONARY_MGR = 13402;

    // Replication job
    @RecoverableOnReplayFailed
    public static final short OP_REPLICATION_JOB = 13500;

    /**
     * NOTICE: OperationType cannot use a value exceeding 20000, and an error will be reported if it exceeds
     */
    public static final short OP_TYPE_EOF = 20000;

    public static final ImmutableSet<Short> RECOVERABLE_OPERATIONS = buildRecoverableOperations();

    private static ImmutableSet<Short> buildRecoverableOperations() {
        ImmutableSet.Builder<Short> builder = ImmutableSet.builder();
        Set<Short> allOperations = new HashSet<>();

        for (Field field : OperationType.class.getFields()) {
            if (field.getType() != short.class) {
                continue;
            }
            short opType = Short.MIN_VALUE;
            try {
                opType = (short) field.get(null);
            } catch (IllegalAccessException e) {
                LOG.error("get value from {} failed, will exit.", field.getName(), e);
                System.exit(-1);
            }

            if (opType > OperationType.OP_TYPE_EOF) {
                LOG.error("OperationType cannot use a value exceeding 20000, " +
                        "and an error will be reported if it exceeds : {} = {}", field.getName(), opType);
                System.exit(-1);
            }

            if (!allOperations.add(opType)) {
                LOG.error("Duplicate operation type {} with value {}, will exit.", field.getName(), opType);
                System.exit(-1);
            }

            Annotation annotation = field.getAnnotation(RecoverableOnReplayFailed.class);
            if (annotation != null) {
                builder.add(opType);
            }
        }

        return builder.build();
    }
}
