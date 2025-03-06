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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/ErrorCode.java

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

package com.starrocks.common;

import java.util.MissingFormatArgumentException;

/**
 * 1、ErrorCode: The most granular error message, recording the error cause at the bottom of the call stack
 * <p>
 * 2、SqlState: Coarse-grained error information is also recorded in ErrorCode
 * Main references: <a href="https://www.postgresql.org/docs/15/errcodes-appendix.html">...</a>
 * <p>
 * 3、ErrorType: The most coarse-grained error message, which determines the error category
 * based on the first two digits of SQLSTATE. For example, Internal Error, Syntax Error
 */
public enum ErrorCode {
    ERR_CANT_CREATE_TABLE(1005, new byte[] {'H', 'Y', '0', '0', '0'}, "Can't create table '%s' (errno: %s)"),
    ERR_DB_CREATE_EXISTS(1007, new byte[] {'H', 'Y', '0', '0', '0'}, "Can't create database '%s'; database exists"),
    ERR_DB_DROP_EXISTS(1008, new byte[] {'H', 'Y', '0', '0', '0'},
            "Can't drop database '%s'; database doesn't exist"),
    ERR_AUTHENTICATION_FAIL(1045, new byte[] {'2', '8', '0', '0', '0'},
            "Access denied for user '%s' (using password: %s)"),
    ERR_NO_DB_ERROR(1046, new byte[] {'3', 'D', '0', '0', '0'}, "No database selected"),
    ERR_UNKNOWN_COM_ERROR(1047, new byte[] {'0', '8', 'S', '0', '1'}, "Unknown command"),
    ERR_TABLE_EXISTS_ERROR(1050, new byte[] {'4', '2', 'S', '0', '1'}, "Table '%s' already exists"),
    ERR_NON_UNIQ_ERROR(1052, new byte[] {'2', '3', '0', '0', '0'}, "Column '%s' in is ambiguous"),
    ERR_ILLEGAL_COLUMN_REFERENCE_ERROR(1053, new byte[] {'2', '3', '0', '0', '1'},
            "Illegal column/field reference '%s' of semi-/anti-join"),
    ERR_BAD_FUNC_ERROR(1055, new byte[] {'4', '2', '0', '0', '0'}, "Unknown function '%s'"),
    ERR_WRONG_VALUE_COUNT(1058, new byte[] {'2', '1', 'S', '0', '1'}, "Column count doesn't match value count"),
    ERR_DUP_FIELDNAME(1060, new byte[] {'4', '2', 'S', '2', '1'}, "Duplicate column name '%s'"),
    ERR_NONUNIQ_TABLE(1066, new byte[] {'4', '2', '0', '0', '0'}, "Not unique table/alias: '%s'"),
    ERR_NO_SUCH_THREAD(1094, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown thread id: %d"),
    ERR_NO_TABLES_USED(1096, new byte[] {'H', 'Y', '0', '0', '0'}, "No tables used"),
    ERR_WRONG_DB_NAME(1102, new byte[] {'4', '2', '0', '0', '0'}, "Incorrect database name '%s'"),
    ERR_WRONG_TABLE_NAME(1104, new byte[] {'4', '2', '0', '0', '0'}, "Incorrect table name '%s'"),
    ERR_UNKNOWN_ERROR(1105, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown error"),
    ERR_FIELD_SPECIFIED_TWICE(1110, new byte[] {'4', '2', '0', '0', '0'}, "Column '%s' specified twice"),
    ERR_TABLE_MUST_HAVE_COLUMNS(1113, new byte[] {'4', '2', '0', '0', '0'}, "A table must have at least 1 column"),
    ERR_UNKNOWN_CHARACTER_SET(1115, new byte[] {'4', '2', '0', '0', '0'}, "Unknown character set: '%s'"),
    ERR_TOO_MANY_COLUMNS(1117, new byte[] {'4', '2', '0', '0', '0'},
            "The number of columns in a table must be less than or equal to %d," +
                    " Please decrease the number of columns or increase frontend config 'max_column_number_per_table'."),
    ERR_IP_NOT_ALLOWED(1130, new byte[] {'4', '2', '0', '0', '0'},
            "Host %s is not allowed to connect to this MySQL server"),
    ERR_NONEXISTING_GRANT(1141, new byte[] {'4', '2', '0', '0', '0'},
            "There is no such grant defined for user '%s' on host '%s'"),
    ERR_WRONG_COLUMN_NAME(1166, new byte[] {'4', '2', '0', '0', '0'}, "Incorrect column name '%s'"),
    ERR_UNKNOWN_SYSTEM_VARIABLE(1193, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown system variable '%s', " +
            "the most similar variables are %s"),
    ERR_TOO_MANY_USER_CONNECTIONS(1203, new byte[] {'4', '2', '0', '0', '0'},
            "User %s already has more than 'max_user_connections' active connections"),
    ERR_LOCAL_VARIABLE(1228, new byte[] {'H', 'Y', '0', '0', '0'},
            "Variable '%s' is a SESSION variable and can't be used with SET GLOBAL"),
    ERR_GLOBAL_VARIABLE(1229, new byte[] {'H', 'Y', '0', '0', '0'},
            "Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL"),
    ERR_NO_DEFAULT(1230, new byte[] {'4', '2', '0', '0', '0'}, "Variable '%s' doesn't have a default value"),
    ERR_WRONG_VALUE_FOR_VAR(1231, new byte[] {'4', '2', '0', '0', '0'},
            "Variable '%s' can't be set to the value of '%s'"),
    ERR_WRONG_TYPE_FOR_VAR(1232, new byte[] {'4', '2', '0', '0', '0'}, "Incorrect argument type to variable '%s'"),
    ERR_DERIVED_MUST_HAVE_ALIAS(1248, new byte[] {'4', '2', '0', '0', '0'},
            "Every derived table must have its own alias"),
    ERR_NOT_SUPPORTED_AUTH_MODE(1251, new byte[] {'0', '8', '0', '0', '4'},
            "Client does not support authentication protocol requested by server; consider upgrading MySQL client"),
    ERR_UNKNOWN_STORAGE_ENGINE(1286, new byte[] {'4', '2', '0', '0', '0'}, "Unknown storage engine '%s'"),
    ERR_UNSUPPORTED_PS(1295, "HY000".getBytes(),
            "This command is not supported in the prepared statement protocol yet"),
    ERR_UNKNOWN_TIME_ZONE(1298, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown or incorrect time zone: '%s'"),
    ERR_WRONG_OBJECT(1347, new byte[] {'H', 'Y', '0', '0', '0'}, "'%s'.'%s' is not '%s'"),
    ERR_VIEW_WRONG_LIST(1353, new byte[] {'H', 'Y', '0', '0', '0'},
            "View's SELECT and view's field list have different column counts"),
    ERR_NO_DEFAULT_FOR_FIELD(1364, new byte[] {'H', 'Y', '0', '0', '0'},
            "Field '%s' is not null but doesn't have a default value"),
    ERR_NO_SUCH_QUERY(1365, new byte[] {'4', '2', '0', '0', '0'}, "Unknown query id: %s"),

    ERR_CANNOT_USER(1396, new byte[] {'H', 'Y', '0', '0', '0'}, "Operation %s failed for %s"),
    ERR_NON_INSERTABLE_TABLE(1471, new byte[] {'H', 'Y', '0', '0', '0'},
            "The target table %s of the %s is not insertable-into"),
    ERR_DROP_PARTITION_NON_EXISTENT(1507, new byte[] {'H', 'Y', '0', '0', '0'},
            "Error in list of partitions to %s"),
    ERR_DROP_LAST_PARTITION(1508, new byte[] {'H', 'Y', '0', '0', '0'},
            "Cannot remove all partitions, use DROP TABLE instead"),
    ERR_SAME_NAME_PARTITION(1517, new byte[] {'H', 'Y', '0', '0', '0'}, "Duplicate partition name %s"),
    ERR_AUTH_PLUGIN_NOT_LOADED(1524, new byte[] {'H', 'Y', '0', '0', '0'}, "Plugin '%s' is not loaded"),
    ERR_WRONG_PARTITION_NAME(1567, new byte[] {'H', 'Y', '0', '0', '0'}, "Incorrect partition name '%s'"),
    ERR_VARIABLE_IS_READONLY(1621, new byte[] {'H', 'Y', '0', '0', '0'}, "Variable '%s' is a read only variable"),
    ERR_UNKNOWN_PARTITION(1735, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown partition '%s' in table '%s'"),
    ERR_PARTITION_CLAUSE_ON_NONPARTITIONED(1747, new byte[] {'H', 'Y', '0', '0', '0'},
            "PARTITION () clause on non partitioned table"),
    ERR_EMPTY_PARTITION_IN_TABLE(1748, new byte[] {'H', 'Y', '0', '0', '0'},
            "data cannot be inserted into table with empty partition. " +
                    "Use `SHOW PARTITIONS FROM %s` to see the currently partitions of this table. "),

    // Following is StarRocks's error code, which start from 5000
    ERR_WRONG_PROC_PATH(5001, new byte[] {'H', 'Y', '0', '0', '0'}, "Proc path '%s' doesn't exist"),
    ERR_COL_NOT_MENTIONED(5002, new byte[] {'H', 'Y', '0', '0', '0'},
            "'%s' must be explicitly mentioned in column permutation"),
    ERR_OLAP_KEY_MUST_BEFORE_VALUE(5003, new byte[] {'H', 'Y', '0', '0', '0'},
            "Key column must before value column"),
    ERR_TABLE_MUST_HAVE_KEYS(5004, new byte[] {'H', 'Y', '0', '0', '0'},
            "Table must have at least 1 key column"),
    ERR_UNKNOWN_CLUSTER_ID(5005, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown cluster id '%s'"),
    ERR_UNKNOWN_PLAN_HINT(5006, new byte[] {'H', 'Y', '0', '0', '0'}, "Unknown plan hint '%s'"),
    ERR_PLAN_HINT_CONFILT(5007, new byte[] {'H', 'Y', '0', '0', '0'}, "Conflict plan hint '%s'"),
    ERR_INSERT_HINT_NOT_SUPPORT(5008, new byte[] {'H', 'Y', '0', '0', '0'},
            "INSERT hints are only supported for partitioned table"),
    ERR_PARTITION_CLAUSE_NO_ALLOWED(5009, new byte[] {'H', 'Y', '0', '0', '0'},
            "PARTITION clause is not valid for INSERT into unpartitioned table"),
    ERR_COL_NUMBER_NOT_MATCH(5010, new byte[] {'H', 'Y', '0', '0', '0'},
            "Number of columns don't equal number of SELECT statement's select list"),
    ERR_UNRESOLVED_TABLE_REF(5011, new byte[] {'H', 'Y', '0', '0', '0'},
            "Unresolved table reference '%s'"),
    ERR_BAD_NUMBER(5012, new byte[] {'H', 'Y', '0', '0', '0'}, "'%s' is not a number"),
    ERR_BAD_TIMEUNIT(5013, new byte[] {'H', 'Y', '0', '0', '0'}, "Unsupported time unit '%s'"),
    ERR_BAD_TABLE_STATE(5014, new byte[] {'H', 'Y', '0', '0', '0'}, "Table state is not NORMAL: '%s'"),
    ERR_BAD_PARTITION_STATE(5015, new byte[] {'H', 'Y', '0', '0', '0'}, "Partition state is not NORMAL: '%s':'%s'"),
    ERR_PARTITION_HAS_LOADING_JOBS(5016, new byte[] {'H', 'Y', '0', '0', '0'}, "Partition has loading jobs: '%s'"),
    ERR_NOT_KEY_COLUMN(5017, new byte[] {'H', 'Y', '0', '0', '0'}, "Column is not a key column: '%s'"),
    ERR_INVALID_VALUE(5018, new byte[] {'H', 'Y', '0', '0', '0'}, "Invalid %s: '%s'. Expected values should be %s"),
    ERR_NO_ALTER_OPERATION(5023, new byte[] {'H', 'Y', '0', '0', '0'},
            "No operation in alter statement"),
    ERR_TIMEOUT(5024, new byte[] {'5', '3', '4', '0', '0'}, "%s reached its timeout of %d seconds, %s"),
    ERR_UNSUPPORTED_TYPE_IN_CTAS(5026, new byte[] {'H', 'Y', '0', '0', '0'},
            "Unsupported type '%s' in create table as select statement"),
    ERR_MISSING_PARAM(5027, new byte[] {'H', 'Y', '0', '0', '0'}, "Missing param: %s "),
    ERR_WRONG_NAME_FORMAT(5063, new byte[] {'4', '2', '0', '0', '0'},
            "Incorrect %s name '%s'"),
    ERR_COMMON_ERROR(5064, new byte[] {'4', '2', '0', '0', '0'},
            "%s"),
    ERR_COLOCATE_TABLE_MUST_HAS_SAME_REPLICATION_NUM(5063, new byte[] {'4', '2', '0', '0', '0'},
            "Colocate tables must have same replication num: %s, with group %s," +
                    " partition info %s"),
    ERR_COLOCATE_TABLE_MUST_HAS_SAME_BUCKET_NUM(5063, new byte[] {'4', '2', '0', '0', '0'},
            "Colocate tables must have same bucket num: %s, with group %s," +
                    " current info %s"),
    ERR_COLOCATE_TABLE_MUST_HAS_SAME_DISTRIBUTION_COLUMN_SIZE(5063, new byte[] {'4', '2', '0', '0', '0'},
            "Colocate tables distribution columns size must be the same : %s, with group %s," +
                    " current info %s"),
    ERR_COLOCATE_TABLE_MUST_HAS_SAME_DISTRIBUTION_COLUMN_TYPE(5063, new byte[] {'4', '2', '0', '0', '0'},
            "Colocate tables distribution columns must have the same data type with group %s," +
                    " current col: %s, should be: %s, current info %s"),
    ERR_COLOCATE_NOT_COLOCATE_TABLE(5064, new byte[] {'4', '2', '0', '0', '0'},
            "Table %s is not a colocated table"),
    ERROR_DYNAMIC_PARTITION_TIME_UNIT(5065, new byte[] {'4', '2', '0', '0', '0'},
            "Unsupported time unit %s. Expect DAY/WEEK/MONTH/YEAR."),
    ERROR_DYNAMIC_PARTITION_START_ZERO(5066, new byte[] {'4', '2', '0', '0', '0'},
            "Dynamic partition start must less than 0"),
    ERROR_DYNAMIC_PARTITION_START_FORMAT(5066, new byte[] {'4', '2', '0', '0', '0'},
            "Invalid dynamic partition start %s"),
    ERROR_DYNAMIC_PARTITION_END_ZERO(5066, new byte[] {'4', '2', '0', '0', '0'},
            "Dynamic partition end must greater than 0"),
    ERROR_DYNAMIC_PARTITION_END_FORMAT(5066, new byte[] {'4', '2', '0', '0', '0'},
            "Invalid dynamic partition end %s"),
    ERROR_DYNAMIC_PARTITION_END_EMPTY(5066, new byte[] {'4', '2', '0', '0', '0'},
            "Dynamic partition end is empty"),
    ERROR_DYNAMIC_PARTITION_BUCKETS_FORMAT(5067, new byte[] {'4', '2', '0', '0', '0'},
            "Invalid dynamic partition buckets %s"),
    ERROR_DYNAMIC_PARTITION_BUCKETS_EMPTY(5066, new byte[] {'4', '2', '0', '0', '0'},
            "Dynamic partition buckets is empty"),
    ERROR_DYNAMIC_PARTITION_ENABLE(5068, new byte[] {'4', '2', '0', '0', '0'},
            "Invalid dynamic partition enable: %s. Expected true or false"),
    ERROR_DYNAMIC_PARTITION_PREFIX(5069, new byte[] {'4', '2', '0', '0', '0'},
            "Invalid dynamic partition prefix: %s."),
    ERR_OPERATION_DISABLED(5070, new byte[] {'4', '2', '0', '0', '0'},
            "Operation %s is disabled. %s"),
    ERROR_DYNAMIC_PARTITION_REPLICATION_NUM_ZERO(5071, new byte[] {'4', '2', '0', '0', '0'},
            "Dynamic partition replication num must greater than 0"),
    ERROR_DYNAMIC_PARTITION_REPLICATION_NUM_FORMAT(5072, new byte[] {'4', '2', '0', '0', '0'},
            "Invalid dynamic partition replication num: %s."),
    ERROR_CREATE_TABLE_LIKE_EMPTY(5073, new byte[] {'4', '2', '0', '0', '0'},
            "Origin create table stmt is empty"),
    ERROR_REFRESH_EXTERNAL_TABLE_FAILED(5074, new byte[] {'4', '2', '0', '0', '0'},
            "refresh external table failed: %s"),
    ERROR_CREATE_TABLE_LIKE_UNSUPPORTED_VIEW(5075, new byte[] {'4', '2', '0', '0', '0'},
            "Create table like does not support create view."),
    ERR_QUERY_CANCELLED_BY_CRASH(5077, new byte[] {'X', 'X', '0', '0', '0'},
            "Query cancelled by crash of backends."),
    ERR_BAD_CATALOG_ERROR(5078, new byte[] {'4', '2', '0', '0', '0'},
            "Unknown catalog '%s'"),
    ERROR_NO_RG_ERROR(5079, new byte[] {'4', '2', '0', '0', '0'},
            "Unknown resource group '%s' "),
    ERR_BAD_CATALOG_AND_DB_ERROR(5080, new byte[] {'4', '2', '0', '0', '0'},
            "Unknown catalog.db '%s'"),
    ERR_UNSUPPORTED_SQL_PATTERN(5081, new byte[] {'4', '2', '0', '0', '0'},
            "Only support like 'function_pattern' syntax."),
    ERR_WRONG_LABEL_NAME(5082, new byte[] {'4', '2', '0', '0', '0'},
            "Incorrect label name '%s'"),
    ERR_PRIVILEGE_TABLE_NOT_FOUND(5085, new byte[] {'4', '2', '0', '0', '0'},
            "Table not found when checking privilege"),
    ERR_PRIVILEGE_DB_NOT_FOUND(5086, new byte[] {'4', '2', '0', '0', '0'},
            "Db [%s] not found when checking privilege"),
    ERR_PRIVILEGE_ROUTINELODE_JOB_NOT_FOUND(5089, new byte[] {'4', '2', '0', '0', '0'},
            "Routine load job [%s] not found when checking privilege"),
    ERR_PRIVILEGE_EXPORT_JOB_NOT_FOUND(5090, new byte[] {'4', '2', '0', '0', '0'},
            "Export job [%s] not found when checking privilege"),
    ERROR_DYNAMIC_PARTITION_HISTORY_PARTITION_NUM_ZERO(5092, new byte[] {'4', '2', '0', '0', '0'},
            "Dynamic history partition num must greater than 0"),

    /*
      !!!!!!!!!!!!!!!!!!!!!!     IMPORTANT   !!!!!!!!!!!!!!!!!!!!!!
      <p>
      The following ErrorCode has been reviewed.
      If you want to add an error code, please add it in the specific
      number segment according to the number segment of your own module.
     */

    /**
     * 5100 - 5199: Configuration
     */
    ERROR_SET_CONFIG_FAILED(5101, new byte[] {'F', '0', '0', '0', '0'}, "Set config failed: %s"),
    ERROR_CONFIG_NOT_EXIST(5102, new byte[] {'F', '0', '0', '0', '0'}, "Config '%s' does not exist or is not mutable"),

    /**
     * 5200 - 5299: Authentication and Authorization
     */
    ERR_PASSWD_LENGTH(5201, new byte[] {'H', 'Y', '0', '0', '0'},
            "Password hash should be a %d-digit hexadecimal number"),
    ERR_SQL_IN_BLACKLIST_ERROR(5202, new byte[] {'4', '2', '0', '0', '0'},
            "Access denied; This sql is in blacklist, please contact your admin"),
    ERR_ACCESS_DENIED(5203, new byte[] {'4', '2', '0', '0', '0'},
            "Access denied; you need (at least one of) the %s privilege(s) on %s%s for this operation. " +
                    ErrorCode.ERR_ACCESS_DENIED_HINT_MSG_FORMAT),
    ERR_ACCESS_DENIED_FOR_EXTERNAL_ACCESS_CONTROLLER(5204, new byte[] {'4', '2', '0', '0', '0'},
            "Access denied; you need (at least one of) the %s privilege(s) on %s%s for this operation."),
    ERR_SECURE_TRANSPORT_REQUIRED(5205, new byte[] {'H', 'Y', '0', '0', '0'},
            "Connections using insecure transport are prohibited"),
    ERR_GROUP_ACCESS_DENY(5206, new byte[] {'4', '2', '0', '0', '0'},
            "Group Access Denied"),

    /**
     * 5300 - 5399: Lock and Transaction
     */
    ERR_LOCK_ERROR(5300, new byte[] {'5', '5', 'P', '0', '3'}, "Failed to acquire lock: %s"),
    ERR_BEGIN_TXN_FAILED(5301, new byte[] {'5', '5', 'P', '0', '3'}, "Failed to begin transaction: %s"),
    ERR_TXN_NOT_EXIST(5302, new byte[] {'2', '5', 'P', '0', '1'}, "Transaction %s does not exist"),
    ERR_TXN_IMPORT_SAME_TABLE(5303, new byte[] {'2', '5', 'P', '0', '1'},
            "NOT allowed to read or write tables that have been subjected to DML operations before"),
    ERR_TXN_FORBID_CROSS_DB(5304, new byte[] {'2', '5', 'P', '0', '1'},
            "Cannot execute cross-database transactions. All DML target tables must belong to the same db"),

    /**
     * 5400 - 5499: Internal error
     */
    ERR_CHANGE_TO_SSL_CONNECTION_FAILED(5400, new byte[] {'X', 'X', '0', '0', '0'},
            "Failed to change to SSL connection"),
    ERR_FORWARD_TOO_MANY_TIMES(5401, new byte[] {'X', 'X', '0', '0', '0'}, "forward too many times %d"),

    /**
     * 5500 - 5599: DDL operation failure
     */
    ERR_LOC_AWARE_UNSUPPORTED_FOR_COLOCATE_TBL(5500, new byte[] {'4', '2', '0', '0', '0'},
            "table '%s' has location property and cannot be colocated"),
    ERR_BAD_DB_ERROR(5501, new byte[] {'3', 'F', '0', '0', '0'}, "Unknown database '%s'"),
    ERR_BAD_TABLE_ERROR(5502, new byte[] {'4', '2', '6', '0', '2'}, "Unknown table '%s'"),
    ERR_NOT_OLAP_TABLE(5503, new byte[] {'4', '2', '0', '0', '0'}, "Table '%s' is not a OLAP table"),
    ERR_MULTI_SUB_PARTITION(5504, new byte[] {'4', '2', '0', '0', '0'},
            "Partition '%s' has sub partitions, should specify the partition id"),
    ERR_NO_SUCH_PARTITION(5505, new byte[] {'4', '2', '0', '0', '0'}, "Partition '%s' doesn't exist"),
    ERR_NO_DEFAULT_STORAGE_VOLUME(5506, new byte[] {'5', '5', '0', '0', '0'},
            "The default storage volume does not exist. " +
                    "A default storage volume can be created by following these steps: " +
                    "1. Create a storage volume. 2. Set the storage volume as default"),
    ERR_GIN_REPLICATED_STORAGE_NOT_SUPPORTED(5507, new byte[] {'0', 'A', '0', '0', '0'},
            "Can not enable replicated storage when the table has GIN"),
    ERR_BATCH_DROP_PARTITION_UNSUPPORTED_FOR_NONRANGEPARTITIONINFO(5507, new byte[] {'4', '2', '0', '0', '0'},
            "Batch drop partition only support RangePartitionInfo"),
    ERR_BATCH_DROP_PARTITION_UNSUPPORTED_FOR_MULTIPARTITIONCOLUMNS(5508, new byte[] {'4', '2', '0', '0', '0'},
            "Batch deletion of partitions only support range partition tables with only a column, current column num is  [%s]"),
    ERR_BAD_FIELD_ERROR(5509, new byte[] {'4', '2', 'S', '2', '2'}, "Unknown column '%s' in '%s'"),
    ERR_TOO_MANY_BUCKETS(5510, new byte[] {'4', '2', '0', '0', '0'},
            "The number of buckets is too large, the maximum is %d. Please reduce the number of buckets " +
                    "or increase frontend config max_bucket_number_per_partition."),
    ERR_COLUMN_RENAME_ONLY_FOR_OLAP_TABLE(5511, new byte[] {'4', '2', '0', '0', '0'},
            "Column renaming is only supported for olap table"),
    ERR_CANNOT_RENAME_COLUMN_IN_INTERNAL_DB(5512, new byte[] {'4', '2', '0', '0', '0'},
            "Can not rename column in internal database: %s"),
    ERR_CANNOT_RENAME_COLUMN_OF_NOT_NORMAL_TABLE(5513, new byte[] {'4', '2', '0', '0', '0'},
            "Can not rename column of table in %s state"),
    ERR_CATALOG_EXISTED_ERROR(5514, new byte[] {'4', '2', '0', '0', '0'}, "Catalog '%s' already exists"),

    /**
     * 5600 - 5699: DML operation failure
     */
    ERR_NO_FILES_FOUND(5600, new byte[] {'5', '8', '0', '3', '0'},
            "No files were found matching the pattern(s) or path(s): '%s'. You should check whether there are " +
                    "files under the path, and make sure the process has the permission to access the path"),
    ERR_EXPR_REFERENCED_COLUMN_NOT_FOUND(5601, new byte[] {'4', '2', '0', '0', '0'},
            "Referenced column '%s' in expr '%s' can't be found in column list, derived column is '%s'"),
    ERR_MAPPING_EXPR_INVALID(5602, new byte[] {'4', '2', '0', '0', '0'},
            "Expr '%s' analyze error: %s, derived column is '%s'"),
    ERR_NO_PARTITIONS_HAVE_DATA_LOAD(5603, new byte[] {'0', '2', '0', '0', '0'},
            "No partitions have data available for loading. If you are sure there may be no data to be loaded, " +
                    "you can use `ADMIN SET FRONTEND CONFIG ('empty_load_as_error' = 'false')` " +
                    "to ensure such load jobs can succeed"),
    ERR_INSERT_COLUMN_COUNT_MISMATCH(5604, new byte[] {'4', '2', '6', '0', '1'},
            "Inserted target column count: %d doesn't match select/value column count: %d"),
    ERR_ILLEGAL_BYTES_LENGTH(5605, new byte[] {'4', '2', '0', '0', '0'}, "The valid bytes length for '%s' is [%d, %d]"),
    ERR_TOO_MANY_ERROR_ROWS(5606, new byte[] {'2', '2', '0', '0', '0'},
            "%s. Check the 'TrackingSQL' field for detailed information. If you are sure that the data has many errors, " +
                    "you can set '%s' property to a greater value through ALTER ROUTINE LOAD and RESUME the job"),
    ERR_ROUTINE_LOAD_OFFSET_INVALID(5607, new byte[] {'0', '2', '0', '0', '0'},
            "Consume offset: %d is greater than the latest offset: %d in kafka partition: %d. " +
                    "You can modify 'kafka_offsets' property through ALTER ROUTINE LOAD and RESUME the job"),
    ERR_INSERT_COLUMN_NAME_MISMATCH(5608, new byte[] {'4', '2', '6', '0', '1'},
            "%s column: %s has no matching %s column"),
    ERR_FAILED_WHEN_INSERT(5609, new byte[] {'2', '2', '0', '0', '0'}, "Failed when executing INSERT : '%s'"),
    ERR_LOAD_HAS_FILTERED_DATA(5610, new byte[] {'2', '2', '0', '0', '0'}, "Insert has filtered data : %s"),
    ERR_LOAD_DATA_PARSE_ERROR(5611, new byte[] {'2', '2', '0', '0', '0'},
            "%s. Check the 'TrackingSQL' field for detailed information."),

    /**
     * 5700 - 5799: Partition
     */
    ERR_ADD_PARTITION_WITH_ERROR_PARTITION_TYPE(5700, new byte[] {'4', '2', '0', '0', '0'},
            "Cannot add a Range partition to a table that partition type is not of Range"),

    ERR_ADD_PARTITION_WITH_ERROR_STEP_LENGTH(5701, new byte[] {'4', '2', '0', '0', '0'},
            "Step length [%d] in the operation is not equal to the partition step length [%d] stored in the table"),

    ERR_MULTI_PARTITION_COLUMN_NOT_SUPPORT_ADD_MULTI_RANGE(5702, new byte[] {'4', '2', '0', '0', '0'},
            "Can't add multi range partition to multi partition column table"),

    ERR_MULTI_PARTITION_STEP_LQ_ZERO(5703, new byte[] {'4', '2', '0', '0', '0'},
            "The interval of the Multi-Range Partition must be greater than 0"),

    /**
     * 5800 - 5899: Pipe
     */
    ERR_BAD_PIPE_STATEMENT(5800, new byte[] {'4', '2', '0', '0', '0'}, "Bad pipe statement: '%s'"),
    ERR_UNKNOWN_PIPE(5801, new byte[] {'4', '2', '0', '0', '0'}, "Unknown pipe '%s'"),
    ERR_PIPE_EXISTS(5802, new byte[] {'4', '2', '0', '0', '0'}, "Pipe exists"),
    ERR_UNKNOWN_PROPERTY(5803, new byte[] {'4', '2', '0', '0', '0'}, "Unknown property %s"),

    /**
     * 5900 - 5999: warehouse
     */
    ERR_UNKNOWN_WAREHOUSE(5901, new byte[] {'4', '2', '0', '0', '0'}, "Warehouse %s not exist."),
    ERR_WAREHOUSE_EXISTS(5902, new byte[] {'4', '2', '0', '0', '0'}, "Warehouse %s already exists."),
    ERR_WAREHOUSE_SUSPENDED(5903, new byte[] {'4', '2', '0', '0', '0'}, "Warehouse %s has been suspended."),
    ERR_WAREHOUSE_UNAVAILABLE(5904, new byte[] {'4', '2', '0', '0', '0'}, "Warehouse %s is not available."),
    ERR_NO_NODES_IN_WAREHOUSE(5905, new byte[] {'4', '2', '0', '0', '0'},
            "No alive backend or compute node in warehouse %s."),
    ERR_INVALID_WAREHOUSE_NAME(5906, new byte[] {'4', '2', '0', '0', '0'}, "Warehouse name can not be null or empty"),
    ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE(5907, new byte[] {'4', '2', '0', '0', '0'},
            "unsupported statement in shared_nothing mode"),

    /**
     * 6000 - 6100: Planner
     */
    ERR_PLAN_VALIDATE_ERROR(6000, new byte[] {'0', '7', '0', '0', '0'},
            "Incorrect logical plan found in operator: %s. Invalid reason: %s"),
    ERR_INVALID_DATE_ERROR(6001, new byte[] {'2', '2', '0', '0', '0'}, "Incorrect %s value %s"),

    ERR_INVALID_PARAMETER(6013, new byte[] {'4', '2', '0', '0', '0'}, "Invalid parameter %s"),

    ERR_MISSING_KEY_COLUMNS(6014, new byte[] {'4', '2', '0', '0', '0'},
            "missing key columns:%s for primary key table"),

    ERR_MISSING_DEPENDENCY_FOR_GENERATED_COLUMN(6015, new byte[] {'4', '2', '0', '0', '0'},
            "missing dependency column for generated column %s"),
    ;

    public static final String ERR_ACCESS_DENIED_HINT_MSG_FORMAT = "Please ask the admin to grant permission(s) or" +
            " try activating existing roles using <set [default] role>. Current role(s): %s. Inactivated role(s): %s.";

    ErrorCode(int code, byte[] sqlState, String errorMsg) {
        this.code = code;
        this.sqlState = sqlState;
        this.errorMsg = errorMsg;
    }

    // This is error code
    private final int code;
    // This sql state is compatible with ANSI SQL
    private final byte[] sqlState;
    // Error message format
    private final String errorMsg;

    public int getCode() {
        return code;
    }

    public byte[] getSqlState() {
        return sqlState;
    }

    public String formatErrorMsg(Object... args) {
        try {
            return String.format(errorMsg, args);
        } catch (MissingFormatArgumentException e) {
            return errorMsg;
        }
    }
}
