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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/FeConstants.java

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

public class FeConstants {
    public static final long DEFAULT_DB_DATA_QUOTA_BYTES = Long.MAX_VALUE;
    public static final long DEFAULT_DB_REPLICA_QUOTA_SIZE = Long.MAX_VALUE;
    public static final int SYNC_TASK_RUNS_STATE_INTERVAL = 5000; // 5s
    // dpp version
    public static final String DPP_VERSION = "3_2_0";
    // bloom filter false positive probability
    public static final double DEFAULT_BLOOM_FILTER_FPP = 0.05;
    // general model
    @Deprecated
    // for rollback compatible
    public static final int META_VERSION = 92;
    // Current starrocks metadata version. Use this version to write journals and image
    public static final int STARROCKS_META_VERSION = StarRocksFEMetaVersion.VERSION_CURRENT;
    // use \N to indicate NULL
    public static final String NULL_STRING = "\\N";
    public static final int AGG_FUNC_VERSION = 3;
    /*
     * Those two fields is responsible for determining the default key columns in duplicate table.
     * If user does not specify key of duplicate table in create table stmt,
     * the default key columns will be supplemented by StarRocks.
     * The default key columns are first 36 bytes(DEFAULT_DUP_KEYS_BYTES) of the columns in define order.
     * If the number of key columns in the first 36 is less than 3(DEFAULT_DUP_KEYS_COUNT),
     * the first 3 columns will be used.
     */
    public static final int SHORTKEY_MAX_COLUMN_COUNT = 3;
    public static final int SHORTKEY_MAXSIZE_BYTES = 36;
    public static final String BACKEND_NODE_NOT_FOUND_ERROR =
            "Backend node not found. Check if any backend node is down.";
    public static final String COMPUTE_NODE_NOT_FOUND_ERROR =
            "Compute node not found. Check if any compute node is down.";

    public static boolean USE_MOCK_DICT_MANAGER = false;

    public static int checkpoint_interval_second = 60; // 1 minutes
    // set this flag true to skip some step when running FE unit test
    public static boolean runningUnitTest = false;
    // Set this flag false to suppress showing local shuffle columns in verbose explain, when running FE unit tests.
    public static boolean showScanNodeLocalShuffleColumnsInExplain = true;
    // set to true when replay from query dump
    public static boolean isReplayFromQueryDump = false;
    // set false to resolve ut
    public static boolean enablePruneEmptyOutputScan = true;
    public static boolean showJoinLocalShuffleInExplain = true;

    // Every 3GB, corresponds a new tablet. Assume compression ratio equals to 3,
    // the raw data of one tablet equals to 10GB approximately
    public static final long AUTO_DISTRIBUTION_UNIT = 3221225472L;

    public static final String DOCUMENT_SHOW_ALTER =
            "https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/data-manipulation/SHOW%20ALTER";
    public static final String DOCUMENT_SHOW_ALTER_MATERIALIZED_VIEW =
            "https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/data-manipulation" +
                    "/SHOW%20ALTER%20MATERIALIZED%20VIEW";
    public static final String DOCUMENT_SHOW_BACKUP =
            "https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/data-manipulation/SHOW%20BACKUP";
    public static final String DOCUMENT_SHOW_RESTORE =
            "https://docs.starrocks.io/en-us/latest/sql-reference/sql-statements/data-manipulation/SHOW%20RESTORE";

    public static String getNodeNotFoundError(boolean chooseComputeNode) {
        return chooseComputeNode ? COMPUTE_NODE_NOT_FOUND_ERROR : BACKEND_NODE_NOT_FOUND_ERROR;
    }
}
