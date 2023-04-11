// This file is made available under Elastic License 2.0.
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
    // Database and table's default configurations, we will never change them
    public static short default_replication_num = 3;
    /*
     * Those two fields is responsible for determining the default key columns in duplicate table.
     * If user does not specify key of duplicate table in create table stmt,
     * the default key columns will be supplemented by StarRocks.
     * The default key columns are first 36 bytes(DEFAULT_DUP_KEYS_BYTES) of the columns in define order.
     * If the number of key columns in the first 36 is less than 3(DEFAULT_DUP_KEYS_COUNT),
     * the first 3 columns will be used.
     */
    public static int shortkey_max_column_count = 3;
    public static int shortkey_maxsize_bytes = 36;
    public static long default_db_data_quota_bytes = Long.MAX_VALUE;
    public static long default_db_replica_quota_size = Long.MAX_VALUE;

    public static int checkpoint_interval_second = 60; // 1 minutes

    public static int sync_task_runs_state_interval = 5000; // 5s

    // dpp version
    public static String dpp_version = "3_2_0";

    // bloom filter false positive probability
    public static double default_bloom_filter_fpp = 0.05;

    // set to true to skip some step when running FE unit test
    public static boolean runningUnitTest = false;
    // Set this flag to false to suppress showing local shuffle columns in verbose explain, when running FE unit tests.
    public static boolean showLocalShuffleColumnsInExplain = true;

    // set to true when replay from query dump
    public static boolean isReplayFromQueryDump = false;
    // default scheduler interval is 10 seconds
    public static int default_scheduler_interval_millisecond = 10000;

    // general model
    // Current meta data version. Use this version to write journals and image
    // for community meta version
    public static int meta_version = FeMetaVersion.VERSION_CURRENT;

    // Current starrocks meta data version. Use this version to write journals and image
    public static int starrocks_meta_version = StarRocksFEMetaVersion.VERSION_CURRENT;

    // use \N to indicate NULL
    public static String null_string = "\\N";

    public static boolean USE_MOCK_DICT_MANAGER = false;

    public static final int DEFAULT_TABLET_NUMBER = 128;

    public static final int AGG_FUNC_VERSION = 3;

    // Every 3GB, corresponds a new tablet. Assume compression ratio equals to 3,
    // the raw data of one tablet equals to 10GB approximately
    public static final long AUTO_DISTRIBUTION_UNIT = 3221225472L;
}
