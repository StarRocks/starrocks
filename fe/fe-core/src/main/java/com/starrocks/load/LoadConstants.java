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

package com.starrocks.load;

public class LoadConstants {
    public static final String RUNTIME_DETAILS_LOAD_ID = "load_id";
    public static final String RUNTIME_DETAILS_TXN_ID = "txn_id";
    public static final String RUNTIME_DETAILS_CLIENT_IP = "client_ip";
    public static final String RUNTIME_DETAILS_ETL_INFO = "etl_info";
    public static final String RUNTIME_DETAILS_ETL_START_TIME = "etl_start_time";
    public static final String RUNTIME_DETAILS_ETL_FINISH_TIME = "etl_finish_time";
    public static final String RUNTIME_DETAILS_FILE_NUM = "file_num";
    public static final String RUNTIME_DETAILS_FILE_SIZE = "file_size";
    public static final String RUNTIME_DETAILS_TASK_NUM = "task_num";
    public static final String RUNTIME_DETAILS_UNFINISHED_BACKENDS = "unfinished_backends";
    public static final String RUNTIME_DETAILS_BACKENDS = "backends";
    public static final String RUNTIME_DETAILS_PLAN_TIME_MS = "plan_time_ms";
    public static final String RUNTIME_DETAILS_RECEIVE_DATA_TIME_MS = "receive_data_time_ms";
    public static final String RUNTIME_DETAILS_BEGIN_TXN_TIME_MS = "begin_txn_time_ms";

    public static final String PROPERTIES_TIMEOUT = "timeout";
    public static final String PROPERTIES_MAX_FILTER_RATIO = "max_filter_ratio";
    public static final String PROPERTIES_JOB_NAME = "job_name";
}
