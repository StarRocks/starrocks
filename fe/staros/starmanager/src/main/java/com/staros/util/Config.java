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


package com.staros.util;

public class Config extends ConfigBase {
    /**
     * star manager ip address
     */
    @ConfField
    public static String STARMGR_IP = "127.0.0.1";

    /**
     * star manager rpc port
     */
    @ConfField
    public static int STARMGR_RPC_PORT = 6090;

    /**
     * External Resource provisioner address, if empty, try to connect to builtin resource provisioner
     */
    @ConfField
    public static String RESOURCE_PROVISIONER_ADDRESS = "";

    /**
     * S3 object storage bucket
     */
    @ConfField
    public static String S3_BUCKET = "";
    /**
     * S3 object storage endpoint
     */
    @ConfField
    public static String S3_ENDPOINT = "";
    /**
     * S3 object storage region
     */
    @ConfField
    public static String S3_REGION = "";

    /**
     * S3 path in the bucket to be used as the root path.
     * Default: Empty, can use all paths under s3://<bucket>/
     */
    @ConfField
    public static String S3_PATH_PREFIX = "";

    /**
     * credential type for aws resource access
     * it can be one of 'default','simple','instance_profile' and 'assume_role'
     * nothing needed if 'default' or 'instance_profile' is set
     * ak/sk pair should be provided for 'simple'
     * iam_role_arn and external_id should be provided for 'assume_role'
     * default value is 'simple'
     */
    @ConfField
    public static String AWS_CREDENTIAL_TYPE = "simple";

    /* access key id for simple credential */
    @ConfField
    public static String SIMPLE_CREDENTIAL_ACCESS_KEY_ID = "";

    /* access key secret for simple credential */
    @ConfField
    public static String SIMPLE_CREDENTIAL_ACCESS_KEY_SECRET = "";

    /* iam role arn for assume role credential */
    @ConfField
    public static String ASSUME_ROLE_CREDENTIAL_ARN = "";

    /* external id for assume role credential */
    @ConfField
    public static String ASSUME_ROLE_CREDENTIAL_EXTERNAL_ID = "";

    @ConfField
    public static String HDFS_URL = "";

    @ConfField
    public static String AZURE_BLOB_ENDPOINT = "";

    @ConfField
    public static String AZURE_BLOB_PATH = "";

    @ConfField
    public static String AZURE_BLOB_SHARED_KEY = "";

    @ConfField
    public static String AZURE_BLOB_SAS_TOKEN = "";

    @ConfField
    public static String AZURE_BLOB_TENANT_ID = "";

    @ConfField
    public static String AZURE_BLOB_CLIENT_ID = "";

    @ConfField
    public static String AZURE_BLOB_CLIENT_SECRET = "";

    @ConfField
    public static String AZURE_BLOB_CLIENT_CERTIFICATE_PATH = "";

    @ConfField
    public static String AZURE_BLOB_AUTHORITY_HOST = "";

    @ConfField
    public static String AZURE_ADLS2_ENDPOINT = "";

    @ConfField
    public static String AZURE_ADLS2_PATH = "";

    @ConfField
    public static String AZURE_ADLS2_SHARED_KEY = "";

    @ConfField
    public static String AZURE_ADLS2_SAS_TOKEN = "";

    @ConfField
    public static String AZURE_ADLS2_TENANT_ID = "";

    @ConfField
    public static String AZURE_ADLS2_CLIENT_ID = "";

    @ConfField
    public static String AZURE_ADLS2_CLIENT_SECRET = "";

    @ConfField
    public static String AZURE_ADLS2_CLIENT_CERTIFICATE_PATH = "";

    @ConfField
    public static String AZURE_ADLS2_AUTHORITY_HOST = "";

    @ConfField
    public static String GCP_GS_ENDPOINT = "";

    @ConfField
    public static String GCP_GS_PATH = "";

    @ConfField
    public static boolean GCP_GS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT = true;

    @ConfField
    public static String GCP_GS_SERVICE_ACCOUNT_EMAIL = "";

    @ConfField
    public static String GCP_GS_SERVICE_ACCOUNT_PRIVATE_KEY_ID = "";

    @ConfField
    public static String GCP_GS_SERVICE_ACCOUNT_PRIVATE_KEY = "";

    @ConfField
    public static String GCP_GS_IMPERSONATION = "";

    /* default file store type used */
    @ConfField
    public static String DEFAULT_FS_TYPE = "S3";

    @ConfField
    public static String LOG_DIR = "./log";

    /**
     * log level, it can be "DEBUG", "INFO", "WARN" or "ERROR"
     */
    @ConfField
    public static String LOG_LEVEL = "INFO";

    /**
     * worker heartbeat interval in second
     */
    @ConfField
    public static int WORKER_HEARTBEAT_INTERVAL_SEC = 10;

    /**
     * worker heartbeat retry count before set it to DOWN state
     */
    @ConfField
    public static int WORKER_HEARTBEAT_RETRY_COUNT = 3;

    /**
     * shard checker loop interval in second
     */
    @ConfField
    public static int SHARD_CHECKER_LOOP_INTERVAL_SEC = 10;

    /**
     * Whether triggering a scheduling to default worker group when a new shard is created.
     */
    @ConfField
    public static boolean SCHEDULER_TRIGGER_SCHEDULE_WHEN_CREATE_SHARD = true;

    /**
     * Tolerance of max skewness of replicas in a shard group without triggering a balance.
     */
    @ConfField
    public static int SCHEDULER_BALANCE_MAX_SKEW = 1;

    /**
     * Whether enable the balance with shard nums factor in consideration
     */
    @ConfField
    public static boolean ENABLE_BALANCE_SHARD_NUM_BETWEEN_WORKERS = false;

    /**
     * The threshold to trigger the balance between workers in the same group
     * the number is calculated as `(max(shardNums) - min(shardNums)) / avg`
     */
    @ConfField
    public static double BALANCE_WORKER_SHARDS_THRESHOLD_IN_PERCENT = 0.15;
    /**
     * disable background shard schedule check
     */
    @ConfField
    public static boolean DISABLE_BACKGROUND_SHARD_SCHEDULE_CHECK = false;

    /**
     * Whether allows workers to use 0 as a valid group id and creates a worker group ahead to hold them.
     * Default is false. Need turn it ON explicitly in StarRocks FE.
     */
    @ConfField
    public static boolean ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY = false;

    /**
     * Whether enable builtin resource provisioner GRPC service for testing purpose
     */
    @ConfField
    public static boolean ENABLE_BUILTIN_RESOURCE_PROVISIONER_FOR_TEST = false;

    /**
     * Worker Group Spec definition file, refer to src/main/resources/default_worker_group_spec.json as example.
     */
    @ConfField
    public static String RESOURCE_MANAGER_WORKER_GROUP_SPEC_RESOURCE_FILE = "";

    /**
     * data directory for builtin provision server, if empty, builtin provision server will not persist its data.
     */
    @ConfField
    public static String BUILTIN_PROVISION_SERVER_DATA_DIR = "";

    /**
     * Max grpc message size allowed as inbound and outbound, default: 64MB, grpc default: 4MB
     * Estimated number of shards to be added in a batch: 262k
     * Estimated single shard info size: 256 bytes
     */
    @ConfField
    public static int GRPC_CHANNEL_MAX_MESSAGE_SIZE = 64 * 1024 * 1024;

    /**
     * Max batch size to call addShard in a single RPC, 8192 shards, (estimated payload size: 2MB)
     */
    @ConfField
    public static int SCHEDULER_MAX_BATCH_ADD_SHARD_SIZE = 8192;

    /**
     * Max batch size to call listShardGroup in a single RPC
     */
    @ConfField
    public static int LIST_SHARD_GROUP_BATCH_SIZE = 8192;

    /**
     * GRPC TIMEOUT in seconds. Default: 5 seconds. Affects following scenario
     * - StarMgr -> Worker RPC timeout
     */
    @ConfField
    public static int GRPC_RPC_TIME_OUT_SEC = 5;

    /**
     * GRPC TIMEOUT in seconds for heartbeat. Default: 2 seconds.
     * Affect: Starmgr -> worker heartbeat rpc timeout
     */
    @ConfField
    public static int WORKER_HEARTBEAT_GRPC_RPC_TIME_OUT_SEC = 2;

    /**
     * Default timeout in seconds when a shard replica stays in REPLICA_SCALE_OUT.
     * If the replica stays in the state longer than this period, the state will be
     * force change to REPLICA_OK state.
     */
    @ConfField
    public static int SHARD_REPLICA_SCALE_OUT_TIMEOUT_SECS = 900;

    /**
     * Whether enable replace file store
     */
    @ConfField
    public static boolean STARMGR_REPLACE_FILESTORE_ENABLED = false;

    /**
     * How long the dead replica will be taking as expired if the corresponding worker is down long enough.
     */
    @ConfField
    public static int SHARD_DEAD_REPLICA_EXPIRE_SECS = 300;
}
