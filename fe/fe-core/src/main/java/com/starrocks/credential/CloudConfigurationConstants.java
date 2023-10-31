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

package com.starrocks.credential;

/**
 * Mapping used config key in StarRocks
 */
public class CloudConfigurationConstants {
    // Credential for AWS s3
    public static final String AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR = "aws.s3.use_aws_sdk_default_behavior";
    public static final String AWS_S3_USE_INSTANCE_PROFILE = "aws.s3.use_instance_profile";
    public static final String AWS_S3_ACCESS_KEY = "aws.s3.access_key";
    public static final String AWS_S3_SECRET_KEY = "aws.s3.secret_key";
    public static final String AWS_S3_SESSION_TOKEN = "aws.s3.session_token";
    public static final String AWS_S3_IAM_ROLE_ARN = "aws.s3.iam_role_arn";
    public static final String AWS_S3_EXTERNAL_ID = "aws.s3.external_id";
    public static final String AWS_S3_REGION = "aws.s3.region";
    public static final String AWS_S3_ENDPOINT = "aws.s3.endpoint";

    // Configuration for AWS s3

    /**
     * Enable S3 path style access ie disabling the default virtual hosting behaviour.
     * Useful for S3A-compliant storage providers as it removes the need to set up DNS for virtual hosting.
     * Default value: [false]
     */
    public static final String AWS_S3_ENABLE_PATH_STYLE_ACCESS = "aws.s3.enable_path_style_access";

    /**
     * Enables or disables SSL connections to AWS services.
     * You must set true if you want to assume role.
     * Default value: [true]
     */
    public static final String AWS_S3_ENABLE_SSL = "aws.s3.enable_ssl";

    public static final String AWS_GLUE_USE_AWS_SDK_DEFAULT_BEHAVIOR = "aws.glue.use_aws_sdk_default_behavior";
    public static final String AWS_GLUE_USE_INSTANCE_PROFILE = "aws.glue.use_instance_profile";
    public static final String AWS_GLUE_ACCESS_KEY = "aws.glue.access_key";
    public static final String AWS_GLUE_SECRET_KEY = "aws.glue.secret_key";
    public static final String AWS_GLUE_SESSION_TOKEN = "aws.glue.session_token";
    public static final String AWS_GLUE_IAM_ROLE_ARN = "aws.glue.iam_role_arn";
    public static final String AWS_GLUE_EXTERNAL_ID = "aws.glue.external_id";
    public static final String AWS_GLUE_REGION = "aws.glue.region";
    public static final String AWS_GLUE_ENDPOINT = "aws.glue.endpoint";

    // Credential for Azure storage
    // For Azure Blob Storage

    // Endpoint already contains storage account, so if user set endpoint, they don't need to set storage account anymore
    public static final String AZURE_BLOB_ENDPOINT = "azure.blob.endpoint";
    public static final String AZURE_BLOB_STORAGE_ACCOUNT = "azure.blob.storage_account";
    public static final String AZURE_BLOB_SHARED_KEY = "azure.blob.shared_key";
    public static final String AZURE_BLOB_CONTAINER = "azure.blob.container";
    public static final String AZURE_BLOB_SAS_TOKEN = "azure.blob.sas_token";
    // For Azure Data Lake Storage Gen1 (ADLS1)
    public static final String AZURE_ADLS1_USE_MANAGED_SERVICE_IDENTITY = "azure.adls1.use_managed_service_identity";
    public static final String AZURE_ADLS1_OAUTH2_CLIENT_ID = "azure.adls1.oauth2_client_id";
    public static final String AZURE_ADLS1_OAUTH2_CREDENTIAL = "azure.adls1.oauth2_credential";
    public static final String AZURE_ADLS1_OAUTH2_ENDPOINT = "azure.adls1.oauth2_endpoint";
    // For Azure Data Lake Storage Gen2 (ADLS2)
    public static final String AZURE_ADLS2_OAUTH2_USE_MANAGED_IDENTITY = "azure.adls2.oauth2_use_managed_identity";
    public static final String AZURE_ADLS2_OAUTH2_TENANT_ID = "azure.adls2.oauth2_tenant_id";
    public static final String AZURE_ADLS2_OAUTH2_CLIENT_ID = "azure.adls2.oauth2_client_id";
    public static final String AZURE_ADLS2_STORAGE_ACCOUNT = "azure.adls2.storage_account";
    public static final String AZURE_ADLS2_SHARED_KEY = "azure.adls2.shared_key";
    public static final String AZURE_ADLS2_OAUTH2_CLIENT_SECRET = "azure.adls2.oauth2_client_secret";
    public static final String AZURE_ADLS2_OAUTH2_CLIENT_ENDPOINT = "azure.adls2.oauth2_client_endpoint";

    // Credential for Google Cloud Platform (GCP)
    // For Google Cloud Storage (GCS)
    public static final String GCP_GCS_USE_COMPUTE_ENGINE_SERVICE_ACCOUNT =
            "gcp.gcs.use_compute_engine_service_account";
    public static final String GCP_GCS_SERVICE_ACCOUNT_EMAIL = "gcp.gcs.service_account_email";
    public static final String GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY = "gcp.gcs.service_account_private_key";
    public static final String GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID = "gcp.gcs.service_account_private_key_id";
    public static final String GCP_GCS_SERVICE_ACCOUNT_IMPERSONATION_SERVICE_ACCOUNT =
            "gcp.gcs.impersonation_service_account";

    // Credential for HDFS
    public static final String HDFS_AUTHENTICATION = "hadoop.security.authentication";
    @Deprecated
    public static final String HDFS_USERNAME_DEPRECATED = "username";
    public static final String HDFS_USERNAME = "hadoop.username";
    @Deprecated
    public static final String HDFS_PASSWORD_DEPRECATED = "password";
    public static final String HDFS_PASSWORD = "hadoop.password";
    public static final String HDFS_KERBEROS_PRINCIPAL_DEPRECATED = "kerberos_principal";
    public static final String HDFS_KERBEROS_PRINCIPAL = "hadoop.kerberos.principal";
    public static final String HDFS_KERBEROS_TICKET_CACHE_PATH = "hadoop.security.kerberos.ticket.cache.path";
    @Deprecated
    public static final String HDFS_KERBEROS_KEYTAB_DEPRECATED = "kerberos_keytab";
    public static final String HADOOP_KERBEROS_KEYTAB = "hadoop.kerberos.keytab";

    @Deprecated
    public static final String HDFS_KERBEROS_KEYTAB_CONTENT_DEPRECATED = "kerberos_keytab_content";
    public static final String HADOOP_KERBEROS_KEYTAB_CONTENT = "hadoop.kerberos.keytab_content";

    // Credential for Aliyun OSS
    public static final String ALIYUN_OSS_ACCESS_KEY = "aliyun.oss.access_key";
    public static final String ALIYUN_OSS_SECRET_KEY = "aliyun.oss.secret_key";
    public static final String ALIYUN_OSS_ENDPOINT = "aliyun.oss.endpoint";
}
