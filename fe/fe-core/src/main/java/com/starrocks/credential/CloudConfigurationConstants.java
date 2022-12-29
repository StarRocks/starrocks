// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    public static final String AWS_GLUE_IAM_ROLE_ARN = "aws.glue.iam_role_arn";
    public static final String AWS_GLUE_EXTERNAL_ID = "aws.glue.external_id";
    public static final String AWS_GLUE_REGION = "aws.glue.region";
    public static final String AWS_GLUE_ENDPOINT = "aws.glue.endpoint";
}
