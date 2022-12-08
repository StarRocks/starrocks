// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.credential;

/**
 * Mapping used config key in StarRocks
 */
public class CredentialConstants {

    public static final String AWS_S3_USE_INSTANCE_PROFILE = "aws.s3.use_instance_profile";
    public static final String AWS_S3_ACCESS_KEY = "aws.s3.access_key";
    public static final String AWS_S3_SECRET_KEY = "aws.s3.secret_key";
    public static final String AWS_S3_IAM_ROLE_ARN = "aws.s3.iam_role_arn";
    public static final String AWS_S3_EXTERNAL_ID = "aws.s3.external_id";
    public static final String AWS_S3_REGION = "aws.s3.region";
    public static final String AWS_S3_ENDPOINT = "aws.s3.endpoint";

    public static final String AWS_GLUE_USE_INSTANCE_PROFILE = "aws.glue.use_instance_profile";
    public static final String AWS_GLUE_ACCESS_KEY = "aws.glue.access_key";
    public static final String AWS_GLUE_SECRET_KEY = "aws.glue.secret_key";
    public static final String AWS_GLUE_IAM_ROLE_ARN = "aws.glue.iam_role_arn";
    public static final String AWS_GLUE_EXTERNAL_ID = "aws.glue.external_id";
    public static final String AWS_GLUE_REGION = "aws.glue.region";
    public static final String AWS_GLUE_ENDPOINT = "aws.glue.endpoint";
}
