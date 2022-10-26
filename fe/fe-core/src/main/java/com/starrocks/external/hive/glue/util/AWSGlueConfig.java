// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.glue.util;

import com.amazonaws.ClientConfiguration;

public final class AWSGlueConfig {

    private AWSGlueConfig() {
    }

    public static final String AWS_GLUE_ENDPOINT = "aws.glue.endpoint";
    public static final String AWS_REGION = "aws.region";
    public static final String AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS
            = "aws.catalog.credentials.provider.factory.class";

    public static final String AWS_GLUE_MAX_RETRY = "aws.glue.max-error-retries";
    public static final int DEFAULT_MAX_RETRY = 5;

    public static final String AWS_GLUE_MAX_CONNECTIONS = "aws.glue.max-connections";
    public static final int DEFAULT_MAX_CONNECTIONS = ClientConfiguration.DEFAULT_MAX_CONNECTIONS;

    public static final String AWS_GLUE_CONNECTION_TIMEOUT = "aws.glue.connection-timeout";
    public static final int DEFAULT_CONNECTION_TIMEOUT = ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT;

    public static final String AWS_GLUE_SOCKET_TIMEOUT = "aws.glue.socket-timeout";
    public static final int DEFAULT_SOCKET_TIMEOUT = ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;

    public static final String AWS_GLUE_DB_CACHE_ENABLE = "aws.glue.cache.db.enable";
    public static final String AWS_GLUE_DB_CACHE_SIZE = "aws.glue.cache.db.size";
    public static final String AWS_GLUE_DB_CACHE_TTL_MINS = "aws.glue.cache.db.ttl-mins";

    public static final String AWS_GLUE_TABLE_CACHE_ENABLE = "aws.glue.cache.table.enable";
    public static final String AWS_GLUE_TABLE_CACHE_SIZE = "aws.glue.cache.table.size";
    public static final String AWS_GLUE_TABLE_CACHE_TTL_MINS = "aws.glue.cache.table.ttl-mins";
}
