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

package com.starrocks.connector.hive.glue.util;

public final class AWSGlueConfig {

    private AWSGlueConfig() {
    }

    public static final String AWS_GLUE_DB_CACHE_ENABLE = "aws.hive.metastore.glue.cache.db.enable";
    public static final String AWS_GLUE_DB_CACHE_SIZE = "aws.hive.metastore.glue.cache.db.size";
    public static final String AWS_GLUE_DB_CACHE_TTL_MINS = "aws.hive.metastore.glue.cache.db.ttl-mins";

    public static final String AWS_GLUE_TABLE_CACHE_ENABLE = "aws.hive.metastore.glue.cache.table.enable";
    public static final String AWS_GLUE_TABLE_CACHE_SIZE = "aws.hive.metastore.glue.cache.table.size";
    public static final String AWS_GLUE_TABLE_CACHE_TTL_MINS = "aws.hive.metastore.glue.cache.table.ttl-mins";

    public static final String NUM_PARTITION_SEGMENTS_CONF = "aws.hive.metastore.glue.partition.num.segments";
    public static final String CUSTOM_EXECUTOR_FACTORY_CONF = "aws.hive.metastore.glue.executorservice.factory.class";
}
