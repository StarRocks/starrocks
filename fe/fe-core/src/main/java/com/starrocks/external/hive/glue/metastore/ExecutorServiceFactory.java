// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.glue.metastore;

import org.apache.hadoop.hive.conf.HiveConf;

import java.util.concurrent.ExecutorService;

/*
 * Interface for creating an ExecutorService
 */
public interface ExecutorServiceFactory {
    public ExecutorService getExecutorService(HiveConf conf);
}
