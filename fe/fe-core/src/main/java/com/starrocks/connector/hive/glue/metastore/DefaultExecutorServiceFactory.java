// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.glue.metastore;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultExecutorServiceFactory implements ExecutorServiceFactory {
    private static final int NUM_EXECUTOR_THREADS = 5;

    private static final ExecutorService GLUE_METASTORE_DELEGATE_THREAD_POOL = Executors.newFixedThreadPool(
            NUM_EXECUTOR_THREADS, new ThreadFactoryBuilder()
                    .setNameFormat(GlueMetastoreClientDelegate.GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT)
                    .setDaemon(true).build()
    );

    @Override
    public ExecutorService getExecutorService(HiveConf conf) {
        return GLUE_METASTORE_DELEGATE_THREAD_POOL;
    }
}
