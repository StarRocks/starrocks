// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.iceberg;

import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.ThreadPools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class IcebergRepository {
    private static final Logger LOG = LogManager.getLogger(IcebergRepository.class);

    private final ExecutorService icebergRefreshExecutor =
            ThreadPoolManager.newDaemonFixedThreadPool(Config.iceberg_table_refresh_threads,
                    Integer.MAX_VALUE, "iceberg-refresh-pool", true);

    private final Map<Table, Future> icebergRefreshMap = new ConcurrentHashMap<>();

    public void refreshTable(Table table) {
        icebergRefreshMap.put(table, icebergRefreshExecutor.submit(() -> table.refresh()));
    }

    public Future getTable(Table table) {
        return icebergRefreshMap.remove(table);
    }

    public IcebergRepository() {
        if (Config.enable_iceberg_custom_worker_thread) {
            LOG.info("Default iceberg worker thread number changed " + Config.iceberg_worker_num_threads);
            Properties props = System.getProperties();
            props.setProperty(ThreadPools.WORKER_THREAD_POOL_SIZE_PROP, String.valueOf(Config.iceberg_worker_num_threads));
        }
    }
}
