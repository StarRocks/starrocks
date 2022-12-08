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


package com.starrocks.connector.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import org.apache.iceberg.Table;
import org.apache.iceberg.util.ThreadPools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class IcebergRepository {
    private static final Logger LOG = LogManager.getLogger(IcebergRepository.class);

    private final ExecutorService icebergRefreshExecutor =
            ThreadPoolManager.newDaemonFixedThreadPool(Config.iceberg_table_refresh_threads,
                    Integer.MAX_VALUE, "iceberg-refresh-pool", true);

    private final Cache<Table, Future<?>> icebergRefreshCache = CacheBuilder.newBuilder()
            .expireAfterWrite(Config.iceberg_table_refresh_expire_sec, TimeUnit.SECONDS).build();

    public void refreshTable(Table table) {
        icebergRefreshCache.put(table, icebergRefreshExecutor.submit(table::refresh));
    }

    public Future<?> getTable(Table table) {
        Future<?> res = icebergRefreshCache.getIfPresent(table);
        Preconditions.checkNotNull(res, "Table must exist in refresh cache " + table.name());
        return res;
    }

    public IcebergRepository() {
        if (Config.enable_iceberg_custom_worker_thread) {
            LOG.info("Default iceberg worker thread number changed " + Config.iceberg_worker_num_threads);
            Properties props = System.getProperties();
            props.setProperty(ThreadPools.WORKER_THREAD_POOL_SIZE_PROP, String.valueOf(Config.iceberg_worker_num_threads));
        }
    }
}
