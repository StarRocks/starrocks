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

package com.starrocks.connector.starrocks;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.config.ConnectorConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

public class StarRocksConnector implements Connector {
    private final String catalogName;
    private final Function<StarRocksConnectorConfig, StarRocksFeClient> feClientFactory;
    private StarRocksConnectorConfig config;
    private StarRocksFeClient feClient;
    private StarRocksMetadataCache metadataCache;
    private ExecutorService cacheRefreshExecutor;

    public StarRocksConnector(ConnectorContext context) {
        this(context, StarRocksFeClient::new);
    }

    StarRocksConnector(ConnectorContext context, Function<StarRocksConnectorConfig, StarRocksFeClient> feClientFactory) {
        this.catalogName = context.getCatalogName();
        this.feClientFactory = feClientFactory;
    }

    @Override
    public void bindConfig(ConnectorConfig config) {
        this.config = (StarRocksConnectorConfig) config;
        this.feClient = feClientFactory.apply(this.config);
        this.feClient.getCapabilities();
        // Build the cache eagerly: bindConfig runs on the single-threaded DDL/replay
        // path, so the per-query getMetadata() calls only ever read these fields —
        // no lazy-init race. With caching disabled no cache object exists at all and
        // StarRocksMetadata talks straight to the FE client.
        shutdownRefreshExecutor();
        if (this.config.isCacheEnabled()) {
            this.cacheRefreshExecutor = Executors.newFixedThreadPool(this.config.getCacheRefreshThreadNum(),
                    new ThreadFactoryBuilder().setDaemon(true)
                            .setNameFormat("starrocks-catalog-cache-refresh-" + catalogName + "-%d").build());
            this.metadataCache = new StarRocksMetadataCache(feClient, this.config.toCacheOptions(),
                    null, null, cacheRefreshExecutor);
        } else {
            this.metadataCache = null;
        }
    }

    @Override
    public void shutdown() {
        shutdownRefreshExecutor();
    }

    private void shutdownRefreshExecutor() {
        if (cacheRefreshExecutor != null) {
            cacheRefreshExecutor.shutdownNow();
            cacheRefreshExecutor = null;
        }
    }

    /**
     * Returns a fresh StarRocksMetadata per call: MetadataMgr registers one
     * instance per (query, catalog), so per-query state (the pinned statistics
     * snapshots) lives on the metadata instance while the caches stay shared
     * on the connector.
     */
    @Override
    public ConnectorMetadata getMetadata() {
        return new StarRocksMetadata(catalogName, feClient, metadataCache);
    }
}
