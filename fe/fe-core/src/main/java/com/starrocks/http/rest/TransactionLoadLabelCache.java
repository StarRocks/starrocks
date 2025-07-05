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

package com.starrocks.http.rest;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransactionLoadLabelCache {
    private static final Logger LOG = LogManager.getLogger(TransactionLoadLabelCache.class);
    private final Cache<String, Long> cache;
    private final ScheduledExecutorService scheduler;

    public TransactionLoadLabelCache() {
        Caffeine<Object, Object> builder = Caffeine.newBuilder();
        cache = builder.initialCapacity(512).maximumWeight(getMaxCapacity())
                .weigher((key, value) -> 1)
                .expireAfterAccess((long) Config.prepared_transaction_default_timeout_second + 100, TimeUnit.SECONDS)
                .removalListener((key, value, cause) -> {
                    LOG.debug("Evicted transaction label node mapping: {}->{} ({})", key, value, cause);
                })
                .recordStats()
                .build();

        scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("LabelCacheRefresh-%d").build());
        scheduler.scheduleAtFixedRate(() -> {
            try {
                refreshCapacity();
                refreshExpireTime();
            } catch (Exception e) {
                LOG.error("Failed to refresh cache properties", e);
            }
        }, 0, 5, TimeUnit.MINUTES);
    }

    public long getMaxCapacity() {
        long current = (long) GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getTotalBackendNumber() +
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getTotalComputeNodeNumber();
        // only for TransactionLoadActionTest
        return Math.max(current * 512, 512);
    }

    public void refreshCapacity() {
        cache.policy().eviction().ifPresent(eviction -> {
            long capacity = getMaxCapacity();
            if (eviction.getMaximum() != capacity) {
                eviction.setMaximum(capacity);
            }
        });
    }

    public void refreshExpireTime() {
        cache.policy().expireAfterAccess().ifPresent(expireAfterAccess -> {
            expireAfterAccess.setExpiresAfter(
                    (long) Config.prepared_transaction_default_timeout_second + 100, TimeUnit.SECONDS);
        });
    }

    public void put(String key, Long value) {
        cache.put(key, value);
    }

    public Long get(String key) {
        return cache.getIfPresent(key);
    }

    @VisibleForTesting
    public void remove(String key) {
        cache.invalidate(key);
    }

    public Long getOrResolveCoordinator(String key, String dbName) throws StarRocksException {
        Long nodeId = get(key);
        if (nodeId == null) {
            String nodeIp = getTxnCoordinator(dbName, key);
            if (nodeIp != null) {
                if (RunMode.isSharedDataMode()) {
                    List<ComputeNode> computeNodes =
                            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getComputeNodeOnlyWithHost(nodeIp);
                    List<Backend> backends =
                            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOnlyWithHost(nodeIp);

                    List<Long> allNodeIds = Stream.concat(computeNodes.stream().map(ComputeNode::getId),
                            backends.stream().map(Backend::getId)).collect(Collectors.toList());

                    if (allNodeIds.size() == 1) {
                        nodeId = allNodeIds.get(0);
                    } else {
                        //TODO:Compatible with deploying multiple CNs/BEs on the same node
                        LOG.error("Failed to get compute node id for ip: {}, computeNodes: {}, backends: {}",
                                nodeIp, computeNodes, backends);
                    }
                } else {
                    List<Backend> backends = GlobalStateMgr.getCurrentState().
                            getNodeMgr().getClusterInfo().getBackendOnlyWithHost(nodeIp);
                    if (backends.size() == 1) {
                        nodeId = backends.get(0).getId();
                    } else {
                        //TODO:Compatible with deploying multiple BEs on the same node
                        LOG.error("Failed to get backend node id for ip: {}, backends: {}", nodeIp, backends);
                    }
                }
            }
        }

        return nodeId;
    }

    private String getTxnCoordinator(String dbName, String label) throws StarRocksException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new StarRocksException(String.format(
                    "The DB passed in by the Transaction with db[%s] and label[%s] has been deleted.", dbName, label));
        }

        TransactionState state = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().
                getLabelTransactionState(db.getId(), label);

        if (state != null) {
            return state.getCoordinator().getIp();
        }

        LOG.error("transaction state is null, database=" + dbName + ", label=" + label);
        return null;
    }

    public boolean exists(String key) {
        return cache.getIfPresent(key) != null;
    }

    public long size() {
        return cache.estimatedSize();
    }
}
