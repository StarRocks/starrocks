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
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.transaction.TransactionState;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Manages the allocation and retrieval of coordinator nodes for transaction stream load.
 * <p>
 * This class maintains a cache that maps transaction labels to the node IDs of their coordinators,
 * allowing for efficient lookup and management of coordinators during the lifecycle of a transaction
 * stream load. By leveraging this cache, the system can avoid querying the transaction state for every
 * request, which reduces the lock contention on the transaction manager.
 */
public class TransactionLoadCoordinatorMgr {

    private static final Logger LOG = LogManager.getLogger(TransactionLoadCoordinatorMgr.class);

    /**
     * A cache that stores the mapping from transaction label to coordinator node ID.
     * The cache's capacity and expiration time are dynamically adjustable based on 
     * configuration changes.
     */
    private final Cache<String, Long> cache;

    public TransactionLoadCoordinatorMgr() {
        this.cache = Caffeine.newBuilder()
                .maximumSize(Config.transaction_stream_load_coordinator_cache_capacity)
                .expireAfterAccess(Config.transaction_stream_load_coordinator_cache_expire_seconds, TimeUnit.SECONDS)
                .evictionListener((key, value, cause) ->
                        LOG.debug("Evict load, label {}, nodeId: {}, cause: {}", key, value, cause))
                .recordStats()
                .build();
        GlobalStateMgr.getCurrentState().getConfigRefreshDaemon().registerListener(this::updateCacheCapacity);
        GlobalStateMgr.getCurrentState().getConfigRefreshDaemon().registerListener(this::updateCacheExpireTime);
    }

    /**
     * Updates the cache's maximum capacity if the configuration value has changed.
     * This method is called when the related configuration is refreshed.
     */
    private void updateCacheCapacity() {
        cache.policy().eviction().ifPresent(eviction -> {
            long oldCapacity = eviction.getMaximum();
            long newCapacity = Config.transaction_stream_load_coordinator_cache_capacity;
            if (oldCapacity != newCapacity) {
                eviction.setMaximum(newCapacity);
                LOG.info("Update cache capacity, old: {}, new: {}", oldCapacity, newCapacity);
            }
        });
    }

    /**
     * The cumulative number of times requested items were found in the cache.
     */
    public long getCacheHitCount() {
        return cache.stats().hitCount();
    }

    /**
     * The cumulative number of times requested items were not found in the cache.
     */
    public long getCacheMissCount() {
        return cache.stats().missCount();
    }

    /**
     * The cumulative number of entries evicted from the cache.
     */
    public long getCacheEvictionCount() {
        return cache.stats().evictionCount();
    }

    /**
     * Updates the cache's expiration time if the configuration value has changed.
     * This method is called when the related configuration is refreshed.
     */
    private void updateCacheExpireTime() {
        cache.policy().expireAfterAccess().ifPresent(expire -> {
            long oldExpire = expire.getExpiresAfter(TimeUnit.SECONDS);
            long newExpire = Config.transaction_stream_load_coordinator_cache_expire_seconds;
            if (oldExpire != newExpire) {
                expire.setExpiresAfter(newExpire, TimeUnit.SECONDS);
                LOG.info("Update cache expire time, old: {}, new: {}", oldExpire, newExpire);
            }
        });
    }

    /**
     * Allocates a coordinator node for a given transaction label and warehouse name.
     * The selected node is cached for future retrievals.
     * If the label already exists in cache, returns the cached node to ensure
     * subsequent operations (like rollback) are routed to the correct BE.
     *
     * @param label         the transaction label
     * @param warehouseName the name of the warehouse
     * @return the allocated {@link ComputeNode}
     * @throws StarRocksException if allocation fails or no suitable node is found
     */
    public @NonNull ComputeNode allocate(String label, String warehouseName) throws StarRocksException {
        // Check if the label already exists in cache to avoid overwriting
        // This ensures that repeated BEGIN requests for the same label
        // are routed to the same BE where the transaction context exists
        Long existingNodeId = cache.getIfPresent(label);
        if (existingNodeId != null) {
            return getNodeFromId(existingNodeId);
        }

        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        final CRAcquireContext acquireContext = CRAcquireContext.of(warehouseName);
        final ComputeResource computeResource = warehouseManager.acquireComputeResource(acquireContext);
        List<Long> nodeIds = LoadAction.selectNodes(computeResource);
        Long chosenNodeId = nodeIds.get(0);
        ComputeNode node = getNodeFromId(chosenNodeId);
        cache.put(label, chosenNodeId);
        return node;
    }

    /**
     * Retrieves the coordinator node for a given transaction label and database name.
     * If the label is not present in the cache, attempts to retrieve the node from the transaction state.
     *
     * @param label  the transaction label
     * @param dbName the database name
     * @return the corresponding {@link ComputeNode}
     * @throws StarRocksException if the node cannot be found
     */
    public @NonNull ComputeNode get(String label, String dbName) throws StarRocksException {
        Long nodeId = cache.getIfPresent(label);
        return nodeId != null ? getNodeFromId(nodeId) : getNodeFromTransactionState(label, dbName);
    }

    /**
     * Remove key form cache. It is only used in test cases
     *
     * @param label  the transaction label
     */
    @VisibleForTesting
    public void remove(String label) {
        cache.invalidate(label);
    }

    /**
     * Calculate the length of the cache. It is only used in test cases
     */
    @VisibleForTesting
    public long size() {
        return cache.estimatedSize();
    }

    /**
     * It is only used in test cases
     */
    @VisibleForTesting
    public void put(String key, Long value) {
        cache.put(key, value);
    }

    /**
     * Checks if a transaction label exists in the cache.
     *
     * @param label the transaction label
     * @return true if the label exists in the cache, false otherwise
     */
    public boolean existInCache(String label) {
        return cache.getIfPresent(label) != null;
    }

    /**
     * Retrieves a {@link ComputeNode} by its node ID.
     *
     * @param nodeId the ID of the node
     * @return the corresponding {@link ComputeNode}
     * @throws StarRocksException if the node cannot be found
     */
    @VisibleForTesting
    public @NonNull ComputeNode getNodeFromId(Long nodeId) throws StarRocksException {
        ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr()
                .getClusterInfo().getBackendOrComputeNode(nodeId);
        if (node == null) {
            throw new StarRocksException(String.format("Can't find node id %s. You can check the cluster " +
                    "to see whether the node exists ", nodeId));
        }
        return node;
    }

    /**
     * Retrieves a {@link ComputeNode} from the transaction state using the label and database name.
     *
     * @param label  the transaction label
     * @param dbName the database name
     * @return the corresponding {@link ComputeNode}
     * @throws StarRocksException if the transaction or node cannot be found
     */
    private @NonNull ComputeNode getNodeFromTransactionState(String label, String dbName) throws StarRocksException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new StarRocksException(String.format("Can't find db[%s] for label[%s]. The db may be dropped.", dbName, label));
        }

        TransactionState state = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().
                getLabelTransactionState(db.getId(), label);
        if (state == null) {
            throw new StarRocksException(String.format("Can't find the transaction with db [%s] and label[%s]." +
                    " The possible reasons: 1. FE leader is started or switched; " +
                    "2. The transaction timeouts and is cleaned.", dbName, label));
        }

        TransactionState.TxnCoordinator txnCoordinator = state.getCoordinator();
        if (!TransactionState.TxnSourceType.BE.equals(txnCoordinator.sourceType)) {
            throw new StarRocksException(String.format("The source type for transaction stream load " +
                            "with db [%s] and label[%s] should be %s, but is %s.",
                    dbName, label, TransactionState.TxnSourceType.BE, txnCoordinator.sourceType));
        }
        return getNodeFromId(txnCoordinator.getBackendId());
    }
}