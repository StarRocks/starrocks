// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.staros.util.LockCloseable;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.lake.proto.DeleteTabletRequest;
import com.starrocks.lake.proto.DeleteTabletResponse;
import com.starrocks.persist.ShardInfo;
import com.starrocks.rpc.LakeServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ShardDeleter extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(ShardDeleter.class);

    @SerializedName(value = "shardIds")
    private final Set<Long> shardIds;

    private final ReentrantReadWriteLock rwLock;

    public ShardDeleter() {
        shardIds = Sets.newHashSet();
        rwLock = new ReentrantReadWriteLock();
    }

    public void addUnusedShardId(Set<Long> tableIds) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            shardIds.addAll(tableIds);
        }
    }

    private void deleteUnusedShard() {
        // delete shard and drop lakeTablet
        if (shardIds.isEmpty()) {
            return;
        }

        Map<Long, Set<Long>> shardIdsByBeMap = new HashMap<>();
        // group shards by be
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            for (long shardId : shardIds) {
                try {
                    long backendId = GlobalStateMgr.getCurrentState().getStarOSAgent().getPrimaryBackendIdByShard(shardId);
                    shardIdsByBeMap.computeIfAbsent(backendId, k -> Sets.newHashSet()).add(shardId);
                } catch (UserException ignored1) {
                    // ignore error
                }
            }
        }

        Set<Long> deletedShards = Sets.newHashSet();

        for (Map.Entry<Long, Set<Long>> entry : shardIdsByBeMap.entrySet()) {
            long backendId = entry.getKey();
            Set<Long> shards = entry.getValue();

            // 1. drop tablet
            TNetworkAddress address = new TNetworkAddress();
            Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
            address.setHostname(backend.getHost());
            address.setPort(backend.getBrpcPort());

            LakeServiceClient client = new LakeServiceClient(address);
            DeleteTabletRequest request = new DeleteTabletRequest();
            request.tabletIds = Lists.newArrayList(shards);

            try {
                Future<DeleteTabletResponse> responseFuture = client.deleteTablet(request);
                DeleteTabletResponse response = responseFuture.get();
                if (response != null && response.failedTablets != null && !response.failedTablets.isEmpty()) {
                    LOG.info("failedTablets is {}", response.failedTablets);
                    response.failedTablets.forEach(shards::remove);
                }
            } catch (Exception e) {
                LOG.error(e);
                continue;
            }

            // 2. delete shard
            try {
                GlobalStateMgr.getCurrentState().getStarOSAgent().deleteShards(shards);
            } catch (DdlException e) {
                LOG.warn("failed to delete shard from starMgr");
                continue;
            }

            deletedShards.addAll(shards);
        }

        // 3. succ both, remove from the map
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            shardIds.removeAll(deletedShards);
        }
        GlobalStateMgr.getCurrentState().getEditLog().logDeleteUnusedShard(deletedShards);
    }

    @Override
    protected void runAfterCatalogReady() {
        deleteUnusedShard();
    }

    public void replayDeleteUnusedShard(ShardInfo shardInfo) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            this.shardIds.removeAll(shardInfo.getShardIds());
        }
    }

    public void replayAddUnusedShard(ShardInfo shardInfo) {
        addUnusedShardId(shardInfo.getShardIds());
    }

}