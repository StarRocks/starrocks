// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.


package com.starrocks.catalog.lake;

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.staros.util.LockCloseable;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.lake.proto.DropTabletRequest;
import com.starrocks.lake.proto.DropTabletResponse;
import com.starrocks.persist.ShardInfo;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ShardDeleter extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(ShardDeleter.class);

    @SerializedName(value = "shardIds")
    private Set<Long> shardIds;

    private ReentrantReadWriteLock rwLock;

    public ShardDeleter() {
        shardIds = Sets.newHashSet();
        rwLock = new ReentrantReadWriteLock();
    }

    public void addUnusedShardId(Set<Long> tableIds) {
        // for debug
        LOG.info("add shardId {} in ShardDelete", tableIds);
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            shardIds.addAll(tableIds);
        }
    }

    private void deleteUnusedShard() {
        // delete shard and drop lakeTablet
        if (shardIds.isEmpty()) {
            // for debug
            LOG.info("shardIds in deleteShard() is empty.");
            return;
        }

        Map<Long, Set<Long>> shardIdsByBeMap = new HashMap<>();
        // group shards by be
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            for (long shardId : shardIds) {
                try {
                    long backendId = GlobalStateMgr.getCurrentState().getStarOSAgent()
                            .getPrimaryBackendIdByShard(shardId);
                    shardIdsByBeMap.computeIfAbsent(backendId, k -> Sets.newHashSet()).add(shardId);
                } catch (UserException e) {
                    continue;
                }
            }
        }

        Iterator<Map.Entry<Long, Set<Long>>> it = shardIdsByBeMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Set<Long>> entry = it.next();
            long backendId = entry.getKey();
            Set<Long> shards = entry.getValue();

            // 1. drop tablet
            {
                TNetworkAddress address = new TNetworkAddress();
                Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
                address.setHostname(backend.getHost());
                address.setPort(backend.getBrpcPort());
                LakeService lakeService = BrpcProxy.getInstance().getLakeService(address);

                DropTabletRequest request = new DropTabletRequest();
                List<Long> tabletIds = new ArrayList<>(shards);
                request.tabletIds = tabletIds;

                Future<DropTabletResponse> responseFuture = lakeService.dropTablet(request);
                try {
                    DropTabletResponse response = responseFuture.get();
                    if (response != null && response.failedTablets != null && !response.failedTablets.isEmpty()) {
                        LOG.info("failedTablets is {}", response.failedTablets);
                        shards.removeAll(response.failedTablets);
                    } 

                } catch (Exception e) {
                    LOG.error(e);
                    continue;
                }
            }

            // 2. delete shard
            try {
                GlobalStateMgr.getCurrentState().getStarOSAgent().deleteShards(shards);
                // for debug
                LOG.info("delete shards {} succ.", shards);
            } catch (DdlException e) {
                LOG.warn("failed to delete shard from starMgr");
                continue;
            }

            // 3. succ both, remove from the map
            // for debug
            LOG.info("delete shard {} and drop lake tablet succ.", shards);
            GlobalStateMgr.getCurrentState().getEditLog().logDeleteUnusedShard(shards);
            try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
                shardIds.removeAll(shards);
            }
            
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        // for debug
        LOG.info("runAfterCatalogReady of ShardDelete");
        deleteUnusedShard();
    }

    public void replayDeleteUnusedShard(ShardInfo shardInfo) {
        // for debug
        LOG.info("enter replayDeleteShard");
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            this.shardIds.removeAll(shardInfo.getShardIds());
        }
    }

    public void replayAddUnusedShard(ShardInfo shardInfo) {
        // for debug
        LOG.info("enter replayAddShard");
        addUnusedShardId(shardInfo.getShardIds());
        LOG.info("shardIds size in replayDeleteShard is {}.", shardIds.size());
    }

}