// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.


package com.starrocks.catalog.lake;

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.lake.proto.DropTabletRequest;
import com.starrocks.lake.proto.DropTabletResponse;
import com.starrocks.persist.ShardInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ShardDelete extends MasterDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(ShardDelete.class);

    @SerializedName(value = "shardIds")
    private Set<Long> shardIds = Sets.newHashSet();

    private Map<Long, Set<Long>> shardIdsByBeMap;

    public ShardDelete() {
        shardIdsByBeMap = new HashMap<>();
    }

    public void addShardId(Set<Long> tableIds) {
        // for debug
        LOG.info("add shardId {} in ShardDelete", tableIds);
        shardIds.addAll(tableIds);
    }

    private void deleteShard() {
        // delete shard and drop lakeTablet
        if (shardIds.isEmpty()) {
            // for debug
            LOG.info("shardIds in deleteShard() is empty.");
            return;
        }

        // group shards by be
        for (long shardId : shardIds) {
            try {
                long backendId = GlobalStateMgr.getCurrentState().getStarOSAgent()
                        .getPrimaryBackendIdByShard(shardId);
                shardIdsByBeMap.computeIfAbsent(backendId, k -> Sets.newHashSet()).add(shardId);
            } catch (UserException e) {
                continue;
            }
        }

        Iterator<Map.Entry<Long, Set<Long>>> it = shardIdsByBeMap.entrySet().iterator();
        while (it.hasNext()) {
            boolean finished = true;
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
                        // for debug
                        LOG.info("failedTablets is {}", response.failedTablets);
                        finished = false;
                    } else {
                        // for debug
                        LOG.info("drop tablet on BE succ.");
                    }
                } catch (ExecutionException | InterruptedException e) {
                    LOG.error(e);
                    finished = false;
                }
            }

            // 2. delete shard
            try {
                // get shard first
                List<Long> tabletIds = new ArrayList<>(shards);
                GlobalStateMgr.getCurrentState().getStarOSAgent().getShards(tabletIds);
                GlobalStateMgr.getCurrentState().getStarOSAgent().deleteShards(shards);
                // for debug
                LOG.info("delete shards {} succ.", shards);
            } catch (DdlException e) {
                LOG.warn("failed to delete shard from starMgr");
                continue;
            }

            // 3. succ both, remove from the map
            if (finished == true) {
                // for debug
                LOG.info("delete shard {} and drop lake tablet succ.", shards);
                GlobalStateMgr.getCurrentState().getEditLog().logRemoveDeleteShard(shards);
                it.remove();
                shardIds.removeAll(shards);
            }
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        // for debug
        LOG.info("runAfterCatalogReady of ShardDelete");
        deleteShard();
    }

    public void replayDeleteShard(ShardInfo shardInfo) {
        // for debug
        LOG.info("enter replayDeleteShard");
        this.shardIds = shardInfo.getShardIds();
        deleteShard();
    }

    public void replayAddShard(ShardInfo shardInfo) {
        // for debug
        LOG.info("enter replayAddShard");
        addShardId(shardInfo.getShardIds());
        LOG.info("shardIds size in replayDeleteShard is {}.", shardIds.size());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ShardDelete read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ShardDelete.class);
    }
}