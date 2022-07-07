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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ShardDelete extends MasterDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(ShardDelete.class);

    @SerializedName(value = "shardIds")
    private Set<Long> shardIds = Sets.newHashSet();

    public void addShardId(Set<Long> tableIds) {
        // for debug
        LOG.info("add shardId {} in ShardDelete", tableIds);
        shardIds.addAll(tableIds);
    }

    private void deleteShard() {
        // delete shard and drop lakeTablet
        if (shardIds.isEmpty()) {
            return;
        }

        // 1. drop tablet
        boolean finished = true;
        try {
            TNetworkAddress address = new TNetworkAddress();

            long backendId = GlobalStateMgr.getCurrentState().getStarOSAgent()
                    .getPrimaryBackendIdByShard(shardIds.stream().peek());
            Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
            address.setHostname(backend.getHost());
            address.setPort(backend.getBrpcPort());
            LakeService lakeService = BrpcProxy.getInstance().getLakeService(address);

            DropTabletRequest request = new DropTabletRequest();
            List<Long> tabletIds = new ArrayList<>(shardIds);
            request.tabletIds = tabletIds;

            Future<DropTabletResponse> responseFuture = lakeService.dropTablet(request);

            try {
                DropTabletResponse response = responseFuture.get();
                if (!response.failedTablets.isEmpty()) {
                    finished = false;
                }
            } catch (ExecutionException | InterruptedException e) {
                LOG.error(e);
                finished = false;
            }

        } catch (UserException e) {
            LOG.warn("failed to get primary backendId");
            finished = false;
        }

        // 2. delete shard
        try {
            GlobalStateMgr.getCurrentState().getStarOSAgent().deleteShards(shardIds);
        } catch (DdlException e) {
            LOG.warn("failed to delete shard from starMgr");
            return;
        }

        // 3.succ both, remove from the map
        if (finished == true) {
            // for debug
            LOG.info("delete shard {} and drop lake tablet succ.", shardIds);
            // TODO: log the batch remove op in Edit log
            GlobalStateMgr.getCurrentState().getEditLog().logRemoveDeleteShard(shardIds);
            shardIds.clear();
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
        LOG.info("shardIdToTablet size in replayDeleteShard is {}.", shardIds.size());
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