// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.


package com.starrocks.catalog.lake;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.lake.proto.DropTabletRequest;
import com.starrocks.lake.proto.DropTabletResponse;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ShardDelete extends MasterDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(ShardDelete.class);

    @SerializedName(value = "shardIdToTablet")
    private Map<Long, LakeTablet> shardIdToTablet;

    public void addShardId(long shardId, LakeTablet tablet) {
        shardIdToTablet.put(shardId, tablet);
    }

    @Override
    protected void runAfterCatalogReady() {
        // delete shard and drop lakeTablet
        Iterator<Map.Entry<Long, LakeTablet>> iterator = shardIdToTablet.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, LakeTablet> entry = iterator.next();
            // 1. delete shard
            long shardId = entry.getKey();
            try {
                Set<Long> shardIds = new HashSet<>();
                shardIds.add(shardId);
                GlobalStateMgr.getCurrentState().getStarOSAgent().deleteShards(shardIds);
            } catch (DdlException e) {
                LOG.warn("failed to delete shard from starMgr");
                break;
            }

            // 2. drop tablet
            boolean finished = true;
            try {
                TNetworkAddress address = new TNetworkAddress();
                LakeTablet tablet = entry.getValue();
                long backendId = tablet.getPrimaryBackendId();
                Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
                address.setHostname(backend.getHost());
                address.setPort(backend.getBrpcPort());
                LakeService lakeService = BrpcProxy.getInstance().getLakeService(address);

                DropTabletRequest request = new DropTabletRequest();
                List<Long> tabletIds = Lists.newArrayList();;
                tabletIds.add(shardId);
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
            }

            // 3.succ both, remove from the map
            if (finished == true) {
                // for debug
                LOG.info("delete shard {} and drop lake tablet succ.", shardId);
                iterator.remove();
            }
        }
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