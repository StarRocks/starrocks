package com.starrocks.catalog.lake;

import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.common.util.MasterDaemon;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.DropReplicaTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
            try {
                LakeTablet tablet = entry.getValue();
                long backendId = ((LakeTablet) tablet).getPrimaryBackendId();
                DropReplicaTask dropTask = new DropReplicaTask(backendId, shardId, -1, true);
                AgentBatchTask batchTask = new AgentBatchTask();
                batchTask.addTask(dropTask);
            } catch (UserException e) {
                LOG.warn("failed to get primary backendId");
                break;
            }

            // 3.succ both, remove from the map
            iterator.remove();
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
