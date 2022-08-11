// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.lake;

import com.baidu.brpc.client.RpcCallback;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.DdlException;
import com.starrocks.lake.proto.DropTableRequest;
import com.starrocks.lake.proto.DropTableResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeServiceAsync;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

class DeleteLakeTableTask implements Runnable {
    private static final Logger LOG = LogManager.getLogger(DeleteLakeTableTask.class);

    private final LakeTable table;

    DeleteLakeTableTask(LakeTable table) {
        this.table = table;
    }

    @Override
    public void run() {
        Tablet anyTablet = null;
        Set<Long> tabletIds = new HashSet<>();
        for (Partition partition : table.getAllPartitions()) {
            List<MaterializedIndex> allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
            for (MaterializedIndex materializedIndex : allIndices) {
                for (Tablet tablet : materializedIndex.getTablets()) {
                    tabletIds.add(tablet.getId());
                    anyTablet = tablet;
                }
            }
        }

        if (tabletIds.isEmpty()) {
            return;
        }

        DropTableRequest request = new DropTableRequest();
        request.tabletId = anyTablet.getId();
        Long beId = Utils.chooseBackend((LakeTablet) anyTablet);
        if (beId == null) {
            return;
        }
        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(beId);
        if (backend == null) {
            return;
        }
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        try {
            LakeServiceAsync lakeServiceAsync = BrpcProxy.getLakeService(address);
            lakeServiceAsync.dropTable(request, new RpcCallback<DropTableResponse>() {
                @Override
                public void success(DropTableResponse dropTableResponse) {
                    try {
                        LOG.info("Cleared lake table storage, deleting shards");
                        GlobalStateMgr.getCurrentStarOSAgent().deleteShards(tabletIds);
                    } catch (DdlException ex) {
                        LOG.warn("Fail to delete shards: {}", ex.getMessage());
                    }
                }

                @Override
                public void fail(Throwable throwable) {
                    LOG.error("Fail to clear lake table storage: {}", throwable.getMessage());
                }
            });
        } catch (Throwable ex) {
            LOG.info("Fail to get lake service proxy: {}", ex.getMessage());
        }
    }
}