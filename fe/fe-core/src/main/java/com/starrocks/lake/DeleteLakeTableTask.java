// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.proto.DropTableRequest;
import com.starrocks.proto.DropTableResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

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
            LakeService lakeService = BrpcProxy.getLakeService(address);
            Future<DropTableResponse> future = lakeService.dropTable(request);
            DropTableResponse response = future.get();
            GlobalStateMgr.getCurrentStarOSAgent().deleteShards(tabletIds);

        } catch (Throwable ex) {
            LOG.info("Fail to get lake service proxy: {}", ex.getMessage());
        }
    }
}