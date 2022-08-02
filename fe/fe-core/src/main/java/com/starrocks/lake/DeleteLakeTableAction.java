// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.lake;

import com.starrocks.catalog.DeleteTableAction;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.DdlException;
import com.starrocks.lake.proto.DropTableRequest;
import com.starrocks.rpc.LakeServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import org.mortbay.log.Log;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

class DeleteLakeTableAction implements DeleteTableAction {
    private final LakeTable table;

    DeleteLakeTableAction(LakeTable table) {
        this.table = table;
    }

    @Override
    public boolean execute() {
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
            return true;
        }

        DropTableRequest request = new DropTableRequest();
        request.tabletId = anyTablet.getId();
        Long beId = Utils.chooseBackend((LakeTablet) anyTablet);
        if (beId == null) {
            return false;
        }
        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(beId);
        if (backend == null) {
            return false;
        }
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        LakeServiceClient client = new LakeServiceClient(address);
        try {
            client.dropTable(request);
            GlobalStateMgr.getCurrentState().getStarOSAgent().deleteShards(tabletIds);
        } catch (RpcException | DdlException e) {
            Log.warn(e);
            return false;
        }
        return true;
    }
}
