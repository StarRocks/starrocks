// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.load.DeleteMgrEPack;
import com.starrocks.load.MultiDeleteInfo;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Map;

public class UpdateDeleteMgrJob extends FailoverGroupJob {

    protected UpdateDeleteMgrJob(FailoverGroup failoverGroup) {
        super(failoverGroup);
    }

    @Override
    public void execute() {
        for (Map.Entry<Long, List<MultiDeleteInfo>> deleteInfos : failoverGroup.getObjectMeta().getDeleteMgr()
                .getDbToDeleteInfos().entrySet()) {
            Long localDbId = failoverGroup.getObjectMap().getLocalDatabaseId(deleteInfos.getKey());
            if (localDbId == null) {
                continue;
            }

            for (MultiDeleteInfo deleteInfo : deleteInfos.getValue()) {
                Long localTableId = failoverGroup.getObjectMap().getLocalTableId(deleteInfo.getTableId());
                if (localTableId == null) {
                    continue;
                }

                MultiDeleteInfo.setDbId(deleteInfo, localDbId);
                MultiDeleteInfo.setTableId(deleteInfo, localTableId);
                ((DeleteMgrEPack) GlobalStateMgr.getServingState().getDeleteMgr()).registerDeleteInfo(deleteInfo);
            }
        }
    }
}
