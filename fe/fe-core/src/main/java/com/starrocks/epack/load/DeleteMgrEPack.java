// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.load;

import com.google.common.collect.Lists;
import com.starrocks.load.DeleteMgr;
import com.starrocks.load.MultiDeleteInfo;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;

public class DeleteMgrEPack extends DeleteMgr {

    public boolean registerDeleteInfo(MultiDeleteInfo deleteInfo) {
        List<MultiDeleteInfo> deleteInfoList = dbToDeleteInfos.computeIfAbsent(deleteInfo.getDbId(),
                dbId -> Lists.newArrayList());
        lock.writeLock().lock();
        try {
            for (MultiDeleteInfo existedDeleteInfo : deleteInfoList) {
                if (existedDeleteInfo.getTableId() == deleteInfo.getTableId()
                        && existedDeleteInfo.getCreateTimeMs() == deleteInfo.getCreateTimeMs()) {
                    return false;
                }
            }

            deleteInfoList.add(deleteInfo);
        } finally {
            lock.writeLock().unlock();
        }

        GlobalStateMgr.getCurrentState().getEditLog().logFinishMultiDelete(deleteInfo, wal -> {
            updateTableDeleteInfo(
                    GlobalStateMgr.getCurrentState(),
                    deleteInfo.getDbId(),
                    deleteInfo.getTableId());
        });
        return true;
    }
}
