// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.load.streamload;

import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.server.GlobalStateMgr;

public class StreamLoadMgrEPack extends StreamLoadMgr {

    public boolean registerLoadTask(StreamLoadTask task) {
        writeLock();
        try {
            if (idToStreamLoadTask.containsKey(task.getLabel())) {
                return false;
            }
            addLoadTask(task);
        } finally {
            writeUnlock();
        }

        GlobalStateMgr.getCurrentState().getEditLog().logCreateStreamLoadJob(task);
        return true;
    }
}
