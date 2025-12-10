// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.load.pipe;

import com.starrocks.load.pipe.Pipe;
import com.starrocks.load.pipe.PipeId;
import com.starrocks.load.pipe.PipeManager;
import com.starrocks.persist.PipeOpEntry;
import com.starrocks.persist.WALApplier;
import com.starrocks.server.GlobalStateMgr;

public class PipeManagerEPack extends PipeManager {

    public void registerOrUpdatePipe(Pipe pipe) {
        try {
            lock.writeLock().lock();
            PipeId pipeId = nameToId.get(pipe.getDbAndName());
            WALApplier walApplier;
            if (pipeId == null) {
                walApplier = wal -> {
                    pipeMap.put(pipe.getPipeId(), pipe);
                    nameToId.put(pipe.getDbAndName(), pipe.getPipeId());
                };
            } else {
                pipe.getPipeId().setId(pipeId.getId());
                walApplier = wal -> pipeMap.put(pipe.getPipeId(), pipe);
            }
            PipeOpEntry opEntry = new PipeOpEntry();
            opEntry.setPipeOp(PipeOpEntry.PipeOpType.PIPE_OP_CREATE);
            opEntry.setPipeJson(pipe.toJson());
            GlobalStateMgr.getCurrentState().getEditLog().logPipeOp(opEntry, walApplier);
        } finally {
            lock.writeLock().unlock();
        }
    }

}
