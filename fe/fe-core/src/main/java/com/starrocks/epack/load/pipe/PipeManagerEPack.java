// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.load.pipe;

import com.starrocks.load.pipe.Pipe;
import com.starrocks.load.pipe.PipeId;
import com.starrocks.load.pipe.PipeManager;

public class PipeManagerEPack extends PipeManager {

    public void registerOrUpdatePipe(Pipe pipe) {
        try {
            lock.writeLock().lock();
            PipeId pipeId = nameToId.get(pipe.getDbAndName());
            if (pipeId == null) {
                pipeMap.put(pipe.getPipeId(), pipe);
                nameToId.put(pipe.getDbAndName(), pipe.getPipeId());
                repo.addPipe(pipe);
            } else {
                pipe.getPipeId().setId(pipeId.getId());
                pipeMap.put(pipeId, pipe);
                repo.alterPipe(pipe);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

}
