// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.load.pipe;

import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.PipeOpEntry;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Repo: persistence for Pipe
 */
public class PipeRepo {

    private static final Logger LOG = LogManager.getLogger(PipeRepo.class);

    private final PipeManager pipeManager;

    public PipeRepo(PipeManager pipeManager) {
        this.pipeManager = pipeManager;
    }

    public void addPipe(Pipe pipe) {
        PipeOpEntry opEntry = new PipeOpEntry();
        opEntry.setPipeOp(PipeOpEntry.PipeOpType.PIPE_OP_CREATE);
        opEntry.setPipeJson(pipe.toJson());
        GlobalStateMgr.getCurrentState().getEditLog().logPipeOp(opEntry);
    }

    public void deletePipe(Pipe pipe) {
        PipeOpEntry opEntry = new PipeOpEntry();
        opEntry.setPipeOp(PipeOpEntry.PipeOpType.PIPE_OP_DROP);
        opEntry.setPipeJson(pipe.toJson());
        GlobalStateMgr.getCurrentState().getEditLog().logPipeOp(opEntry);
    }

    public void alterPipe(Pipe pipe) {
        PipeOpEntry opEntry = new PipeOpEntry();
        opEntry.setPipeOp(PipeOpEntry.PipeOpType.PIPE_OP_ALTER);
        opEntry.setPipeJson(pipe.toJson());
        GlobalStateMgr.getCurrentState().getEditLog().logPipeOp(opEntry);
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        reader.readCollection(Pipe.class, pipeManager::putPipe);
        LOG.info("loaded {} pipes", pipeManager.getAllPipes().size());
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        pipeManager.save(imageWriter);
    }

    public void replay(PipeOpEntry entry) {
        Pipe pipe = Pipe.fromJson(entry.getPipeJson());
        switch (entry.getPipeOp()) {
            case PIPE_OP_CREATE:
            case PIPE_OP_ALTER: {
                pipeManager.putPipe(pipe);
                break;
            }
            case PIPE_OP_DROP: {
                pipeManager.removePipe(pipe);
                break;
            }
            default: {
                LOG.error("Unknown PipeOp: " + entry.getPipeOp());
            }
        }
    }

}

