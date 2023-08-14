// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.pipe;

import com.starrocks.common.Pair;
import com.starrocks.common.io.Text;
import com.starrocks.persist.PipeOpEntry;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

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

    public long saveImage(DataOutputStream output, long checksum) throws IOException {
        Pair<String, Integer> jsonAndChecksum = pipeManager.toJson();
        checksum ^= jsonAndChecksum.second;
        Text.writeString(output, jsonAndChecksum.first);
        return checksum;
    }

    public long loadImage(DataInputStream input, long checksum) throws IOException {
        String imageJson = Text.readString(input);
        PipeManager data = GsonUtils.GSON.fromJson(imageJson, PipeManager.class);
        if (data.getPipesUnlock() != null) {
            Map<PipeId, Pipe> pipes = data.getPipesUnlock();
            for (Pipe pipe : pipes.values()) {
                pipeManager.putPipe(pipe);
            }
            LOG.info("Load {} pipes from image: {}", pipes.size(), pipes);
            checksum ^= pipes.size();
        }
        return checksum;
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

