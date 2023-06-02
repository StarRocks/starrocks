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

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.PipeOpEntry;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreatePipeStmt;
import com.starrocks.sql.ast.DropPipeStmt;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class PipeManager implements Writable {

    private final static Logger LOG = LogManager.getLogger(PipeManager.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @SerializedName(value = "pipes")
    private Map<String, Pipe> pipeMap = new HashMap<>();

    public PipeManager() {
    }

    public void createPipe(CreatePipeStmt stmt) throws DdlException {
        try {
            lock.writeLock().lock();
            boolean existed = pipeMap.containsKey(stmt.getPipeName());
            if (existed) {
                if (!stmt.isIfNotExists()) {
                    throw new DdlException("Pipe already exists");
                }
                return;
            }

            // Add pipe
            long id = GlobalStateMgr.getCurrentState().getNextId();
            Pipe pipe = Pipe.fromStatement(id, stmt);
            pipeMap.put(stmt.getPipeName(), pipe);

            // edit log
            PipeOpEntry opEntry = new PipeOpEntry();
            opEntry.setPipeOp(PipeOpEntry.PipeOpType.PIPE_OP_CREATE);
            opEntry.setPipeJson(pipe.toJson());
            GlobalStateMgr.getCurrentState().getEditLog().logPipeOp(opEntry);
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void dropPipe(DropPipeStmt stmt) throws DdlException {
        try {
            lock.writeLock().lock();
            Pipe pipe = pipeMap.get(stmt.getPipeName());
            if (pipe == null) {
                throw new DdlException("Pipe not exists");
            }
            pipe.pause();
            pipeMap.remove(stmt.getPipeName());

            // edit log
            PipeOpEntry opEntry = new PipeOpEntry();
            opEntry.setPipeOp(PipeOpEntry.PipeOpType.PIPE_OP_DROP);
            opEntry.setPipeJson(pipe.toJson());
            GlobalStateMgr.getCurrentState().getEditLog().logPipeOp(opEntry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<Pipe> getRunnablePipes() {
        try {
            lock.readLock().lock();
            return pipeMap.values().stream().filter(Pipe::isRunnable).collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    //============================== Persistence ===========================================
    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public long saveImage(DataOutputStream output, long checksum) throws IOException {
        checksum ^= pipeMap.size();
        write(output);
        LOG.info("Save pipes to image: " + pipeMap.size());
        return checksum;
    }

    public long loadImage(DataInputStream input, long checksum) throws IOException {
        Preconditions.checkState(MapUtils.isEmpty(pipeMap));
        try {
            String imageJson = Text.readString(input);
            PipeManager data = GsonUtils.GSON.fromJson(imageJson, PipeManager.class);
            if (data != null && data.pipeMap != null) {
                pipeMap.putAll(data.pipeMap);
                LOG.info("Load pipes from image: " + pipeMap.size());
            }
            checksum ^= pipeMap.size();
        } catch (EOFException e) {
            LOG.info("no pipes");
        }
        return checksum;
    }

    public void replay(PipeOpEntry entry) {
        try {
            Pipe pipe = Pipe.fromJson(entry.getPipeJson());
            lock.writeLock().lock();
            switch (entry.getPipeOp()) {
                case PIPE_OP_CREATE: {
                    pipeMap.put(pipe.getName(), pipe);
                    break;
                }
                case PIPE_OP_DROP: {
                    pipeMap.remove(pipe.getName());
                    break;
                }
                case PIPE_OP_ALTER: {
                    Preconditions.checkState(pipeMap.containsKey(pipe.getName()));
                    pipeMap.put(pipe.getName(), pipe);
                    break;
                }
                default: {
                    LOG.error("Unknown PipeOp: " + entry.getPipeOp());
                }
            }
        } finally {
            lock.writeLock().unlock();
        }

    }
}
