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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.starrocks.common.DdlException;
import com.starrocks.persist.PipeOpEntry;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreatePipeStmt;
import com.starrocks.sql.ast.DropPipeStmt;

public class PipeManager {

    private Map<String, Pipe> pipeMap = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

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
            opEntry.setOpType(PipeOpEntry.PipeOpType.PIPE_OP_CREATE);
            opEntry.setPipe(pipe);
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
            opEntry.setOpType(PipeOpEntry.PipeOpType.PIPE_OP_DROP);
            opEntry.setPipe(pipe);
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
}
