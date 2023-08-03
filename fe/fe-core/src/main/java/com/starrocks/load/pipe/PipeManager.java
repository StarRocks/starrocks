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

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.pipe.AlterPipePauseResume;
import com.starrocks.sql.ast.pipe.AlterPipeStmt;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.DropPipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class PipeManager {

    private static final Logger LOG = LogManager.getLogger(PipeManager.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @SerializedName(value = "pipes")
    private Map<PipeId, Pipe> pipeMap = new HashMap<>();
    @SerializedName(value = "nameToId")
    private Map<Pair<Long, String>, PipeId> nameToId = new HashMap<>();

    private final PipeRepo repo;

    public PipeManager() {
        repo = new PipeRepo(this);
    }

    public void createPipe(CreatePipeStmt stmt) throws DdlException {
        try {
            lock.writeLock().lock();
            Pair<Long, String> dbIdAndName = resolvePipeNameUnlock(stmt.getPipeName());
            boolean existed = nameToId.containsKey(dbIdAndName);
            if (existed) {
                if (!stmt.isIfNotExists()) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_PIPE_EXISTS);
                }
                return;
            }

            // Add pipe
            long id = GlobalStateMgr.getCurrentState().getNextId();
            Pipe pipe = Pipe.fromStatement(id, stmt);
            putPipe(pipe);

            repo.addPipe(pipe);
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void dropPipe(DropPipeStmt stmt) throws DdlException {
        Pipe pipe = null;
        try {
            lock.writeLock().lock();
            Pair<Long, String> dbAndName = resolvePipeNameUnlock(stmt.getPipeName());
            PipeId pipeId = nameToId.get(dbAndName);
            if (pipeId == null) {
                if (stmt.isIfExists()) {
                    return;
                }
                ErrorReport.reportSemanticException(ErrorCode.ERR_UNKNOWN_PIPE, stmt.getPipeName());
            }
            pipe = pipeMap.get(nameToId.get(dbAndName));

            pipe.suspend();
            pipe.destroy();
            removePipe(pipe);

            // persistence
            repo.deletePipe(pipe);
        } catch (Throwable e) {
            LOG.error("drop pipe {} failed", pipe, e);
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void dropPipesOfDb(String dbName, long dbId) {
        try {
            lock.writeLock().lock();

            List<PipeId> removed = nameToId.entrySet().stream()
                    .filter(kv -> kv.getKey().first == dbId)
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());
            nameToId.keySet().removeIf(x -> x.first == dbId);
            for (PipeId id : removed) {
                Pipe pipe = pipeMap.get(id);
                if (pipe != null) {
                    pipe.suspend();
                    pipe.destroy();
                    pipeMap.remove(id);
                }
            }
            LOG.info("drop pipes in database " + dbName + ": " + removed);
        } catch (Throwable e) {
            LOG.error("drop pipes in database {}/{} failed", dbName, dbId, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void alterPipe(AlterPipeStmt stmt) throws DdlException {
        LOG.info("alter pipe " + stmt.toString());
        AlterPipePauseResume alterClause = (AlterPipePauseResume) stmt.getAlterPipeClause();
        try {
            lock.writeLock().lock();
            Pair<Long, String> dbAndName = resolvePipeNameUnlock(stmt.getPipeName());
            PipeId pipeId = nameToId.get(dbAndName);
            DdlException.requireNotNull("pipe-" + dbAndName.second, pipeId);
            Pipe pipe = pipeMap.get(pipeId);
            if (alterClause.isSuspend()) {
                pipe.suspend();
            } else if (alterClause.isResume()) {
                pipe.resume();
            }

            // persistence
            repo.alterPipe(pipe);
        } finally {
            lock.writeLock().unlock();
        }
    }

    protected void updatePipe(Pipe pipe) {
        try {
            lock.writeLock().lock();
            repo.alterPipe(pipe);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Pair<Long, String> resolvePipeNameUnlock(PipeName name) {
        long dbId = GlobalStateMgr.getCurrentState().mayGetDb(name.getDbName())
                .map(Database::getId)
                .orElseThrow(() -> ErrorReport.buildSemanticException(ErrorCode.ERR_NO_DB_ERROR));
        return Pair.create(dbId, name.getPipeName());
    }

    private Pipe getPipeByNameUnlock(PipeName name) {
        Pair<Long, String> idName = resolvePipeNameUnlock(name);
        PipeId pipeId = nameToId.get(Pair.create(idName.first, name.getPipeName()));
        if (pipeId == null) {
            return null;
        }
        return pipeMap.get(pipeId);
    }

    public List<Pipe> getRunnablePipes() {
        try {
            lock.readLock().lock();
            return pipeMap.values().stream().filter(Pipe::isRunnable).collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    public PipeRepo getRepo() {
        return repo;
    }

    //============================== RAW CRUD ===========================================
    public Pair<String, Integer> toJson() {
        try {
            lock.readLock().lock();
            return Pair.create(GsonUtils.GSON.toJson(this), pipeMap.size());
        } finally {
            lock.readLock().unlock();
        }
    }

    public void putPipe(Pipe pipe) {
        try {
            lock.writeLock().lock();
            pipeMap.put(pipe.getPipeId(), pipe);
            nameToId.put(pipe.getDbAndName(), pipe.getPipeId());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removePipe(Pipe pipe) {
        try {
            lock.writeLock().lock();
            pipeMap.remove(pipe.getPipeId());
            nameToId.remove(pipe.getDbAndName());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clear() {
        try {
            lock.writeLock().lock();
            pipeMap.clear();
            nameToId.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Map<PipeId, Pipe> getPipesUnlock() {
        return pipeMap;
    }

    public Optional<Pipe> mayGetPipe(long id) {
        // TODO: optimize performance
        return pipeMap.values().stream().filter(x -> x.getId() == id).findAny();
    }

    public Optional<Pipe> mayGetPipe(PipeName name) {
        try {
            lock.readLock().lock();
            return Optional.ofNullable(getPipeByNameUnlock(name));
        } finally {
            lock.readLock().unlock();
        }
    }

}
