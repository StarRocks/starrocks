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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/backup/RepositoryMgr.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.backup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.backup.Status.ErrCode;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.Daemon;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/*
 * A manager to manage all backup repositories
 */
public class RepositoryMgr extends Daemon implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(RepositoryMgr.class);

    // all key should be in lower case
    @SerializedName("rp")
    private final Map<String, Repository> repoNameMap = Maps.newConcurrentMap();
    private final Map<Long, Repository> repoIdMap = Maps.newConcurrentMap();

    private final ReentrantLock lock = new ReentrantLock();

    public RepositoryMgr() {
        super(Repository.class.getSimpleName(), 600L * 1000L /* 10min */);
    }

    @Override
    protected void runOneCycle() {
        for (Repository repo : repoNameMap.values()) {
            if (!repo.ping()) {
                LOG.warn("Failed to connect repository {}. msg: {}", repo.getName(), repo.getErrorMsg());
            }
        }
    }

    public Status addAndInitRepoIfNotExist(Repository repo) {
        lock.lock();
        try {
            if (!repoNameMap.containsKey(repo.getName())) {
                // create repository path and repo info file in remote storage
                Status st = repo.initRepository();
                if (!st.ok()) {
                    return st;
                }

                // write log
                GlobalStateMgr.getCurrentState().getEditLog().logCreateRepository(repo, wal -> {
                    addRepoWithoutLock(repo);
                });
                return Status.OK;
            } else {
                return new Status(ErrCode.COMMON_ERROR, "repository with same name already exist: " + repo.getName());
            }
        } finally {
            lock.unlock();
        }
    }

    public void replayAddRepo(Repository repo) {
        lock.lock();
        try {
            if (!repoNameMap.containsKey(repo.getName())) {
                addRepoWithoutLock(repo);
            }
        } finally {
            lock.unlock();
        }
    }

    private void addRepoWithoutLock(Repository repo) {
        repoNameMap.put(repo.getName(), repo);
        repoIdMap.put(repo.getId(), repo);
        LOG.info("successfully adding repo {} to repository mgr. ", repo.getName());
    }

    public Repository getRepo(String repoName) {
        return repoNameMap.get(repoName);
    }

    public Repository getRepo(long repoId) {
        return repoIdMap.get(repoId);
    }

    public Status removeRepo(String repoName) {
        lock.lock();
        try {
            Repository repo = repoNameMap.get(repoName);
            if (repo != null) {
                // log
                GlobalStateMgr.getCurrentState().getEditLog().logDropRepository(repoName, wal -> {
                    removeRepoWithoutLock(repo);
                });
                return Status.OK;
            }
            return new Status(ErrCode.NOT_FOUND, "repository does not exist");
        } finally {
            lock.unlock();
        }
    }

    public void replayRemoveRepo(String repoName) {
        lock.lock();
        try {
            Repository repo = repoNameMap.get(repoName);
            if (repo != null) {
                removeRepoWithoutLock(repo);
            }
        } finally {
            lock.unlock();
        }
    }

    private void removeRepoWithoutLock(Repository repo) {
        repoIdMap.remove(repo.getId());
        repoNameMap.remove(repo.getName());
        LOG.info("successfully removing repo {} from repository mgr", repo.getName());
    }

    public List<List<String>> getReposInfo() {
        List<List<String>> infos = Lists.newArrayList();
        for (Repository repo : repoIdMap.values()) {
            infos.add(repo.getInfo());
        }
        return infos;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        for (Repository repo : repoNameMap.values()) {
            repoIdMap.put(repo.getId(), repo);
        }
    }
}
