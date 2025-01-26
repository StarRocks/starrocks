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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/SystemHandler.java

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

package com.starrocks.alter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.NetUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AddComputeNodeClause;
import com.starrocks.sql.ast.AddFollowerClause;
import com.starrocks.sql.ast.AddObserverClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.CancelStmt;
import com.starrocks.sql.ast.CleanTabletSchedQClause;
import com.starrocks.sql.ast.CreateImageClause;
import com.starrocks.sql.ast.DecommissionBackendClause;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.ast.DropComputeNodeClause;
import com.starrocks.sql.ast.DropFollowerClause;
import com.starrocks.sql.ast.DropObserverClause;
import com.starrocks.sql.ast.ModifyBackendClause;
import com.starrocks.sql.ast.ModifyBrokerClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * SystemHandler is for
 * 1. add/drop/decommission backends
 * 2. add/drop frontends
 * 3. add/drop/modify brokers
 */
public class SystemHandler extends AlterHandler {
    private static final Logger LOG = LogManager.getLogger(SystemHandler.class);

    public SystemHandler() {
        super("cluster");
    }

    @Override
    // add synchronized to avoid process 2 or more stmts at same time
    public synchronized ShowResultSet process(List<AlterClause> alterClauses, Database dummyDb,
                                              OlapTable dummyTbl) throws UserException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);
        alterClause.accept(SystemHandler.Visitor.getInstance(), null);
        return null;
    }

    protected static class Visitor implements AstVisitor<Void, Void> {
        private static final SystemHandler.Visitor INSTANCE = new SystemHandler.Visitor();

        public static SystemHandler.Visitor getInstance() {
            return INSTANCE;
        }

        @Override
        public Void visitAddFollowerClause(AddFollowerClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().getNodeMgr()
                        .addFrontend(FrontendNodeType.FOLLOWER, clause.getHost(), clause.getPort());
            });
            return null;
        }

        @Override
        public Void visitDropFollowerClause(DropFollowerClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().getNodeMgr()
                        .dropFrontend(FrontendNodeType.FOLLOWER, clause.getHost(), clause.getPort());

            });
            return null;
        }

        @Override
        public Void visitAddObserverClause(AddObserverClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().getNodeMgr()
                        .addFrontend(FrontendNodeType.OBSERVER, clause.getHost(), clause.getPort());
            });
            return null;
        }

        @Override
        public Void visitDropObserverClause(DropObserverClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().getNodeMgr()
                        .dropFrontend(FrontendNodeType.OBSERVER, clause.getHost(), clause.getPort());
            });
            return null;
        }

        @Override
        public Void visitModifyFrontendHostClause(ModifyFrontendAddressClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().getNodeMgr().modifyFrontendHost(clause);
            });
            return null;
        }

        @Override
        public Void visitAddBackendClause(AddBackendClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackends(clause);
            });
            return null;
        }

        @Override
        public Void visitDropBackendClause(DropBackendClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropBackends(clause);
            });
            return null;
        }

        @Override
        public Void visitModifyBackendClause(ModifyBackendClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().modifyBackend(clause);
            });
            return null;
        }

        @Override
        public Void visitDecommissionBackendClause(DecommissionBackendClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                /*
                 * check if the specified backends can be decommissioned
                 * 1. backend should exist.
                 * 2. after decommission, the remaining backend num should meet the replication num.
                 * 3. after decommission, The remaining space capacity can store data on decommissioned backends.
                 */

                SystemInfoService infoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                List<Backend> decommissionBackends = Lists.newArrayList();
                Set<Long> decommissionIds = new HashSet<>();

                long needCapacity = 0L;
                long releaseCapacity = 0L;
                // check if exist
                for (Pair<String, Integer> pair : clause.getHostPortPairs()) {
                    Backend backend = infoService.getBackendWithHeartbeatPort(pair.first, pair.second);
                    if (backend == null) {
                        throw new DdlException("Backend does not exist[" +
                                NetUtils.getHostPortInAccessibleFormat(pair.first, pair.second) + "]");
                    }
                    if (backend.isDecommissioned()) {
                        // already under decommission, ignore it
                        LOG.info(backend.getAddress() + " has already been decommissioned and will be ignored.");
                        continue;
                    }
                    needCapacity += backend.getDataUsedCapacityB();
                    releaseCapacity += backend.getAvailableCapacityB();
                    decommissionBackends.add(backend);
                    decommissionIds.add(backend.getId());
                }

                if (decommissionBackends.isEmpty()) {
                    LOG.info("No backends will be decommissioned.");
                } else {
                    // when decommission backends in shared_data mode, unnecessary to check clusterCapacity or table replica
                    if (RunMode.isSharedNothingMode()) {
                        if (infoService.getClusterAvailableCapacityB() - releaseCapacity < needCapacity) {
                            decommissionBackends.clear();
                            throw new DdlException("It will cause insufficient disk space if these BEs are decommissioned.");
                        }

                        long availableBackendCnt = infoService.getAvailableBackendIds()
                                .stream()
                                .filter(beId -> !decommissionIds.contains(beId))
                                .count();
                        short maxReplicationNum = 0;
                        LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
                        for (long dbId : localMetastore.getDbIds()) {
                            Database db = localMetastore.getDb(dbId);
                            if (db == null) {
                                continue;
                            }
                            Locker locker = new Locker();
                            locker.lockDatabase(db.getId(), LockType.READ);
                            try {
                                for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                                    if (table instanceof OlapTable) {
                                        OlapTable olapTable = (OlapTable) table;
                                        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                                        for (long partitionId : olapTable.getAllPartitionIds()) {
                                            short replicationNum = partitionInfo.getReplicationNum(partitionId);
                                            if (replicationNum > maxReplicationNum) {
                                                maxReplicationNum = replicationNum;
                                                if (availableBackendCnt < maxReplicationNum) {
                                                    decommissionBackends.clear();
                                                    throw new DdlException(
                                                            "It will cause insufficient BE number if these BEs " +
                                                                    "are decommissioned because the table " +
                                                                    db.getFullName() + "." + olapTable.getName() +
                                                                    " requires " + maxReplicationNum + " replicas.");

                                                }
                                            }
                                        }
                                    }
                                }
                            } finally {
                                locker.unLockDatabase(db.getId(), LockType.READ);
                            }
                        }
                    }

                    // set backend's state as 'decommissioned'
                    // for decommission operation, here is no decommission job. the system handler will check
                    // all backend in decommission state
                    for (Backend backend : decommissionBackends) {
                        backend.setDecommissioned(true);
                        GlobalStateMgr.getCurrentState().getEditLog().logBackendStateChange(backend);
                        LOG.info("set backend {} to decommission", backend.getId());
                    }
                }
            });
            return null;
        }

        @Override
        public Void visitModifyBrokerClause(ModifyBrokerClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().getBrokerMgr().execute(clause);
            });
            return null;
        }

        @Override
        public Void visitAddComputeNodeClause(AddComputeNodeClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addComputeNodes(clause);
            });
            return null;
        }

        @Override
        public Void visitDropComputeNodeClause(DropComputeNodeClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropComputeNodes(clause);
            });
            return null;
        }

        @Override
        public Void visitCreateImageClause(CreateImageClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().triggerNewImage();
                if (RunMode.isSharedDataMode()) {
                    StarMgrServer.getCurrentState().triggerNewImage();
                }
            });
            return null;
        }

        @Override
        public Void visitCleanTabletSchedQClause(CleanTabletSchedQClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                GlobalStateMgr.getCurrentState().getTabletScheduler().forceCleanSchedQ();
            });
            return null;
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        super.runAfterCatalogReady();

        // check all decommissioned backends, if there is no tablet on that backend, drop it.
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        // check if decommission is finished
        for (Long beId : systemInfoService.getBackendIds(false)) {
            Backend backend = systemInfoService.getBackend(beId);
            if (backend == null || !backend.isDecommissioned()) {
                continue;
            }

            List<Long> backendTabletIds = invertedIndex.getTabletIdsByBackendId(beId);
            if (backendTabletIds.isEmpty()) {
                if (Config.drop_backend_after_decommission) {
                    try {
                        systemInfoService.dropBackend(beId);
                        LOG.info("no tablet on decommission backend {}, drop it", beId);
                    } catch (DdlException e) {
                        // does not matter, maybe backend not exists
                        LOG.info("backend {} drop failed after decommission {}", beId, e.getMessage());
                    }
                }
            } else {
                LOG.info("backend {} lefts {} replicas to decommission(show up to 20): {}", beId,
                        backendTabletIds.size(),
                        backendTabletIds.stream().limit(20).collect(Collectors.toList()));
            }
        }
    }

    @Override
    public synchronized void cancel(CancelStmt stmt) throws DdlException {
        CancelAlterSystemStmt cancelAlterSystemStmt = (CancelAlterSystemStmt) stmt;

        SystemInfoService infoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        // check if backends is under decommission
        List<Backend> backends = Lists.newArrayList();
        List<Pair<String, Integer>> hostPortPairs = cancelAlterSystemStmt.getHostPortPairs();
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check if exist
            Backend backend = infoService.getBackendWithHeartbeatPort(pair.first, pair.second);
            if (backend == null) {
                throw new DdlException("Backend does not exist[" +
                        NetUtils.getHostPortInAccessibleFormat(pair.first, pair.second) + "]");
            }

            if (!backend.isDecommissioned()) {
                // it's ok. just log
                LOG.info("backend is not decommissioned[{}]", pair.first);
                continue;
            }

            backends.add(backend);
        }

        for (Backend backend : backends) {
            if (backend.setDecommissioned(false)) {
                GlobalStateMgr.getCurrentState().getEditLog().logBackendStateChange(backend);
            } else {
                LOG.info("backend is not decommissioned[{}]", backend.getHost());
            }
        }
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        throw new NotImplementedException();
    }
}
