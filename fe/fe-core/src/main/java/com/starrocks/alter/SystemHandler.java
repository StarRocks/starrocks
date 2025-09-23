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
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.StarRocksException;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AddComputeNodeClause;
import com.starrocks.sql.ast.AddFollowerClause;
import com.starrocks.sql.ast.AddObserverClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
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

import java.util.List;
import java.util.Map;
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
    private static final long RECYCLE_BIN_CHECK_INTERVAL = 10 * 60 * 1000L; // 10 min
    private long lastRecycleBinCheckTime = 0L;

    public SystemHandler() {
        super("cluster");
    }

    @Override
    // add synchronized to avoid process 2 or more stmts at same time
    public synchronized ShowResultSet process(List<AlterClause> alterClauses, Database dummyDb,
                                              OlapTable dummyTbl) throws StarRocksException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);
        alterClause.accept(SystemHandler.Visitor.getInstance(), null);
        return null;
    }

    protected static class Visitor implements AstVisitorExtendInterface<Void, Void> {
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
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().decommissionBackend(clause);
            });
            return null;
        }

        @Override
        public Void visitModifyBrokerClause(ModifyBrokerClause clause, Void context) {
            ErrorReport.wrapWithRuntimeException(() -> {
                BrokerMgr brokerMgr = GlobalStateMgr.getCurrentState().getBrokerMgr();
                switch (clause.getOp()) {
                    case OP_ADD:
                        brokerMgr.addBrokers(clause.getBrokerName(), clause.getHostPortPairs());
                        break;
                    case OP_DROP:
                        brokerMgr.dropBrokers(clause.getBrokerName(), clause.getHostPortPairs());
                        break;
                    case OP_DROP_ALL:
                        brokerMgr.dropAllBroker(clause.getBrokerName());
                        break;
                    default:
                        break;
                }
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
            if (canDropBackend(backendTabletIds)) {
                if (Config.drop_backend_after_decommission) {
                    try {
                        systemInfoService.dropBackend(beId);
                        if (backendTabletIds.isEmpty()) {
                            LOG.info("no tablet on decommission backend {}, drop it", beId);
                        } else {
                            LOG.info("force drop decommission backend {}, the tablets on it are all in recycle bin", beId);
                        }
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

    /**
     * If the following conditions are met, it can be forced to drop the backend
     * 1. All the tablets are in recycle bin.
     * 2. All the replication number of tablets is bigger than the retained backend number
     *    (which means there is no backend to migrate, so decommission is blocked),
     *    and at least one healthy replica on retained backend.
     * 3. There are at least 1 available backend.
     */
    protected boolean canDropBackend(List<Long> backendTabletIds) {
        if (backendTabletIds.isEmpty()) {
            return true;
        }

        // There is only on replica for shared data mode, so tablets can be migrated to other backends.
        if (RunMode.isSharedDataMode()) {
            return false;
        }

        if (lastRecycleBinCheckTime + RECYCLE_BIN_CHECK_INTERVAL > System.currentTimeMillis()) {
            return false;
        }
        lastRecycleBinCheckTime = System.currentTimeMillis();

        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        int availableBECnt =  systemInfoService.getAvailableBackends().size();
        if (availableBECnt < 1) {
            return false;
        }

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentState().getRecycleBin();
        List<Backend> retainedBackends = systemInfoService.getRetainedBackends();
        int retainedHostCnt = (int) retainedBackends.stream().map(Backend::getHost).distinct().count();
        Set<Long> retainedBackendIds = retainedBackends.stream().map(Backend::getId).collect(Collectors.toSet());
        for (Long tabletId : backendTabletIds) {
            TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
            if (tabletMeta == null) {
                continue;
            }

            if (!recycleBin.isTabletInRecycleBin(tabletMeta)) {
                return false;
            }

            Map<Long, Replica> replicas = invertedIndex.getReplicas(tabletId);
            if (replicas == null) {
                continue;
            }
            // It means the replica can be migrated to retained backends.
            if (replicas.size() <= retainedHostCnt) {
                return false;
            }

            // Make sure there is at least one normal replica on retained backends.
            boolean hasNormalReplica = false;
            for (Replica replica : replicas.values()) {
                if (replica.getState() == ReplicaState.NORMAL && retainedBackendIds.contains(replica.getBackendId())) {
                    hasNormalReplica = true;
                    break;
                }
            }
            if (!hasNormalReplica) {
                return false;
            }
        }

        return true;
    }

    @Override
    public synchronized void cancel(CancelStmt stmt) throws DdlException {
        CancelAlterSystemStmt cancelAlterSystemStmt = (CancelAlterSystemStmt) stmt;
        ErrorReport.wrapWithRuntimeException(() -> GlobalStateMgr.getCurrentState().getNodeMgr()
                .getClusterInfo().cancelDecommissionBackend(cancelAlterSystemStmt));
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        throw new NotImplementedException();
    }
}
