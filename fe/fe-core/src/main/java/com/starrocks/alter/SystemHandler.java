// This file is made available under Elastic License 2.0.
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
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AddComputeNodeClause;
import com.starrocks.sql.ast.AddFollowerClause;
import com.starrocks.sql.ast.AddObserverClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterLoadErrorUrlClause;
import com.starrocks.sql.ast.CancelAlterSystemStmt;
import com.starrocks.sql.ast.CancelStmt;
import com.starrocks.sql.ast.CleanTabletSchedQClause;
import com.starrocks.sql.ast.CreateImageClause;
import com.starrocks.sql.ast.DecommissionBackendClause;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.ast.DropComputeNodeClause;
import com.starrocks.sql.ast.DropFollowerClause;
import com.starrocks.sql.ast.DropObserverClause;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.sql.ast.ModifyBrokerClause;
import com.starrocks.sql.ast.ModifyFrontendAddressClause;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/*
 * SystemHandler is for
 * 1. add/drop/decommission backends
 * 2. add/drop frontends
 * 3. add/drop/modify brokers
 */
public class SystemHandler extends AlterHandler {
    private static final Logger LOG = LogManager.getLogger(SystemHandler.class);

    private static final long MAX_REMAINED_TABLET_TO_CHECK_ON_DECOMM = 1000;

    public SystemHandler() {
        super("cluster");
    }

    @Override
    protected void runAfterCatalogReady() {
        super.runAfterCatalogReady();
        runAlterJobV2();
    }

    private void dropDecommissionedBackend(SystemInfoService systemInfoService, long beId) {
        try {
            systemInfoService.dropBackend(beId);
            LOG.info("no tablet on decommission backend {}, drop it", beId);
        } catch (DdlException e) {
            // does not matter, maybe backend not exists
            LOG.info("backend {} drop failed after decommission {}", beId, e.getMessage());
        }
    }

    // check all decommissioned backends, if there is no tablet on that backend, drop it.
    private void runAlterJobV2() {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        // check if decommission is finished
        for (Long beId : systemInfoService.getBackendIds(false)) {
            Backend backend = systemInfoService.getBackend(beId);
            if (backend == null || !backend.isDecommissioned()) {
                continue;
            }

            List<Long> backendTabletIds = invertedIndex.getTabletIdsByBackendId(beId);
            if (backendTabletIds.isEmpty()) {
                if (Config.drop_backend_after_decommission) {
                    dropDecommissionedBackend(systemInfoService, beId);
                }
            } else {
                LOG.info("backend {} lefts {} replicas to decommission(show up to 20): {}", beId,
                        backendTabletIds.size(),
                        backendTabletIds.stream().limit(20).collect(Collectors.toList()));
            }
        }
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        throw new NotImplementedException();
    }

    @Override
    // add synchronized to avoid process 2 or more stmts at same time
    public synchronized ShowResultSet process(List<AlterClause> alterClauses, Database dummyDb,
                                              OlapTable dummyTbl) throws UserException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);
        if (alterClause instanceof AddBackendClause) {
            // add backend
            AddBackendClause addBackendClause = (AddBackendClause) alterClause;
            GlobalStateMgr.getCurrentSystemInfo().addBackends(addBackendClause.getHostPortPairs());
        } else if (alterClause instanceof ModifyBackendAddressClause) {
            // update Backend Address
            ModifyBackendAddressClause modifyBackendAddressClause = (ModifyBackendAddressClause) alterClause;
            return GlobalStateMgr.getCurrentSystemInfo().modifyBackendHost(modifyBackendAddressClause);
        } else if (alterClause instanceof DropBackendClause) {
            // drop backend
            DropBackendClause dropBackendClause = (DropBackendClause) alterClause;
            GlobalStateMgr.getCurrentSystemInfo().dropBackends(dropBackendClause);
        } else if (alterClause instanceof DecommissionBackendClause) {
            // decommission
            DecommissionBackendClause decommissionBackendClause = (DecommissionBackendClause) alterClause;
            // check request
            List<Backend> decommissionBackends = checkDecommission(decommissionBackendClause);

            // set backend's state as 'decommissioned'
            // for decommission operation, here is no decommission job. the system handler will check
            // all backend in decommission state
            for (Backend backend : decommissionBackends) {
                backend.setDecommissioned(true);
                GlobalStateMgr.getCurrentState().getEditLog().logBackendStateChange(backend);
                LOG.info("set backend {} to decommission", backend.getId());
            }

        } else if (alterClause instanceof AddObserverClause) {
            AddObserverClause clause = (AddObserverClause) alterClause;
            GlobalStateMgr.getCurrentState().addFrontend(FrontendNodeType.OBSERVER, clause.getHost(), clause.getPort());
        } else if (alterClause instanceof DropObserverClause) {
            DropObserverClause clause = (DropObserverClause) alterClause;
            GlobalStateMgr.getCurrentState()
                    .dropFrontend(FrontendNodeType.OBSERVER, clause.getHost(), clause.getPort());
        } else if (alterClause instanceof AddFollowerClause) {
            AddFollowerClause clause = (AddFollowerClause) alterClause;
            GlobalStateMgr.getCurrentState().addFrontend(FrontendNodeType.FOLLOWER, clause.getHost(), clause.getPort());
        } else if (alterClause instanceof ModifyFrontendAddressClause) {
            // update Frontend Address
            ModifyFrontendAddressClause modifyFrontendAddressClause = (ModifyFrontendAddressClause) alterClause;
            GlobalStateMgr.getCurrentState().modifyFrontendHost(modifyFrontendAddressClause);
        } else if (alterClause instanceof DropFollowerClause) {
            DropFollowerClause clause = (DropFollowerClause) alterClause;
            GlobalStateMgr.getCurrentState()
                    .dropFrontend(FrontendNodeType.FOLLOWER, clause.getHost(), clause.getPort());
        } else if (alterClause instanceof ModifyBrokerClause) {
            ModifyBrokerClause clause = (ModifyBrokerClause) alterClause;
            GlobalStateMgr.getCurrentState().getBrokerMgr().execute(clause);
        } else if (alterClause instanceof AlterLoadErrorUrlClause) {
            AlterLoadErrorUrlClause clause = (AlterLoadErrorUrlClause) alterClause;
            GlobalStateMgr.getCurrentState().getLoadInstance().setLoadErrorHubInfo(clause.getProperties());
        } else if (alterClause instanceof AddComputeNodeClause) {
            AddComputeNodeClause addComputeNodeClause = (AddComputeNodeClause) alterClause;
            GlobalStateMgr.getCurrentSystemInfo().addComputeNodes(addComputeNodeClause.getHostPortPairs());
        } else if (alterClause instanceof DropComputeNodeClause) {
            DropComputeNodeClause dropComputeNodeClause = (DropComputeNodeClause) alterClause;
            GlobalStateMgr.getCurrentSystemInfo().dropComputeNodes(dropComputeNodeClause.getHostPortPairs());
        } else if (alterClause instanceof CreateImageClause) {
            GlobalStateMgr.getCurrentState().triggerNewImage();
        } else if (alterClause instanceof CleanTabletSchedQClause) {
            GlobalStateMgr.getCurrentState().getTabletScheduler().forceCleanSchedQ();
        } else {
            Preconditions.checkState(false, alterClause.getClass());
        }
        return null;
    }

    private List<Backend> checkDecommission(DecommissionBackendClause decommissionBackendClause)
            throws DdlException {
        return checkDecommission(decommissionBackendClause.getHostPortPairs());
    }

    /*
     * check if the specified backends can be decommissioned
     * 1. backend should exist.
     * 2. after decommission, the remaining backend num should meet the replication num.
     * 3. after decommission, The remaining space capacity can store data on decommissioned backends.
     */
    public static List<Backend> checkDecommission(List<Pair<String, Integer>> hostPortPairs)
            throws DdlException {
        SystemInfoService infoService = GlobalStateMgr.getCurrentSystemInfo();
        List<Backend> decommissionBackends = Lists.newArrayList();

        long needCapacity = 0L;
        long releaseCapacity = 0L;
        // check if exist
        for (Pair<String, Integer> pair : hostPortPairs) {
            Backend backend = infoService.getBackendWithHeartbeatPort(pair.first, pair.second);
            if (backend == null) {
                throw new DdlException("Backend does not exist[" + pair.first + ":" + pair.second + "]");
            }
            if (backend.isDecommissioned()) {
                // already under decommission, ignore it
                LOG.info(backend.getHost() + " has already been decommissioned and will be ignored.");
                continue;
            }
            needCapacity += backend.getDataUsedCapacityB();
            releaseCapacity += backend.getAvailableCapacityB();
            decommissionBackends.add(backend);
        }

        if (decommissionBackends.isEmpty()) {
            LOG.info("No backends will be decommissioned.");
            return decommissionBackends;
        }

        if (infoService.getClusterAvailableCapacityB() - releaseCapacity < needCapacity) {
            decommissionBackends.clear();
            throw new DdlException("It will cause insufficient disk space if these BEs are decommissioned.");
        }

        short maxReplicationNum = 0;
        LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        for (long dbId : localMetastore.getDbIds()) {
            Database db = localMetastore.getDb(dbId);
            if (db == null) {
                continue;
            }
            db.readLock();
            try {
                for (Table table : db.getTables()) {
                    if (table instanceof OlapTable) {
                        OlapTable olapTable = (OlapTable) table;
                        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                        for (Partition partition : olapTable.getAllPartitions()) {
                            short replicationNum = partitionInfo.getReplicationNum(partition.getId());
                            if (replicationNum > maxReplicationNum) {
                                maxReplicationNum = replicationNum;
                                if (infoService.getAvailableBackendIds().size() - decommissionBackends.size() <
                                        maxReplicationNum) {
                                    decommissionBackends.clear();
                                    throw new DdlException(
                                            "It will cause insufficient BE number if these BEs are decommissioned " +
                                            "because the table " + db.getFullName() + "." + olapTable.getName() + " requires " +
                                            maxReplicationNum + " replicas.");
                                }
                            }
                        }
                    }
                }
            } finally {
                db.readUnlock();
            }
        }

        return decommissionBackends;
    }

    @Override
    public synchronized void cancel(CancelStmt stmt) throws DdlException {
        CancelAlterSystemStmt cancelAlterSystemStmt = (CancelAlterSystemStmt) stmt;

        SystemInfoService infoService = GlobalStateMgr.getCurrentSystemInfo();
        // check if backends is under decommission
        List<Backend> backends = Lists.newArrayList();
        List<Pair<String, Integer>> hostPortPairs = cancelAlterSystemStmt.getHostPortPairs();
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check if exist
            Backend backend = infoService.getBackendWithHeartbeatPort(pair.first, pair.second);
            if (backend == null) {
                throw new DdlException("Backend does not exists[" + pair.first + "]");
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

}
