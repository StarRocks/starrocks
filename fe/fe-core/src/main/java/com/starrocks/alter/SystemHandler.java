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
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.alter.AlterJob.JobState;
import com.starrocks.analysis.AddBackendClause;
import com.starrocks.analysis.AddFollowerClause;
import com.starrocks.analysis.AddObserverClause;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.AlterLoadErrorUrlClause;
import com.starrocks.analysis.CancelAlterSystemStmt;
import com.starrocks.analysis.CancelStmt;
import com.starrocks.analysis.DecommissionBackendClause;
import com.starrocks.analysis.DropBackendClause;
import com.starrocks.analysis.DropFollowerClause;
import com.starrocks.analysis.DropObserverClause;
import com.starrocks.analysis.ModifyBrokerClause;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentTask;
import com.starrocks.thrift.TTabletInfo;
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
    public void handleFinishedReplica(AgentTask task, TTabletInfo finishTabletInfo, long reportVersion)
            throws MetaNotFoundException {
    }

    @Override
    protected void runAfterCatalogReady() {
        super.runAfterCatalogReady();
        runOldAlterJob();
        runAlterJobV2();
    }

    @Deprecated
    private void runOldAlterJob() {
        // just remove all old decommission jobs. the decommission state is already marked in Backend,
        // and we no long need decommission job.
        alterJobs.clear();
        finishedOrCancelledAlterJobs.clear();
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
    public synchronized void process(List<AlterClause> alterClauses, String clusterName, Database dummyDb,
                                     OlapTable dummyTbl) throws UserException {
        Preconditions.checkArgument(alterClauses.size() == 1);
        AlterClause alterClause = alterClauses.get(0);

        if (alterClause instanceof AddBackendClause) {
            // add backend
            AddBackendClause addBackendClause = (AddBackendClause) alterClause;
            final String destClusterName = addBackendClause.getDestCluster();

            if ((!Strings.isNullOrEmpty(destClusterName) || addBackendClause.isFree()) &&
                    Config.disable_cluster_feature) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_INVALID_OPERATION, "ADD BACKEND TO CLUSTER");
            }

            if (!Strings.isNullOrEmpty(destClusterName)
                    && GlobalStateMgr.getCurrentState().getCluster(destClusterName) == null) {
                throw new DdlException("Cluster: " + destClusterName + " does not exist.");
            }
            GlobalStateMgr.getCurrentSystemInfo().addBackends(addBackendClause.getHostPortPairs(),
                    addBackendClause.isFree(), addBackendClause.getDestCluster());
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
        } else {
            Preconditions.checkState(false, alterClause.getClass());
        }
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
        // check if exist
        for (Pair<String, Integer> pair : hostPortPairs) {
            Backend backend = infoService.getBackendWithHeartbeatPort(pair.first, pair.second);
            if (backend == null) {
                throw new DdlException("Backend does not exist[" + pair.first + ":" + pair.second + "]");
            }
            if (backend.isDecommissioned()) {
                // already under decommission, ignore it
                continue;
            }
            decommissionBackends.add(backend);
        }

        // TODO(cmy): check if replication num can be met
        // TODO(cmy): check remaining space

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

    @Override
    public void replayInitJob(AlterJob alterJob, GlobalStateMgr globalStateMgr) {
        DecommissionBackendJob decommissionBackendJob = (DecommissionBackendJob) alterJob;
        LOG.debug("replay init decommision backend job: {}", decommissionBackendJob.getBackendIdsString());
        addAlterJob(alterJob);
    }

    @Override
    public void replayFinish(AlterJob alterJob, GlobalStateMgr globalStateMgr) {
        LOG.debug("replay finish decommision backend job: {}",
                ((DecommissionBackendJob) alterJob).getBackendIdsString());
        removeAlterJob(alterJob.getTableId());
        alterJob.setState(JobState.FINISHED);
        addFinishedOrCancelledAlterJob(alterJob);
    }

    @Override
    public void replayCancel(AlterJob alterJob, GlobalStateMgr globalStateMgr) {
        throw new NotImplementedException();
    }
}
