// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/alter/DecommissionBackendJob.java

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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.cluster.Cluster;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.io.Text;
import com.starrocks.persist.BackendIdsUpdateInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.Backend.BackendState;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentTask;
import com.starrocks.thrift.TTabletInfo;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Deprecated
public class DecommissionBackendJob extends AlterJob {

    public enum DecommissionType {
        SystemDecommission, // after finished system decommission, the backend will be removed from starrocks.
        ClusterDecommission // after finished cluster decommission, the backend will be removed from cluster.
    }

    private static final Logger LOG = LogManager.getLogger(DecommissionBackendJob.class);

    private static final Joiner JOINER = Joiner.on("; ");

    // all backends need to be decommissioned
    private Map<String, Map<Long, Backend>> clusterBackendsMap;
    private Set<Long> allClusterBackendIds;

    // add backendId to 'finishedBackendIds' only if no tablets exist in that
    // backend
    private Set<Long> finishedBackendIds;

    private DecommissionType decommissionType;

    public DecommissionBackendJob() {
        // for persist
        super(JobType.DECOMMISSION_BACKEND);

        clusterBackendsMap = Maps.newHashMap();
        allClusterBackendIds = Sets.newHashSet();

        finishedBackendIds = Sets.newHashSet();
        decommissionType = DecommissionType.SystemDecommission;
    }

    /**
     * in Multi-Tenancy example "clusterA:1,2,3;clusterB:4,5,6"
     *
     * @return
     */
    public String getBackendIdsString() {
        final Joiner joiner = Joiner.on(",");
        final Set<String> clusterBackendsSet = new HashSet<String>();
        for (String cluster : clusterBackendsMap.keySet()) {
            final Map<Long, Backend> backends = clusterBackendsMap.get(cluster);
            final String backendStr = joiner.join(backends.keySet());
            final StringBuilder builder = new StringBuilder(cluster);
            builder.append(":").append(backendStr);
            clusterBackendsSet.add(builder.toString());
        }
        String res = JOINER.join(clusterBackendsSet);
        return res;
    }

    @Override
    public void addReplicaId(long parentId, long replicaId, long backendId) {
        throw new NotImplementedException();
    }

    @Override
    public void setReplicaFinished(long parentId, long replicaId) {
        throw new NotImplementedException();
    }

    @Override
    public synchronized boolean sendTasks() {
        // do nothing.
        // In previous implementation, we send clone task actively.
        // But now, TabletChecker will do all the things, here we just skip PENDING phase.
        this.state = JobState.RUNNING;
        return true;
    }

    @Override
    public synchronized void cancel(OlapTable olapTable, String msg) {
        // set state
        this.state = JobState.CANCELLED;
        if (msg != null) {
            this.cancelMsg = msg;
        }

        this.finishedTime = System.currentTimeMillis();

        // no need to log
        LOG.info("finished cancel decommission backend");
    }

    @Override
    public void removeReplicaRelatedTask(long parentId, long tabletId, long replicaId, long backendId) {
        throw new NotImplementedException();
    }

    @Override
    public synchronized void handleFinishedReplica(AgentTask task, TTabletInfo finishTabletInfo, long reportVersion)
            throws MetaNotFoundException {
        throw new NotImplementedException();
    }

    @Override
    public synchronized int tryFinishJob() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        SystemInfoService systemInfo = GlobalStateMgr.getCurrentSystemInfo();

        LOG.debug("start try finish decommission backend job: {}", getBackendIdsString());
        for (String cluster : clusterBackendsMap.keySet()) {
            final Map<Long, Backend> backends = clusterBackendsMap.get(cluster);
            // check if tablets in one backend has full replicas
            Iterator<Long> backendIter = backends.keySet().iterator();
            while (backendIter.hasNext()) {
                long backendId = backendIter.next();
                Backend backend = systemInfo.getBackend(backendId);
                if (backend == null || !backend.isDecommissioned()) {
                    backendIter.remove();
                    LOG.info("backend[{}] is not decommissioned. remove from decommission jobs");
                    continue;
                }

                if (finishedBackendIds.contains(backendId)) {
                    continue;
                }

                List<Long> backendTabletIds = invertedIndex.getTabletIdsByBackendId(backendId);
                if (backendTabletIds.isEmpty()) {
                    LOG.info("no tablet in {}", backend);
                    finishedBackendIds.add(backendId);
                    continue;
                }

                LOG.info("{} lefts {} replicas to migrate: {}", backend, backendTabletIds.size(),
                        backendTabletIds.size() <= 20 ? backendTabletIds : "too many");
            } // end for backends
        }

        if (finishedBackendIds.size() >= allClusterBackendIds.size()) {
            // use '>=' not '==', because backend may be removed from backendIds
            // after it finished.
            // drop backend
            if (decommissionType == DecommissionType.SystemDecommission) {
                for (long backendId : allClusterBackendIds) {
                    try {
                        systemInfo.dropBackend(backendId);
                    } catch (DdlException e) {
                        // it's ok, backend has already been dropped
                        LOG.info("drop backend[{}] failed. cause: {}", backendId, e.getMessage());
                    }
                }
            } else {
                // Shrinking capacity in cluster
                if (decommissionType == DecommissionType.ClusterDecommission) {
                    for (String clusterName : clusterBackendsMap.keySet()) {
                        final Map<Long, Backend> idToBackend = clusterBackendsMap.get(clusterName);
                        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
                        List<Long> backendList = Lists.newArrayList();
                        for (long id : idToBackend.keySet()) {
                            final Backend backend = idToBackend.get(id);
                            backend.clearClusterName();
                            backend.setBackendState(BackendState.free);
                            backend.setDecommissioned(false);
                            backendList.add(id);
                            cluster.removeBackend(id);
                        }
                        BackendIdsUpdateInfo updateInfo = new BackendIdsUpdateInfo(backendList);
                        GlobalStateMgr.getCurrentState().getEditLog().logUpdateClusterAndBackendState(updateInfo);
                    }
                }
            }

            this.finishedTime = System.currentTimeMillis();
            this.state = JobState.FINISHED;

            GlobalStateMgr.getCurrentState().getEditLog().logFinishDecommissionBackend(this);

            LOG.info("finished {} decommission {} backends: {}", decommissionType.toString(),
                    allClusterBackendIds.size(), getBackendIdsString());
            return 1;
        } else {
            Set<Long> unfinishedBackendIds = Sets.newHashSet();
            for (Long backendId : allClusterBackendIds) {
                if (!finishedBackendIds.contains(backendId)) {
                    unfinishedBackendIds.add(backendId);
                }
            }
            LOG.info("waiting {} backends to finish tablets migration: {}", unfinishedBackendIds.size(),
                    unfinishedBackendIds);
            return 0;
        }
    }

    @Override
    public synchronized void clear() {
        finishedBackendIds.clear();
    }

    @Override
    public void replayInitJob(Database db) {
        // do nothing
    }

    @Override
    public void replayFinishing(Database db) {
        // do nothing
    }

    @Override
    public void replayFinish(Database db) {
        // do nothing
    }

    @Override
    public void replayCancel(Database db) {
        // do nothing
    }

    @Override
    public void getJobInfo(List<List<Comparable>> jobInfos, OlapTable tbl) {
        // do nothing
    }

    /**
     * to Backward compatibility
     *
     * @param in
     * @throws IOException
     */
    public synchronized void readFields(DataInput in) throws IOException {
        super.readFields(in);

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_30) {
            long clusterMapSize = in.readLong();
            while (clusterMapSize-- > 0) {
                final String cluster = Text.readString(in);
                long backendMspSize = in.readLong();
                Map<Long, Backend> backends = Maps.newHashMap();
                while (backendMspSize-- > 0) {
                    final long id = in.readLong();
                    final Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(id);
                    backends.put(id, backend);
                    allClusterBackendIds.add(id);
                }
                clusterBackendsMap.put(cluster, backends);
            }

            if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_33) {
                String str = Text.readString(in);
                // this is only for rectify misspellings...
                if (str.equals("SystemDecomission")) {
                    str = "SystemDecommission";
                } else if (str.equals("ClusterDecomission")) {
                    str = "ClusterDecommission";
                }
                decommissionType = DecommissionType.valueOf(str);
            }
        } else {
            int backendNum = in.readInt();
            Map<Long, Backend> backends = Maps.newHashMap();
            for (int i = 0; i < backendNum; i++) {
                final long backendId = in.readLong();
                allClusterBackendIds.add(backendId);
                final Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
                backends.put(backendId, backend);
            }
            clusterBackendsMap.put(SystemInfoService.DEFAULT_CLUSTER, backends);
        }
    }

    @Override
    public synchronized void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeLong(clusterBackendsMap.keySet().size());
        for (String cluster : clusterBackendsMap.keySet()) {
            final Map<Long, Backend> backends = clusterBackendsMap.get(cluster);
            Text.writeString(out, cluster);
            out.writeLong(backends.keySet().size());
            for (Long id : backends.keySet()) {
                out.writeLong(id);
            }
        }

        Text.writeString(out, decommissionType.toString());
    }

    public static DecommissionBackendJob read(DataInput in) throws IOException {
        DecommissionBackendJob decommissionBackendJob = new DecommissionBackendJob();
        decommissionBackendJob.readFields(in);
        return decommissionBackendJob;
    }

    @Override
    public void finishJob() {
        // do nothing
    }
}
