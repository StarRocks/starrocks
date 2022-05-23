// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/system/SystemInfoService.java

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

package com.starrocks.system;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DropBackendClause;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.cluster.Cluster;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend.BackendState;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SystemInfoService {
    private static final Logger LOG = LogManager.getLogger(SystemInfoService.class);

    public static final String DEFAULT_CLUSTER = "default_cluster";

    private volatile ImmutableMap<Long, Backend> idToBackendRef;
    private volatile ImmutableMap<Long, AtomicLong> idToReportVersionRef;

    // last backend id used by round robin for sequential choosing backends for
    // tablet creation
    private final ConcurrentHashMap<String, Long> lastBackendIdForCreationMap;
    // last backend id used by round robin for sequential choosing backends in
    // other jobs
    private final ConcurrentHashMap<String, Long> lastBackendIdForOtherMap;

    private long lastBackendIdForCreation = -1;
    private long lastBackendIdForOther = -1;

    private volatile ImmutableMap<Long, DiskInfo> pathHashToDishInfoRef;

    public SystemInfoService() {
        idToBackendRef = ImmutableMap.<Long, Backend>of();
        idToReportVersionRef = ImmutableMap.<Long, AtomicLong>of();

        lastBackendIdForCreationMap = new ConcurrentHashMap<String, Long>();
        lastBackendIdForOtherMap = new ConcurrentHashMap<String, Long>();
        pathHashToDishInfoRef = ImmutableMap.<Long, DiskInfo>of();
    }

    // for deploy manager
    public void addBackends(List<Pair<String, Integer>> hostPortPairs, boolean isFree) throws DdlException {
        addBackends(hostPortPairs, isFree, "");
    }

    /**
     * @param hostPortPairs : backend's host and port
     * @param isFree        : if true the backend is not owned by any cluster
     * @param destCluster   : if not null or empty backend will be added to destCluster
     * @throws DdlException
     */
    public void addBackends(List<Pair<String, Integer>> hostPortPairs,
                            boolean isFree, String destCluster) throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) != null) {
                throw new DdlException("Same backend already exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            addBackend(pair.first, pair.second, isFree, destCluster);
        }
    }

    // for test
    public void addBackend(Backend backend) {
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(backend.getId(), backend);
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.put(backend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);
    }

    private void setBackendOwner(Backend backend, String clusterName) {
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster(clusterName);
        Preconditions.checkState(cluster != null);
        cluster.addBackend(backend.getId());
        backend.setOwnerClusterName(clusterName);
        backend.setBackendState(BackendState.using);
    }

    // Final entry of adding backend
    private void addBackend(String host, int heartbeatPort, boolean isFree, String destCluster) throws DdlException {
        Backend newBackend = new Backend(GlobalStateMgr.getCurrentState().getNextId(), host, heartbeatPort);
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(newBackend.getId(), newBackend);
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);

        if (!Strings.isNullOrEmpty(destCluster)) {
            // add backend to destCluster
            setBackendOwner(newBackend, destCluster);
        } else if (!isFree) {
            // add backend to DEFAULT_CLUSTER
            setBackendOwner(newBackend, DEFAULT_CLUSTER);
        } else {
            // backend is free
        }

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logAddBackend(newBackend);
        LOG.info("finished to add {} ", newBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    public void dropBackends(DropBackendClause dropBackendClause) throws DdlException {
        List<Pair<String, Integer>> hostPortPairs = dropBackendClause.getHostPortPairs();
        boolean needCheckUnforce = !dropBackendClause.isOldStyle() && !dropBackendClause.isForce();

        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) == null) {
                throw new DdlException("backend does not exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            dropBackend(pair.first, pair.second, needCheckUnforce);
        }
    }

    // for decommission
    public void dropBackend(long backendId) throws DdlException {
        Backend backend = getBackend(backendId);
        if (backend == null) {
            throw new DdlException("Backend[" + backendId + "] does not exist");
        }

        dropBackend(backend.getHost(), backend.getHeartbeatPort(), false);
    }

    private void checkUnforce(Backend droppedBackend) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<Long> tabletIds = GlobalStateMgr.getCurrentInvertedIndex().getTabletIdsByBackendId(droppedBackend.getId());
        List<Long> dbs = globalStateMgr.getDbIds();

        dbs.stream().map(globalStateMgr::getDb).forEach(db -> {
            db.readLock();
            try {
                db.getTables().stream()
                        .filter(table -> table.getType() == Table.TableType.OLAP)
                        .map(table -> (OlapTable) table)
                        .filter(table -> table.getTableProperty().getReplicationNum() == 1)
                        .forEach(table -> {
                            table.getAllPartitions().forEach(partition -> {
                                String errMsg = String.format("Tables such as [%s.%s] on the backend[%s:%d]" +
                                                " have only one replica. To avoid data loss," +
                                                " please change the replication_num of [%s.%s] to three." +
                                                " ALTER SYSTEM DROP BACKEND <backends> FORCE" +
                                                " can be used to forcibly drop the backend. ",
                                        db.getFullName(), table.getName(), droppedBackend.getHost(),
                                        droppedBackend.getHeartbeatPort(), db.getFullName(), table.getName());

                                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)
                                        .forEach(rollupIdx -> {
                                            boolean existIntersection = rollupIdx.getTablets().stream()
                                                    .map(Tablet::getId).anyMatch(tabletIds::contains);

                                            if (existIntersection) {
                                                throw new RuntimeException(errMsg);
                                            }
                                        });
                            });
                        });
            } finally {
                db.readUnlock();
            }
        });
    }

    // final entry of dropping backend
    public void dropBackend(String host, int heartbeatPort, boolean needCheckUnforce) throws DdlException {
        if (getBackendWithHeartbeatPort(host, heartbeatPort) == null) {
            throw new DdlException("backend does not exists[" + host + ":" + heartbeatPort + "]");
        }

        Backend droppedBackend = getBackendWithHeartbeatPort(host, heartbeatPort);
        if (needCheckUnforce) {
            try {
                checkUnforce(droppedBackend);
            } catch (RuntimeException e) {
                throw new DdlException(e.getMessage());
            }
        }

        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.remove(droppedBackend.getId());
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.remove(droppedBackend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);

        // update cluster
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster(droppedBackend.getOwnerClusterName());
        if (null != cluster) {
            cluster.removeBackend(droppedBackend.getId());
        } else {
            LOG.error("Cluster " + droppedBackend.getOwnerClusterName() + " no exist.");
        }
        // log
        GlobalStateMgr.getCurrentState().getEditLog().logDropBackend(droppedBackend);
        LOG.info("finished to drop {}", droppedBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    // only for test
    public void dropAllBackend() {
        // update idToBackend
        idToBackendRef = ImmutableMap.<Long, Backend>of();
        // update idToReportVersion
        idToReportVersionRef = ImmutableMap.<Long, AtomicLong>of();
    }

    public Backend getBackend(long backendId) {
        return idToBackendRef.get(backendId);
    }

    public boolean checkBackendAvailable(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        return backend != null && backend.isAvailable();
    }

    public boolean checkBackendAlive(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        return backend != null && backend.isAlive();
    }

    public Backend getBackendWithHeartbeatPort(String host, int heartPort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getHeartbeatPort() == heartPort) {
                return backend;
            }
        }
        return null;
    }

    public Backend getBackendWithBePort(String host, int bePort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getBePort() == bePort) {
                return backend;
            }
        }
        return null;
    }

    public List<Long> getBackendIds(boolean needAlive) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        List<Long> backendIds = Lists.newArrayList(idToBackend.keySet());
        if (!needAlive) {
            return backendIds;
        } else {
            Iterator<Long> iter = backendIds.iterator();
            while (iter.hasNext()) {
                Backend backend = this.getBackend(iter.next());
                if (backend == null || !backend.isAlive()) {
                    iter.remove();
                }
            }
            return backendIds;
        }
    }

    public int backendSize() {
        return idToBackendRef.size();
    }

    public List<Long> getDecommissionedBackendIds() {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        List<Long> backendIds = Lists.newArrayList(idToBackend.keySet());

        Iterator<Long> iter = backendIds.iterator();
        while (iter.hasNext()) {
            Backend backend = this.getBackend(iter.next());
            if (backend == null || !backend.isDecommissioned()) {
                iter.remove();
            }
        }
        return backendIds;
    }

    /**
     * get cluster's backend id list
     *
     * @param name
     * @return
     */
    public List<Backend> getClusterBackends(String name) {
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        final List<Backend> ret = Lists.newArrayList();

        if (Strings.isNullOrEmpty(name)) {
            return ret;
        }

        for (Backend backend : copiedBackends.values()) {
            if (name.equals(backend.getOwnerClusterName())) {
                ret.add(backend);
            }
        }
        return ret;
    }

    /**
     * get cluster's backend id list
     *
     * @param clusterName
     * @return
     */
    public List<Long> getClusterBackendIds(String clusterName) {
        if (Strings.isNullOrEmpty(clusterName)) {
            return null;
        }

        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        final List<Long> beIds = Lists.newArrayList();

        for (Backend backend : idToBackend.values()) {
            if (clusterName.equals(backend.getOwnerClusterName())) {
                beIds.add(backend.getId());
            }
        }
        return beIds;
    }

    /**
     * get cluster's backend id list
     *
     * @param clusterName
     * @return
     */
    public List<Long> getClusterBackendIds(String clusterName, boolean needAlive) {
        final Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        final List<Long> ret = new ArrayList<Long>();

        if (Strings.isNullOrEmpty(clusterName)) {
            return null;
        }

        if (needAlive) {
            for (Backend backend : copiedBackends.values()) {
                if (backend != null && clusterName.equals(backend.getOwnerClusterName())
                        && backend.isAlive()) {
                    ret.add(backend.getId());
                }
            }
        } else {
            for (Backend backend : copiedBackends.values()) {
                if (clusterName.equals(backend.getOwnerClusterName())) {
                    ret.add(backend.getId());
                }
            }
        }

        return ret;
    }

    public List<Long> seqChooseBackendIdsByStorageMedium(int backendNum, boolean needAlive, boolean isCreate,
                                                         String clusterName, TStorageMedium storageMedium) {
        final List<Backend> backends =
                getClusterBackends(clusterName).stream().filter(v -> !v.diskExceedLimitByStorageMedium(storageMedium))
                        .collect(Collectors.toList());
        return seqChooseBackendIds(backendNum, needAlive, isCreate, clusterName, backends);
    }

    public List<Long> seqChooseBackendIds(int backendNum, boolean needAlive, boolean isCreate,
                                          String clusterName) {
        final List<Backend> backends =
                getClusterBackends(clusterName).stream().filter(v -> !v.diskExceedLimit()).collect(Collectors.toList());
        return seqChooseBackendIds(backendNum, needAlive, isCreate, clusterName, backends);
    }

    // choose backends by round-robin
    // return null if not enough backend
    // use synchronized to run serially
    public synchronized List<Long> seqChooseBackendIds(int backendNum, boolean needAlive, boolean isCreate,
                                                       String clusterName, final List<Backend> srcBackends) {
        long lastBackendId;

        if (clusterName.equals(DEFAULT_CLUSTER)) {
            if (isCreate) {
                lastBackendId = lastBackendIdForCreation;
            } else {
                lastBackendId = lastBackendIdForOther;
            }
        } else {
            if (isCreate) {
                if (lastBackendIdForCreationMap.containsKey(clusterName)) {
                    lastBackendId = lastBackendIdForCreationMap.get(clusterName);
                } else {
                    lastBackendId = -1;
                    lastBackendIdForCreationMap.put(clusterName, lastBackendId);
                }
            } else {
                if (lastBackendIdForOtherMap.containsKey(clusterName)) {
                    lastBackendId = lastBackendIdForOtherMap.get(clusterName);
                } else {
                    lastBackendId = -1;
                    lastBackendIdForOtherMap.put(clusterName, lastBackendId);
                }
            }
        }

        // host -> BE list
        Map<String, List<Backend>> backendMaps = Maps.newHashMap();
        for (Backend backend : srcBackends) {
            if (backendMaps.containsKey(backend.getHost())) {
                backendMaps.get(backend.getHost()).add(backend);
            } else {
                List<Backend> list = Lists.newArrayList();
                list.add(backend);
                backendMaps.put(backend.getHost(), list);
            }
        }

        // if more than one backend exists in same host, select a backend at random
        List<Backend> backends = Lists.newArrayList();
        for (List<Backend> list : backendMaps.values()) {
            Collections.shuffle(list);
            backends.add(list.get(0));
        }

        List<Long> backendIds = Lists.newArrayList();
        // get last backend index
        int lastBackendIndex = -1;
        int index = -1;
        for (Backend backend : backends) {
            index++;
            if (backend.getId() == lastBackendId) {
                lastBackendIndex = index;
                break;
            }
        }
        Iterator<Backend> iterator = Iterators.cycle(backends);
        index = -1;
        boolean failed = false;
        // 2 cycle at most
        int maxIndex = 2 * backends.size();
        while (iterator.hasNext() && backendIds.size() < backendNum) {
            Backend backend = iterator.next();
            index++;
            if (index <= lastBackendIndex) {
                continue;
            }

            if (index > maxIndex) {
                failed = true;
                break;
            }

            if (needAlive) {
                if (!backend.isAlive() || backend.isDecommissioned()) {
                    continue;
                }
            }

            long backendId = backend.getId();
            if (!backendIds.contains(backendId)) {
                backendIds.add(backendId);
                lastBackendId = backendId;
            } else {
                failed = true;
                break;
            }
        }

        if (clusterName.equals(DEFAULT_CLUSTER)) {
            if (isCreate) {
                lastBackendIdForCreation = lastBackendId;
            } else {
                lastBackendIdForOther = lastBackendId;
            }
        } else {
            // update last backendId
            if (isCreate) {
                lastBackendIdForCreationMap.put(clusterName, lastBackendId);
            } else {
                lastBackendIdForOtherMap.put(clusterName, lastBackendId);
            }
        }
        if (backendIds.size() != backendNum) {
            failed = true;
        }

        if (!failed) {
            return backendIds;
        }

        // debug
        for (Backend backend : backends) {
            LOG.debug("random select: {}", backend.toString());
        }

        return null;
    }

    public ImmutableMap<Long, Backend> getIdToBackend() {
        return idToBackendRef;
    }

    public ImmutableMap<Long, Backend> getBackendsInCluster(String cluster) {
        if (Strings.isNullOrEmpty(cluster)) {
            return idToBackendRef;
        }

        Map<Long, Backend> retMaps = Maps.newHashMap();
        for (Backend backend : idToBackendRef.values().asList()) {
            if (cluster.equals(backend.getOwnerClusterName())) {
                retMaps.put(backend.getId(), backend);
            }
        }
        return ImmutableMap.copyOf(retMaps);
    }

    public long getBackendReportVersion(long backendId) {
        AtomicLong atomicLong;
        if ((atomicLong = idToReportVersionRef.get(backendId)) == null) {
            return -1L;
        } else {
            return atomicLong.get();
        }
    }

    public void updateBackendReportVersion(long backendId, long newReportVersion, long dbId) {
        AtomicLong atomicLong = null;
        if ((atomicLong = idToReportVersionRef.get(backendId)) != null) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db != null) {
                db.readLock();
                try {
                    atomicLong.set(newReportVersion);
                    LOG.debug("update backend {} report version: {}, db: {}", backendId, newReportVersion, dbId);
                } finally {
                    db.readUnlock();
                }
            } else {
                LOG.warn("failed to update backend report version, db {} does not exist", dbId);
            }
        } else {
            LOG.warn("failed to update backend report version, backend {} does not exist", backendId);
        }
    }

    public long saveBackends(DataOutputStream dos, long checksum) throws IOException {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        int backendCount = idToBackend.size();
        checksum ^= backendCount;
        dos.writeInt(backendCount);
        for (Map.Entry<Long, Backend> entry : idToBackend.entrySet()) {
            long key = entry.getKey();
            checksum ^= key;
            dos.writeLong(key);
            entry.getValue().write(dos);
        }
        return checksum;
    }

    public long loadBackends(DataInputStream dis, long checksum) throws IOException {
        int count = dis.readInt();
        checksum ^= count;
        for (int i = 0; i < count; i++) {
            long key = dis.readLong();
            checksum ^= key;
            Backend backend = Backend.read(dis);
            replayAddBackend(backend);
        }
        return checksum;
    }

    public void clear() {
        this.idToBackendRef = null;
        this.idToReportVersionRef = null;
    }

    public static Pair<String, Integer> validateHostAndPort(String hostPort) throws AnalysisException {
        hostPort = hostPort.replaceAll("\\s+", "");
        if (hostPort.isEmpty()) {
            throw new AnalysisException("Invalid host port: " + hostPort);
        }

        String[] pair = hostPort.split(":");
        if (pair.length != 2) {
            throw new AnalysisException("Invalid host port: " + hostPort);
        }

        String host = pair[0];
        if (Strings.isNullOrEmpty(host)) {
            throw new AnalysisException("Host is null");
        }

        int heartbeatPort = -1;
        try {
            // validate host
            if (!InetAddressValidator.getInstance().isValid(host)) {
                // maybe this is a hostname
                // if no IP address for the host could be found, 'getByName'
                // will throw
                // UnknownHostException
                InetAddress inetAddress = InetAddress.getByName(host);
                host = inetAddress.getHostAddress();
            }

            // validate port
            heartbeatPort = Integer.parseInt(pair[1]);

            if (heartbeatPort <= 0 || heartbeatPort >= 65536) {
                throw new AnalysisException("Port is out of range: " + heartbeatPort);
            }

            return new Pair<String, Integer>(host, heartbeatPort);
        } catch (UnknownHostException e) {
            throw new AnalysisException("Unknown host: " + e.getMessage());
        } catch (Exception e) {
            throw new AnalysisException("Encounter unknown exception: " + e.getMessage());
        }
    }

    public void replayAddBackend(Backend newBackend) {
        // update idToBackend
        if (GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_30) {
            newBackend.setOwnerClusterName(DEFAULT_CLUSTER);
            newBackend.setBackendState(BackendState.using);
        }
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(newBackend.getId(), newBackend);
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.put(newBackend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);

        // to add be to DEFAULT_CLUSTER
        if (newBackend.getBackendState() == BackendState.using) {
            final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster(DEFAULT_CLUSTER);
            if (null != cluster) {
                // replay log
                cluster.addBackend(newBackend.getId());
            } else {
                // This happens in loading image when fe is restarted, because loadCluster is after loadBackend,
                // cluster is not created. Be in cluster will be updated in loadCluster.
            }
        }
    }

    public void replayDropBackend(Backend backend) {
        LOG.debug("replayDropBackend: {}", backend);
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.remove(backend.getId());
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.remove(backend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);

        // update cluster
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster(backend.getOwnerClusterName());
        if (null != cluster) {
            cluster.removeBackend(backend.getId());
        } else {
            LOG.error("Cluster " + backend.getOwnerClusterName() + " no exist.");
        }
    }

    public void updateBackendState(Backend be) {
        long id = be.getId();
        Backend memoryBe = getBackend(id);
        if (memoryBe == null) {
            // backend may already be dropped. this may happen when
            // 1. SystemHandler drop the decommission backend
            // 2. at same time, user try to cancel the decommission of that backend.
            // These two operations do not guarantee the order.
            return;
        }
        memoryBe.setBePort(be.getBePort());
        memoryBe.setAlive(be.isAlive());
        memoryBe.setDecommissioned(be.isDecommissioned());
        memoryBe.setHttpPort(be.getHttpPort());
        memoryBe.setBeRpcPort(be.getBeRpcPort());
        memoryBe.setBrpcPort(be.getBrpcPort());
        memoryBe.setLastUpdateMs(be.getLastUpdateMs());
        memoryBe.setLastStartTime(be.getLastStartTime());
        memoryBe.setDisks(be.getDisks());
        memoryBe.setBackendState(be.getBackendState());
        memoryBe.setOwnerClusterName(be.getOwnerClusterName());
        memoryBe.setDecommissionType(be.getDecommissionType());
    }

    private long getClusterAvailableCapacityB(String clusterName) {
        List<Backend> clusterBackends = getClusterBackends(clusterName);
        long capacity = 0L;
        for (Backend backend : clusterBackends) {
            // Here we do not check if backend is alive,
            // We suppose the dead backends will back to alive later.
            if (backend.isDecommissioned()) {
                // Data on decommissioned backend will move to other backends,
                // So we need to minus size of those data.
                capacity -= backend.getDataUsedCapacityB();
            } else {
                capacity += backend.getAvailableCapacityB();
            }
        }
        return capacity;
    }

    public void checkClusterCapacity(String clusterName) throws DdlException {
        if (getClusterAvailableCapacityB(clusterName) <= 0L) {
            throw new DdlException("Cluster " + clusterName + " has no available capacity");
        }
    }

    /*
     * Try to randomly get a backend id by given host.
     * If not found, return -1
     */
    public long getBackendIdByHost(String host) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        List<Backend> selectedBackends = Lists.newArrayList();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host)) {
                selectedBackends.add(backend);
            }
        }

        if (selectedBackends.isEmpty()) {
            return -1L;
        }

        Collections.shuffle(selectedBackends);
        return selectedBackends.get(0).getId();
    }

    public Set<String> getClusterNames() {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        Set<String> clusterNames = Sets.newHashSet();
        for (Backend backend : idToBackend.values()) {
            if (!Strings.isNullOrEmpty(backend.getOwnerClusterName())) {
                clusterNames.add(backend.getOwnerClusterName());
            }
        }
        return clusterNames;
    }

    /*
     * Check if the specified disks' capacity has reached the limit.
     * bePathsMap is (BE id -> list of path hash)
     * If floodStage is true, it will check with the floodStage threshold.
     *
     * return Status.OK if not reach the limit
     */
    public Status checkExceedDiskCapacityLimit(Multimap<Long, Long> bePathsMap, boolean floodStage) {
        LOG.debug("pathBeMap: {}", bePathsMap);
        ImmutableMap<Long, DiskInfo> pathHashToDiskInfo = pathHashToDishInfoRef;
        for (Long beId : bePathsMap.keySet()) {
            for (Long pathHash : bePathsMap.get(beId)) {
                DiskInfo diskInfo = pathHashToDiskInfo.get(pathHash);
                if (diskInfo != null && diskInfo.exceedLimit(floodStage)) {
                    return new Status(TStatusCode.CANCELLED,
                            "disk " + pathHash + " on backend " + beId + " exceed limit usage");
                }
            }
        }
        return Status.OK;
    }

    // update the path info when disk report
    // there is only one thread can update path info, so no need to worry about concurrency control
    public void updatePathInfo(List<DiskInfo> addedDisks, List<DiskInfo> removedDisks) {
        Map<Long, DiskInfo> copiedPathInfos = Maps.newHashMap(pathHashToDishInfoRef);
        for (DiskInfo diskInfo : addedDisks) {
            copiedPathInfos.put(diskInfo.getPathHash(), diskInfo);
        }
        for (DiskInfo diskInfo : removedDisks) {
            copiedPathInfos.remove(diskInfo.getPathHash());
        }
        ImmutableMap<Long, DiskInfo> newPathInfos = ImmutableMap.copyOf(copiedPathInfos);
        pathHashToDishInfoRef = newPathInfos;
        LOG.debug("update path infos: {}", newPathInfos);
    }
}

