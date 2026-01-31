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


package com.staros.manager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.staros.exception.ExceptionCode;
import com.staros.exception.InternalErrorStarException;
import com.staros.exception.InvalidArgumentStarException;
import com.staros.exception.NoAliveWorkersException;
import com.staros.exception.NotExistStarException;
import com.staros.exception.NotLeaderStarException;
import com.staros.exception.StarException;
import com.staros.filestore.FilePath;
import com.staros.filestore.FileStore;
import com.staros.filestore.FileStoreMgr;
import com.staros.heartbeat.HeartbeatManager;
import com.staros.journal.DelegateJournalSystem;
import com.staros.journal.DummyJournalSystem;
import com.staros.journal.Journal;
import com.staros.journal.JournalReplayer;
import com.staros.journal.JournalSystem;
import com.staros.journal.StarMgrJournal;
import com.staros.metrics.MetricsSystem;
import com.staros.proto.CreateMetaGroupInfo;
import com.staros.proto.CreateShardGroupInfo;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.CreateShardJournalInfo;
import com.staros.proto.DeleteShardGroupInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.LeaderInfo;
import com.staros.proto.MetaGroupInfo;
import com.staros.proto.MetaGroupJournalInfo;
import com.staros.proto.ReplicaInfo;
import com.staros.proto.ReplicaState;
import com.staros.proto.ReplicaUpdateInfo;
import com.staros.proto.ReplicationType;
import com.staros.proto.SectionType;
import com.staros.proto.ServiceInfo;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.ShardInfoList;
import com.staros.proto.ShardReportInfo;
import com.staros.proto.StarManagerImageMetaFooter;
import com.staros.proto.StarManagerImageMetaHeader;
import com.staros.proto.UpdateMetaGroupInfo;
import com.staros.proto.UpdateShardGroupInfo;
import com.staros.proto.UpdateShardInfo;
import com.staros.proto.UpdateWorkerGroupInfo;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerInfo;
import com.staros.schedule.Scheduler;
import com.staros.schedule.ShardSchedulerV2;
import com.staros.section.Section;
import com.staros.section.SectionReader;
import com.staros.section.SectionWriter;
import com.staros.service.Service;
import com.staros.service.ServiceManager;
import com.staros.service.ServiceTemplate;
import com.staros.shard.Shard;
import com.staros.shard.ShardChecker;
import com.staros.shard.ShardGroup;
import com.staros.shard.ShardManager;
import com.staros.util.Config;
import com.staros.util.Constant;
import com.staros.util.IdGenerator;
import com.staros.util.LockCloseable;
import com.staros.util.LogRateLimiter;
import com.staros.util.LogUtils;
import com.staros.util.ReplicaStateComparator;
import com.staros.util.Utils;
import com.staros.worker.Worker;
import com.staros.worker.WorkerGroup;
import com.staros.worker.WorkerManager;
import io.prometheus.metrics.core.datapoints.DistributionDataPoint;
import io.prometheus.metrics.core.datapoints.Timer;
import io.prometheus.metrics.core.metrics.Histogram;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class StarManager {
    public static final byte[] IMAGE_META_MAGIC_BYTES = "STARMGR1".getBytes(StandardCharsets.UTF_8);

    private static final Logger LOG = LogManager.getLogger(StarManager.class);
    // allow at most LOG_LIMIT records per second
    private static final double LOG_LIMIT = 1;
    private static final LogRateLimiter LOG_LIMITER = new LogRateLimiter(LOG, LOG_LIMIT);
    // For Metrics
    private static final Histogram HISTOGRAM_WORKER_HEARTBEAT =
            MetricsSystem.registerHistogram("starmgr_process_worker_heartbeat",
                    "Histogram stats for starmgr processing worker heartbeat latency",
                    Lists.newArrayList("service_id"));

    private AtomicBoolean isLeader;

    private ServiceManager serviceManager;
    private WorkerManager workerManager;
    private ShardSchedulerV2 shardScheduler;
    private ShardChecker shardChecker;
    private HeartbeatManager heartbeatManager;
    private JournalReplayer journalReplayer;
    private JournalSystem journalSystem;
    private IdGenerator idGenerator;

    // current instance's listening info, allow the followers to know where the leader is
    private InetSocketAddress listenAddress;
    // The leader's info if current instance is a FOLLOWER
    private LeaderInfo leaderInfo = LeaderInfo.newBuilder().build();

    // FOR TEST
    public StarManager() {
        this(new DummyJournalSystem());
    }

    public StarManager(JournalSystem journalSystem) {
        this.journalSystem = new DelegateJournalSystem(journalSystem);
        this.idGenerator = new IdGenerator(this.journalSystem);
        isLeader = new AtomicBoolean(false);
        serviceManager = new ServiceManager(this.journalSystem, idGenerator);
        workerManager = new WorkerManager(this.journalSystem, idGenerator);
        shardScheduler = new ShardSchedulerV2(serviceManager, workerManager);
        shardChecker = new ShardChecker(serviceManager, workerManager, shardScheduler);
        heartbeatManager = new HeartbeatManager(workerManager);
        journalReplayer = new JournalReplayer(this);
        serviceManager.setShardScheduler(shardScheduler);
    }

    // pre check, mostly used to check if we can accept non-read request
    private void checkLeader() throws StarException {
        if (!isLeader.get()) {
            // leaderInfo can be null, I may not know who is the leader yet.
            String leaderMsg = leaderInfo.getHost().isEmpty() ? "unknown leader" : leaderInfo.toString();
            throw new NotLeaderStarException(leaderInfo,
                    "request rejected, current star manager is not leader. Current leader: " + leaderMsg);
        }
    }

    public void setListenAddressInfo(String address, int port) {
        listenAddress = InetSocketAddress.createUnresolved(address, port);
    }

    public void becomeLeader() {
        journalSystem.onBecomeLeader();

        isLeader.set(true);

        startBackgroundThreads();

        // reset leader info to myself since I am the leader now
        leaderInfo = myLeaderInfo();
        // write LEADER_CHANGE journal to let the FOLLOWERS know my location
        Journal journal = StarMgrJournal.logLeaderInfo(leaderInfo);
        journalSystem.write(journal);

        LOG.info("star manager background threads start, now is leader.");
    }

    public void becomeFollower() {
        journalSystem.onBecomeFollower();

        isLeader.set(false);

        stopBackgroundThreads();

        // leaderInfo will be obtained either from image load or from LEADER_CHANGE edit log
        LOG.info("star manager background threads stop, now is follower.");
    }

    public void startBackgroundThreads() {
        workerManager.start();
        shardScheduler.start();
        shardChecker.start();
        heartbeatManager.start();
    }

    public void stopBackgroundThreads() {
        heartbeatManager.stop();
        shardChecker.stop();
        shardScheduler.stop();
        workerManager.stop();
    }

    public void registerService(String serviceTemplateName, List<String> serviceComponents) throws StarException {
        checkLeader();

        serviceManager.registerService(serviceTemplateName, serviceComponents);
    }

    public void deregisterService(String serviceTemplateName) throws StarException {
        checkLeader();

        serviceManager.deregisterService(serviceTemplateName);
    }

    public String bootstrapService(String serviceTemplateName, String serviceName) throws StarException {
        checkLeader();

        String serviceId = serviceManager.bootstrapService(serviceTemplateName, serviceName);

        workerManager.bootstrapService(serviceId);

        return serviceId;
    }

    public void shutdownService(String serviceId) throws StarException {
        checkLeader();

        serviceManager.shutdownService(serviceId);
    }

    public ServiceInfo getServiceInfoById(String serviceId) throws StarException {
        return serviceManager.getServiceInfoById(serviceId);
    }

    public ServiceInfo getServiceInfoByName(String serviceName) throws StarException {
        return serviceManager.getServiceInfoByName(serviceName);
    }

    public long createWorkerGroup(String serviceId, String owner, WorkerGroupSpec spec, Map<String, String> labels,
                                  Map<String, String> properties, int replicaNumber,
                                  ReplicationType replicationType, WarmupLevel warmupLevel) throws StarException {
        checkLeader();
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new NotExistStarException("service {} not exist.", serviceId);
            }
            return workerManager.createWorkerGroup(serviceId, owner, spec, labels, properties, replicaNumber,
                    replicationType, warmupLevel);
        }
    }

    public List<WorkerGroupDetailInfo> listWorkerGroups(String serviceId, List<Long> groupIds,
                                                        Map<String, String> filterLabelsMap, boolean includeWorkersInfo) {
        if (!groupIds.isEmpty() && !filterLabelsMap.isEmpty()) {
            throw new InvalidArgumentStarException("Can only specify group id list or filtering labels but not both!");
        }
        if (!groupIds.isEmpty()) {
            return workerManager.listWorkerGroupsById(serviceId, groupIds, includeWorkersInfo);
        } else {
            // both empty, also fallback into this branch, take it as list all
            return workerManager.listWorkerGroups(serviceId, filterLabelsMap, includeWorkersInfo);
        }
    }

    public void deleteWorkerGroup(String serviceId, long groupId) throws StarException {
        checkLeader();
        workerManager.deleteWorkerGroup(serviceId, groupId);
    }

    public void updateWorkerGroup(String serviceId, long groupId, WorkerGroupSpec spec, Map<String, String> labels,
                                  Map<String, String> props, int replicaNumber, ReplicationType replicationType,
                                  WarmupLevel warmupLevel)
            throws StarException {
        checkLeader();
        workerManager.updateWorkerGroup(serviceId, groupId, spec, labels, props, replicaNumber, replicationType, warmupLevel);
    }

    public long addWorker(String serviceId, long groupId, String ipPort) throws StarException {
        checkLeader();
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new NotExistStarException("service {} not exist.", serviceId);
            }
            return workerManager.addWorker(serviceId, groupId, ipPort);
        }
    }

    public void removeWorker(String serviceId, long groupId, long workerId) throws StarException {
        checkLeader();

        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }

            workerManager.removeWorker(serviceId, groupId, workerId);
        }
    }

    public WorkerInfo getWorkerInfo(String serviceId, long workerId) throws StarException {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }

            WorkerInfo workerInfo = workerManager.getWorkerInfo(workerId);
            if (!workerInfo.getServiceId().equals(serviceId)) {
                throw new StarException(ExceptionCode.INVALID_ARGUMENT,
                        String.format("worker %d not belong to service %s.",
                        workerId, serviceId));
            }

            return workerInfo;
        }
    }

    public WorkerInfo getWorkerInfo(String serviceId, String ipPort) throws StarException {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }

            WorkerInfo workerInfo = workerManager.getWorkerInfo(ipPort);
            if (!workerInfo.getServiceId().equals(serviceId)) {
                throw new StarException(ExceptionCode.INVALID_ARGUMENT,
                        String.format("worker %s not belong to service %s.",
                        ipPort, serviceId));
            }

            return workerInfo;
        }
    }

    public List<ShardInfo> createShard(String serviceId,
            List<CreateShardInfo> createShardInfos) throws StarException {
        checkLeader();

        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            FileStoreMgr fsMgr = getFileStoreMgr(serviceId);

            return shardManager.createShard(createShardInfos, fsMgr);
        }
    }

    public void deleteShard(String serviceId, List<Long> shardIds) throws StarException {
        checkLeader();

        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }

            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            shardManager.deleteShard(shardIds);

            // just let shard scheduler do remove shard, do nothing here
        }
    }

    public void updateShard(String serviceId, List<UpdateShardInfo> updateShardInfos) throws StarException {
        checkLeader();

        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            shardManager.updateShard(updateShardInfos);
        }
    }

    private List<ShardInfo> gatherWorkerInfoForShard(String serviceId, List<ShardInfo> shardInfos, long workerGroupId)
            throws StarException {
        List<Long> targetWorkerIds = Collections.emptyList();
        if (workerGroupId == Constant.DEFAULT_ID && !Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY) {
            try {
                // if the service has no default worker group yet, NotExistStarException will be thrown.
                targetWorkerIds = workerManager.getDefaultWorkerGroup(serviceId).getAllWorkerIds(true);
            } catch (StarException exception) {
                // log exception and ignore it
                LOG.info("Fail to get default worker group for service:{}, error:{}.", serviceId, exception.getMessage());
            }
        } else {
            targetWorkerIds = workerManager.getWorkerGroup(serviceId, workerGroupId).getAllWorkerIds(true);
        }

        // cache WorkerInfo into map to avoid fetch from workerManager repeatedly
        Map<Long, WorkerInfo> workerInfoMap = new HashMap<>();
        List<ShardInfo> shardInfosFinal = new ArrayList<>(shardInfos.size());

        for (ShardInfo shardInfo : shardInfos) {
            List<ReplicaInfo> validReplicaInfos =
                    filterShardReplicaInfoByState(shardInfo.getReplicaInfoList(), targetWorkerIds);
            // fill in the worker info with the valid replicaInfo
            ShardInfo.Builder shardInfoBuilder = shardInfo.toBuilder().clearReplicaInfo();
            for (ReplicaInfo replicaInfo : validReplicaInfos) {
                try {
                    long workerId = replicaInfo.getWorkerInfo().getWorkerId();
                    WorkerInfo workerInfo = workerInfoMap.get(workerId);
                    if (workerInfo == null) {
                        // with exception with not exist if the workerId get deleted.
                        workerInfo = workerManager.getWorkerInfo(workerId);
                        workerInfoMap.put(workerId, workerInfo);
                    }
                    ReplicaInfo replicaInfoFinal = replicaInfo.toBuilder()
                            .setWorkerInfo(workerInfo)
                            .build();
                    shardInfoBuilder.addReplicaInfo(replicaInfoFinal);
                } catch (NotExistStarException e) {
                    // Ignore the exception
                } catch (StarException e) {
                    if (e.getExceptionCode() != ExceptionCode.NOT_EXIST) {
                        // Just for backwards compatibility if some of the codes still throw StarException+ErrorCode
                        throw e;
                    }
                }
            }
            shardInfosFinal.add(shardInfoBuilder.build());
        }
        return shardInfosFinal;
    }

    private List<ReplicaInfo> filterShardReplicaInfoByState(List<ReplicaInfo> replicaInfoList, List<Long> aliveWorkers) {
        // filter out unrelated workers and unhealthy workers
        // sort by replica State in the order of (OK < SCALE_IN < SCALE_OUT)
        List<ReplicaInfo> remainReplicas = replicaInfoList.stream()
                .filter(x -> aliveWorkers.contains(x.getWorkerInfo().getWorkerId()))
                .sorted((c1, c2) -> ReplicaStateComparator.compare(c1.getReplicaState(), c2.getReplicaState()))
                .collect(Collectors.toList());
        List<ReplicaInfo> finalReplicas = new ArrayList<>();
        for (ReplicaInfo replicaInfo : remainReplicas) {
            if (replicaInfo.getReplicaState() == ReplicaState.REPLICA_SCALE_OUT) {
                if (finalReplicas.isEmpty()) {
                    // add at most one replica with REPLICA_SCALE_OUT state if the candidate list is still empty
                    finalReplicas.add(replicaInfo);
                }
                break;
            } else {
                finalReplicas.add(replicaInfo);
            }
        }
        return finalReplicas;
    }

    public List<ShardInfo> getShardInfo(String serviceId, List<Long> shardIds, long workerGroupId) throws StarException {
        ShardManager shardManager;
        try (LockCloseable ignored = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }

            shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
        }
        List<ShardInfo> shardInfoList = gatherWorkerInfoForShard(serviceId, shardManager.getShardInfo(shardIds), workerGroupId);
        if (checkReplicaAndMayTriggerSchedule(serviceId, shardInfoList, workerGroupId)) {
            // schedule event triggered, try to collect replica info again
            shardInfoList = gatherWorkerInfoForShard(serviceId, shardManager.getShardInfo(shardIds), workerGroupId);
        }
        return shardInfoList;
    }

    public List<ShardInfoList> listShardInfo(String serviceId, List<Long> groupIds, long workerGroupId,
            boolean withoutReplicaInfo) throws StarException {
        ShardManager shardManager;
        try (LockCloseable ignored = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }

            shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
        }
        List<ShardInfoList> results = new ArrayList<>(groupIds.size());
        for (long groupId : groupIds) {
            List<Long> singleGroupList = Collections.singletonList(groupId);
            List<ShardInfo> shardInfoList = shardManager.listShardInfo(singleGroupList, withoutReplicaInfo).get(0);
            if (withoutReplicaInfo) {
                results.add(ShardInfoList.newBuilder().addAllShardInfos(shardInfoList).build());
            } else {
                List<ShardInfo> updatedShardInfoList = gatherWorkerInfoForShard(serviceId, shardInfoList, workerGroupId);
                if (checkReplicaAndMayTriggerSchedule(serviceId, updatedShardInfoList, workerGroupId)) {
                    // schedule event triggered, try to collect the replica info again
                    shardInfoList = shardManager.listShardInfo(singleGroupList, withoutReplicaInfo).get(0);
                    updatedShardInfoList = gatherWorkerInfoForShard(serviceId, shardInfoList, workerGroupId);
                }
                results.add(ShardInfoList.newBuilder().addAllShardInfos(updatedShardInfoList).build());
            }
        }
        return results;
    }

    /**
     * check shardInfo replica if meets the expectedCount and trigger schedule as necessary in sync/async mode
     *
     * @param serviceId     service Id
     * @param shardInfoList list of shard info
     * @param workerGroupId target worker group id
     * @return Whether scheduler events are triggered
     * @throws StarException
     */
    @VisibleForTesting
    public boolean checkReplicaAndMayTriggerSchedule(String serviceId, List<ShardInfo> shardInfoList,
                                                      long workerGroupId) throws StarException {
        List<Long> shardToSchedule = shardInfoList.stream()
                .filter(x -> x.getReplicaInfoCount() == 0)
                .map(ShardInfo::getShardId)
                .collect(Collectors.toList());
        if (!shardToSchedule.isEmpty()) { // synchronize schedule and wait for schedule result
            if (!isLeader.get()) {
                LOG_LIMITER.info("Need on-demand schedule {} to workerGroup:{}, but this is NOT the leader",
                        shardToSchedule, workerGroupId);
                // check Leader will throw NotLeaderStarException with current leaderInfo.
                checkLeader();
                return false;
            }
            try {
                shardScheduler.scheduleAddToGroup(serviceId, shardToSchedule, workerGroupId);
            } catch (NoAliveWorkersException exception) {
                // limit the log output if the workerGroup doesn't contain any alive workers, known for sure it will fail.
                LOG_LIMITER.info("On-demand schedule {} failed. Error: {}", shardToSchedule, exception.getMessage());
                return false;
            } catch (StarException exception) {
                LOG_LIMITER.info("On-demand schedule {} to workerGroup:{} failed. Error:{}", shardToSchedule, workerGroupId,
                        exception.getMessage());
            }
            return true;
        }
        return false;
    }

    public List<ShardGroupInfo> createShardGroup(String serviceId,
            List<CreateShardGroupInfo> createShardGroupInfos) throws StarException {
        checkLeader();

        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            return shardManager.createShardGroup(createShardGroupInfos);
        }
    }

    public void deleteShardGroup(String serviceId, List<Long> groupIds, boolean deleteShards) throws StarException {
        checkLeader();

        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            shardManager.deleteShardGroup(groupIds, deleteShards);
        }
    }

    public void updateShardGroup(String serviceId, List<UpdateShardGroupInfo> updateShardGroupInfos) throws StarException {
        checkLeader();

        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            shardManager.updateShardGroup(updateShardGroupInfos);
        }
    }

    public Pair<List<ShardGroupInfo>, Long> listShardGroupInfo(String serviceId, boolean includeAnonymousGroup, long startGroupId)
            throws StarException {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }

            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            return shardManager.listShardGroupInfo(includeAnonymousGroup, startGroupId);
        }
    }

    public List<ShardGroupInfo> getShardGroupInfo(String serviceId, List<Long> shardGroupIds) throws StarException {
        ShardManager shardManager;
        try (LockCloseable ignored = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }

            shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
        }
        return shardManager.getShardGroupInfo(shardGroupIds);
    }

    public MetaGroupInfo createMetaGroup(String serviceId, CreateMetaGroupInfo createMetaGroupInfo) throws StarException {
        checkLeader();

        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            return shardManager.createMetaGroup(createMetaGroupInfo);
        }
    }

    public void deleteMetaGroup(String serviceId, long metaGroupId) throws StarException {
        checkLeader();

        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            shardManager.deleteMetaGroup(metaGroupId);
        }
    }

    public void updateMetaGroup(String serviceId, UpdateMetaGroupInfo updateMetaGroupInfo) throws StarException {
        checkLeader();

        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            shardManager.updateMetaGroup(updateMetaGroupInfo);
        }
    }

    public MetaGroupInfo getMetaGroupInfo(String serviceId, long metaGroupId) throws StarException {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            return shardManager.getMetaGroupInfo(metaGroupId);
        }
    }

    public List<MetaGroupInfo> listMetaGroupInfo(String serviceId) throws StarException {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }

            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            return shardManager.listMetaGroupInfo();
        }
    }

    public boolean queryMetaGroupStable(String serviceId, long metaGroupId, long workerGroupId) throws StarException {
        ShardManager shardManager;
        List<Long> workerIds;
        try (LockCloseable ignored = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }
            if (workerGroupId == Constant.DEFAULT_ID) {
                workerIds = workerManager.getDefaultWorkerGroup(serviceId).getAllWorkerIds(false);
            } else {
                workerIds = workerManager.getWorkerGroup(serviceId, workerGroupId).getAllWorkerIds(false);
            }
            shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
        }
        return shardManager.isMetaGroupStable(metaGroupId, workerIds);
    }

    public void processWorkerHeartbeat(String serviceId, long workerId, long startTime, Map<String, String> workerProperties,
                                       List<Long> shardIds, List<ShardReportInfo> shardReportInfos) {
        // check leader first, error out at the earliest
        checkLeader();
        long currentTime = System.currentTimeMillis();
        // send the heavy work to thread pool. don't occupy the grpc worker thread for long time.
        workerManager.submitAsyncTask(
                () -> processWorkerHeartbeatAsync(serviceId, workerId, startTime, workerProperties, shardIds,
                        shardReportInfos, currentTime));
    }

    @VisibleForTesting
    void processWorkerHeartbeatAsync(String serviceId, long workerId, long startTime,
                                             Map<String, String> workerProperties, List<Long> shardIds,
                                             List<ShardReportInfo> shardReportInfos, long timestamp) {
        if (!isLeader.get()) {
            return;
        }

        DistributionDataPoint dp = HISTOGRAM_WORKER_HEARTBEAT.labelValues(serviceId);
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock()); Timer timer = dp.startTimer()) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST,
                        String.format("service %s not exist.", serviceId));
            }

            LOG.debug("process heartbeat from worker {} for service {}.", workerId, serviceId);

            boolean isRestart = workerManager.processWorkerHeartbeat(serviceId, workerId, startTime,
                    shardIds.size(), workerProperties, timestamp);

            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            if (isRestart) {
                shardManager.scheduleShardsBelongToWorker(workerId);
            }
            // validate and possibly remove invalid replicas from the worker
            shardManager.validateWorkerReportedReplicas(shardIds, workerId);
            checkAndUpdateShard(serviceManager, serviceId, workerId, shardReportInfos);
        } catch (Throwable throwable) { // Swallow any exception
            LOG.info("Async processing worker heartbeat failed with exception:{}, message: {}",
                    throwable.getClass().getName(), throwable.getMessage());
        }
    }

    protected void checkAndUpdateShard(ServiceManager serviceManager, String serviceId, long workerId,
                                    List<ShardReportInfo> shardReportInfos) {
        FileStoreMgr fsMgr = serviceManager.getFileStoreMgr(serviceId);
        Preconditions.checkNotNull(fsMgr);
        ShardManager shardManager = serviceManager.getShardManagerNoLock(serviceId);
        Preconditions.checkNotNull(shardManager);

        Worker worker = workerManager.getWorker(workerId);
        if (worker == null) {
            throw new NotExistStarException("Worker:{} does not exist", workerId);
        }
        // NotExistStarException will be thrown if the WorkerGroup doesn't exist
        WorkerGroup wg = workerManager.getWorkerGroup(serviceId, worker.getGroupId());
        final List<Long> workerIds = wg.replicationEnabled() ? wg.getAllWorkerIds(false) : Collections.emptyList();

        List<Long> todoShardIds = new ArrayList<>();
        shardReportInfos.forEach(shardReportInfo -> {
            long shardId = shardReportInfo.getShardId();
            Shard shard = shardManager.getShard(shardId);
            if (shard == null) {
                return;
            }

            FilePath filePath = shard.getFilePath();
            if (filePath == null) {
                return;
            }

            FileStore fileStore = fsMgr.getFileStore(filePath.fs.key());
            if (fileStore == null) {
                return;
            }

            // If the reported hash code is 0, it implies that the starlet is on an older version.
            // Therefore, we will switch to using the version number to decide if the shard needs an update.
            if (shardReportInfo.getHashCode() == 0) {
                if (shardReportInfo.getFileStoreVersion() < fileStore.getVersion()) {
                    todoShardIds.add(shardId);
                }
            } else {
                if (shard.hashCodeWithReplicas(workerIds) != shardReportInfo.getHashCode()) {
                    todoShardIds.add(shardId);
                }
            }
        });
        if (!todoShardIds.isEmpty()) {
            // Add to worker asynchronously, not blocking the current starmgr rpc thread
            shardScheduler.scheduleAsyncAddToWorker(serviceId, todoShardIds, workerId);
        }
    }

    public FilePathInfo allocateFilePath(String serviceId, FileStoreType fsType, String suffix,
                                         String fsKey, String rootDir) {
        checkLeader();

        FileStore fs = null;
        FileStoreMgr fsMgr = serviceManager.getFileStoreMgr(serviceId);
        fs = fsMgr.allocFileStore(fsKey);
        if (fs == null) {
            throw new InvalidArgumentStarException(String.format("file store with key %s not exists", fsKey));
        }

        // if rootDir is not provided, use serviceId as rootDir (default behaviour)
        if (rootDir == null || rootDir.isEmpty()) {
            rootDir = serviceId;
        }
        FilePath fp = new FilePath(fs, String.format("%s/%s", rootDir, suffix));
        return fp.toProtobuf();
    }

    public String addFileStore(String serviceId, FileStoreInfo fsInfo) throws StarException {
        checkLeader();

        String fsKey = fsInfo.getFsKey();
        if (fsKey.isEmpty()) {
            fsKey = UUID.randomUUID().toString();
            fsInfo = fsInfo.toBuilder().setFsKey(fsKey).build();
        }
        FileStoreMgr fsMgr = serviceManager.getFileStoreMgr(serviceId);
        FileStore fs = FileStore.fromProtobuf(fsInfo);
        fsMgr.addFileStore(fs);
        return fsKey;
    }

    public void removeFileStore(String serviceId, String fsKey, String fsName) throws StarException {
        checkLeader();

        if (fsKey.isEmpty() && fsName.isEmpty()) {
            throw new InvalidArgumentStarException("neither fskey nor fsname is provided");
        }

        FileStoreMgr fsMgr = serviceManager.getFileStoreMgr(serviceId);
        if (!fsKey.isEmpty()) {
            fsMgr.removeFileStore(fsKey);
        } else {
            fsMgr.removeFileStoreByName(fsName);
        }
    }

    public List<FileStoreInfo> listFileStore(String serviceId, FileStoreType fsType) throws StarException {
        FileStoreMgr fsMgr = serviceManager.getFileStoreMgr(serviceId);
        List<FileStore> fileStores = fsMgr.listFileStore(fsType);
        return fileStores.stream().map(FileStore::toProtobuf).collect(Collectors.toList());
    }

    public void updateFileStore(String serviceId, FileStoreInfo fsInfo) throws StarException {
        checkLeader();
        serviceManager.updateFileStore(serviceId, fsInfo);
    }

    public void replaceFileStore(String serviceId, FileStoreInfo fsInfo) throws StarException {
        checkLeader();
        if (!Config.STARMGR_REPLACE_FILESTORE_ENABLED) {
            throw new InvalidArgumentStarException("starmgr replace file store disabled");
        }
        serviceManager.replaceFileStore(serviceId, fsInfo);
    }

    public FileStoreInfo getFileStore(String serviceId, String fsName, String fsKey) throws StarException {
        if (fsKey.isEmpty() && fsName.isEmpty()) {
            throw new InvalidArgumentStarException("neither fskey nor fsname is provided");
        }

        FileStoreMgr fsMgr = serviceManager.getFileStoreMgr(serviceId);

        FileStore fileStore = !fsKey.isEmpty() ? fsMgr.getFileStore(fsKey) : fsMgr.getFileStoreByName(fsName);
        if (fileStore == null) {
            throw new StarException(ExceptionCode.NOT_EXIST,
                    String.format("file store with name %s not exists.", fsName));
        }
        return fileStore.toProtobuf();
    }

    public ShardManager getShardManager(String serviceId) {
        return serviceManager.getShardManager(serviceId);
    }

    private ShardManager getShardManagerNoLock(String serviceId) {
        return serviceManager.getShardManagerNoLock(serviceId);
    }

    public FileStoreMgr getFileStoreMgr(String serviceId) {
        return serviceManager.getFileStoreMgr(serviceId);
    }

    // FOR TEST
    public ServiceManager getServiceManager() {
        return serviceManager;
    }

    // FOR TEST
    public WorkerManager getWorkerManager() {
        return workerManager;
    }

    // FOR TEST
    public IdGenerator getIdGenerator() {
        return idGenerator;
    }

    // FOR TEST
    public HeartbeatManager getHeartbeatManager() {
        return heartbeatManager;
    }

    public void replay(Journal journal) {
        journalReplayer.replay(journal);
    }

    public void replayRegisterService(ServiceTemplate serviceTemplate) {
        serviceManager.replayRegisterService(serviceTemplate);
    }

    public void replayDeregisterService(String serviceTemplateName) {
        serviceManager.replayDeregisterService(serviceTemplateName);
    }

    public void replayBootstrapService(Service service) {
        serviceManager.replayBootstrapService(service);
        workerManager.bootstrapService(service.getServiceId());
    }

    public void replayShutdownService(Service service) {
        serviceManager.replayShutdownService(service);
    }

    public void replayCreateShard(String serviceId, CreateShardJournalInfo info) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay create shard, should not happen!", serviceId);
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            assert shardManager != null;
            shardManager.replayCreateShard(info);
        }
    }

    public void replayDeleteShard(String serviceId, List<Long> shardIds) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay delete shard, should not happen!", serviceId);
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            assert shardManager != null;
            shardManager.replayDeleteShard(shardIds);
        }
    }

    public void replayUpdateShard(String serviceId, List<Shard> shards) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay update shard, should not happen!", serviceId);
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            assert shardManager != null;
            shardManager.replayUpdateShard(shards);
        }
    }

    public void replayCreateShardGroup(String serviceId, List<ShardGroup> shardGroups) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay create shard group, should not happen!", serviceId);
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            assert shardManager != null;
            shardManager.replayCreateShardGroup(shardGroups);
        }
    }

    public void replayDeleteShardGroup(String serviceId, DeleteShardGroupInfo info) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay delete shard group, should not happen!", serviceId);
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            assert shardManager != null;
            shardManager.replayDeleteShardGroup(info);
        }
    }

    public void replayUpdateShardGroup(String serviceId, List<ShardGroup> shardGroups) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay update shard group, should not happen!", serviceId);
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            assert shardManager != null;
            shardManager.replayUpdateShardGroup(shardGroups);
        }
    }

    public void replayCreateMetaGroup(String serviceId, MetaGroupJournalInfo info) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay create meta group, should not happen!", serviceId);
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            assert shardManager != null;
            shardManager.replayCreateMetaGroup(info);
        }
    }

    public void replayDeleteMetaGroup(String serviceId, MetaGroupJournalInfo info) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay delete meta group, should not happen!", serviceId);
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            assert shardManager != null;
            shardManager.replayDeleteMetaGroup(info);
        }
    }

    public void replayUpdateMetaGroup(String serviceId, MetaGroupJournalInfo info) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay update meta group, should not happen!", serviceId);
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            assert shardManager != null;
            shardManager.replayUpdateMetaGroup(info);
        }
    }

    public void replayCreateWorkerGroup(String serviceId, WorkerGroup group) {
        try (LockCloseable ignored = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay CreateWorkerGroup, should not happen!", serviceId);
                return;
            }
            workerManager.replayCreateWorkerGroup(serviceId, group);
        }
    }

    public void replayDeleteWorkerGroup(String serviceId, long groupId) throws StarException {
        workerManager.replayDeleteWorkerGroup(serviceId, groupId);
    }

    public void replayUpdateWorkerGroup(String serviceId, UpdateWorkerGroupInfo info) {
        workerManager.replayUpdateWorkerGroup(serviceId, info);
    }

    public void replayAddWorker(String serviceId, Worker worker) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay add worker, should not happen!", serviceId);
            }
            workerManager.replayAddWorker(serviceId, worker);
        }
    }

    public void replayUpdateWorker(String serviceId, List<Worker> workers) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay update worker, should not happen!", serviceId);
            }
            workerManager.replayUpdateWorker(serviceId, workers);
        }
    }

    public void replayRemoveWorker(String serviceId, long groupId, long workerId) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay remove worker, should not happen!", serviceId);
            }
            workerManager.replayRemoveWorker(serviceId, groupId, workerId);
        }
    }

    public void replaySetId(long id) {
        idGenerator.setNextId(id);
    }

    public void replayLeaderChange(LeaderInfo info) {
        LOG.info("replay leader change from '{}' to '{}'", toString(leaderInfo), toString(info));
        leaderInfo = info.toBuilder().build();
    }

    String toString(LeaderInfo info) {
        if (info.getHost().isEmpty() && info.getPort() == 0) {
            return "";
        }
        return String.format("%s:%d", info.getHost(), info.getPort());
    }

    public void replayAddFileStore(String serviceId, FileStoreInfo fsInfo) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay add file store, should not happen!",
                        serviceId);
            }
            FileStoreMgr fsMgr = getFileStoreMgr(serviceId);
            assert fsMgr != null;
            fsMgr.replayAddFileStore(fsInfo);
        }
    }

    public void replayRemoveFileStore(String serviceId, String fsKey) {
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                LogUtils.fatal(LOG, "service {} not exist when replay remove file store, should not happen!",
                        serviceId);
            }
            FileStoreMgr fsMgr = getFileStoreMgr(serviceId);
            assert fsMgr != null;
            fsMgr.replayRemoveFileStore(fsKey);
        }
    }

    public void replayUpdateFileStore(String serviceId, FileStoreInfo fsInfo) {
        serviceManager.replayUpdateFileStore(serviceId, fsInfo);
    }

    public void replayReplaceFileStore(String serviceId, FileStoreInfo fsInfo) {
        serviceManager.replayReplaceFileStore(serviceId, fsInfo);
    }

    private LeaderInfo myLeaderInfo() {
        if (listenAddress == null) {
            // Don't know who I am. orz
            return LeaderInfo.newBuilder().build();
        } else {
            return LeaderInfo.newBuilder()
                    .setHost(listenAddress.getHostString())
                    .setPort(listenAddress.getPort())
                    .build();
        }
    }

    // WARNING: BE VERY CAREFUL ABOUT THIS FUNCTION!
    public void dumpMeta(OutputStream out) throws IOException {
        LOG.info("start dump star manager meta data ...");
        // write MAGIC HEADER: STARMGR1, DO NOT CHANGE IT, EVER!
        out.write(IMAGE_META_MAGIC_BYTES);
        // write MetaHeader, DO NOT directly write new data here, change the protobuf definition
        // Raw header setting all fields except the `checksum` field
        StarManagerImageMetaHeader rawHeader = StarManagerImageMetaHeader.newBuilder()
                .setGenerateTime(System.currentTimeMillis())
                .setReplayJournalId(journalSystem.getReplayId())
                .setNextGlobalId(idGenerator.getNextPersistentId())
                .setLeaderInfo(leaderInfo)
                .setDigestAlgorithm(Constant.DEFAULT_DIGEST_ALGORITHM)
                .clearChecksum()
                .build();
        byte[] checksum = null;
        try {
            checksum = MessageDigest.getInstance("MD5").digest(rawHeader.toByteArray());
        } catch (NoSuchAlgorithmException exception) {
            throw new InternalErrorStarException("Can't calculate checksum. Error:{}", exception.getMessage());
        }
        StarManagerImageMetaHeader header = StarManagerImageMetaHeader.newBuilder().mergeFrom(rawHeader)
                .setChecksum(ByteString.copyFrom(checksum))
                .build();
        header.writeDelimitedTo(out);

        DigestOutputStream mdStream = Utils.getDigestOutputStream(out, Constant.DEFAULT_DIGEST_ALGORITHM);
        // Save all the components, be careful about the writing orders, which affects the restore correctness.
        try (SectionWriter writer = new SectionWriter(mdStream)) {
            try (OutputStream stream = writer.appendSection(SectionType.SECTION_STARMGR_SERVICEMGR)) {
                serviceManager.dumpMeta(stream);
            }
            try (OutputStream stream = writer.appendSection(SectionType.SECTION_STARMGR_WORKERMGR)) {
                workerManager.dumpMeta(stream);
            }
            // Future components can be added here. It is safe to remove unused components when needed.
        }
        // flush the mdStream because we are going to write to `out` directly.
        mdStream.flush();

        // write MetaFooter, DO NOT directly write new data here, change the protobuf definition
        StarManagerImageMetaFooter.Builder footerBuilder = StarManagerImageMetaFooter.newBuilder();
        if (mdStream.getMessageDigest() != null) {
            footerBuilder.setChecksum(ByteString.copyFrom(mdStream.getMessageDigest().digest()));
        }
        footerBuilder.build().writeDelimitedTo(out);
        LOG.info("end dump star manager meta data.");
    }

    // WARNING: BE VERY CAREFUL ABOUT THIS FUNCTION!
    public void loadMeta(InputStream in) throws IOException {
        LOG.info("start load star manager meta data ...");
        // try to read the first byte, ensure it is not an empty stream.
        int firstByte = in.read();
        if (firstByte == -1) {
            LOG.warn("load star manager meta data found empty stream, do nothing.");
            return;
        }
        // READ MAGIC HEADER, the first byte is already read into `firstByte`
        int len = IMAGE_META_MAGIC_BYTES.length;
        byte[] magic = new byte[len];
        magic[0] = (byte) firstByte;
        int n = in.read(magic, 1, len - 1);
        Preconditions.checkState(n == len - 1);
        if (!Arrays.equals(IMAGE_META_MAGIC_BYTES, magic)) {
            throw new IOException("verify star manager meta file raw header failed, meta is not valid.");
        }

        // read MetaHeader
        StarManagerImageMetaHeader header = StarManagerImageMetaHeader.parseDelimitedFrom(in);
        if (header == null) {
            throw new EOFException();
        }
        ByteString expectedChecksum = header.getChecksum();
        if (expectedChecksum.isEmpty()) {
            throw new IOException("Data integrity check failed. Expect to have checksum in ImageMetaHeader");
        }
        StarManagerImageMetaHeader rawHeader = StarManagerImageMetaHeader.newBuilder()
                .mergeFrom(header)
                .clearChecksum()
                .build();
        try {
            byte[] checksum = MessageDigest.getInstance("MD5").digest(rawHeader.toByteArray());
            if (!expectedChecksum.equals(ByteString.copyFrom(checksum))) {
                throw new IOException("checksum mismatch");
            }
        } catch (NoSuchAlgorithmException exception) {
            LOG.warn("Failed to calculate checksum of the header, error: {}. Ignored for now!", exception.getMessage());
        }

        long generateTime = header.getGenerateTime();
        long replayJournalId = header.getReplayJournalId();
        // restore state from MetaHeader
        journalSystem.setReplayId(replayJournalId);
        idGenerator.setNextId(header.getNextGlobalId());
        // restore current leader info
        leaderInfo = header.getLeaderInfo();

        DigestInputStream digestInput = Utils.getDigestInputStream(in, header.getDigestAlgorithm());
        // load all the components
        try (SectionReader reader = new SectionReader(digestInput)) {
            reader.forEach(this::loadSectionMeta);
        }

        // read MetaFooter
        StarManagerImageMetaFooter footer = StarManagerImageMetaFooter.parseDelimitedFrom(in);
        if (footer == null) {
            throw new EOFException();
        }
        // usually, if any byte corrupted, either NPE or AssertError throws before reaching here. In case lucky enough that no
        // errors happen before, the checksum will discover the error and will throw IOException.
        Utils.validateChecksum(digestInput.getMessageDigest(), footer.getChecksum());
        LOG.info("end load star manager meta data at {}, replay journal id {}.",
                new SimpleDateFormat("MM-dd HH:mm:ss").format(new Date(generateTime)), replayJournalId);
    }

    private void loadSectionMeta(Section section) throws IOException {
        switch (section.getHeader().getSectionType()) {
            case SECTION_STARMGR_WORKERMGR:
                workerManager.loadMeta(section.getStream());
                break;
            case SECTION_STARMGR_SERVICEMGR:
                serviceManager.loadMeta(section.getStream());
                break;
            default:
                LOG.warn("Unknown section type:{} when loadMeta in StarManager, ignore it!",
                        section.getHeader().getSectionType());
                break;
        }
    }

    // for debug purpose, dump human readable json meta
    public String dump() throws IOException {
        String name = "star_manager_meta";
        File file = new File(name);
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        serviceManager.dump(dos);
        workerManager.dump(dos);
        return Config.STARMGR_IP + ":" + file.getAbsolutePath();
    }

    public String getServiceIdByIdOrName(String service) throws StarException {
        String serviceId;
        try {
            ServiceInfo serviceInfo = getServiceInfoByName(service);
            serviceId = serviceInfo.getServiceId();
        } catch (StarException e) {
            ServiceInfo serviceInfo = getServiceInfoById(service);
            serviceId = serviceInfo.getServiceId();
        }
        return serviceId;
    }

    public void removeShardGroupReplicas(String serviceId, Long shardGroupId) throws StarException {
        checkLeader();
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST, "service {} not exist", serviceId);
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            shardManager.removeShardGroupReplicas(shardGroupId);
        }
    }

    /**
     * Only For Testing
     **/
    @VisibleForTesting
    Scheduler getScheduler() {
        return shardScheduler;
    }

    public void updateShardReplicaInfo(String serviceId, long workerId, List<ReplicaUpdateInfo> replicaUpdateInfos)
            throws StarException {
        checkLeader();
        try (LockCloseable lock = new LockCloseable(serviceManager.readLock())) {
            if (!serviceManager.existService(serviceId)) {
                throw new StarException(ExceptionCode.NOT_EXIST, "service {} not exist", serviceId);
            }
            ShardManager shardManager = getShardManagerNoLock(serviceId);
            Preconditions.checkNotNull(shardManager);
            shardManager.updateShardReplicaInfo(workerId, replicaUpdateInfos);
        }
    }
}
