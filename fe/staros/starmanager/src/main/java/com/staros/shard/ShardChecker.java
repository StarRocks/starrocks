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


package com.staros.shard;

import com.google.common.base.Preconditions;
import com.staros.exception.CheckInterruptedException;
import com.staros.exception.StarException;
import com.staros.proto.PlacementPolicy;
import com.staros.replica.Replica;
import com.staros.schedule.ReplicaWorkerInvertIndex;
import com.staros.schedule.ScheduleScorer;
import com.staros.schedule.Scheduler;
import com.staros.schedule.select.FirstNSelector;
import com.staros.service.ServiceManager;
import com.staros.util.AbstractServer;
import com.staros.util.Config;
import com.staros.util.Constant;
import com.staros.worker.Worker;
import com.staros.worker.WorkerGroup;
import com.staros.worker.WorkerManager;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ShardChecker extends AbstractServer {
    private static final Logger LOG = LogManager.getLogger(ShardChecker.class);

    private final ServiceManager serviceManager;
    private final WorkerManager workerManager;
    private final Scheduler scheduler;
    // TODO: use thread pool
    private final Thread checkThread;
    private final FirstNSelector selector;
    private final long coolDownMs = 60 * 1000; // 60 seconds
    private AtomicLong allows = new AtomicLong();
    // Temp stats for workerGroup level shard balance
    private Map<Long, WorkerGroupStats> workerGroupStatsMap;
    // Refresh the group if the stats longer than this period. Default: 5 seconds
    // short interval will make the worker nums more accurate with the cost of more computing resource usage.
    private static final long WORKER_GROUP_STAT_REFRESH_INTERVAL_MS = 5000;

    static class WorkerGroupStats {
        WorkerGroup workerGroup;
        long lastUpdateTimeStamp = 0;
        boolean balanced = true;
        SplittableRandom randomGen = new SplittableRandom();
        // the probability for a given shardGroup to balance or not
        double shardGroupBalanceProbability = 0;
        double averageNum = 0;

        public WorkerGroupStats(WorkerGroup workerGroup) {
            this.workerGroup = workerGroup;
        }

        public boolean needBalance() {
            if (lastUpdateTimeStamp + WORKER_GROUP_STAT_REFRESH_INTERVAL_MS < System.currentTimeMillis()) {
                refresh();
            }
            return !balanced && randomGen.nextDouble() < shardGroupBalanceProbability;
        }

        public boolean isPreferredBalanceDirection(long srcWorkerId, long targetWorkerId) {
            Worker srcWorker = workerGroup.getWorker(srcWorkerId);
            Worker tgtWorker = workerGroup.getWorker(targetWorkerId);
            if (srcWorker == null || tgtWorker == null) {
                return false;
            }
            // For a worker group with a snapshot of the number of tablets, the average number of tablets is a
            // fixed number. Ensure the source worker is above the average and the destination worker is under
            // the average. In addition, their difference should be larger than 2, so after the migration, it
            // won't be possible to move the tablet from the destination to the source again.
            return srcWorker.getNumOfShards() > averageNum && tgtWorker.getNumOfShards() < averageNum
                    && srcWorker.getNumOfShards() > tgtWorker.getNumOfShards() + 2;
        }

        public void refresh() {
            long total = 0;
            long count = 0;
            long min = Long.MAX_VALUE;
            long max = 0;
            for (Long id : workerGroup.getAllWorkerIds(true)) {
                Worker worker = workerGroup.getWorker(id);
                if (worker == null) {
                    continue;
                }
                long num = worker.getNumOfShards();
                if (num > max) {
                    max = num;
                }
                if (num < min) {
                    min = num;
                }
                total += worker.getNumOfShards();
                ++count;
            }

            balanced = true;
            lastUpdateTimeStamp = System.currentTimeMillis();
            if (count == 0) {
                shardGroupBalanceProbability = 0;
                return;
            }
            averageNum = ((double) total) / count;
            if ((max - min) / averageNum > Config.BALANCE_WORKER_SHARDS_THRESHOLD_IN_PERCENT) {

                // Set to be the 1/5 of the imbalance percent by default
                // For a given workerGroup, if the min/max diversity is 20%, then it will be 4% probability
                shardGroupBalanceProbability =
                        Double.min((max - min) / averageNum / 5, Config.BALANCE_WORKER_SHARDS_THRESHOLD_IN_PERCENT / 2);
                randomGen = new SplittableRandom(System.currentTimeMillis());
                balanced = false;
                LOG.debug("Balance Probability for workerGroup:{}: {}", workerGroup.getGroupId(),
                        shardGroupBalanceProbability);
            }
        }
    }

    static class ReplicaStat {
        int shutdownReplicas = 0;
        int deadReplica = 0;
        int okReplicas = 0;
        int scaleoutReplicas = 0;
        int scaleinReplicas = 0;
    }

    public ShardChecker(ServiceManager serviceManager, WorkerManager workerManager, Scheduler scheduler) {
        this.serviceManager = serviceManager;
        this.workerManager = workerManager;
        this.scheduler = scheduler;
        this.selector = new FirstNSelector();
        this.checkThread = new Thread(this::runCheckThread);
    }

    @Override
    public void doStart() {
        checkThread.start();
    }

    @Override
    public void doStop() {
        try {
            checkThread.interrupt();
            checkThread.join();
        } catch (InterruptedException e) {
            LOG.warn("join shard checker thread failed! {}", e.getMessage());
        }
    }

    /**
     * Sampling logging, avoid too frequent logging overwhelming log file
     *
     * @param level   log level
     * @param message log message
     */
    private void sampleLogging(Level level, String message) {
        long now = System.currentTimeMillis();
        long allowed = allows.get();
        if (now > allowed && allows.compareAndSet(allowed, now + coolDownMs)) {
            LOG.log(level, message);
        }
    }

    private void runCheckThread() {
        while (isRunning()) {
            if (Config.DISABLE_BACKGROUND_SHARD_SCHEDULE_CHECK) {
                // reduce frequency logging, check interval is 10s, sample logging is 1 min.
                sampleLogging(Level.WARN,
                        "DISABLE_BACKGROUND_SHARD_SCHEDULE_CHECK is turned on. Disabling balance shards " +
                        "ability!");
            } else {
                try {
                    LOG.debug("running shard check once.");
                    for (String serviceId : serviceManager.getServiceIdSet()) {
                        shardHealthCheckForService(serviceId);
                    }

                    // TODO: shard balance inside a shard group, does not need to do it every loop
                    for (String serviceId : serviceManager.getServiceIdSet()) {
                        shardGroupBalanceCheckForService(serviceId);
                    }
                    // release the workerGroupStat after the check.
                    workerGroupStatsMap = null;
                } catch (CheckInterruptedException exception) {
                    // The check is interrupted in the middle.
                    LOG.info("Check interrupted: {}", exception.getMessage());
                }
            }

            try {
                Thread.sleep(Config.SHARD_CHECKER_LOOP_INTERVAL_SEC * 1000L);
            } catch (InterruptedException e) {
                LOG.info("shard checker thread interrupted! {}", e.getMessage());
            }
        }
    }

    /**
     * check the shards inside a service
     *
     * @param serviceId the target service id
     */
    protected void shardHealthCheckForService(String serviceId) {
        LOG.debug("Start shard replica health check for service: {}", serviceId);
        ShardManager shardManager = serviceManager.getShardManager(serviceId);
        if (shardManager == null) {
            LOG.info("ShardManager not exist for service {}, skip the healthy check.", serviceId);
            return;
        }
        // The iterator obtained from a ConcurrentHashMap may not reflect the recent change after the iterator created,
        // this is fine since it is a periodic check, the invisible edition will be visible in next round of check.
        Iterator<Map.Entry<Long, Shard>> iterator = shardManager.getShardIterator();
        try {
            while (iterator.hasNext()) {
                Map.Entry<Long, Shard> entry = iterator.next();
                long shardId = entry.getKey();
                Shard shard = shardManager.getShard(shardId);
                if (shard == null) {
                    continue;
                }
                try {
                    shardHealthCheck(shard);
                } catch (StarException exception) {
                    LOG.info("Got exception during processing service:{}, shard:{} health check, skip it.",
                            serviceId, shardId, exception);
                }
                if (Config.DISABLE_BACKGROUND_SHARD_SCHEDULE_CHECK) {
                    throw new CheckInterruptedException("Interrupted shard health check.");
                }
            }
        } catch (ConcurrentModificationException exception) {
            LOG.info("Fail to check shards due to ConcurrentModificationException. Error: {}", exception.getMessage());
        }
    }

    /**
     * Check given shard's replica status, remove dead replica and add missing replicas. The task is expected to
     * run with low priority.
     *
     * @param shard the shard to be checked
     */
    protected void shardHealthCheck(Shard shard) throws StarException {
        ShardManager shardManager = serviceManager.getShardManager(shard.getServiceId());

        List<Replica> replicas = shard.getReplica();
        // Divide replicas into scaleIn/scaleOut/Ok replicas group by workerGroupId.
        // workerGroup -> ReplicaStat (numOfOK, num of ScaleIn, numOfScaleOut, numOfDown, numOfDead)
        Map<Long, ReplicaStat> replicaStatsMap = new HashMap<>();
        // Always add group-0 into the checklist.
        // If the Service is created with ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY = true,
        //   the group-0 will always be checked,
        // If the service is created with ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY = false,
        //   workerManager.getWorkerGroup(0) will return null and the check will be skipped.
        replicaStatsMap.put(Constant.DEFAULT_ID, new ReplicaStat());
        List<Long> expiredReplicas = new ArrayList<>();
        for (Replica replica : replicas) {
            long workerId = replica.getWorkerId();
            Worker w = workerManager.getWorker(workerId);
            if (w == null) {
                // The replicas on unknown workers will be removed immediately
                expiredReplicas.add(workerId);
                continue;
            }
            replicaStatsMap.computeIfAbsent(w.getGroupId(), k -> new ReplicaStat());
            ReplicaStat stat = replicaStatsMap.get(w.getGroupId());
            if (w.isShutdown()) {
                stat.shutdownReplicas++;
            } else if (!w.isAlive()) {
                if (w.replicaExpired()) {
                    expiredReplicas.add(workerId);
                } else {
                    stat.deadReplica++;
                }
            } else {
                switch (replica.getState()) {
                    case REPLICA_OK:
                        stat.okReplicas++;
                        break;
                    case REPLICA_SCALE_IN:
                        stat.scaleinReplicas++;
                        break;
                    case REPLICA_SCALE_OUT:
                        if (!w.warmupEnabled() ||
                                replica.stateTimedOut(1000L * Config.SHARD_REPLICA_SCALE_OUT_TIMEOUT_SECS)) {
                            List<Long> shardIds = new ArrayList<>(1);
                            shardIds.add(shard.getShardId());
                            shardManager.scaleOutShardReplicasDone(shardIds, replica.getWorkerId());
                            stat.okReplicas++;
                        } else {
                            stat.scaleoutReplicas++;
                        }
                }
            }
        }

        // TODO: sophisticated control of removal by adding more state of worker: UP/DISCONNECTED/DOWN/CONNECTING
        // * Unknown/Expired replicas can be removed directly
        // * DISCONNECTED/CONNECTING workers are unhealthy workers, but may be functional if giving more time.
        // Check remain replicas if meets the expected number of replicas.
        String serviceId = shard.getServiceId();
        long shardId = shard.getShardId();
        if (!expiredReplicas.isEmpty()) {
            for (long workerId : expiredReplicas) {
                try {
                    scheduler.scheduleRemoveFromWorker(shard.getServiceId(), shardId, workerId);
                } catch (StarException exception) {
                    // log a message and continue
                    LOG.debug("Fail to remove shard replica for service:{}, shard:{}, worker:{}, error:",
                            serviceId, shardId, workerId, exception);
                }
            }
        }

        // Group the shards' replicas by workerGroup, determine if it needs to remove redundant replicas or need to add
        // new replicas
        for (Map.Entry<Long, ReplicaStat> entry : replicaStatsMap.entrySet()) {
            long workerGroupId = entry.getKey();
            ReplicaStat stat = entry.getValue();
            WorkerGroup wg = workerManager.getWorkerGroupNoException(serviceId, workerGroupId);
            if (wg == null) {
                continue;
            }
            long expectedNum = wg.getReplicaNumber();
            // When both healthy replicas and non-healthy replicas exist,
            // 1. request to add replicas up to expected count
            // 2. request to remove replicas down to expected count
            try {
                // New replicas will be created under the following conditions:
                //   1. no live replicas (OK == 0, SCALE_OUT == 0), or
                //   2. OK + SCALE_OUT + DEAD < expectedNum
                long liveReplicas = stat.okReplicas + stat.scaleoutReplicas;
                if (liveReplicas == 0 || liveReplicas + stat.deadReplica < expectedNum) {
                    // give a try to schedule the shard again to the workerGroup, let scheduler choose a proper worker.
                    LOG.debug("Request schedule new replicas for service:{}, shard:{}, workerGroup:{},"
                                    + " expected num: {}, actual: live replicas = {}, dead replicas = {}",
                            serviceId, shardId, workerGroupId, expectedNum, liveReplicas, stat.deadReplica);
                    scheduler.scheduleAsyncAddToGroup(serviceId, shardId, workerGroupId);
                }
                // OK + SCALE_IN: the serving replicas right now, make sure the number doesn't exceed the expected num.
                // It is possible that sum(OK + SCALE_OUT) > expectedNum, the redundant replicas will be removed once
                // the SCALE_OUT replicas turn to OK status.
                // NOTE: dead replicas is out of the consideration here until they are back online or get expired and
                // removed from the Shard directly.
                if (stat.okReplicas + stat.scaleinReplicas > expectedNum) {
                    LOG.debug("Remove redundant replicas for service:{}, shard:{}, workerGroup:{},"
                                    + " expected num: {}, actual num: {}",
                            serviceId, shardId, workerGroupId, expectedNum, stat.okReplicas + stat.scaleinReplicas);
                    scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
                }
            } catch (StarException exception) {
                LOG.info("Fail to schedule tasks to scheduler. error:", exception);
            }
        }
    }

    void buildWorkerGroupStatsMap(String serviceId, List<Long> workerGroupIds) {
        if (!Config.ENABLE_BALANCE_SHARD_NUM_BETWEEN_WORKERS) {
            return;
        }
        Map<Long, WorkerGroupStats> newStats = new ConcurrentHashMap<>();
        for (Long groupId : workerGroupIds) {
            WorkerGroup workerGroup = workerManager.getWorkerGroup(serviceId, groupId);
            if (workerGroup == null) {
                continue;
            }
            WorkerGroupStats stats = new WorkerGroupStats(workerGroup);
            // prepare the stats for each workerGroup
            stats.refresh();
            newStats.put(groupId, stats);
        }
        workerGroupStatsMap = newStats;
    }

    WorkerGroupStats getWorkerGroupStats(long groupId) {
        if (!Config.ENABLE_BALANCE_SHARD_NUM_BETWEEN_WORKERS) {
            return null;
        }
        if (workerGroupStatsMap == null) {
            return null;
        }
        return workerGroupStatsMap.get(groupId);
    }

    boolean needBalanceBetweenWorkers(long groupId) {
        WorkerGroupStats stats = getWorkerGroupStats(groupId);
        if (stats == null) {
            return false;
        }
        return stats.needBalance();
    }

    boolean isPreferredBalanceDirection(long groupId, long srcWorkerId, long destWorkerId) {
        WorkerGroupStats stats = getWorkerGroupStats(groupId);
        if (stats == null) {
            return false;
        }
        return stats.isPreferredBalanceDirection(srcWorkerId, destWorkerId);
    }

    /**
     * Check and balance shard group if needed
     *
     * @param serviceId target service id
     */
    protected void shardGroupBalanceCheckForService(String serviceId) {
        LOG.debug("Start shard group balance health check for service: {}", serviceId);
        ShardManager shardManager = serviceManager.getShardManager(serviceId);
        if (shardManager == null) {
            LOG.info("ShardManager not exist for service {}, skip the shard group balance check.", serviceId);
            return;
        }
        // TODO: take a snapshot of the balance of tablets between workers,
        //  to determine if the balance between workergroup should be taking into consideration
        List<Long> workerGroupIds = workerManager.getAllWorkerGroupIds(serviceId);
        if (workerGroupIds.isEmpty()) {
            // empty workerGroup for the service, nothing to do
            return;
        }
        buildWorkerGroupStatsMap(serviceId, workerGroupIds);

        // Get a snapshot of shard group ids, new created shard group will be checked in next round.
        List<Long> shardGroupIds = shardManager.getAllShardGroupIds();
        for (long groupId : shardGroupIds) {
            ShardGroup group = shardManager.getShardGroup(groupId);
            if (group == null) {
                continue;
            }
            if (group.getShardIds().isEmpty()) {
                LOG.debug("empty shard group {} in service {}. skip it!", groupId, serviceId);
                continue;
            }
            try {
                PlacementPolicy policy = group.getPlacementPolicy();
                switch (policy) {
                    case PACK:
                        balancePackShardGroup(shardManager, group);
                        break;
                    case SPREAD:
                        balanceSpreadShardGroup(shardManager, group);
                        break;
                    case EXCLUDE:
                        balanceExcludeShardGroup(shardManager, group);
                    case NONE:
                    case RANDOM:
                    default:
                        break;
                }
            } catch (StarException exception) {
                LOG.info("Got exception during processing service:{}, shardgroup:{} balance check, skip it.",
                        serviceId, groupId, exception);
            }
            if (Config.DISABLE_BACKGROUND_SHARD_SCHEDULE_CHECK) {
                throw new CheckInterruptedException("Interrupted shardgroup balance check.");
            }
        }
    }

    private void balanceExcludeShardGroup(ShardManager shardManager, ShardGroup group) {
        // (A EXCLUDE B) && (B EXCLUDE C) => (A EXCLUDE C), NO!
        ReplicaWorkerInvertIndex index = new ReplicaWorkerInvertIndex();
        index.buildFrom(workerManager, shardManager, group);
        for (long workerGroupId : index.getAllWorkerGroupIds()) {
            WorkerGroup workerGroup = workerManager.getWorkerGroupNoException(group.getServiceId(), workerGroupId);
            if (workerGroup == null) {
                continue;
            }
            List<Long> workerIds = workerGroup.getAllWorkerIds(true);
            if (workerIds.size() < 2) {
                continue;
            }

            // Divide workers into two sets, HAVE replicas v.s. NO replicas
            // move replicas from HAVE replicas to NO replicas.
            // DON'T try to move around for following case:
            // workerA: (X, Y, Z), workerB: (W) --> workerA: (X, Y), workerB: (Z, W)
            // It breaks the EXCLUDE rule, makes no sense to balance around.
            List<Long> srcWorkerIds = new ArrayList<>();
            List<Long> tgtWorkerIds = new ArrayList<>();
            for (long workerId : workerIds) {
                if (index.getReplicaShardList(workerId).isEmpty()) {
                    tgtWorkerIds.add(workerId);
                } else {
                    srcWorkerIds.add(workerId);
                }
            }
            if (tgtWorkerIds.isEmpty()) {
                // no available workers to move replica
                return;
            }

            // Sort by its replicaNum in reverseOrder
            srcWorkerIds.sort(
                    (o1, o2) -> Integer.compare(index.getReplicaShardList(o2).size(), index.getReplicaShardList(o1).size()));

            ScheduleScorer tgtScore = new ScheduleScorer(tgtWorkerIds);
            tgtScore.apply(workerManager);

            Iterator<Long> srcIt = srcWorkerIds.iterator();
            while (srcIt.hasNext() && !tgtScore.isEmpty()) {
                // srcWorkerId: least score, targetWorkerId: most score, move from srcWorkerId -> targetWorkerId
                long srcWorkerId = srcIt.next();
                List<Long> tgtIdList = tgtScore.selectHighEnd(selector, 1);
                if (tgtIdList.isEmpty()) {
                    break;
                }
                long tgtWorkerId = tgtIdList.get(0);
                List<Long> candidates = new ArrayList<>(index.getReplicaShardList(srcWorkerId));
                if (candidates.size() <= 1) {
                    // No need to move any replica
                    break;
                }
                long selected = candidates.get(0);
                String serviceId = group.getServiceId();
                try {
                    LOG.debug("[ExcludeGroup] Try to balance shard:{} (service:{}) replica from worker:{} => worker:{}",
                            selected, serviceId, srcWorkerId, tgtWorkerId);
                    scheduler.scheduleAddToWorker(serviceId, selected, tgtWorkerId);
                    // remove tgtWorkerId from scorer
                    tgtScore.remove(tgtWorkerId);
                    // only submit the asyncRemoveFromWorker if AddToWorker success.
                    scheduler.scheduleAsyncRemoveFromWorker(
                            serviceId, Collections.nCopies(1, selected), srcWorkerId);
                } catch (Exception exception) {
                    LOG.info("[ExcludeGroup] Fail to balance shard:{} in service:{}, form worker:{} to worker:{}. Error:",
                            selected, serviceId, srcWorkerId, tgtWorkerId, exception);
                }
            }
        }
    }

    private void balancePackShardGroup(ShardManager shardManager, ShardGroup group) {
        // (A PACK B) && (B PACK C) => (A PACK C), YES!
        // TODO: mark the PACK shard group UNSTABLE?
        // recursively find all PACK shard groups who are connected by the same shard.
        List<ShardGroup> packGroups = new ArrayList<>();
        List<Shard> packShards = new ArrayList<>();
        collectAllShardsAndGroupsRecursively(shardManager, group, packGroups, packShards);

        Optional<Long> minGroupId = packGroups.stream().map(ShardGroup::getGroupId).min(Comparator.naturalOrder());
        Preconditions.checkState(minGroupId.isPresent());
        if (minGroupId.get() < group.getGroupId()) {
            // must have been processed by the minimal group id.
            return;
        }

        ReplicaWorkerInvertIndex index = new ReplicaWorkerInvertIndex();
        packShards.forEach(x -> index.addReplicas(workerManager, x));
        for (long workerGroupId : index.getAllWorkerGroupIds()) {
            WorkerGroup workerGroup = workerManager.getWorkerGroupNoException(group.getServiceId(), workerGroupId);
            if (workerGroup == null) {
                continue;
            }
            List<Long> workerIds = workerGroup.getAllWorkerIds(true);
            if (workerIds.size() < 2) {
                continue;
            }
            if (workerIds.size() < workerGroup.getReplicaNumber()) {
                // Can't fulfill the requirement any way.
                LOG.debug("Worker group:{} only has {} alive workers. Shard in PACK shard group requires {} replicas. Skip it.",
                        workerGroupId, workerIds.size(), workerGroup.getReplicaNumber());
                continue;
            }
            balancePackGroupInSingleWorkerGroup(workerIds, group, workerGroup.getReplicaNumber(), packGroups, index);
        }
    }

    /**
     * Collect all shards and PACK shard groups starting from a specific shard group recursively
     *
     * @param shardManager shard manager
     * @param startGroup   the shard group to start the visit
     * @param groups       result group list
     * @param shards       result shard list
     * @throws StarException StarException throws when replica number check fails
     */
    private void collectAllShardsAndGroupsRecursively(ShardManager shardManager, ShardGroup startGroup,
                                                     List<ShardGroup> groups, List<Shard> shards) throws StarException {
        List<ShardGroup> todoGroup = new ArrayList<>();
        todoGroup.add(startGroup);
        List<Shard> blockList = new ArrayList<>();
        while (!todoGroup.isEmpty()) {
            List<ShardGroup> newGroups = new ArrayList<>();
            for (ShardGroup grp : todoGroup) {
                if (groups.contains(grp)) { // group already checked
                    continue;
                }
                groups.add(grp);
                for (long shardId : grp.getShardIds()) {
                    Shard shard = shardManager.getShard(shardId);
                    if (shard == null || shards.contains(shard) || blockList.contains(shard)) {
                        // the shard doesn't exist or is already checked
                        continue;
                    }
                    boolean hasPrecedenceGroup = false;
                    for (long gid : shard.getGroupIds()) {
                        ShardGroup grp2 = shardManager.getShardGroup(gid);
                        if (grp2 != null) {
                            if (grp2.getPlacementPolicy() == PlacementPolicy.PACK) {
                                newGroups.add(grp2);
                            } else if (grp2.getPlacementPolicy().getNumber() > PlacementPolicy.PACK.getNumber() &&
                                    grp2.getShardIds().size() > 1) {
                                // skip the shard, let exclude group adjusting the distribution.
                                hasPrecedenceGroup = true;
                            }
                        }
                    }
                    if (hasPrecedenceGroup) {
                        blockList.add(shard);
                    } else {
                        shards.add(shard);
                    }
                }
            }
            todoGroup = newGroups;
        }
    }

    private void balancePackGroupInSingleWorkerGroup(List<Long> workerIds, ShardGroup currentGroup, int replicaNum,
                                                     List<ShardGroup> groups, ReplicaWorkerInvertIndex index) {
        ScheduleScorer scorer = new ScheduleScorer(workerIds);
        List<Long> workersWithReplicas = new ArrayList<>();
        for (long workerId : workerIds) {
            int numReplicas = index.getReplicaShardList(workerId).size();
            if (numReplicas == 0) {
                continue;
            }
            workersWithReplicas.add(workerId);
            scorer.apply(PlacementPolicy.PACK, Collections.nCopies(numReplicas, workerId));
        }
        if (workersWithReplicas.size() == replicaNum) {
            // ALL DONE, shard health checker will take care of the missing replicas
            return;
        }
        scorer.apply(workerManager);
        List<Long> selectedWorkers = scorer.selectHighEnd(selector, replicaNum);
        if (selectedWorkers.size() != replicaNum) {
            // defensive coding, right now it is impossible as long as workerIds.size() >= replicaNum
            LOG.info("Failed to select {} workers from candidates while doing shard group:{} balance check, skip it!",
                    replicaNum, currentGroup.getGroupId());
            return;
        }

        LOG.debug("[PackGroup] shardGroup: {}. Existing workers with replica: {}, selected tgargetWorkers: {}",
                currentGroup.getGroupId(), workersWithReplicas, selectedWorkers);

        List<Long> existShardIds = new ArrayList<>();
        workersWithReplicas.forEach(x -> existShardIds.addAll(index.getReplicaShardList(x)));
        List<Long> validTargetShardIdList = new ArrayList<>();
        for (ShardGroup packGroup : groups) {
            // skip the shard groups who have no replicas in this worker group at all.
            if (packGroup.getShardIds().stream().anyMatch(existShardIds::contains)) {
                validTargetShardIdList.addAll(packGroup.getShardIds());
            }
        }

        for (long wid : selectedWorkers) {
            Collection<Long> existingIdList = index.getReplicaShardList(wid);
            List<Long> todoIdList = validTargetShardIdList.stream()
                    .filter(x -> !existingIdList.contains(x))
                    .collect(Collectors.toList());
            if (todoIdList.isEmpty()) {
                continue;
            }
            try {
                // add new replica and wait for the result.
                // NOTE: this is a batch adding operation without considering shard existing replica count,
                //  so it will get replicas back to expected count if any shard has missing replicas.
                scheduler.scheduleAddToWorker(currentGroup.getServiceId(), todoIdList, wid);
            } catch (StarException exception) {
                LOG.info("Fail to schedule new replicas of shard:{} to worker:{}, error:", todoIdList, wid, exception);
                return;
            }
        }

        // remove selectedWorkers, replicas on the remaining workers will be cleaned
        workersWithReplicas.removeIf(selectedWorkers::contains);
        for (long wid : workersWithReplicas) {
            Collection<Long> todoList = index.getReplicaShardList(wid);
            if (todoList.isEmpty()) {
                continue;
            }
            // schedule remove jobs asynchronously.
            LOG.debug("Submit async task to remove shard replicas:{} from worker:{}", todoList, wid);
            scheduler.scheduleAsyncRemoveFromWorker(currentGroup.getServiceId(), new ArrayList<>(todoList), wid);
        }
    }

    private void balanceSpreadShardGroup(ShardManager shardManager, ShardGroup group) {
        // (A SPREAD B) && (B SPREAD C) => (A SPREAD C), NO!
        //
        // Assume a new replica of a shard is to be added to the worker group.
        // all target workers are scored from most preferred to least preferred.
        // The balancer is to move a few replica from least preferred worker to most preferred worker.
        ReplicaWorkerInvertIndex index = new ReplicaWorkerInvertIndex();
        index.buildFrom(workerManager, shardManager, group);
        for (long workerGroupId : index.getAllWorkerGroupIds()) {
            balanceSpreadGroupInSingleWorkerGroup(shardManager, group, index, workerGroupId);
        }
    }

    private void balanceSpreadGroupInSingleWorkerGroup(ShardManager shardManager, ShardGroup group,
                                                       ReplicaWorkerInvertIndex index, long workerGroupId) {
        WorkerGroup workerGroup = workerManager.getWorkerGroupNoException(group.getServiceId(), workerGroupId);
        if (workerGroup == null) {
            return;
        }
        boolean warmupEnabled = workerGroup.warmupEnabled();
        List<Long> workerIds = workerGroup.getAllWorkerIds(true);
        if (workerIds.size() <= 1) {
            return;
        }
        boolean done = false;
        // determine whether it is needed to consider balancing shards between the workers in this shardgroup
        boolean hasConsideredWorkersImbalance = false;
        while (!done) { // repeat the loop until the group is balanced in the target worker group
            if (workerIds.size() <= 1) {
                return;
            }
            ScheduleScorer scorer = new ScheduleScorer(workerIds);
            for (long workerId : workerIds) {
                int numReplicas = index.getReplicaShardList(workerId).size();
                if (numReplicas == 0) {
                    continue;
                }
                scorer.apply(group.getPlacementPolicy(), Collections.nCopies(numReplicas, workerId));
            }
            scorer.apply(workerManager);
            List<Map.Entry<Long, Double>> sortedEntries = scorer.getScores().entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.naturalOrder()))
                    .collect(Collectors.toList());

            // srcWorkerId: least score, targetWorkerId: most score, move from srcWorkerId -> targetWorkerId
            long srcWorkerId = sortedEntries.get(0).getKey();
            long targetWorkerId = sortedEntries.get(sortedEntries.size() - 1).getKey();
            Collection<Long> srcList = index.getReplicaShardList(srcWorkerId);
            Collection<Long> tgtList = index.getReplicaShardList(targetWorkerId);
            // TODO: sortedEntries is not strictly sorted by replica numbers of this shard group.
            //  It is possible that a worker with more replicas of this shard group is ahead of
            //  a worker with less replicas of this shard group due to its load and total replicas.
            if (srcList.size() <= tgtList.size() + Config.SCHEDULER_BALANCE_MAX_SKEW) {
                // The distribution is good enough from shardGroup perspective, but give a chance to check
                // if the migration benefits the balance of shard numbers distribution among workers in the workerGroup.
                if (srcList.size() == tgtList.size()) {
                    // Perfectly balanced between all workers.
                    done = true;
                    continue;
                }
                if (hasConsideredWorkersImbalance) {
                    // Already taken the duty in previous check
                    done = true;
                    continue;
                }
                if (!isPreferredBalanceDirection(workerGroupId, srcWorkerId, targetWorkerId)) {
                    // Doesn't benefit the shards balance between workers at all
                    done = true;
                    continue;
                }
                // everything matches, let's do the lottery draw
                hasConsideredWorkersImbalance = true;
                if (!needBalanceBetweenWorkers(workerGroupId)) {
                    done = true;
                    continue;
                }
            }
            // Select a shard from `srcList` and migrate to `tgtList`
            // `relaxCandidates` contains all the shards that are not null and don't have replica in target worker.
            List<Shard> relaxCandidates = srcList.stream()
                    .filter(x -> !tgtList.contains(x)) // doesn't have replica in target worker
                    .map(shardManager::getShard)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            if (relaxCandidates.isEmpty()) {
                done = true;
                continue;
            }

            // `candidates` contains the shards from `relaxCandidates` and filtered out those shards who have precedent
            // shard groups than current shard group (SPREAD), which means the shard is in PACK/EXCLUDE meta group as well
            List<Shard> candidates = relaxCandidates.stream()
                    .filter(x -> !hasPrecedenceShardGroup(shardManager, group.getPlacementPolicy(), x.getGroupIds()))
                    .collect(Collectors.toList());
            List<Shard> finalSelects = new ArrayList<>();
            if (!candidates.isEmpty()) {
                // TODO:
                //  the shard with more replicas will be highly preferred
                //  or if the shard doesn't have enough expected num of replicas, will be skipped in this round.
                int pickIndex = ThreadLocalRandom.current().nextInt(candidates.size());
                finalSelects.add(candidates.get(pickIndex));
            } else {
                // use `relaxCandidates`
                for (Shard current : relaxCandidates) {
                    // all shards that satisfy following conditions
                    // 1. be within the same PACK GROUP as `current`
                    // 2. has at least one replica in this worker group
                    ShardGroup startGroup = null;
                    for (long currentShardGroupId : current.getGroupIds()) {
                        ShardGroup tmpGroup = shardManager.getShardGroup(currentShardGroupId);
                        if (tmpGroup == null || tmpGroup.getPlacementPolicy() != PlacementPolicy.PACK) {
                            continue;
                        }
                        startGroup = tmpGroup;
                        break;
                    }
                    if (startGroup == null) {
                        // should not happen, but just let it go.
                        continue;
                    }
                    List<Shard> packedShards = new ArrayList<>();
                    // not used, just want to reuse collectAllShardsAndGroupsRecursively() interface
                    List<ShardGroup> packGroups = new ArrayList<>();
                    collectAllShardsAndGroupsRecursively(shardManager, startGroup, packGroups, packedShards);
                    // get the shard list that has replica in the worker group
                    List<Shard> effectiveShards = new ArrayList<>();
                    List<ShardGroup> effectiveShardGroups = new ArrayList<>();
                    for (Shard shard : packedShards) {
                        if (shard.getReplicaWorkerIds().stream().anyMatch(workerIds::contains)) {
                            effectiveShards.add(shard);
                            // plan to move the shard, will all the groups the shard belongs to agree the movement?
                            for (long groupId : shard.getGroupIds()) {
                                ShardGroup groupObj = shardManager.getShardGroup(groupId);
                                if (groupObj == null) {
                                    continue;
                                }
                                if (groupObj.getPlacementPolicy() != PlacementPolicy.PACK
                                        && !effectiveShardGroups.contains(groupObj)) {
                                    effectiveShardGroups.add(groupObj);
                                }
                            }
                        }
                    }
                    int agree = 0;
                    int reject = 0;
                    for (ShardGroup affectGroup : effectiveShardGroups) {
                        // measure from the affectedGroup point of view, if the movement is acceptable
                        ReplicaWorkerInvertIndex tempIndex = new ReplicaWorkerInvertIndex();
                        tempIndex.buildFrom(workerManager, shardManager, affectGroup);
                        double scoreBeforeMove = calculateScore(tempIndex, workerIds, affectGroup.getPlacementPolicy());
                        for (Shard toMove : effectiveShards) {
                            // assume move the shard from srcWorkerId to targetWorkerId
                            if (toMove.getGroupIds().contains(affectGroup.getGroupId())) {
                                tempIndex.removeReplica(toMove.getShardId(), srcWorkerId);
                                tempIndex.addReplica(toMove.getShardId(), targetWorkerId, workerGroupId);
                            }
                        }
                        // NOTE: this `epsilon` has a relationship with the number of nodes in the workerGroup.
                        // It defines how to detect the real changes in the meanwhile, ignore unstable calculation deviation
                        // * Given a worker group with `n` nodes, with each node has Xi (i=0..n-1) replicas, the minimal
                        // deviation of moving one to the other is: 2 / n
                        // Choose a value of `epsilon` <= 2 / n, to be able to detect even a single replica movement in a group
                        // of n nodes.
                        final double epsilon = 0.001d; // n <= 2000
                        // score again
                        double scoreAfterMove = calculateScore(tempIndex, workerIds, affectGroup.getPlacementPolicy());
                        if (Math.abs(scoreAfterMove - scoreBeforeMove) > epsilon) {
                            // only consider when the score changes dramatically, otherwise take it as neutral.
                            // can avoid unstable adjustment back and forth.
                            if (scoreAfterMove > scoreBeforeMove) {
                                ++reject;
                                break;
                            } else if (scoreAfterMove < scoreBeforeMove) {
                                ++agree;
                            }
                        }
                    }
                    if (agree > 0 && reject == 0) {
                        // someone agrees, no one objects, make the move
                        finalSelects.addAll(effectiveShards);
                        break;
                    }
                    // exclude the case that all are neutral to the movement.
                }
                if (finalSelects.isEmpty()) {
                    done = true;
                    continue;
                }
            }
            // Consolidate the selection:
            //  `finalSelects`: remove from srcWorkerId, add to targetWorkerId
            List<Long> allTodoIds = finalSelects.stream().map(Shard::getShardId).collect(Collectors.toList());
            Preconditions.checkState(!allTodoIds.isEmpty());
            boolean addSuccess = false;
            try {
                LOG.debug("Try to balance shard:{} (service:{}) replica from worker:{} => worker:{}",
                        allTodoIds, group.getServiceId(), srcWorkerId, targetWorkerId);
                scheduler.scheduleAddToWorker(group.getServiceId(), allTodoIds, targetWorkerId);
                addSuccess = true;
                // update inverted index with the new schedule info, only the shard who belongs to the `group`
                finalSelects.forEach(x -> {
                    if (x.getGroupIds().contains(group.getGroupId())) {
                        index.addReplica(x.getShardId(), targetWorkerId, workerGroupId);
                    }
                });
            } catch (Exception exception) {
                LOG.info("Fail to balance shard:{} in service:{}, form worker:{} to worker:{}. Error:",
                        allTodoIds, group.getServiceId(), srcWorkerId, targetWorkerId, exception);
                // remove targetWorkerId from the list, so it will be considered as unavailable in this round of check.
                workerIds.remove(targetWorkerId);
            }
            if (addSuccess) {
                try {
                    if (warmupEnabled) {
                        // mark the balanced replica as SCALE-IN instead of removed directly
                        shardManager.scaleInShardReplicas(allTodoIds, srcWorkerId);
                    } else {
                        if (allTodoIds.size() > 1) {
                            // the group removal action will affect the shard replica distribution and
                            // eventually affect next round's choice.
                            scheduler.scheduleRemoveFromWorker(group.getServiceId(), allTodoIds, srcWorkerId);
                        } else {
                            scheduler.scheduleAsyncRemoveFromWorker(group.getServiceId(), allTodoIds, srcWorkerId);
                            // assume remove success, won't affect balance result too much even it is not done yet or failure
                            // eventually because the info was updated into `index`
                        }
                    }
                    finalSelects.forEach(x -> {
                        if (x.getGroupIds().contains(group.getGroupId())) {
                            index.removeReplica(x.getShardId(), srcWorkerId);
                        }
                    });
                } catch (Exception exception) {
                    LOG.debug("Fail to remove shard:{} replica from worker:{}", allTodoIds, srcWorkerId);
                    // ignore scheduleAsyncRemoveFromWorker failure
                }
            }
        }
    }

    /** Calculate score of given distribution. The lower score, the better result.
     */
    private double calculateScore(ReplicaWorkerInvertIndex index, List<Long> workerIds, PlacementPolicy policy) {
        switch (policy) {
            case SPREAD:
                // replica spread variance, the smaller, the better
                return calculateDeviation(index, workerIds);
            case EXCLUDE:
                // percentage of emtpy workers, lower better.
                return workerIds.stream().filter(x -> index.getReplicaShardList(x).isEmpty()).count() / (double) workerIds.size();
            default:
                return 0;
        }
    }

    // D(X) = E(X^2) - E^2(X)
    private double calculateDeviation(ReplicaWorkerInvertIndex index, List<Long> workerIds) {
        double mean = 0;
        double sMean = 0;
        for (long id : workerIds) {
            int n = index.getReplicaShardList(id).size();
            mean += n;
            sMean += n * n;
        }
        mean = mean / workerIds.size();
        return sMean / workerIds.size() - mean * mean;
    }

    /**
     * Determine if any shard group in given shard group id list has a higher precedence than policy.
     *
     * @param shardManager shard manager, to get shard group by id
     * @param policy       baseline policy
     * @param groupIds     the list of shard groups to be compared.
     * @return true if there are higher shard group precedence.
     */
    private boolean hasPrecedenceShardGroup(ShardManager shardManager, PlacementPolicy policy, List<Long> groupIds) {
        for (long id : groupIds) {
            ShardGroup group = shardManager.getShardGroup(id);
            if (group == null) {
                continue;
            }
            if (group.getPlacementPolicy().getNumber() > policy.getNumber() && group.getShardIds().size() > 1) {
                return true;
            }
        }
        return false;
    }
}
