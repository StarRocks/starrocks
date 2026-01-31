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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.staros.exception.ExceptionCode;
import com.staros.exception.InternalErrorStarException;
import com.staros.exception.NotExistStarException;
import com.staros.exception.StarException;
import com.staros.filecache.FileCache;
import com.staros.filestore.DelegatedFileStore;
import com.staros.filestore.FilePath;
import com.staros.filestore.FileStore;
import com.staros.filestore.FileStoreMgr;
import com.staros.journal.Journal;
import com.staros.journal.JournalSystem;
import com.staros.journal.StarMgrJournal;
import com.staros.metrics.MetricsSystem;
import com.staros.proto.CacheEnableState;
import com.staros.proto.CreateMetaGroupInfo;
import com.staros.proto.CreateShardGroupInfo;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.CreateShardJournalInfo;
import com.staros.proto.DeleteMetaGroupInfo;
import com.staros.proto.DeleteShardGroupInfo;
import com.staros.proto.MetaGroupInfo;
import com.staros.proto.MetaGroupJournalInfo;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.PlacementPreference;
import com.staros.proto.PlacementRelationship;
import com.staros.proto.ReplicaState;
import com.staros.proto.ReplicaUpdateInfo;
import com.staros.proto.SectionType;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.ShardManagerImageMetaFooter;
import com.staros.proto.ShardManagerImageMetaHeader;
import com.staros.proto.UpdateMetaGroupInfo;
import com.staros.proto.UpdateShardGroupInfo;
import com.staros.proto.UpdateShardInfo;
import com.staros.replica.Replica;
import com.staros.schedule.Scheduler;
import com.staros.section.Section;
import com.staros.section.SectionReader;
import com.staros.section.SectionWriter;
import com.staros.util.Config;
import com.staros.util.Constant;
import com.staros.util.IdGenerator;
import com.staros.util.LockCloseable;
import com.staros.util.LogUtils;
import com.staros.util.Utils;
import io.prometheus.metrics.core.datapoints.GaugeDataPoint;
import io.prometheus.metrics.core.metrics.Gauge;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.staros.util.Utils.executeNoExceptionOrDie;

/**
 * Shard Manager manages all the shard for single service, including
 * shard group operations and shard operations.
 */
public class ShardManager {
    private static final Logger LOG = LogManager.getLogger(ShardManager.class);

    private final String serviceId;

    private final Map<Long, Shard> shards; // <shardId, Shard>
    private final Map<Long, ShardGroup> shardGroups; // <groupId, ShardGroup>
    private final Map<Long, MetaGroup> metaGroups; // <metaGroupId, MetaGroup>

    private final ReentrantReadWriteLock lock;

    private final JournalSystem journalSystem;
    private final IdGenerator idGenerator;

    private final Scheduler shardScheduler;

    // save the shared fileStores object referenced by each shard, rebuilt from memory, no need to persist
    private final Map<String, DelegatedFileStore> shardFileStoreSnapshot;

    // TODO: better use `GaugeWithCallback` to collect the size on demand. But prometheus's GaugeWithCallback is hard to use.
    private static final Gauge METRIC_TOTAL_SHARD =
            MetricsSystem.registerGauge("starmgr_num_shards", "total number of shards in starmgr",
                    Lists.newArrayList("serviceId"));
    private static final Gauge METRIC_TOTAL_SHARDGROUP =
            MetricsSystem.registerGauge("starmgr_num_shard_groups", "total number of shard groups in starmgr",
                    Lists.newArrayList("serviceId"));

    private final GaugeDataPoint totalShardGauge;
    private final GaugeDataPoint totalShardGroupGauge;

    private enum ShardReplicaOp {
        ADD,
        ADD_TEMP,
        DELETE,
        SCALE_OUT,
        SCALE_OUT_TEMP,
        SCALE_IN,
        SCALE_OUT_DONE,
        TEMP_TO_NORMAL,
        CANCEL_SCALE_IN,
    }

    private static final Map<ShardReplicaOp, BiFunction<Shard, Long, Boolean>> SHARD_REPLICA_OP_BI_FUNCTION_MAP =
            new ImmutableMap.Builder<ShardReplicaOp, BiFunction<Shard, Long, Boolean>>()
                    .put(ShardReplicaOp.ADD, (shard, workerId) -> shard.addReplica(workerId, ReplicaState.REPLICA_OK))
                    .put(ShardReplicaOp.ADD_TEMP, Shard::addTempReplica)
                    .put(ShardReplicaOp.DELETE, Shard::removeReplica)
                    .put(ShardReplicaOp.SCALE_OUT, Shard::scaleOutReplica)
                    .put(ShardReplicaOp.SCALE_OUT_TEMP, Shard::scaleOutTempReplica)
                    .put(ShardReplicaOp.SCALE_IN, Shard::scaleInReplica)
                    .put(ShardReplicaOp.SCALE_OUT_DONE, Shard::scaleOutReplicaDone)
                    .put(ShardReplicaOp.TEMP_TO_NORMAL, Shard::convertTempReplicaToNormal)
                    .put(ShardReplicaOp.CANCEL_SCALE_IN, Shard::cancelScaleInReplica)
                    .build();

    public ShardManager(String serviceId, JournalSystem journalSystem, IdGenerator idGenerator,
                        Scheduler shardScheduler) {
        this.serviceId = serviceId;
        this.shards = new ConcurrentHashMap<>();
        this.shardGroups = new HashMap<>();
        this.metaGroups = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();

        this.journalSystem = journalSystem;
        this.idGenerator = idGenerator;
        this.shardScheduler = shardScheduler;

        this.shardFileStoreSnapshot = new HashMap<>();

        this.totalShardGauge = METRIC_TOTAL_SHARD.labelValues(serviceId);
        this.totalShardGroupGauge = METRIC_TOTAL_SHARDGROUP.labelValues(serviceId);

        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            // create default shard group
            ShardGroup shardGroup = new ShardGroup(serviceId, Constant.DEFAULT_ID);
            operateShardGroupInternal(Constant.DEFAULT_ID, shardGroup, true /* isAdd */);
        }
    }

    public String getServiceId() {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return serviceId;
        }
    }

    public List<ShardGroupInfo> createShardGroup(List<CreateShardGroupInfo> createShardGroupInfos) throws StarException {
        if (createShardGroupInfos.isEmpty()) {
            throw new StarException(ExceptionCode.INVALID_ARGUMENT, "shard group info can not be empty.");
        }

        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            List<ShardGroup> shardGroupsToCreate = new ArrayList<>(createShardGroupInfos.size());
            for (CreateShardGroupInfo info : createShardGroupInfos) {
                ShardGroup shardGroup = new ShardGroup(serviceId, idGenerator.getNextId(), info.getPolicy(), false,
                        Constant.DEFAULT_ID /* metaGroupId */, info.getLabelsMap(), info.getPropertiesMap());
                shardGroupsToCreate.add(shardGroup);
            }

            List<ShardGroupInfo> shardGroupInfos = new ArrayList<>(shardGroupsToCreate.size());
            Journal journal = StarMgrJournal.logCreateShardGroup(serviceId, shardGroupsToCreate);
            journalSystem.write(journal);
            executeNoExceptionOrDie(() -> {
                for (ShardGroup shardGroup : shardGroupsToCreate) {
                    operateShardGroupInternal(shardGroup.getGroupId(), shardGroup, true /* isAdd */);
                    shardGroupInfos.add(shardGroup.toProtobuf());
                }
            });
            return shardGroupInfos;
        }
    }


    public void deleteShardGroup(List<Long> groupIds, boolean deleteShards) throws StarException {
        deleteShardGroupInternal(groupIds, deleteShards, false /* isReplay */);
    }

    private void deleteShardGroupInternal(List<Long> groupIds, boolean deleteShards, boolean isReplay) throws StarException {
        if (groupIds.isEmpty() && !isReplay) {
            throw new StarException(ExceptionCode.INVALID_ARGUMENT, "shard group id can not be empty.");
        }
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            List<ShardGroup> groupsToDelete = new ArrayList<>(groupIds.size());

            for (Long groupId : groupIds) {
                if (groupId == Constant.DEFAULT_ID) {
                    throw new StarException(ExceptionCode.INVALID_ARGUMENT,
                            String.format("default shard group %d can not be deleted in service %s.", groupId, serviceId));
                }
                ShardGroup shardGroup = shardGroups.get(groupId);
                if (shardGroup == null) {
                    continue;
                }
                groupsToDelete.add(shardGroup);
            }
            if (groupsToDelete.isEmpty()) {
                return;
            }
            if (!isReplay) {
                List<Long> groupIdsToDelete = groupsToDelete.stream().map(ShardGroup::getGroupId).collect(Collectors.toList());
                DeleteShardGroupInfo info = DeleteShardGroupInfo.newBuilder()
                        .addAllGroupIds(groupIdsToDelete)
                        .setCascadeDeleteShard(deleteShards)
                        .build();

                Journal journal = StarMgrJournal.logDeleteShardGroup(serviceId, info);
                journalSystem.write(journal);
            }
            executeNoExceptionOrDie(() -> {
                for (ShardGroup shardGroup : groupsToDelete) {
                    operateShardGroupInternal(shardGroup.getGroupId(), null /* shardGroup */, false /* isAdd */);
                    List<Long> shardIds = shardGroup.getShardIds();
                    if (deleteShards) {
                        // delete shards
                        for (long id : shardIds) {
                            Shard shard = shards.get(id);
                            if (shard == null) {
                                // shard can be removed by other shard group that is deleted prior to this shard group
                                continue;
                            }
                            removeShardInternalNoLock(shards.get(id));
                        }
                    } else {
                        // update shards' group_id list
                        for (long id : shardIds) {
                            shards.get(id).quitGroup(shardGroup.getGroupId());
                        }
                    }
                }
            });
        }
    }

    public void updateShardGroup(List<UpdateShardGroupInfo> updateShardGroupInfos) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            List<ShardGroup> shardGroupsToUpdate = new ArrayList<>();
            List<Shard> allShardsToUpdate = new ArrayList<>();
            for (UpdateShardGroupInfo updateShardGroupInfo : updateShardGroupInfos) {
                if (updateShardGroupInfo.getEnableCache() == CacheEnableState.NOT_SET) {
                    continue; // enableCache has not been set
                }
                ShardGroup shardGroup = shardGroups.get(updateShardGroupInfo.getGroupId());
                if (shardGroup == null) {
                    continue; // it's no need to care about non-exist shardGroups
                }
                List<Long> shardIds = shardGroup.getShardIds();
                List<Shard> shardsToUpdate = new ArrayList<>(shardIds.size());
                for (Long shardId : shardIds) {
                    Shard shard = shards.get(shardId);
                    if (shard == null) {
                        continue; // it's no need to care abort non-exist shards
                    }
                    if (updateShardGroupInfo.getEnableCache() == CacheEnableState.ENABLED
                            && !shard.getFileCache().getFileCacheEnable()) {
                        shard.setFileCacheEnable(true);
                        shardsToUpdate.add(shard);
                    } else if (updateShardGroupInfo.getEnableCache() == CacheEnableState.DISABLED
                            && shard.getFileCache().getFileCacheEnable()) {
                        shard.setFileCacheEnable(false);
                        shardsToUpdate.add(shard);
                    }
                }
                if (!shardsToUpdate.isEmpty()) {
                    allShardsToUpdate.addAll(shardsToUpdate);
                    shardGroupsToUpdate.add(shardGroup);
                }
            }

            if (!shardGroupsToUpdate.isEmpty()) {
                try {
                    Journal journal = StarMgrJournal.logUpdateShardGroup(serviceId, shardGroupsToUpdate);
                    journalSystem.write(journal);
                } catch (StarException e) {
                    // revert back
                    for (Shard shard : allShardsToUpdate) {
                        shard.setFileCacheEnable(!shard.getFileCache().getFileCacheEnable());
                    }
                    throw e;
                }
            }
        }
    }

    // return shard groups info and next group id, if all shard groups are filled, next group id is set to 0
    public Pair<List<ShardGroupInfo>, Long> listShardGroupInfo(boolean includeAnonymousGroup, long startGroupId)
            throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            List<ShardGroupInfo> shardGroupInfos = new ArrayList<>();
            TreeMap<Long, ShardGroup> sortedShardGroups = new TreeMap<>(shardGroups);
            SortedMap<Long, ShardGroup> truncatedSortedShardGroups = sortedShardGroups.tailMap(startGroupId);
            long nextGroupId = 0;
            long lastGroupId = 0;
            for (ShardGroup shardGroup : truncatedSortedShardGroups.values()) {
                if (!includeAnonymousGroup && shardGroup.isAnonymous()) {
                    continue;
                }
                if (shardGroupInfos.size() >= Config.LIST_SHARD_GROUP_BATCH_SIZE) {
                    nextGroupId = lastGroupId + 1;
                    break;
                }
                lastGroupId = shardGroup.getGroupId();
                shardGroupInfos.add(shardGroup.toProtobuf());
            }
            return Pair.of(shardGroupInfos, nextGroupId);
        }
    }

    public List<ShardGroupInfo> getShardGroupInfo(List<Long> shardGroupIds) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            List<ShardGroupInfo> shardGroupInfos = new ArrayList<>(shardGroupIds.size());
            for (Long shardGroupId : shardGroupIds) {
                ShardGroup shardGroup = shardGroups.get(shardGroupId);
                if (shardGroup == null) {
                    throw new StarException(ExceptionCode.NOT_EXIST,
                            String.format("shard group %d not exist", shardGroupId));
                }
                shardGroupInfos.add(shardGroup.toProtobuf());
            }
            return shardGroupInfos;
        }
    }

    /**
     * Add shard into shards and update shard groups as well.
     * This method doesn't acquire lock, the caller should obtain ShardManager's lock before calling this.
     *
     * @param shard the shard to be added.
     * @return old shard if the same shard id exists
     */
    private Shard addShardInternalNoLock(Shard shard) {
        assert shard != null;

        FilePath path = shard.getFilePath();
        if (path != null && path.fs != null) {
            FileStore fs = setOrUpdateFileStoreSnapshotInternal(path.fs);
            // rebuild the FilePath with FileStore from `shardFileStoreSnapshot`
            FilePath newPath = new FilePath(fs, path.suffix);
            shard.setFilePath(newPath);
        }

        Shard old = shards.put(shard.getShardId(), shard);
        totalShardGauge.set(shards.size());
        for (Long groupId : shard.getGroupIds()) {
            ShardGroup shardGroup = shardGroups.get(groupId);
            assert shardGroup != null;
            // ignore shard group return value on purpose
            shardGroup.addShardId(shard.getShardId());
        }
        return old;
    }


    /**
     * Remove shard from shards and shard groups.
     * This method doesn't acquire lock, the caller should obtain ShardManager's lock before calling this.
     *
     * @param shard the shard to be removed.
     * @return the shard to be removed
     */
    private Shard removeShardInternalNoLock(Shard shard) {
        assert shard != null;
        Shard old = shards.remove(shard.getShardId());
        totalShardGauge.set(shards.size());
        for (Long groupId : shard.getGroupIds()) {
            ShardGroup shardGroup = shardGroups.get(groupId);
            if (shardGroup == null) {
                // shardGroup could be removed already
                continue;
            }
            // ignore shard group return value on purpose
            shardGroup.removeShardId(shard.getShardId());

            // shard group created by placement preference
            // delete it after reference count less than 2
            if (shardGroup.isAnonymous() &&
                    shardGroup.getMetaGroupId() == Constant.DEFAULT_ID &&
                    shardGroup.getShardIds().size() < 2) {
                for (long id : shardGroup.getShardIds()) {
                    shards.get(id).quitGroup(shardGroup.getGroupId());
                }
                operateShardGroupInternal(shardGroup.getGroupId(), null /* shardGroup */, false /* isAdd */);
            }
        }
        return old;
    }

    private void commitCreateShard(List<Shard> shardList, List<ShardGroup> anonymousShardGroups) {
        // TODO: optimize logic here
        for (ShardGroup anonymousShardGroup : anonymousShardGroups) {
            operateShardGroupInternal(anonymousShardGroup.getGroupId(), anonymousShardGroup, true /* isAdd */);
        }
        for (Shard shard : shardList) {
            // ignore return value on purpose
            addShardInternalNoLock(shard);
        }
        for (ShardGroup anonymousShardGroup : anonymousShardGroups) {
            for (Long sid : anonymousShardGroup.getShardIds()) {
                // ignore return value on purpose
                shards.get(sid).joinGroup(anonymousShardGroup.getGroupId());
            }
        }
    }

    private void checkPlacementPreference(PlacementPreference preference) throws StarException {
        if (preference.getPlacementPolicy() != PlacementPolicy.PACK) {
            throw new StarException(ExceptionCode.INVALID_ARGUMENT,
                    String.format("shard placement preference does not support %s policy in service %s.",
                            preference.getPlacementPolicy().name(), serviceId));
        }
        if (preference.getPlacementRelationship() != PlacementRelationship.WITH_SHARD) {
            throw new StarException(ExceptionCode.INVALID_ARGUMENT,
                    String.format("shard placement preference does not support %s relationship in service %s.",
                            preference.getPlacementRelationship().name(), serviceId));
        }
        long targetId = preference.getRelationshipTargetId();
        Shard targetShard = shards.get(targetId);
        if (targetShard == null) {
            throw new StarException(ExceptionCode.NOT_EXIST,
                    String.format("shard placement preference target id %d not exist in service %s.",
                            targetId, serviceId));
        }
        Preconditions.checkState(preference.getPlacementPolicy() == PlacementPolicy.PACK);
    }

    public List<ShardInfo> createShard(List<CreateShardInfo> createShardInfos, FileStoreMgr fsMgr) throws StarException {
        List<Shard> shardsToCreate = new ArrayList<>(createShardInfos.size());
        Multimap<Long, Long> shardsToSchedule = ArrayListMultimap.create();
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            // prepare shards
            List<ShardGroup> anonymousShardGroups = new ArrayList<>();
            for (CreateShardInfo info : createShardInfos) {
                long shardId = info.getShardId();
                if (shardId == Constant.DEFAULT_ID) {
                    // application did not assign one
                    shardId = idGenerator.getNextId();
                }

                if (shards.containsKey(shardId)) {
                    throw new StarException(ExceptionCode.ALREADY_EXIST,
                            String.format("shard %d already exist in service %s.", shardId, serviceId));
                }
                Preconditions.checkArgument(info.getReplicaCount() == 0 || info.getReplicaCount() == 1,
                        "replica count in createShard request is deprecated");
                int shardReplicaNum = 1;
                List<Long> groupIds = info.getGroupIdsList();
                for (Long groupId : groupIds) {
                    ShardGroup group = shardGroups.get(groupId);
                    if (group == null) {
                        throw new StarException(ExceptionCode.NOT_EXIST,
                                String.format("shard group %d not exist in service %s.", groupId, serviceId));
                    }
                    if (group.getPlacementPolicy() == PlacementPolicy.PACK) {
                        // Keep it here just for backward-compatibility, so that the new version can be still
                        // successfully rollback to an old version without breaking the PACK group constraints.
                        int replicaNum = getFirstShardReplicaNumFromGroup(group, -1);
                        if (replicaNum != -1 && shardReplicaNum != replicaNum) {
                            throw new StarException(ExceptionCode.INVALID_ARGUMENT,
                                    String.format("Inconsistent shard replica num:%d when join a PACK shard group:%d.",
                                            shardReplicaNum, groupId));
                        }
                    }
                }
                ShardGroup anonymousShardGroup;
                if (!info.getPlacementPreferencesList().isEmpty()) {
                    // TODO: support multi preferences
                    if (info.getPlacementPreferencesList().size() != 1) {
                        throw new StarException(ExceptionCode.INVALID_ARGUMENT,
                                String.format("shard placement preference does not support multiple in service %s.",
                                        serviceId));
                    }
                    PlacementPreference preference = info.getPlacementPreferencesList().get(0);
                    checkPlacementPreference(preference);
                    anonymousShardGroup = new ShardGroup(serviceId, idGenerator.getNextId(),
                            preference.getPlacementPolicy(), true /* anonymous */, Constant.DEFAULT_ID /* metaGroupId */);
                    anonymousShardGroup.addShardId(preference.getRelationshipTargetId());
                    anonymousShardGroup.addShardId(shardId);
                    anonymousShardGroups.add(anonymousShardGroup);
                }

                FilePath path;
                if (!info.hasPathInfo()) {
                    FileStore fs = fsMgr.allocFileStore("");
                    path = new FilePath(fs, String.format("%s/%s", serviceId, ""));
                } else {
                    FileStore fs = fsMgr.getFileStore(info.getPathInfo().getFsInfo().getFsKey());
                    if (fs == null) {
                        throw new StarException(ExceptionCode.NOT_EXIST,
                                String.format("file store with key '%s' not exist", info.getPathInfo().getFsInfo().getFsKey()));
                    }
                    path = FilePath.fromFullPath(fs, info.getPathInfo().getFullPath());
                }

                FileCache cache = FileCache.fromProtobuf(info.getCacheInfo());
                Shard shard = new Shard(serviceId, groupIds, shardId, path, cache);

                Map<String, String> shardProperties = info.getShardPropertiesMap();
                shard.setProperties(shardProperties);

                shardsToCreate.add(shard);
                long workerGroupId = info.getScheduleToWorkerGroup();
                if (workerGroupId != Constant.DEFAULT_ID || Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY) {
                    shardsToSchedule.put(workerGroupId, shardId);
                }
            }

            CreateShardJournalInfo.Builder builder = CreateShardJournalInfo.newBuilder();
            for (Shard shard : shardsToCreate) {
                builder.addShardInfos(shard.toProtobuf());
            }
            for (ShardGroup anonymousShardGroup : anonymousShardGroups) {
                builder.addShardGroupInfos(anonymousShardGroup.toProtobuf());
            }
            Journal journal = StarMgrJournal.logCreateShard(serviceId, builder.build());
            journalSystem.write(journal);

            // commit shards
            executeNoExceptionOrDie(() -> commitCreateShard(shardsToCreate, anonymousShardGroups));
        }
        List<Long> shardIds = shardsToCreate.stream().map(Shard::getShardId).collect(Collectors.toList());
        if (Config.SCHEDULER_TRIGGER_SCHEDULE_WHEN_CREATE_SHARD) {
            for (long workerGroupId : shardsToSchedule.asMap().keySet()) {
                List<Long> shardIdList = new ArrayList<>(shardsToSchedule.get(workerGroupId));
                try {
                    shardScheduler.scheduleAddToGroup(serviceId, shardIdList, workerGroupId);
                } catch (StarException e) {
                    // Ignore the failure for now, shardChecker will take care.
                    LOG.warn("Fail to schedule new created shards to default workerGroup for service: {}," +
                            " ignore the error for now. Error:{}", serviceId, e.getMessage());
                }
            }
        }
        return getShardInfo(shardIds);
    }

    public void deleteShard(List<Long> shardIds) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            // validate all shards
            List<Shard> shardsToDelete = new ArrayList<>(shardIds.size());
            for (Long shardId : shardIds) {
                Shard shard = shards.get(shardId);
                if (shard == null) {
                    continue; // it's ok to delete non-exist shard
                }
                shardsToDelete.add(shard);
            }

            if (shardsToDelete.isEmpty()) {
                return;
            }
            Journal journal = StarMgrJournal.logDeleteShard(serviceId, shardIds);
            journalSystem.write(journal);
            executeNoExceptionOrDie(() -> {
                for (Shard shard : shardsToDelete) {
                    Shard old = removeShardInternalNoLock(shard);
                    assert old != null;
                }
            });
        }
    }

    public void updateShard(List<UpdateShardInfo> updateShardInfos) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            List<Shard> shardsToUpdate = new ArrayList<>();
            for (UpdateShardInfo updateShardInfo : updateShardInfos) {
                if (updateShardInfo.getEnableCache() == CacheEnableState.NOT_SET) {
                    continue; // enableCache has not been set
                }
                Shard shard = shards.get(updateShardInfo.getShardId());
                if (shard == null) {
                    continue; // it's no need to care about non-exist shards
                }
                if (updateShardInfo.getEnableCache() == CacheEnableState.ENABLED
                        && !shard.getFileCache().getFileCacheEnable()) {
                    shard.setFileCacheEnable(true);
                    shardsToUpdate.add(shard);
                } else if (updateShardInfo.getEnableCache() == CacheEnableState.DISABLED
                        && shard.getFileCache().getFileCacheEnable()) {
                    shard.setFileCacheEnable(false);
                    shardsToUpdate.add(shard);
                }
            }

            if (!shardsToUpdate.isEmpty()) {
                try {
                    Journal journal = StarMgrJournal.logUpdateShard(serviceId, shardsToUpdate);
                    journalSystem.write(journal);
                } catch (StarException e) {
                    // revert back
                    for (Shard shard : shardsToUpdate) {
                        shard.setFileCacheEnable(!shard.getFileCache().getFileCacheEnable());
                    }
                    throw e;
                }
            }
        }
    }

    /**
     * Get shard info in service
     *
     * @throws StarException if shard does not exist or shard not belong to this service
     */
    public List<ShardInfo> getShardInfo(List<Long> shardIds) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            List<ShardInfo> shardInfos = new ArrayList<>(shardIds.size());
            for (Long shardId : shardIds) {
                Shard shard = shards.get(shardId);
                if (shard == null) {
                    throw new StarException(ExceptionCode.NOT_EXIST,
                            String.format("shard %d not exist.", shardId));
                }
                shardInfos.add(shard.toProtobuf());
            }

            return shardInfos;
        }
    }

    public List<List<ShardInfo>> listShardInfo(List<Long> groupIds, boolean withoutReplicaInfo)
            throws StarException {
        if (groupIds.isEmpty()) {
            throw new StarException(ExceptionCode.INVALID_ARGUMENT, "shard group id can not be empty.");
        }

        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            List<List<ShardInfo>> shardInfos = new ArrayList<>(groupIds.size());

            for (Long groupId : groupIds) {
                ShardGroup shardGroup = shardGroups.get(groupId);
                if (shardGroup == null) {
                    throw new StarException(ExceptionCode.NOT_EXIST, String.format("shard group %d not exist.", groupId));
                }

                List<ShardInfo> infos = new ArrayList<>(shardGroup.getShardIds().size());
                for (Long shardId : shardGroup.getShardIds()) {
                    Shard shard = shards.get(shardId);
                    infos.add(shard.toProtobuf(withoutReplicaInfo));
                }

                shardInfos.add(infos);
            }

            return shardInfos;
        }
    }

    // TODO: the returned shard is not safe, need look into it later
    public Shard getShard(long shardId) {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return shards.get(shardId);
        }
    }

    public ShardGroup getShardGroup(long groupId) {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return shardGroups.get(groupId);
        }
    }

    public MetaGroup getMetaGroup(long groupId) {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return metaGroups.get(groupId);
        }
    }

    // NOTE: avoid use this interface in large scale
    public List<Long> getAllShardIds() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            return new ArrayList<>(shards.keySet());
        }
    }

    // Use this interface to iterate all the shards one by one, it is thread-safe with underlying ConcurrentHashMap
    public Iterator<Map.Entry<Long, Shard>> getShardIterator() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            return shards.entrySet().iterator();
        }
    }

    public List<Long> getAllShardGroupIds() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            return new ArrayList<>(shardGroups.keySet());
        }
    }

    public List<Long> getAllMetaGroupIds() {
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            return new ArrayList<>(metaGroups.keySet());
        }
    }

    // return size of anonymous group
    private int verifyShardGroupInfoForMetaGroup(List<Long> shardGroupIds, int expectSize) throws StarException {
        // TODO: maybe also verify all shard id and group id?
        for (Long shardGroupId : shardGroupIds) {
            ShardGroup shardGroup = shardGroups.get(shardGroupId);
            if (shardGroup == null) {
                throw new StarException(ExceptionCode.INVALID_ARGUMENT,
                        String.format("shard group %d not exist.", shardGroupId));
            }
            if (expectSize == -1) { // use the first group size
                expectSize = shardGroup.getShardIds().size();
            }
            if (expectSize != shardGroup.getShardIds().size()) {
                throw new StarException(ExceptionCode.INVALID_ARGUMENT,
                        String.format("shard size mismatch, expect %d, has %d.",
                                expectSize, shardGroup.getShardIds().size()));
            }
        }
        return expectSize;
    }

    public MetaGroupInfo createMetaGroup(CreateMetaGroupInfo createMetaGroupInfo) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            long metaGroupId;
            if (createMetaGroupInfo.getMetaGroupId() == Constant.DEFAULT_ID) {
                // application did not assign one
                metaGroupId = idGenerator.getNextId();
            } else {
                metaGroupId = createMetaGroupInfo.getMetaGroupId();
            }
            if (metaGroups.containsKey(metaGroupId)) {
                throw new StarException(ExceptionCode.ALREADY_EXIST,
                        String.format("meta group %d already exists.", metaGroupId));
            }
            PlacementPolicy placementPolicy = createMetaGroupInfo.getPlacementPolicy();
            if (placementPolicy != PlacementPolicy.PACK) {
                // TODO: support EXCLUDE policy
                throw new StarException(ExceptionCode.INVALID_ARGUMENT,
                        String.format("meta group placement policy %s not allowed.",
                                placementPolicy.name()));
            }

            List<ShardGroup> anonymousShardGroups;
            List<Long> shardGroupIds = createMetaGroupInfo.getShardGroupIdsList();
            if (!shardGroupIds.isEmpty()) {
                int size = verifyShardGroupInfoForMetaGroup(shardGroupIds, -1 /* expectSize */);

                anonymousShardGroups = prepareAnonymousShardGroup(metaGroupId, size, placementPolicy,
                        null /* anonymousShardGroupIds */);
                validateShardReplicaNumInGroup(placementPolicy, anonymousShardGroups, shardGroupIds);
            } else {
                anonymousShardGroups = new ArrayList<>();
            }

            List<Long> anonymousShardGroupIds =
                    anonymousShardGroups.stream().map(ShardGroup::getGroupId).collect(Collectors.toList());
            MetaGroup metaGroup = new MetaGroup(serviceId, metaGroupId,
                    anonymousShardGroupIds,
                    createMetaGroupInfo.getPlacementPolicy());

            MetaGroupJournalInfo journalInfo = MetaGroupJournalInfo.newBuilder()
                    .setMetaGroupInfo(metaGroup.toProtobuf())
                    .setCreateInfo(createMetaGroupInfo)
                    .build();
            Journal journal = StarMgrJournal.logCreateMetaGroup(serviceId, journalInfo);
            journalSystem.write(journal);
            executeNoExceptionOrDie(() -> {
                commitAnonymousShardGroup(metaGroup, anonymousShardGroups, shardGroupIds);
                metaGroups.put(metaGroupId, metaGroup);
            });

            return metaGroup.toProtobuf();
        }
    }

    /**
     * validate every group.get(x) can be added into targetGroups.get(x)
     *
     * @param targetGroups target pack group list
     * @param groupIds     source shard groups
     */
    private void validateShardReplicaNumInGroup(PlacementPolicy policy, List<ShardGroup> targetGroups, List<Long> groupIds) {
        if (policy != PlacementPolicy.PACK) {
            return;
        }
        final int INVALID_REPLICA_NUM = -1;
        List<Integer> expectReplicaNum = new ArrayList<>(targetGroups.size());
        targetGroups.forEach(x -> {
            int replicaNum = getFirstShardReplicaNumFromGroup(x, INVALID_REPLICA_NUM);
            expectReplicaNum.add(replicaNum);
        });

        for (long gid : groupIds) {
            ShardGroup group = shardGroups.get(gid);
            Preconditions.checkNotNull(group);
            int pos = 0;
            for (long sid : group.getShardIds()) {
                Shard shard = shards.get(sid);
                Preconditions.checkNotNull(shard);
                int replicaNum = shard.getExpectedReplicaNum();
                if (expectReplicaNum.get(pos) == INVALID_REPLICA_NUM) {
                    expectReplicaNum.add(pos, replicaNum);
                } else if (expectReplicaNum.get(pos) != replicaNum) {
                    throw new StarException(ExceptionCode.INVALID_ARGUMENT,
                            String.format("shard:%d replicaNum: %d, target group expected replica num: %d",
                                    sid, replicaNum, expectReplicaNum.get(pos)));
                }
                ++pos;
            }
        }
    }

    /**
     * Get the replica number of the first shard in the shard group, return valueIfEmpty if no valid shard in group.
     *
     * @param group        target shard group
     * @param valueIfEmpty value returned if group is empty
     * @return the first valid shard replica number or `valueIfEmpty` if no valid shard can be found.
     */
    @Deprecated
    private int getFirstShardReplicaNumFromGroup(ShardGroup group, int valueIfEmpty) {
        int result = valueIfEmpty;
        for (long shardId : group.getShardIds()) {
            if (shards.containsKey(shardId)) {
                result = shards.get(shardId).getExpectedReplicaNum();
                break;
            }
        }
        return result;
    }

    public void deleteMetaGroup(long metaGroupId) throws StarException {
        deleteMetaGroupInternal(metaGroupId, false /* isReplay */);
    }

    public void deleteMetaGroupInternal(long metaGroupId, boolean isReplay) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            MetaGroup metaGroup = metaGroups.get(metaGroupId);

            if (metaGroup == null) {
                return;
            }

            if (!isReplay) {
                DeleteMetaGroupInfo deleteInfo = DeleteMetaGroupInfo.newBuilder()
                        .setMetaGroupId(metaGroupId)
                        .build();
                MetaGroupJournalInfo journalInfo = MetaGroupJournalInfo.newBuilder()
                        .setDeleteInfo(deleteInfo)
                        .build();
                Journal journal = StarMgrJournal.logDeleteMetaGroup(serviceId, journalInfo);
                journalSystem.write(journal);
            }
            executeNoExceptionOrDie(() -> {
                List<Long> anonymousShardGroupIds = metaGroup.getShardGroupIds();
                // 1. remove all shards from anonymous group
                for (Long groupId : anonymousShardGroupIds) {
                    ShardGroup shardGroup = shardGroups.get(groupId);
                    assert shardGroup != null;
                    for (Long shardId : shardGroup.getShardIds()) {
                        Shard shard = shards.get(shardId);
                        assert shard != null;
                        boolean v = shard.quitGroup(groupId);
                        assert v;
                    }
                }
                // 2. remove anonymous group
                for (Long groupId : anonymousShardGroupIds) {
                    ShardGroup old = operateShardGroupInternal(groupId, null /* shardGroup */, false /* isAdd */);
                    assert old != null;
                }
                // 3. remove meta group itself
                MetaGroup old = metaGroups.remove(metaGroupId);
                assert old != null;
            });
        }
    }

    private List<ShardGroup> prepareAnonymousShardGroup(long metaGroupId, int anonymousGroupSize, PlacementPolicy placementPolicy,
            List<Long> anonymousShardGroupIds) throws StarException {
        if (anonymousGroupSize == 0) {
            throw new StarException(ExceptionCode.INVALID_ARGUMENT, "anonymous shard group size can not be 0.");
        }
        List<ShardGroup> anonymousShardGroups = new ArrayList<>();
        for (int i = 0; i < anonymousGroupSize; ++i) {
            long groupId;
            if (anonymousShardGroupIds != null) { // for replay, id already assigned
                groupId = anonymousShardGroupIds.get(i);
            } else {
                groupId = idGenerator.getNextId();
            }
            ShardGroup anonymousShardGroup = new ShardGroup(serviceId, groupId,
                    placementPolicy, true /* anonymous */, metaGroupId);
            anonymousShardGroups.add(anonymousShardGroup);
        }
        return anonymousShardGroups;
    }

    private void commitAnonymousShardGroup(MetaGroup metaGroup, List<ShardGroup> anonymousShardGroups, List<Long> shardGroupIds) {
        Map<Long, ShardGroup> updateShardList = new HashMap<>();

        // 1. add shard to anonymous group
        List<ShardGroup> shardGroupList = shardGroupIds.stream().map(shardGroups::get).collect(Collectors.toList());
        for (int pos = 0; pos < anonymousShardGroups.size(); ++pos) {
            List<Long> batchShardIds = new ArrayList<>(shardGroupList.size());
            for (ShardGroup group : shardGroupList) {
                batchShardIds.add(group.getShardIds().get(pos));
            }

            ShardGroup anonShardGroup = anonymousShardGroups.get(pos);
            List<Long> addedShardIds = anonShardGroup.batchAddShardId(batchShardIds);
            // Shard and ShardGroup is a bidirectional relationship, there is no possibility that a shard is
            // in a shard group's shard id list but the shardgroup is not in the shard's groupList, nor the
            // vice versa.
            for (long id : addedShardIds) {
                updateShardList.put(id, anonShardGroup);
            }
        }

        // 2. add anonymous group to shard
        for (Map.Entry<Long, ShardGroup> entry : updateShardList.entrySet()) {
            Shard shard = shards.get(entry.getKey());
            shard.joinGroup(entry.getValue().getGroupId());
        }

        // 3. add anonymous group to shard group
        for (ShardGroup shardGroup : anonymousShardGroups) {
            operateShardGroupInternal(shardGroup.getGroupId(), shardGroup, true /* isAdd */);
        }

        // 4. add anonymous group to meta group
        List<Long> anonymousShardGroupIds =
                anonymousShardGroups.stream().map(ShardGroup::getGroupId).collect(Collectors.toList());
        metaGroup.setShardGroupIds(anonymousShardGroupIds);
    }

    private MetaGroup verifyAndGetMetaGroup(long metaGroupId) throws StarException {
        if (metaGroupId == Constant.DEFAULT_ID) {
            throw new StarException(ExceptionCode.INVALID_ARGUMENT, "meta group id not set.");
        }
        MetaGroup metaGroup = metaGroups.get(metaGroupId);
        if (metaGroup == null) {
            throw new StarException(ExceptionCode.NOT_EXIST,
                    String.format("meta group id %d not exist.", metaGroupId));
        }
        return metaGroup;
    }

    private Pair<MetaGroup, MetaGroup> verifyUpdateMetaGroupInfo(UpdateMetaGroupInfo updateMetaGroupInfo) throws StarException {
        MetaGroup src = null;
        MetaGroup dst = null;
        UpdateMetaGroupInfo.InfoCase icase = updateMetaGroupInfo.getInfoCase();
        switch (icase) {
            case JOIN_INFO: {
                dst = verifyAndGetMetaGroup(updateMetaGroupInfo.getJoinInfo().getMetaGroupId());
                break;
            }
            case QUIT_INFO: {
                src = verifyAndGetMetaGroup(updateMetaGroupInfo.getQuitInfo().getMetaGroupId());
                break;
            }
            case TRANSFER_INFO: {
                src = verifyAndGetMetaGroup(updateMetaGroupInfo.getTransferInfo().getSrcMetaGroupId());
                dst = verifyAndGetMetaGroup(updateMetaGroupInfo.getTransferInfo().getDstMetaGroupId());
                break;
            }
            case INFO_NOT_SET: {
                throw new StarException(ExceptionCode.INVALID_ARGUMENT, "update meta group info type not set.");
            }
        }
        return Pair.of(src, dst);
    }

    public void updateMetaGroup(UpdateMetaGroupInfo updateMetaGroupInfo) throws StarException {
        updateMetaGroupInternal(updateMetaGroupInfo, false /* isReplay */, null /* anonymousShardGroupIdsForReplay */);
    }

    private void updateMetaGroupInternal(UpdateMetaGroupInfo updateMetaGroupInfo,
                                         boolean isReplay,
                                         List<Long> anonymousShardGroupIdsForReplay) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            Pair<MetaGroup, MetaGroup> pair = verifyUpdateMetaGroupInfo(updateMetaGroupInfo);
            MetaGroup src = pair.getKey();
            MetaGroup dst = pair.getValue();

            // verify shard group
            List<Long> shardGroupIds = updateMetaGroupInfo.getShardGroupIdsList();
            if (shardGroupIds.isEmpty()) {
                throw new StarException(ExceptionCode.INVALID_ARGUMENT, "empty shard group list.");
            }
            if (src != null) {
                verifyShardGroupInfoForMetaGroup(shardGroupIds, src.getShardGroupIds().size() /* expectSize */);
            }
            int dstSize = -1;
            if (dst != null) {
                int expectSize = -1;
                if (!dst.getShardGroupIds().isEmpty()) {
                    expectSize = dst.getShardGroupIds().size();
                }
                dstSize = verifyShardGroupInfoForMetaGroup(shardGroupIds, expectSize);
            }

            // prepare dst anonymous shard group
            List<ShardGroup> anonymousShardGroups = new ArrayList<>();
            if (dst != null) {
                if (dst.getShardGroupIds().isEmpty()) {
                    anonymousShardGroups = prepareAnonymousShardGroup(dst.getMetaGroupId(), dstSize, dst.getPlacementPolicy(),
                            anonymousShardGroupIdsForReplay);
                } else {
                    for (Long groupId : dst.getShardGroupIds()) {
                        anonymousShardGroups.add(shardGroups.get(groupId));
                    }
                }
                validateShardReplicaNumInGroup(dst.getPlacementPolicy(), anonymousShardGroups, shardGroupIds);
            }

            if (!isReplay) {
                MetaGroupJournalInfo.Builder journalInfoBuilder = MetaGroupJournalInfo.newBuilder();
                if (dst != null) {
                    MetaGroupInfo.Builder infoBuilder = MetaGroupInfo.newBuilder().mergeFrom(dst.toProtobuf());
                    if (dst.getShardGroupIds().isEmpty()) {
                        List<Long> anonymousShardGroupIds =
                                anonymousShardGroups.stream().map(ShardGroup::getGroupId).collect(Collectors.toList());
                        infoBuilder.addAllShardGroupIds(anonymousShardGroupIds);
                    }
                    journalInfoBuilder.setMetaGroupInfo(infoBuilder.build());
                }
                journalInfoBuilder.setUpdateInfo(updateMetaGroupInfo);
                Journal journal = StarMgrJournal.logUpdateMetaGroup(serviceId, journalInfoBuilder.build());
                journalSystem.write(journal);
            }
            final List<ShardGroup> finalAnonymousShardGroups = anonymousShardGroups;
            executeNoExceptionOrDie(() -> {
                // remove from src meta group
                if (src != null) {
                    for (Long shardGroupId : shardGroupIds) {
                        ShardGroup shardGroup = shardGroups.get(shardGroupId);
                        int idx = 0;
                        for (Long shardId : shardGroup.getShardIds()) {
                            ShardGroup anonymousShardGroup = shardGroups.get(src.getShardGroupIds().get(idx));
                            boolean v1 = anonymousShardGroup.removeShardId(shardId);
                            assert v1;
                            Shard shard = shards.get(shardId);
                            boolean v2 = shard.quitGroup(anonymousShardGroup.getGroupId());
                            assert v2;
                            idx++;
                        }
                    }
                    tryDeleteMetaGroupAfterUpdate(updateMetaGroupInfo, src);
                }
                // add to dst meta group
                if (dst != null) {
                    commitAnonymousShardGroup(dst, finalAnonymousShardGroups, shardGroupIds);
                }
            });
        }
    }

    // if delete option is set and meta group has no shard anymore, delete it
    private void tryDeleteMetaGroupAfterUpdate(UpdateMetaGroupInfo updateMetaGroupInfo, MetaGroup metaGroup) {
        boolean deleteIfEmpty = false;
        switch (updateMetaGroupInfo.getInfoCase()) {
            case QUIT_INFO: {
                deleteIfEmpty = updateMetaGroupInfo.getQuitInfo().getDeleteMetaGroupIfEmpty();
                break;
            }
            case TRANSFER_INFO: {
                deleteIfEmpty = updateMetaGroupInfo.getTransferInfo().getDeleteSrcMetaGroupIfEmpty();
                break;
            }
        }

        if (!deleteIfEmpty) {
            return;
        }

        List<Long> anonymousShardGroupIds = metaGroup.getShardGroupIds();
        for (Long groupId : anonymousShardGroupIds) {
            ShardGroup shardGroup = shardGroups.get(groupId);
            assert shardGroup != null;
            if (!shardGroup.getShardIds().isEmpty()) {
                return;
            }
        }

        // set isReplay to true to prevent journal writing
        deleteMetaGroupInternal(metaGroup.getMetaGroupId(), true /* isReplay */);
    }

    public MetaGroupInfo getMetaGroupInfo(long metaGroupId) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            MetaGroup metaGroup = metaGroups.get(metaGroupId);
            if (metaGroup == null) {
                throw new NotExistStarException("meta group {} not exist.", metaGroupId);
            }
            return metaGroup.toProtobuf();
        }
    }

    public List<MetaGroupInfo> listMetaGroupInfo() throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            List<MetaGroupInfo> metaGroupInfos = new ArrayList<>();
            for (MetaGroup metaGroup : metaGroups.values()) {
                metaGroupInfos.add(metaGroup.toProtobuf());
            }
            return metaGroupInfos;
        }
    }

    public boolean isMetaGroupStable(long metaGroupId, List<Long> workerIds) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            MetaGroup metaGroup = metaGroups.get(metaGroupId);
            if (metaGroup == null) {
                throw new NotExistStarException("meta group {} not exist.", metaGroupId);
            }
            if (metaGroup.getPlacementPolicy() != PlacementPolicy.PACK) {
                return true;
            }
            if (workerIds.isEmpty()) {
                return true;
            }
            for (long groupId : metaGroup.getShardGroupIds()) {
                if (!isShardGroupStableInternal(metaGroupId, groupId, workerIds)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isShardGroupStableInternal(long metaGroupId, long groupId, List<Long> workerIds) throws StarException {
        ShardGroup group = shardGroups.get(groupId);
        if (group == null) {
            throw new InternalErrorStarException("meta group:{} internal state error, subgroup: {}!",
                    metaGroupId, groupId);
        }
        if (group.getPlacementPolicy() != PlacementPolicy.PACK) {
            throw new InternalErrorStarException("meta group:{} internal state error, subgroup: {}!",
                    metaGroupId, groupId);
        }
        List<Long> compareList = null;
        for (long shardId : group.getShardIds()) {
            Shard shard = shards.get(shardId);
            if (shard == null) {
                throw new InternalErrorStarException("meta group:{} internal state error, sub-shard: {}!",
                        metaGroupId, shardId);
            }
            List<Long> replicaList = shard.getReplicaWorkerIds();
            replicaList.retainAll(workerIds);
            replicaList.sort(null);
            if (compareList == null) {
                compareList = replicaList;
            } else if (!compareList.equals(replicaList)) {
                return false;
            }
        }
        return true;
    }

    public void scheduleShardsBelongToWorker(long workerId) {
        List<Long> shardIds;
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            // TODO: might be time cost iterating the entire set
            shardIds = shards.entrySet()
                    .stream()
                    .filter(x -> x.getValue().hasReplica(workerId))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
        }
        if (!shardIds.isEmpty()) {
            shardScheduler.scheduleAsyncAddToWorker(serviceId, shardIds, workerId);
        }
    }

    public void addShardReplicas(List<Long> shardIds, long workerId, boolean isTemp) {
        ShardReplicaOp op = isTemp ? ShardReplicaOp.ADD_TEMP : ShardReplicaOp.ADD;
        updateShardReplicaInfoInternal(shardIds, workerId, op);
    }

    public void cancelScaleInShardReplica(List<Long> shardIds, long workerId) {
        updateShardReplicaInfoInternal(shardIds, workerId, ShardReplicaOp.CANCEL_SCALE_IN);
    }

    public void removeShardReplicas(List<Long> shardIds, long workerId) {
        updateShardReplicaInfoInternal(shardIds, workerId, ShardReplicaOp.DELETE);
    }

    public void scaleInShardReplicas(List<Long> shardIds, long workerId) {
        updateShardReplicaInfoInternal(shardIds, workerId, ShardReplicaOp.SCALE_IN);
    }

    public void scaleOutShardReplicas(List<Long> shardIds, long workerId, boolean isTemp) {
        ShardReplicaOp op = isTemp ? ShardReplicaOp.SCALE_OUT_TEMP : ShardReplicaOp.SCALE_OUT;
        updateShardReplicaInfoInternal(shardIds, workerId, op);
    }

    public void scaleOutShardReplicasDone(List<Long> shardIds, long workerId) {
        updateShardReplicaInfoInternal(shardIds, workerId, ShardReplicaOp.SCALE_OUT_DONE);
    }

    public void convertTempShardReplicaToNormal(List<Long> shardIds, long workerId) {
        updateShardReplicaInfoInternal(shardIds, workerId, ShardReplicaOp.TEMP_TO_NORMAL);
    }

    /**
     * Add/Remove shard replica of workerId, write journal if necessary
     * @param shardIds list of shards whose replicas will be changed
     * @param workerId target worker id
     */
    private void updateShardReplicaInfoInternal(List<Long> shardIds, long workerId, ShardReplicaOp op) {
        if (shardIds.isEmpty()) {
            return;
        }

        BiFunction<Shard, Long, Boolean> biFunc = SHARD_REPLICA_OP_BI_FUNCTION_MAP.get(op);
        Preconditions.checkNotNull(biFunc, "Unknown ShardReplicaOp op: " + op);

        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            List<Shard> shardsToUpdate = new ArrayList<>();
            List<Long> missingIds = new ArrayList<>();

            for (Long shardId : shardIds) {
                Shard shard = shards.get(shardId);
                if (shard == null) {
                    // it's possible that shard is already deleted
                    missingIds.add(shardId);
                    continue;
                }
                if (biFunc.apply(shard, workerId)) {
                    shardsToUpdate.add(shard);
                }
            }

            // write shard info to disk after updating memory
            if (!shardsToUpdate.isEmpty()) {
                try {
                    Journal journal = StarMgrJournal.logUpdateShard(serviceId, shardsToUpdate);
                    // NOTE: no need to wait for the EditLog's completion.
                    // The journal is still submit to the queue under ShardManager's write lock, so the order of the
                    // EditLog is still assured.
                    // - If the journal fails to write eventually, the status will be recovered either by process restart
                    //  and editLog replay. Or eventually overwritten by subsequent logUpdateShard
                    // - If the journal write success with a long delay, the affection is that the follower can't get
                    //  the up-to-date Shard replication info and believe that the shard needs an on-demand reschedule
                    //  and as a result, respond the client with NOT_LEADER and the request will be retried to LEADER
                    //  again, which introduces additional load to LEADER.
                    // This trade-off is acceptable in current situation.
                    journalSystem.writeAsync(journal);
                } catch (StarException e) {
                    // NOTE: shard schedule does not offer strong consistency, if log shard info failed,
                    //       availability always proceeds consistency, so here we consider
                    //       shard schedule succeed even if journal write failure happens
                    List<Long> shardIdsToPrint = shardsToUpdate.stream().map(Shard::getShardId).collect(Collectors.toList());
                    LOG.error("log shard info after schedule failed, {}. shards:{}, service:{}.",
                            e.getMessage(), shardIdsToPrint, serviceId);
                }
            }

            if (!missingIds.isEmpty()) {
                LOG.warn("shard {} not exist when update shard info from shard scheduler!", missingIds);
            }
        }
    }

    /**
     * Check missing replicas from the list of shards for the specific worker id, and remove non-exist replicas
     * @param shardIds list of shard id
     * @param workerId target worker id
     */
    public void validateWorkerReportedReplicas(List<Long> shardIds, long workerId) {
        if (shardIds.isEmpty()) {
            return;
        }

        List<Long> missingIds = new ArrayList<>();
        try (LockCloseable ignore = new LockCloseable(lock.readLock())) {
            shardIds.forEach(x -> {
                Shard shard = shards.get(x);
                // shard is deleted or shard does not have the replica
                if (shard == null || !shard.hasReplica(workerId)) {
                    missingIds.add(x);
                }
            });
        }
        // TODO: handle possible race condition.
        //   one of shardA's replica is scheduled to workerId, the RPC is made successful, but the updateShardInfo in
        //   scheduling is not yet completed, in this time window, worker's heartbeat reports the shard which can't be
        //   found in this shards check, and hence will ask worker to remove it again.
        if (!missingIds.isEmpty()) {
            LOG.debug("shard {} not exist or have outdated info when update shard info from worker heartbeat, " +
                    "schedule remove from worker {}.", missingIds, workerId);
            shardScheduler.scheduleAsyncRemoveFromWorker(serviceId, missingIds, workerId);
        }
    }

    public void replayCreateShard(CreateShardJournalInfo info) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            List<Shard> shardList = new ArrayList<>();
            for (ShardInfo shardInfo : info.getShardInfosList()) {
                Shard shard = Shard.fromProtobuf(shardInfo);
                shardList.add(shard);
            }
            List<ShardGroup> anonymousShardGroups = new ArrayList<>();
            for (ShardGroupInfo shardGroupInfo : info.getShardGroupInfosList()) {
                ShardGroup shardGroup = ShardGroup.fromProtobuf(shardGroupInfo);
                anonymousShardGroups.add(shardGroup);
            }

            commitCreateShard(shardList, anonymousShardGroups);

            // TODO: state machine
        }
    }

    public void replayDeleteShard(List<Long> shardIds) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            for (Long shardId : shardIds) {
                Shard shard = shards.get(shardId);
                if (shard == null) {
                    LOG.warn("shard {} not exist when replay delete shard, just ignore.", shardId);
                    continue;
                }
                Shard old = removeShardInternalNoLock(shard);
                assert old != null;
            }
            // TODO: state machine
        }
    }

    public void replayUpdateShard(List<Shard> shardList) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            for (Shard shard : shardList) {
                Shard old = addShardInternalNoLock(shard);
                if (old == null) {
                    LogUtils.fatal(LOG, "shard {} not exist when replay update shard, should not happen!", shard.getShardId());
                }
            }
            // TODO: state machine
        }
    }

    public void replayCreateShardGroup(List<ShardGroup> groups) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            for (ShardGroup shardGroup : groups) {
                ShardGroup old = operateShardGroupInternal(shardGroup.getGroupId(), shardGroup, true /* isAdd */);
                if (old != null) {
                    LogUtils.fatal(LOG, "shard group {} already exist when replay create shard group, should not happen!",
                            shardGroup.getGroupId());
                }
            }
            // TODO: state machine
        }
    }

    public void replayDeleteShardGroup(DeleteShardGroupInfo info) {
        deleteShardGroupInternal(info.getGroupIdsList(), info.getCascadeDeleteShard(), true /* isReplay */);
    }

    public void replayUpdateShardGroup(List<ShardGroup> groupList) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            for (ShardGroup shardGroup : groupList) {
                ShardGroup old = shardGroups.put(shardGroup.getGroupId(), shardGroup);
                if (old == null) {
                    LogUtils.fatal(LOG, "shard group {} not exist when replay update shard group, should not happen!",
                            shardGroup.getGroupId());
                }
            }
            // TODO: state machine
        }
    }

    public void replayCreateMetaGroup(MetaGroupJournalInfo info) throws StarException {
        CreateMetaGroupInfo createMetaGroupInfo = info.getCreateInfo();
        MetaGroup metaGroup = MetaGroup.fromProtobuf(info.getMetaGroupInfo());

        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            long metaGroupId = metaGroup.getMetaGroupId();
            if (metaGroups.containsKey(metaGroupId)) {
                throw new StarException(ExceptionCode.ALREADY_EXIST,
                        String.format("meta group %d already exists.", metaGroupId));
            }

            List<ShardGroup> anonymousShardGroups = new ArrayList<>();
            List<Long> shardGroupIds = createMetaGroupInfo.getShardGroupIdsList();
            if (!shardGroupIds.isEmpty()) {
                int size = verifyShardGroupInfoForMetaGroup(shardGroupIds, -1 /* expectSize */);

                anonymousShardGroups = prepareAnonymousShardGroup(metaGroupId, size, metaGroup.getPlacementPolicy(),
                        metaGroup.getShardGroupIds());
            }

            // skip the shard replica num check since this is a replay operation
            commitAnonymousShardGroup(metaGroup, anonymousShardGroups, shardGroupIds);

            metaGroups.put(metaGroupId, metaGroup);
        }
    }

    public void replayDeleteMetaGroup(MetaGroupJournalInfo info) throws StarException {
        long metaGroupId = info.getDeleteInfo().getMetaGroupId();
        deleteMetaGroupInternal(metaGroupId, true /* isReplay */);
    }

    public void replayUpdateMetaGroup(MetaGroupJournalInfo info) throws StarException {
        UpdateMetaGroupInfo updateMetaGroupInfo = info.getUpdateInfo();
        MetaGroup dstMetaGroup = MetaGroup.fromProtobuf(info.getMetaGroupInfo());
        updateMetaGroupInternal(updateMetaGroupInfo, true /* isReplay */, dstMetaGroup.getShardGroupIds());
    }

    // FOR TEST
    public int getShardCount() {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return shards.size();
        }
    }

    // FOR TEST
    public int getShardGroupCount() {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return shardGroups.size();
        }
    }

    // FOR TEST
    public int getMetaGroupCount() {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            return metaGroups.size();
        }
    }

    // FOR TEST
    public void overrideShards(Map<Long, Shard> shards) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            // override shard group
            shardGroups.clear();
            for (Shard shard : shards.values()) {
                for (Long groupId : shard.getGroupIds()) {
                    ShardGroup shardGroup = shardGroups.get(groupId);
                    if (shardGroup == null) {
                        shardGroup = new ShardGroup(serviceId, groupId);
                        operateShardGroupInternal(shardGroup.getGroupId(), shardGroup, true /* isAdd */);
                    }
                    shardGroup.addShardId(shard.getShardId());
                }
            }
            // override shard
            this.shards.clear();
            this.shards.putAll(shards);
            totalShardGauge.set(shards.size());
        }
    }

    /**
     * dump shardManager's meta
     *
     * @param out Output stream
     * @throws IOException I/O exception
     * <pre>
     *  +--------------------+
     *  |  SHARDMGR_HEADER   | (number of meta group, number of shard group, number of shard)
     *  +--------------------+
     *  | SHARDMGR_METAGROUP |
     *  +------------------- +
     *  |     METAGROUP_1    |
     *  |     METAGROUP_2    |
     *  |       ...          |
     *  |     METAGROUP_N    |
     *  +--------------------+
     *  |SHARGMGR_SHARD_GROUP|
     *  +--------------------+
     *  |    SHARDGROUP_1    |
     *  |    SHARDGROUP_2    |
     *  |       ...          |
     *  |    SHARDGROUP_N    |
     *  +--------------------+
     *  |   SHARGMGR_SHARD   |
     *  +--------------------+
     *  |      SHARD_1       |
     *  |      SHARD_2       |
     *  |       ...          |
     *  |      SHARD_N       |
     *  +--------------------+
     *  |  SHARDMGR_FOOTER   | (checksum)
     *  +--------------------+
     * </pre>
     */
    public void dumpMeta(OutputStream out) throws IOException {
        try (LockCloseable ignored = new LockCloseable(lock.readLock())) {
            LOG.debug("start dump shard manager meta data ...");

            // write header
            ShardManagerImageMetaHeader header = ShardManagerImageMetaHeader.newBuilder()
                    .setDigestAlgorithm(Constant.DEFAULT_DIGEST_ALGORITHM)
                    .setNumShard(shards.size())
                    .setNumShardGroup(shardGroups.size())
                    .setNumMetaGroup(metaGroups.size())
                    .build();
            header.writeDelimitedTo(out);

            DigestOutputStream mdStream = Utils.getDigestOutputStream(out, Constant.DEFAULT_DIGEST_ALGORITHM);
            try (SectionWriter writer = new SectionWriter(mdStream)) {
                // shards in the shard group is rebuilt by during replay shards
                try (OutputStream stream = writer.appendSection(SectionType.SECTION_SHARDMGR_META_GROUP)) {
                    dumpMetaGroups(stream);
                }
                // shard group must be written before shards
                try (OutputStream stream = writer.appendSection(SectionType.SECTION_SHARDMGR_SHARD_GROUP)) {
                    dumpShardGroups(stream);
                }
                try (OutputStream stream = writer.appendSection(SectionType.SECTION_SHARDMGR_SHARD)) {
                    dumpShards(stream);
                }
            }
            // flush the mdStream because we are going to write to `out` directly.
            mdStream.flush();

            // write MetaFooter, DO NOT directly write new data here, change the protobuf definition
            ShardManagerImageMetaFooter.Builder footerBuilder = ShardManagerImageMetaFooter.newBuilder();
            if (mdStream.getMessageDigest() != null) {
                footerBuilder.setChecksum(ByteString.copyFrom(mdStream.getMessageDigest().digest()));
            }
            footerBuilder.build().writeDelimitedTo(out);
            LOG.debug("end dump shard manager meta data.");
        }
    }

    public void loadMeta(InputStream in) throws IOException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            LOG.debug("start load shard manager meta data ...");

            // read header
            ShardManagerImageMetaHeader header = ShardManagerImageMetaHeader.parseDelimitedFrom(in);
            if (header == null) {
                throw new EOFException();
            }
            DigestInputStream digestInput = Utils.getDigestInputStream(in, header.getDigestAlgorithm());

            try (SectionReader reader = new SectionReader(digestInput)) {
                reader.forEach(x -> loadShardManagerSection(x, header));
            }

            // read MetaFooter
            ShardManagerImageMetaFooter footer = ShardManagerImageMetaFooter.parseDelimitedFrom(in);
            if (footer == null) {
                throw new EOFException();
            }
            Utils.validateChecksum(digestInput.getMessageDigest(), footer.getChecksum());
            LOG.debug("end load shard manager meta data.");
        }
    }

    private void loadShardManagerSection(Section section, ShardManagerImageMetaHeader header) throws IOException {
        switch (section.getHeader().getSectionType()) {
            case SECTION_SHARDMGR_SHARD:
                loadShards(section.getStream(), header.getNumShard());
                break;
            case SECTION_SHARDMGR_SHARD_GROUP:
                loadShardGroups(section.getStream(), header.getNumShardGroup());
                break;
            case SECTION_SHARDMGR_META_GROUP:
                loadMetaGroups(section.getStream(), header.getNumMetaGroup());
                break;
            default:
                LOG.warn("Unknown section type:{} when loadMeta in ShardManager, ignore it!",
                        section.getHeader().getSectionType());
                break;
        }
    }

    private void dumpMetaGroups(OutputStream stream) throws IOException {
        for (MetaGroup mg : metaGroups.values()) {
            mg.toProtobuf().writeDelimitedTo(stream);
        }
    }

    private void loadMetaGroups(InputStream stream, int numMetaGroup) throws IOException {
        for (int i = 0; i < numMetaGroup; ++i) {
            MetaGroupInfo info = MetaGroupInfo.parseDelimitedFrom(stream);
            if (info == null) {
                throw new EOFException();
            }
            MetaGroup metaGroup = MetaGroup.fromProtobuf(info);
            metaGroups.put(metaGroup.getMetaGroupId(), metaGroup);
        }
    }

    private void dumpShardGroups(OutputStream stream) throws IOException {
        for (ShardGroup group : shardGroups.values()) {
            group.toProtobuf().writeDelimitedTo(stream);
        }
    }

    private void loadShardGroups(InputStream stream, int numShardGroup) throws IOException {
        for (int i = 0; i < numShardGroup; ++i) {
            ShardGroupInfo info = ShardGroupInfo.parseDelimitedFrom(stream);
            if (info == null) {
                throw new EOFException();
            }
            ShardGroup shardGroup = ShardGroup.fromProtobuf(info);
            operateShardGroupInternal(shardGroup.getGroupId(), shardGroup, true /* isAdd */);
        }
    }

    private void dumpShards(OutputStream stream) throws IOException {
        for (Shard shard : shards.values()) {
            shard.toProtobuf().writeDelimitedTo(stream);
        }
    }

    private void loadShards(InputStream stream, int numShard) throws IOException {
        for (int i = 0; i < numShard; ++i) {
            ShardInfo info = ShardInfo.parseDelimitedFrom(stream);
            if (info == null) {
                throw new EOFException();
            }
            Shard shard = Shard.fromProtobuf(info);
            Shard old = addShardInternalNoLock(shard);
            assert old == null;
        }
    }

    public void updateDelegatedFileStoreSnapshot(FileStore fileStore) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            setOrUpdateFileStoreSnapshotInternal(fileStore);
        }
    }

    public void replaceDelegatedFileStoreSnapshot(FileStore fileStore) {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            setOrReplaceFileStoreSnapshotInternal(fileStore);
        }
    }

    // NOTE: change `shardFileStoreSnapshot` under no lock
    private FileStore setOrUpdateFileStoreSnapshotInternal(FileStore fileStore) {
        DelegatedFileStore fs = shardFileStoreSnapshot.get(fileStore.key());
        if (fs == null) {
            // deepcopy fileStore and put into shardFileStoreSnapshot
            DelegatedFileStore copy = new DelegatedFileStore(FileStore.fromProtobuf(fileStore.toProtobuf()));
            shardFileStoreSnapshot.put(fileStore.key(), copy);
            return shardFileStoreSnapshot.get(fileStore.key());
        }

        if (fileStore.type() != fs.type()) {
            LOG.warn("Unexpected fileStore type mismatch. fsKey: {}, fsType:{} != fsType:{}", fileStore.key(), fs.type(),
                    fileStore.type());
            // return the origin fileStore, don't cache anything. Let it be.
            return fileStore;
        }

        if (fs.getVersion() < fileStore.getVersion()) {
            // swap to the newer version delegation, all the shards are still referring the same DelegatedFileStore
            fs.swapDelegation(fileStore.toProtobuf());
        }
        return fs;
    }

    // NOTE: change `shardFileStoreSnapshot` under no lock
    private FileStore setOrReplaceFileStoreSnapshotInternal(FileStore fileStore) {
        DelegatedFileStore fs = shardFileStoreSnapshot.get(fileStore.key());
        if (fs == null) {
            // deepcopy fileStore and put into shardFileStoreSnapshot
            DelegatedFileStore copy = new DelegatedFileStore(FileStore.fromProtobuf(fileStore.toProtobuf()));
            shardFileStoreSnapshot.put(fileStore.key(), copy);
            return shardFileStoreSnapshot.get(fileStore.key());
        }

        if (fs.getVersion() < fileStore.getVersion()) {
            // swap to the newer version delegation, all the shards are still referring the same DelegatedFileStore
            fs.swapDelegation(fileStore.toProtobuf());
        }
        return fs;
    }

    public void updateShardReplicaInfo(long workerId, List<ReplicaUpdateInfo> replicaUpdateInfos) {
        List<Long> shardIds = replicaUpdateInfos.stream().map(ReplicaUpdateInfo::getShardId).collect(Collectors.toList());
        updateShardReplicaInfoInternal(shardIds, workerId, ShardReplicaOp.SCALE_OUT_DONE);
    }

    public void dump(DataOutputStream out) throws IOException {
        List<Long> metaGroupIds = getAllMetaGroupIds();
        for (long groupId : metaGroupIds) {
            MetaGroup metaGroup = getMetaGroup(groupId);
            if (metaGroup != null) {
                String s = JsonFormat.printer().print(metaGroup.toProtobuf()) + "\n";
                out.writeBytes(s);
            }
        }

        List<Long> shardGroupIds = getAllShardGroupIds();
        for (long groupId : shardGroupIds) {
            ShardGroup shardGroup = getShardGroup(groupId);
            if (shardGroup != null) {
                String s = JsonFormat.printer().print(shardGroup.toProtobuf()) + "\n";
                out.writeBytes(s);
            }
        }

        List<Long> shardIds = getAllShardIds();
        for (long shardId : shardIds) {
            Shard shard = getShard(shardId);
            if (shard != null) {
                String s = JsonFormat.printer().print(shard.toDebugProtobuf()) + "\n";
                out.writeBytes(s);
            }
        }
    }

    private ShardGroup operateShardGroupInternal(long shardGroupId, ShardGroup shardGroup, boolean isAdd) {
        Preconditions.checkState(lock.isWriteLockedByCurrentThread());
        ShardGroup prev = null;
        if (isAdd) {
            prev = shardGroups.put(shardGroupId, shardGroup);
        } else {
            prev = shardGroups.remove(shardGroupId);
        }
        totalShardGroupGauge.set(shardGroups.size());
        return prev;
    }

    public void removeShardGroupReplicas(Long shardGroupId) throws StarException {
        try (LockCloseable ignored = new LockCloseable(lock.writeLock())) {
            ShardGroup shardGroup = getShardGroup(shardGroupId);
            List<Long> shardIds = shardGroup.getShardIds();
            List<Shard> shardsToUpdate = new ArrayList<>();
            Map<Long, List<Replica>> originReplicasInfo = new HashMap<>();

            for (Long shardId : shardIds) {
                Shard shard = getShard(shardId);
                originReplicasInfo.put(shardId, shard.getReplica());
                shard.setReplicas(new ArrayList<>());
                shardsToUpdate.add(shard);
            }

            if (!shardsToUpdate.isEmpty()) {
                try {
                    Journal journal = StarMgrJournal.logUpdateShard(serviceId, shardsToUpdate);
                    journalSystem.write(journal);
                } catch (StarException e) {
                    // revert back
                    for (Shard shard : shardsToUpdate) {
                        shard.setReplicas(originReplicasInfo.get(shard.getShardId()));
                    }
                    throw e;
                }
            }
        }
    }
}
