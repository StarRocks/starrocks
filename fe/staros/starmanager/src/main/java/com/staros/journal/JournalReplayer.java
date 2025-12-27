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


package com.staros.journal;

import com.staros.exception.StarException;
import com.staros.manager.StarManager;
import com.staros.metrics.MetricsSystem;
import com.staros.proto.CreateShardJournalInfo;
import com.staros.proto.DeleteShardGroupInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.JournalEntry;
import com.staros.proto.JournalHeader;
import com.staros.proto.LeaderInfo;
import com.staros.proto.MetaGroupJournalInfo;
import com.staros.proto.OperationType;
import com.staros.proto.UpdateWorkerGroupInfo;
import com.staros.service.Service;
import com.staros.service.ServiceTemplate;
import com.staros.shard.Shard;
import com.staros.shard.ShardGroup;
import com.staros.util.LogUtils;
import com.staros.worker.Worker;
import com.staros.worker.WorkerGroup;
import io.prometheus.metrics.core.metrics.Counter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class JournalReplayer {
    private static final Logger LOG = LogManager.getLogger(JournalReplayer.class);

    private final StarManager starManager;

    private static final Counter REPLAY_JOURNAL_COUNTER =
            MetricsSystem.registerCounter("starmgr_journal_replay_ops", "number of journals replayed");

    public JournalReplayer(StarManager starManager) {
        this.starManager = starManager;
    }

    public void replay(Journal journal) {
        JournalEntry entry = journal.getEntry();
        JournalHeader header = entry.getHeader();

        String serviceId = header.getServiceId();
        OperationType operationType = header.getOperationType();

        LOG.debug("replay journal {} for service {}.", operationType, serviceId);

        try {
            switch (operationType) {
                case OP_REGISTER_SERVICE:
                    ServiceTemplate serviceTemplate = StarMgrJournal.parseLogRegisterService(journal);
                    starManager.replayRegisterService(serviceTemplate);
                    break;
                case OP_DEREGISTER_SERVICE:
                    String serviceTemplateName = StarMgrJournal.parseLogDeregisterService(journal);
                    starManager.replayDeregisterService(serviceTemplateName);
                    break;
                case OP_BOOTSTRAP_SERVICE:
                    Service service1 = StarMgrJournal.parseLogBootstrapService(journal);
                    starManager.replayBootstrapService(service1);
                    break;
                case OP_SHUTDOWN_SERVICE:
                    Service service2 = StarMgrJournal.parseLogShutdownService(journal);
                    starManager.replayShutdownService(service2);
                    break;
                case OP_CREATE_SHARD:
                    CreateShardJournalInfo createShardJournalInfo = StarMgrJournal.parseLogCreateShard(journal);
                    starManager.replayCreateShard(serviceId, createShardJournalInfo);
                    break;
                case OP_DELETE_SHARD:
                    List<Long> shardIds = StarMgrJournal.parseLogDeleteShard(journal);
                    starManager.replayDeleteShard(serviceId, shardIds);
                    break;
                case OP_UPDATE_SHARD:
                    List<Shard> shardsToUpdate = StarMgrJournal.parseLogUpdateShard(journal);
                    starManager.replayUpdateShard(serviceId, shardsToUpdate);
                    break;
                case OP_CREATE_SHARD_GROUP:
                    List<ShardGroup> shardGroups = StarMgrJournal.parseLogCreateShardGroup(journal);
                    starManager.replayCreateShardGroup(serviceId, shardGroups);
                    break;
                case OP_DELETE_SHARD_GROUP:
                    DeleteShardGroupInfo info = StarMgrJournal.parseLogDeleteShardGroup(journal);
                    starManager.replayDeleteShardGroup(serviceId, info);
                    break;
                case OP_UPDATE_SHARD_GROUP:
                    List<ShardGroup> shardGroupsToUpdate = StarMgrJournal.parseLogUpdateShardGroup(journal);
                    starManager.replayUpdateShardGroup(serviceId, shardGroupsToUpdate);
                    break;
                case OP_CREATE_META_GROUP:
                    MetaGroupJournalInfo createMetaGroupJournalInfo = StarMgrJournal.parseLogCreateMetaGroup(journal);
                    starManager.replayCreateMetaGroup(serviceId, createMetaGroupJournalInfo);
                    break;
                case OP_DELETE_META_GROUP:
                    MetaGroupJournalInfo deleteMetaGroupJournalInfo = StarMgrJournal.parseLogDeleteMetaGroup(journal);
                    starManager.replayDeleteMetaGroup(serviceId, deleteMetaGroupJournalInfo);
                    break;
                case OP_UPDATE_META_GROUP:
                    MetaGroupJournalInfo updateMetaGroupJournalInfo = StarMgrJournal.parseLogUpdateMetaGroup(journal);
                    starManager.replayUpdateMetaGroup(serviceId, updateMetaGroupJournalInfo);
                    break;
                case OP_CREATE_WORKER_GROUP:
                    WorkerGroup group = StarMgrJournal.parseLogCreateWorkerGroup(journal);
                    starManager.replayCreateWorkerGroup(serviceId, group);
                    break;
                case OP_DELETE_WORKER_GROUP:
                    long groupId = StarMgrJournal.parseLogDeleteWorkerGroup(journal);
                    starManager.replayDeleteWorkerGroup(serviceId, groupId);
                    break;
                case OP_UPDATE_WORKER_GROUP:
                    UpdateWorkerGroupInfo updateInfo = StarMgrJournal.parseLogUpdateWorkerGroup(journal);
                    starManager.replayUpdateWorkerGroup(serviceId, updateInfo);
                    break;
                case OP_ADD_WORKER:
                    Worker worker = StarMgrJournal.parseLogAddWorker(journal);
                    starManager.replayAddWorker(serviceId, worker);
                    break;
                case OP_REMOVE_WORKER:
                    Pair<Long, Long> pair = StarMgrJournal.parseLogRemoveWorker(journal);
                    starManager.replayRemoveWorker(serviceId, pair.getKey(), pair.getValue());
                    break;
                case OP_UPDATE_WORKER:
                    List<Worker> w = StarMgrJournal.parseLogUpdateWorker(journal);
                    starManager.replayUpdateWorker(serviceId, w);
                    break;
                case OP_SET_ID:
                    long id = StarMgrJournal.parseLogSetId(journal);
                    starManager.replaySetId(id);
                    break;
                case OP_LEADER_CHANGE:
                    LeaderInfo leaderInfo = StarMgrJournal.parseLogLeaderInfo(journal);
                    starManager.replayLeaderChange(leaderInfo);
                    break;
                case OP_ADD_FILESTORE:
                    FileStoreInfo addFsInfo = StarMgrJournal.parseLogAddFileStore(journal);
                    starManager.replayAddFileStore(serviceId, addFsInfo);
                    break;
                case OP_REMOVE_FILESTORE:
                    String fsKey = StarMgrJournal.parseLogRemoveFileStore(journal);
                    starManager.replayRemoveFileStore(serviceId, fsKey);
                    break;
                case OP_UPDATE_FILESTORE:
                    FileStoreInfo updateFsInfo = StarMgrJournal.parseLogUpdateFileStore(journal);
                    starManager.replayUpdateFileStore(serviceId, updateFsInfo);
                    break;
                case OP_REPLACE_FILESTORE:
                    FileStoreInfo replaceFsInfo = StarMgrJournal.parseLogReplaceFileStore(journal);
                    starManager.replayReplaceFileStore(serviceId, replaceFsInfo);
                    break;
                default:
                    LOG.warn("unknown operation type {} when replay journal.", operationType);
                    break;
            }
            REPLAY_JOURNAL_COUNTER.inc();
        } catch (OutOfMemoryError | NullPointerException | IOException | StarException e) {
            LogUtils.fatal(LOG, "replay journal {} for service {} failed, {}.", operationType, serviceId, e.getMessage());
        }
    }
}
