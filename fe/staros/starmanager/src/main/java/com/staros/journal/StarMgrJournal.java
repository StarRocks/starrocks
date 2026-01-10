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

import com.google.protobuf.InvalidProtocolBufferException;
import com.staros.exception.ExceptionCode;
import com.staros.exception.StarException;
import com.staros.journal.Journal.AbstractJournalOp;
import com.staros.journal.Journal.AbstractListJournalOp;
import com.staros.journal.Journal.AbstractListWritableJournalOp;
import com.staros.journal.Journal.ProtoJournalOp;
import com.staros.journal.Journal.WritableJournalOp;
import com.staros.metrics.MetricsSystem;
import com.staros.proto.CreateShardJournalInfo;
import com.staros.proto.DeleteShardGroupInfo;
import com.staros.proto.DeleteWorkerGroupInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.LeaderInfo;
import com.staros.proto.MetaGroupJournalInfo;
import com.staros.proto.OperationType;
import com.staros.proto.UpdateWorkerGroupInfo;
import com.staros.service.Service;
import com.staros.service.ServiceTemplate;
import com.staros.shard.Shard;
import com.staros.shard.ShardGroup;
import com.staros.util.Constant;
import com.staros.util.Text;
import com.staros.worker.Worker;
import com.staros.worker.WorkerGroup;
import io.prometheus.metrics.core.metrics.Counter;
import org.apache.commons.lang3.tuple.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class StarMgrJournal {
    // monitor the average batch size of LogUpdateShard
    public static final Counter METRIC_LOG_UPDATE_SHARDS_NUM_COUNTER =
            MetricsSystem.registerCounter("starmgr_log_update_shards_num", "total number of shards in logUpdateShard in starmgr");
    public static final Counter METRIC_LOG_UPDATE_SHARDS_OPS_COUNTER =
            MetricsSystem.registerCounter("starmgr_log_update_shards_ops", "total ops of logUpdateShard in starmgr");

    // serialize/deserialize of Worker
    private static final AbstractJournalOp<Worker> WORKER_OP = new WritableJournalOp<Worker>() {
        // serialization using Worker::write(out);
        @Override
        public Worker parseFrom(DataInput in) throws IOException {
            return Worker.read(in);
        }
    };

    // serialize/deserialize of WorkerGroup
    private static final AbstractJournalOp<WorkerGroup> WORKER_GROUP_OP = new WritableJournalOp<WorkerGroup>() {
        // serialization using WorkerGroup::write(out);
        @Override
        public WorkerGroup parseFrom(DataInput in) throws IOException {
            return WorkerGroup.read(in);
        }
    };

    // serialize/deserialize of ServiceTemplate
    private static final AbstractJournalOp<ServiceTemplate> SERVICE_TMPL_OP = new WritableJournalOp<ServiceTemplate>() {
        // serialization using ServiceTemplate::write(out);
        @Override
        public ServiceTemplate parseFrom(DataInput in) throws IOException {
            return ServiceTemplate.read(in);
        }
    };

    // serialize/deserialize of Service
    private static final AbstractJournalOp<Service> SERVICE_OP = new WritableJournalOp<Service>() {
        // serialization using Service::write(out);
        @Override
        public Service parseFrom(DataInput in) throws IOException {
            return Service.read(in);
        }
    };

    // serialize/deserialize of String
    private static final AbstractJournalOp<String> STRING_OP = new AbstractJournalOp<String>() {
        @Override
        public void write(DataOutput out, String data) throws IOException {
            Text.writeBytes(out, data.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String parseFrom(DataInput in) throws IOException {
            return new String(Text.readBytes(in), StandardCharsets.UTF_8);
        }
    };

    // serialize/deserialize of Long
    private static final AbstractJournalOp<Long> LONG_OP = new AbstractJournalOp<Long>() {
        @Override
        public void write(DataOutput out, Long data) throws IOException {
            out.writeLong(data);
        }

        @Override
        public Long parseFrom(DataInput in) throws IOException {
            return in.readLong();
        }
    };

    // serialize/deserialize of List<Shard>
    private static final AbstractListWritableJournalOp<Shard> SHARD_LIST_OP = new AbstractListWritableJournalOp<Shard>() {
        // serialization of writeElement using Shard::write(out);
        @Override
        public Shard parseElement(DataInput in) throws IOException {
            return Shard.read(in);
        }
    };

    // serialize/deserialize of List<ShardGroup>
    private static final AbstractListWritableJournalOp<ShardGroup> SHARD_GROUP_LIST_OP =
            new AbstractListWritableJournalOp<ShardGroup>() {
                // serialization of writeElement using ShardGroup::write(out);
                @Override
                public ShardGroup parseElement(DataInput in) throws IOException {
                    return ShardGroup.read(in);
                }
            };

    // serialize/deserialize of List<Worker>
    private static final AbstractListWritableJournalOp<Worker> WORKER_LIST_OP = new AbstractListWritableJournalOp<Worker>() {
        // serialization of writeElement using Worker::write(out);
        @Override
        public Worker parseElement(DataInput in) throws IOException {
            return Worker.read(in);
        }
    };

    // serialize/deserialize of List<Long>
    private static final AbstractListJournalOp<Long> LONG_LIST_OP = new AbstractListJournalOp<Long>() {
        @Override
        public void writeElement(DataOutput out, Long data) throws IOException {
            out.writeLong(data);
        }

        @Override
        public Long parseElement(DataInput in) throws IOException {
            return in.readLong();
        }
    };

    // serialize/deserialize of DeleteShardGroupInfo
    private static final ProtoJournalOp<DeleteShardGroupInfo> DELETE_SHARD_GROUPS_OP =
            new ProtoJournalOp<DeleteShardGroupInfo>() {
                @Override
                public DeleteShardGroupInfo read(byte[] bytes) {
                    try {
                        return DeleteShardGroupInfo.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        throw new StarException(ExceptionCode.JOURNAL, e.getMessage());
                    }
                }
            };

    // serialize/deserialize of MetaGroupJournalInfo
    private static final ProtoJournalOp<MetaGroupJournalInfo> META_GROUP_OP = new ProtoJournalOp<MetaGroupJournalInfo>() {
        @Override
        public MetaGroupJournalInfo read(byte[] bytes) {
            try {
                return MetaGroupJournalInfo.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new StarException(ExceptionCode.JOURNAL, e.getMessage());
            }
        }
    };

    // serialize/deserialize of CreateShardJournalInfo
    private static final ProtoJournalOp<CreateShardJournalInfo> CREATE_SHARD_OP = new ProtoJournalOp<CreateShardJournalInfo>() {
        @Override
        public CreateShardJournalInfo read(byte[] bytes) {
            try {
                return CreateShardJournalInfo.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new StarException(ExceptionCode.JOURNAL, e.getMessage());
            }
        }
    };

    // serialize/deserialize of DeleteWorkerGroupInfo
    private static final ProtoJournalOp<DeleteWorkerGroupInfo> DELETE_WORKER_GROUP_OP =
            new ProtoJournalOp<DeleteWorkerGroupInfo>() {
                @Override
                public DeleteWorkerGroupInfo read(byte[] bytes) {
                    try {
                        return DeleteWorkerGroupInfo.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        throw new StarException(ExceptionCode.JOURNAL, e.getMessage());
                    }
                }
            };

    // serialize/deserialize of UpdateWorkerGroupInfo
    private static final ProtoJournalOp<UpdateWorkerGroupInfo> UPDATE_WORKER_GROUP_OP =
            new ProtoJournalOp<UpdateWorkerGroupInfo>() {
                @Override
                public UpdateWorkerGroupInfo read(byte[] bytes) {
                    try {
                        return UpdateWorkerGroupInfo.parseFrom(bytes);
                    } catch (InvalidProtocolBufferException e) {
                        throw new StarException(ExceptionCode.JOURNAL, e.getMessage());
                    }
                }
            };

    // serialize/deserialize of AddFileStoreJournalInfo
    private static final ProtoJournalOp<FileStoreInfo> FILE_STORE_OP = new ProtoJournalOp<FileStoreInfo>() {
        @Override
        public FileStoreInfo read(byte[] bytes) {
            try {
                return FileStoreInfo.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new StarException(ExceptionCode.JOURNAL, e.getMessage());
            }
        }
    };

    // serialize/deserialize of LeaderInfo
    private static final ProtoJournalOp<LeaderInfo> LEADER_INFO_OP = new ProtoJournalOp<LeaderInfo>() {
        @Override
        public LeaderInfo read(byte[] bytes) {
            try {
                return LeaderInfo.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new StarException(ExceptionCode.JOURNAL, e.getMessage());
            }
        }
    };

    public static Journal logCreateShard(String serviceId, CreateShardJournalInfo info) throws StarException {
        return CREATE_SHARD_OP.toJournal(OperationType.OP_CREATE_SHARD, serviceId, info);
    }

    public static Journal logDeleteShard(String serviceId, List<Long> shardIds) throws StarException {
        return LONG_LIST_OP.toJournal(OperationType.OP_DELETE_SHARD, serviceId, shardIds);
    }

    public static Journal logUpdateShard(String serviceId, List<Shard> shards) throws StarException {
        METRIC_LOG_UPDATE_SHARDS_NUM_COUNTER.inc(shards.size());
        METRIC_LOG_UPDATE_SHARDS_OPS_COUNTER.inc();
        return SHARD_LIST_OP.toJournal(OperationType.OP_UPDATE_SHARD, serviceId, shards);
    }

    public static Journal logCreateShardGroup(String serviceId, List<ShardGroup> shardGroups) throws StarException {
        return SHARD_GROUP_LIST_OP.toJournal(OperationType.OP_CREATE_SHARD_GROUP, serviceId, shardGroups);
    }

    public static Journal logDeleteShardGroup(String serviceId, DeleteShardGroupInfo deleteGroupInfo) throws StarException {
        return DELETE_SHARD_GROUPS_OP.toJournal(OperationType.OP_DELETE_SHARD_GROUP, serviceId, deleteGroupInfo);
    }

    public static Journal logUpdateShardGroup(String serviceId, List<ShardGroup> groups) throws StarException {
        return SHARD_GROUP_LIST_OP.toJournal(OperationType.OP_UPDATE_SHARD_GROUP, serviceId, groups);
    }

    public static Journal logCreateMetaGroup(String serviceId, MetaGroupJournalInfo metaGroupJournalInfo) throws StarException {
        return META_GROUP_OP.toJournal(OperationType.OP_CREATE_META_GROUP, serviceId, metaGroupJournalInfo);
    }

    public static Journal logDeleteMetaGroup(String serviceId, MetaGroupJournalInfo metaGroupJournalInfo) throws StarException {
        return META_GROUP_OP.toJournal(OperationType.OP_DELETE_META_GROUP, serviceId, metaGroupJournalInfo);
    }

    public static Journal logUpdateMetaGroup(String serviceId, MetaGroupJournalInfo metaGroupJournalInfo) throws StarException {
        return META_GROUP_OP.toJournal(OperationType.OP_UPDATE_META_GROUP, serviceId, metaGroupJournalInfo);
    }

    public static Journal logCreateWorkerGroup(WorkerGroup group) throws StarException {
        return WORKER_GROUP_OP.toJournal(OperationType.OP_CREATE_WORKER_GROUP, group.getServiceId(), group);
    }

    public static Journal logDeleteWorkerGroup(String serviceId, long groupId) {
        DeleteWorkerGroupInfo info = DeleteWorkerGroupInfo.newBuilder().setGroupId(groupId).build();
        return DELETE_WORKER_GROUP_OP.toJournal(OperationType.OP_DELETE_WORKER_GROUP, serviceId, info);
    }

    public static Journal logUpdateWorkerGroup(String serviceId, UpdateWorkerGroupInfo info) {
        return UPDATE_WORKER_GROUP_OP.toJournal(OperationType.OP_UPDATE_WORKER_GROUP, serviceId, info);
    }

    public static Journal logAddWorker(Worker worker) throws StarException {
        return WORKER_OP.toJournal(OperationType.OP_ADD_WORKER, worker.getServiceId(), worker);
    }

    public static Journal logRemoveWorker(String serviceId, long groupId, long workerId) throws StarException {
        List<Long> todo = new ArrayList<>(2);
        todo.add(groupId);
        todo.add(workerId);
        return LONG_LIST_OP.toJournal(OperationType.OP_REMOVE_WORKER, serviceId, todo);
    }

    public static Journal logRegisterService(ServiceTemplate serviceTemplate) throws StarException {
        return SERVICE_TMPL_OP.toJournal(OperationType.OP_REGISTER_SERVICE, Constant.EMPTY_SERVICE_ID, serviceTemplate);
    }

    public static Journal logDeregisterService(String serviceTemplateName) throws StarException {
        return STRING_OP.toJournal(OperationType.OP_DEREGISTER_SERVICE, Constant.EMPTY_SERVICE_ID, serviceTemplateName);
    }

    public static Journal logBootstrapService(Service service) throws StarException {
        return SERVICE_OP.toJournal(OperationType.OP_BOOTSTRAP_SERVICE, service.getServiceId(), service);
    }

    public static Journal logShutdownService(Service service) throws StarException {
        return SERVICE_OP.toJournal(OperationType.OP_SHUTDOWN_SERVICE, service.getServiceId(), service);
    }

    public static Journal logUpdateWorker(String serviceId, List<Worker> workers) throws StarException {
        assert !workers.isEmpty();
        return WORKER_LIST_OP.toJournal(OperationType.OP_UPDATE_WORKER, serviceId, workers);
    }

    public static Journal logSetId(long id) throws StarException {
        return LONG_OP.toJournal(OperationType.OP_SET_ID, Constant.EMPTY_SERVICE_ID, id);
    }

    public static Journal logLeaderInfo(LeaderInfo leader) throws StarException {
        return LEADER_INFO_OP.toJournal(OperationType.OP_LEADER_CHANGE, Constant.EMPTY_SERVICE_ID, leader);
    }

    public static Journal logAddFileStore(String serviceId, FileStoreInfo fs) throws StarException {
        return FILE_STORE_OP.toJournal(OperationType.OP_ADD_FILESTORE, serviceId, fs);
    }

    public static Journal logUpdateFileStore(String serviceId, FileStoreInfo fs) throws StarException {
        return FILE_STORE_OP.toJournal(OperationType.OP_UPDATE_FILESTORE, serviceId, fs);
    }

    public static Journal logReplaceFileStore(String serviceId, FileStoreInfo fs) throws StarException {
        return FILE_STORE_OP.toJournal(OperationType.OP_REPLACE_FILESTORE, serviceId, fs);
    }

    public static Journal logRemoveFileStore(String serviceId, String fsKey) throws StarException {
        return STRING_OP.toJournal(OperationType.OP_REMOVE_FILESTORE, serviceId, fsKey);
    }

    public static CreateShardJournalInfo parseLogCreateShard(Journal journal) throws IOException {
        return CREATE_SHARD_OP.parseFromJournal(OperationType.OP_CREATE_SHARD, journal);
    }

    public static List<Long> parseLogDeleteShard(Journal journal) throws IOException {
        return LONG_LIST_OP.parseFromJournal(OperationType.OP_DELETE_SHARD, journal);
    }

    public static List<Shard> parseLogUpdateShard(Journal journal) throws IOException {
        return SHARD_LIST_OP.parseFromJournal(OperationType.OP_UPDATE_SHARD, journal);
    }

    public static List<ShardGroup> parseLogCreateShardGroup(Journal journal) throws IOException {
        return SHARD_GROUP_LIST_OP.parseFromJournal(OperationType.OP_CREATE_SHARD_GROUP, journal);
    }

    public static DeleteShardGroupInfo parseLogDeleteShardGroup(Journal journal) throws IOException {
        return DELETE_SHARD_GROUPS_OP.parseFromJournal(OperationType.OP_DELETE_SHARD_GROUP, journal);
    }

    public static List<ShardGroup> parseLogUpdateShardGroup(Journal journal) throws IOException {
        return SHARD_GROUP_LIST_OP.parseFromJournal(OperationType.OP_UPDATE_SHARD_GROUP, journal);
    }

    public static MetaGroupJournalInfo parseLogCreateMetaGroup(Journal journal) throws IOException {
        return META_GROUP_OP.parseFromJournal(OperationType.OP_CREATE_META_GROUP, journal);
    }

    public static MetaGroupJournalInfo parseLogDeleteMetaGroup(Journal journal) throws IOException {
        return META_GROUP_OP.parseFromJournal(OperationType.OP_DELETE_META_GROUP, journal);
    }

    public static MetaGroupJournalInfo parseLogUpdateMetaGroup(Journal journal) throws IOException {
        return META_GROUP_OP.parseFromJournal(OperationType.OP_UPDATE_META_GROUP, journal);
    }

    public static WorkerGroup parseLogCreateWorkerGroup(Journal journal) throws StarException {
        return WORKER_GROUP_OP.parseFromJournal(OperationType.OP_CREATE_WORKER_GROUP, journal);
    }

    public static long parseLogDeleteWorkerGroup(Journal journal) {
        DeleteWorkerGroupInfo info = DELETE_WORKER_GROUP_OP.parseFromJournal(OperationType.OP_DELETE_WORKER_GROUP, journal);
        return info.getGroupId();
    }

    public static UpdateWorkerGroupInfo parseLogUpdateWorkerGroup(Journal journal) {
        return UPDATE_WORKER_GROUP_OP.parseFromJournal(OperationType.OP_UPDATE_WORKER_GROUP, journal);
    }

    public static Worker parseLogAddWorker(Journal journal) throws IOException {
        return WORKER_OP.parseFromJournal(OperationType.OP_ADD_WORKER, journal);
    }

    public static List<Worker> parseLogUpdateWorker(Journal journal) throws IOException {
        return WORKER_LIST_OP.parseFromJournal(OperationType.OP_UPDATE_WORKER, journal);
    }

    public static Pair<Long, Long> parseLogRemoveWorker(Journal journal) throws IOException {
        List<Long> data = LONG_LIST_OP.parseFromJournal(OperationType.OP_REMOVE_WORKER, journal);
        assert data.size() == 2;
        long groupId = data.get(0);
        long workerId = data.get(1);
        return Pair.of(groupId, workerId);
    }

    public static ServiceTemplate parseLogRegisterService(Journal journal) throws IOException {
        return SERVICE_TMPL_OP.parseFromJournal(OperationType.OP_REGISTER_SERVICE, journal);
    }

    public static String parseLogDeregisterService(Journal journal) throws IOException {
        return STRING_OP.parseFromJournal(OperationType.OP_DEREGISTER_SERVICE, journal);
    }

    public static Service parseLogBootstrapService(Journal journal) throws IOException {
        return SERVICE_OP.parseFromJournal(OperationType.OP_BOOTSTRAP_SERVICE, journal);
    }

    public static Service parseLogShutdownService(Journal journal) throws IOException {
        return SERVICE_OP.parseFromJournal(OperationType.OP_SHUTDOWN_SERVICE, journal);
    }

    public static long parseLogSetId(Journal journal) throws IOException {
        return LONG_OP.parseFromJournal(OperationType.OP_SET_ID, journal);
    }

    public static LeaderInfo parseLogLeaderInfo(Journal journal) throws IOException {
        return LEADER_INFO_OP.parseFromJournal(OperationType.OP_LEADER_CHANGE, journal);
    }

    public static FileStoreInfo parseLogAddFileStore(Journal journal) throws IOException {
        return FILE_STORE_OP.parseFromJournal(OperationType.OP_ADD_FILESTORE, journal);
    }

    public static FileStoreInfo parseLogUpdateFileStore(Journal journal) throws IOException {
        return FILE_STORE_OP.parseFromJournal(OperationType.OP_UPDATE_FILESTORE, journal);
    }

    public static FileStoreInfo parseLogReplaceFileStore(Journal journal) throws IOException {
        return FILE_STORE_OP.parseFromJournal(OperationType.OP_REPLACE_FILESTORE, journal);
    }

    public static String parseLogRemoveFileStore(Journal journal) throws IOException {
        return STRING_OP.parseFromJournal(OperationType.OP_REMOVE_FILESTORE, journal);
    }
}
