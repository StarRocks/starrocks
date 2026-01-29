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

import com.staros.common.HijackConfig;
import com.staros.common.MemoryJournalSystem;
import com.staros.common.TestHelper;
import com.staros.credential.AwsCredential;
import com.staros.credential.AwsCredentialMgr;
import com.staros.exception.ExceptionCode;
import com.staros.exception.StarException;
import com.staros.filestore.FilePath;
import com.staros.filestore.FileStore;
import com.staros.filestore.FileStoreMgr;
import com.staros.filestore.S3FileStore;
import com.staros.journal.DummyJournalSystem;
import com.staros.journal.Journal;
import com.staros.journal.StarMgrJournal;
import com.staros.metrics.MetricsSystem;
import com.staros.proto.CacheEnableState;
import com.staros.proto.CreateMetaGroupInfo;
import com.staros.proto.CreateShardGroupInfo;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.JoinMetaGroupInfo;
import com.staros.proto.MetaGroupInfo;
import com.staros.proto.OperationType;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.PlacementPreference;
import com.staros.proto.PlacementRelationship;
import com.staros.proto.QuitMetaGroupInfo;
import com.staros.proto.ReplicaState;
import com.staros.proto.ReplicaUpdateInfo;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.TransferMetaGroupInfo;
import com.staros.proto.UpdateMetaGroupInfo;
import com.staros.proto.UpdateShardGroupInfo;
import com.staros.proto.UpdateShardInfo;
import com.staros.schedule.ShardSchedulerV2;
import com.staros.service.ServiceManager;
import com.staros.util.Config;
import com.staros.util.Constant;
import com.staros.util.IdGenerator;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShardManagerTest {
    private String serviceId;
    private final long groupId = Constant.DEFAULT_ID;
    private ShardManager shardManager = null;
    private FileStoreMgr fsMgr = null;
    FilePathInfo pathInfo = null;
    private HijackConfig hijackTriggerScheduling;
    @Mocked
    private ShardSchedulerV2 scheduler;
    private TestHelper helper;

    @Before
    public void prepare() {
        Config.S3_BUCKET = "test-bucket";
        Config.S3_REGION = "test-region";
        Config.S3_ENDPOINT = "test-endpoint";
        Config.SIMPLE_CREDENTIAL_ACCESS_KEY_ID = "test-ak";
        Config.SIMPLE_CREDENTIAL_ACCESS_KEY_SECRET = "test-sk";
        Config.STARMGR_REPLACE_FILESTORE_ENABLED = true;

        helper = new TestHelper(this.getClass().getName() + "-serviceTemplateName-0");
        helper.getServiceManager().setShardScheduler(scheduler);
        helper.createDefaultServiceAndWorkerGroup(this.getClass().getName() + "-serviceName-0");
        serviceId = helper.getDefaultServiceId();

        fsMgr = FileStoreMgr.createFileStoreMgrForTest(serviceId);
        FileStore fs = fsMgr.allocFileStore("");
        FilePath fp = new FilePath(fs, "test-service-id/1/");
        pathInfo = fp.toProtobuf();

        // disable shard scheduling during creating, these tests doesn't care about the scheduling result.
        hijackTriggerScheduling = new HijackConfig("SCHEDULER_TRIGGER_SCHEDULE_WHEN_CREATE_SHARD", "false");
        shardManager = helper.getDefaultShardManager();

        new MockUp<ShardSchedulerV2>() {
            @Mock
            void scheduleAddToDefaultGroup(String serviceId, List<Long> shardIds) throws StarException {
                // Nothing to do
            }
        };
    }

    @After
    public void tearDown() {
        // reset to default value
        hijackTriggerScheduling.reset();
    }

    @Test
    public void testShardManagerCreateShard() {
        // test normal case
        int shardCount = 2;
        List<CreateShardInfo> createShardInfos = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; ++i) {
            CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
            List<Long> groupIds = new ArrayList<>();
            groupIds.add(groupId);
            builder.addAllGroupIds(groupIds);
            builder.setPathInfo(pathInfo);

            Map<String, String> properties = new HashMap<>();
            properties.put(serviceId, String.valueOf(i));
            builder.putAllShardProperties(properties);

            createShardInfos.add(builder.build());
        }

        List<ShardInfo> shardInfos = shardManager.createShard(createShardInfos, fsMgr);
        Assert.assertEquals(shardInfos.size(), shardCount);
        for (int i = 0; i < shardInfos.size(); ++i) {
            Assert.assertEquals(shardInfos.get(i).getServiceId(), serviceId);
            List<Long> groupIds = new ArrayList<>();
            groupIds.add(groupId);
            Assert.assertEquals(shardInfos.get(i).getGroupIdsList(), groupIds);

            Map<String, String> properties = new HashMap<>();
            properties.put(serviceId, String.valueOf(i));
            Assert.assertEquals(properties, shardInfos.get(i).getShardPropertiesMap());
        }

        // test group not exist
        try {
            List<CreateShardInfo> createShardInfos2 = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
                List<Long> groupIds = new ArrayList<>();
                groupIds.add(groupId + 1);
                builder.addAllGroupIds(groupIds);
                builder.setPathInfo(pathInfo);
                createShardInfos2.add(builder.build());
            }

            shardManager.createShard(createShardInfos2, fsMgr);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        // test shard already exist
        try {
            List<CreateShardInfo> createShardInfos3 = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
                List<Long> groupIds = new ArrayList<>();
                groupIds.add(groupId);
                builder.addAllGroupIds(groupIds);
                builder.setShardId(shardInfos.get(i).getShardId());
                builder.setPathInfo(pathInfo);
                createShardInfos3.add(builder.build());
            }
            shardManager.createShard(createShardInfos3, fsMgr);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.ALREADY_EXIST);
        }

        // test shard placement preference wrong number
        try {
            List<CreateShardInfo> createShardInfos4 = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
                PlacementPreference preference = PlacementPreference.newBuilder().build();
                builder.addPlacementPreferences(preference);
                builder.addPlacementPreferences(preference);
                createShardInfos4.add(builder.build());
            }
            shardManager.createShard(createShardInfos4, fsMgr);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test shard placement preference bad policy
        try {
            List<CreateShardInfo> createShardInfos4 = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
                PlacementPreference preference = PlacementPreference.newBuilder().build();
                builder.addPlacementPreferences(preference);
                createShardInfos4.add(builder.build());
            }
            shardManager.createShard(createShardInfos4, fsMgr);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test shard placement preference bad relationship
        try {
            List<CreateShardInfo> createShardInfos5 = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
                PlacementPreference preference = PlacementPreference.newBuilder()
                        .setPlacementPolicy(PlacementPolicy.PACK)
                        .setPlacementRelationship(PlacementRelationship.WITH_NONE)
                        .build();
                builder.addPlacementPreferences(preference);
                createShardInfos5.add(builder.build());
            }
            shardManager.createShard(createShardInfos5, fsMgr);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test shard placement preference bad target id
        try {
            List<CreateShardInfo> createShardInfos6 = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
                PlacementPreference preference = PlacementPreference.newBuilder()
                        .setPlacementPolicy(PlacementPolicy.PACK)
                        .setPlacementRelationship(PlacementRelationship.WITH_SHARD)
                        .setRelationshipTargetId(123456789)
                        .build();
                builder.addPlacementPreferences(preference);
                createShardInfos6.add(builder.build());
            }
            shardManager.createShard(createShardInfos6, fsMgr);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        // test shard placement preference
        // check shard group before
        List<Long> shardIds = shardInfos.stream().map(ShardInfo::getShardId).collect(Collectors.toList());
        for (ShardInfo info : shardInfos) {
            Assert.assertEquals(info.getGroupIdsList().size(), 1);
        }
        List<CreateShardInfo> createShardInfos7 = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; ++i) {
            CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
            PlacementPreference preference = PlacementPreference.newBuilder()
                    .setPlacementPolicy(PlacementPolicy.PACK)
                    .setPlacementRelationship(PlacementRelationship.WITH_SHARD)
                    .setRelationshipTargetId(shardInfos.get(i).getShardId())
                    .build();
            builder.addPlacementPreferences(preference);
            createShardInfos7.add(builder.build());
        }
        List<ShardInfo> shardInfos2 = shardManager.createShard(createShardInfos7, fsMgr);
        List<ShardInfo> shardInfos3 = shardManager.getShardInfo(shardIds);

        // paired shards should have the same anonymous shard group
        List<Long> agids = new ArrayList<>();
        for (ShardInfo info : shardInfos2) {
            Assert.assertEquals(info.getGroupIdsList().size(), 1);
            agids.add(info.getGroupIdsList().get(0));
        }
        int idx = 0;
        for (ShardInfo info : shardInfos3) {
            Assert.assertEquals(info.getGroupIdsList().size(), 2);
            Assert.assertEquals(info.getGroupIdsList().get(1), agids.get(idx++));
        }
        Pair<List<ShardGroupInfo>, Long> pair = shardManager.listShardGroupInfo(true /* includeAnonymousGroup */, 0);
        List<ShardGroupInfo> shardGroupInfos = pair.getKey();
        shardGroupInfos.removeIf(e -> e.getGroupId() == Constant.DEFAULT_ID);
        shardGroupInfos.sort((e1, e2) -> (int) (e1.getGroupId() - e2.getGroupId()));
        List<Long> agids2 = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());
        Assert.assertEquals(agids, agids2);
        idx = 0;
        // anonymous shard group should have shards with placement preference
        for (ShardGroupInfo info : shardGroupInfos) {
            Assert.assertEquals(info.getShardIdsList().size(), 2);
            Assert.assertTrue(info.getAnonymous());
            Assert.assertEquals(info.getPolicy(), PlacementPolicy.PACK);
            Assert.assertTrue(info.getShardIdsList().contains(shardInfos2.get(idx).getShardId()));
            Assert.assertTrue(info.getShardIdsList().contains(shardInfos3.get(idx).getShardId()));
            idx++;
        }
    }

    @Test
    public void testShardManagerDeleteShard() {
        // test normal case
        int shardCount = 2;
        List<CreateShardInfo> createShardInfos = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; ++i) {
            CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
            List<Long> groupIds = new ArrayList<>();
            groupIds.add(groupId);
            builder.addAllGroupIds(groupIds);
            builder.setPathInfo(pathInfo);
            createShardInfos.add(builder.build());
        }

        List<ShardInfo> shardInfos = shardManager.createShard(createShardInfos, fsMgr);
        List<Long> shardIds = shardInfos.stream().map(ShardInfo::getShardId).collect(Collectors.toList());
        Assert.assertEquals(shardIds.size(), shardManager.getShardCount());

        // test normal case
        shardManager.deleteShard(shardIds);
        Assert.assertEquals(0L, shardManager.getShardCount());

        // test delete non-exist shard
        shardManager.deleteShard(shardIds);
        Assert.assertEquals(0L, shardManager.getShardCount());

        /* test anonymous shard group clean */

        List<CreateShardInfo> createShardInfos2 = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; ++i) {
            createShardInfos2.add(CreateShardInfo.newBuilder().build());
        }
        List<ShardInfo> shardInfos2 = shardManager.createShard(createShardInfos2, fsMgr);

        // before creating anonymous shard group
        Assert.assertEquals(shardManager.getShardGroupCount(), 1 /* plus DEFAULT_ID */);

        List<CreateShardInfo> createShardInfos3 = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; ++i) {
            CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
            PlacementPreference preference = PlacementPreference.newBuilder()
                    .setPlacementPolicy(PlacementPolicy.PACK)
                    .setPlacementRelationship(PlacementRelationship.WITH_SHARD)
                    .setRelationshipTargetId(shardInfos2.get(i).getShardId())
                    .build();
            builder.addPlacementPreferences(preference);
            createShardInfos3.add(builder.build());
        }
        List<ShardInfo> shardInfos3 = shardManager.createShard(createShardInfos3, fsMgr);
        // verify shard info
        for (ShardInfo info : shardInfos3) {
            Assert.assertEquals(info.getGroupIdsList().size(), 1);
        }

        // after creating anonymous shard group
        Assert.assertEquals(shardManager.getShardGroupCount(), shardCount + 1 /* plus DEFAULT_ID */);

        shardIds = shardInfos2.stream().map(ShardInfo::getShardId).collect(Collectors.toList());
        shardManager.deleteShard(shardIds);
        // anonymous shard group should be gone
        Assert.assertEquals(shardManager.getShardGroupCount(), 1 /* plus DEFAULT_ID */);

        // verify shard info
        shardIds = shardInfos3.stream().map(ShardInfo::getShardId).collect(Collectors.toList());
        shardInfos3 = shardManager.getShardInfo(shardIds);
        for (ShardInfo info : shardInfos3) {
            Assert.assertEquals(info.getGroupIdsList().size(), 0);
        }
    }

    @Test
    public void testShardManagerUpdateShard() {
        // test normal case
        int shardCount = 2;
        List<CreateShardInfo> createShardInfos = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; ++i) {
            CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
            List<Long> groupIds = new ArrayList<>();
            groupIds.add(groupId);
            builder.addAllGroupIds(groupIds);
            builder.setPathInfo(pathInfo);
            createShardInfos.add(builder.build());
        }

        List<ShardInfo> shardInfos = shardManager.createShard(createShardInfos, fsMgr);
        List<Long> shardIds = shardInfos.stream().map(ShardInfo::getShardId).collect(Collectors.toList());
        Assert.assertEquals(shardIds.size(), shardManager.getShardCount());

        // test normal case, alter enable cache to be true
        List<UpdateShardInfo> updateShardInfos = new ArrayList<>(shardCount);
        updateShardInfos.add(UpdateShardInfo.newBuilder().setShardId(shardIds.get(0))
                .setEnableCache(CacheEnableState.ENABLED).build());
        updateShardInfos.add(UpdateShardInfo.newBuilder().setShardId(shardIds.get(1))
                .setEnableCache(CacheEnableState.ENABLED).build());
        shardManager.updateShard(updateShardInfos);
        List<ShardInfo> shardInfos1 = shardManager.getShardInfo(shardIds);
        for (ShardInfo info : shardInfos1) {
            Assert.assertEquals(info.getFileCache().getEnableCache(), true);
        }

        // test normal case, alter enable cache to be false
        List<UpdateShardInfo> updateShardInfos1 = new ArrayList<>(shardCount);
        updateShardInfos1.add(UpdateShardInfo.newBuilder().setShardId(shardIds.get(0))
                .setEnableCache(CacheEnableState.DISABLED).build());
        updateShardInfos1.add(UpdateShardInfo.newBuilder().setShardId(shardIds.get(1))
                .setEnableCache(CacheEnableState.DISABLED).build());
        shardManager.updateShard(updateShardInfos1);
        List<ShardInfo> shardInfos2 = shardManager.getShardInfo(shardIds);
        for (ShardInfo info : shardInfos2) {
            Assert.assertEquals(info.getFileCache().getEnableCache(), false);
        }

        new MockUp<StarMgrJournal>() {
            @Mock
            Journal logUpdateShard(String serviceId, List<Long> shardIds) throws StarException {
                throw new StarException(ExceptionCode.INVALID_ARGUMENT, "logUpdateShard Exception");
            }
        };
        try {
            shardManager.updateShard(updateShardInfos);
        } catch (StarException e) {
            List<ShardInfo> shardInfos3 = shardManager.getShardInfo(shardIds);
            for (ShardInfo info : shardInfos3) {
                Assert.assertEquals(info.getFileCache().getEnableCache(), false);
            }
        }

        // delete shards
        shardManager.deleteShard(shardIds);
        Assert.assertEquals(0L, shardManager.getShardCount());

        // test delete non-exist shard
        shardManager.updateShard(updateShardInfos);
        Assert.assertEquals(0L, shardManager.getShardCount());
    }

    @Test
    public void testShardManagerGetShard() {
        // test normal case
        int shardCount = 2;
        List<Long> groupIds = new ArrayList<>();
        groupIds.add(groupId);
        List<CreateShardInfo> createShardInfos = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; ++i) {
            CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
            builder.addAllGroupIds(groupIds);
            builder.setPathInfo(pathInfo);
            createShardInfos.add(builder.build());
        }

        List<ShardInfo> shardInfosTmp = shardManager.createShard(createShardInfos, fsMgr);
        List<Long> shardIds = shardInfosTmp.stream().map(ShardInfo::getShardId).collect(Collectors.toList());

        List<ShardInfo> shardInfos = shardManager.getShardInfo(shardIds);

        ShardInfo shard = shardInfos.get(0);
        Assert.assertEquals(shard.getServiceId(), serviceId);
        Assert.assertEquals(shard.getGroupIdsList(), groupIds);
        Assert.assertEquals(shard.getShardId(), (long) shardIds.get(0));

        shard = shardInfos.get(1);
        Assert.assertEquals(shard.getServiceId(), serviceId);
        Assert.assertEquals(shard.getGroupIdsList(), groupIds);
        Assert.assertEquals(shard.getShardId(), (long) shardIds.get(1));

        // test shard not exist
        try {
            List<Long> shardIds2 = new ArrayList<>();
            shardIds2.add(123L);
            shardManager.getShardInfo(shardIds2);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testShardManagerListShard() {
        int shardCount = 2;
        List<Long> groupIds = new ArrayList<>();
        groupIds.add(groupId);
        List<CreateShardInfo> createShardInfos = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; ++i) {
            CreateShardInfo.Builder builder = CreateShardInfo.newBuilder();
            builder.addAllGroupIds(groupIds);
            builder.setPathInfo(pathInfo);
            createShardInfos.add(builder.build());
        }

        List<ShardInfo> shardInfosTmp = shardManager.createShard(createShardInfos, fsMgr);
        List<Long> shardIds = shardInfosTmp.stream().map(ShardInfo::getShardId).collect(Collectors.toList());

        List<List<ShardInfo>> shardInfos = shardManager.listShardInfo(groupIds, false /* withoutReplicaInfo */);
        Assert.assertEquals(shardInfos.size(), 1);

        ShardInfo shard = shardInfos.get(0).get(0);
        Assert.assertEquals(shard.getServiceId(), serviceId);
        Assert.assertEquals(shard.getGroupIdsList(), groupIds);
        Assert.assertEquals(shard.getShardId(), (long) shardIds.get(0));

        shard = shardInfos.get(0).get(1);
        Assert.assertEquals(shard.getServiceId(), serviceId);
        Assert.assertEquals(shard.getGroupIdsList(), groupIds);
        Assert.assertEquals(shard.getShardId(), (long) shardIds.get(1));

        // test empty shard group
        try {
            List<Long> groupIds2 = new ArrayList<>();
            shardManager.listShardInfo(groupIds2, false /* withoutReplicaInfo */);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test shard group not exist
        try {
            List<Long> groupIds3 = new ArrayList<>();
            groupIds3.add(123L);
            shardManager.listShardInfo(groupIds3, false /* withoutReplicaInfo */);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testShardManagerShardGroup() {
        List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());

        // test create shard group
        List<ShardGroupInfo> shardGroupInfos = shardManager.createShardGroup(createShardGroupInfos);
        Assert.assertEquals(shardGroupInfos.size(), createShardGroupInfos.size());
        for (ShardGroupInfo info : shardGroupInfos) {
            Assert.assertEquals(info.getServiceId(), serviceId);
            Assert.assertEquals(info.getShardIdsList().size(), 0);
            Assert.assertEquals(info.getMetaGroupId(), Constant.DEFAULT_ID);
        }
        List<Long> groupIds = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());
        Assert.assertEquals(shardManager.getShardGroupCount(), shardGroupInfos.size() + 1);

        // test shard group info empty
        try {
            List<CreateShardGroupInfo> createShardGroupInfos3 = new ArrayList<>();
            shardManager.createShardGroup(createShardGroupInfos3);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test create shard in group
        List<CreateShardInfo> createShardInfos = new ArrayList<>();
        List<Long> groupIds2 = new ArrayList<>();
        groupIds2.add(groupIds.get(0));
        List<Long> groupIds3 = new ArrayList<>();
        groupIds3.add(groupIds.get(1));
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds2).setPathInfo(pathInfo).build());
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds3).setPathInfo(pathInfo).build());
        List<ShardInfo> shardInfos = shardManager.createShard(createShardInfos, fsMgr);
        Assert.assertEquals(shardManager.getShardCount(), shardInfos.size());

        // test update shard group, alter enable cache to be true
        List<UpdateShardGroupInfo> updateShardGroupInfos = new ArrayList<>();
        updateShardGroupInfos
                .add(UpdateShardGroupInfo.newBuilder().setGroupId(groupIds.get(0))
                        .setEnableCache(CacheEnableState.ENABLED).build());
        updateShardGroupInfos
                .add(UpdateShardGroupInfo.newBuilder().setGroupId(groupIds.get(1))
                        .setEnableCache(CacheEnableState.ENABLED).build());
        shardManager.updateShardGroup(updateShardGroupInfos);
        for (Long groupId : groupIds) {
            ShardGroup shardGroup = shardManager.getShardGroup(groupId);
            List<Long> shardIds = shardGroup.getShardIds();
            List<ShardInfo> shardInfos1 = shardManager.getShardInfo(shardIds);
            for (ShardInfo shardInfo : shardInfos1) {
                Assert.assertEquals(shardInfo.getFileCache().getEnableCache(), true);
            }
        }

        // test update shard group, alter enable cache to be false
        List<UpdateShardGroupInfo> updateShardGroupInfos1 = new ArrayList<>();
        updateShardGroupInfos1
                .add(UpdateShardGroupInfo.newBuilder().setGroupId(groupIds.get(0))
                        .setEnableCache(CacheEnableState.DISABLED).build());
        updateShardGroupInfos1
                .add(UpdateShardGroupInfo.newBuilder().setGroupId(groupIds.get(1))
                        .setEnableCache(CacheEnableState.DISABLED).build());
        shardManager.updateShardGroup(updateShardGroupInfos1);
        for (Long groupId : groupIds) {
            ShardGroup shardGroup = shardManager.getShardGroup(groupId);
            List<Long> shardIds = shardGroup.getShardIds();
            List<ShardInfo> shardInfos2 = shardManager.getShardInfo(shardIds);
            for (ShardInfo shardInfo : shardInfos2) {
                Assert.assertEquals(shardInfo.getFileCache().getEnableCache(), false);
            }
        }

        // test update non-exist shard group
        List<UpdateShardGroupInfo> updateShardGroupInfos2 = new ArrayList<>();
        updateShardGroupInfos2.add(
                UpdateShardGroupInfo.newBuilder().setGroupId(123456L).setEnableCache(CacheEnableState.DISABLED).build());
        shardManager.updateShardGroup(updateShardGroupInfos2);

        // test exception thrown in logUpdateShardGroup
        new MockUp<StarMgrJournal>() {
            @Mock
            Journal logUpdateShardGroup(String serviceId, List<Long> groupIds) throws StarException {
                throw new StarException(ExceptionCode.INVALID_ARGUMENT, "logUpdateShardGroup Exception");
            }
        };
        try {
            shardManager.updateShardGroup(updateShardGroupInfos);
        } catch (StarException e) {
            // if exception thrown, enable cache should be reverted back.
            for (Long groupId : groupIds) {
                ShardGroup shardGroup = shardManager.getShardGroup(groupId);
                List<Long> shardIds = shardGroup.getShardIds();
                List<ShardInfo> shardInfos3 = shardManager.getShardInfo(shardIds);
                for (ShardInfo shardInfo : shardInfos3) {
                    Assert.assertEquals(shardInfo.getFileCache().getEnableCache(), false);
                }
            }
        }

        // test delete shard group and shard
        shardManager.deleteShardGroup(groupIds, true);
        Assert.assertEquals(shardManager.getShardGroupCount(), 1);
        Assert.assertEquals(shardManager.getShardCount(), 0);

        // test shard group info empty
        try {
            List<Long> groupIds4 = new ArrayList<>();
            shardManager.deleteShardGroup(groupIds4, true);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test default group can not be deleted
        try {
            List<Long> groupIds5 = new ArrayList<>();
            groupIds5.add(Constant.DEFAULT_ID);
            shardManager.deleteShardGroup(groupIds5, true);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // try to delete shardgroup without while keeping the shard undeleted
        {
            // create 2 shard groups
            List<Long> gids;
            {
                List<CreateShardGroupInfo> request = new ArrayList<>();
                request.add(CreateShardGroupInfo.newBuilder().build());
                request.add(CreateShardGroupInfo.newBuilder().build());

                // test create shard group
                List<ShardGroupInfo> response = shardManager.createShardGroup(request);
                Assert.assertEquals(response.size(), request.size());
                gids = response.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());
            }
            // create 2 shards in both shard groups
            List<Long> sids;
            {
                List<CreateShardInfo> request = new ArrayList<>();
                request.add(CreateShardInfo.newBuilder().addAllGroupIds(gids).build());
                request.add(CreateShardInfo.newBuilder().addAllGroupIds(gids).build());
                List<ShardInfo> response = shardManager.createShard(request, fsMgr);
                Assert.assertEquals(response.size(), shardInfos.size());
                sids = response.stream().map(ShardInfo::getShardId).collect(Collectors.toList());
            }
            // verify creation result.
            // s1: [g1, g2]
            // s2: [g1, g2]
            // g1: [s1, s2]
            // g2: [s1, s2]
            for (Long id : sids) {
                Assert.assertEquals(shardManager.getShard(id).getGroupIds(), gids);
                Assert.assertEquals(shardManager.getShard(id).getGroupIds(), gids);
            }
            for (Long id : gids) {
                Assert.assertEquals(shardManager.getShardGroup(id).getShardIds(), sids);
                Assert.assertEquals(shardManager.getShardGroup(id).getShardIds(), sids);
            }
            // now delete both shard groups while keeping the shards
            shardManager.deleteShardGroup(gids, false);
            // verify both groups are gone.
            for (Long id : gids) {
                Assert.assertNull(shardManager.getShardGroup(id));
            }
            // verify both shards are still there
            for (Long id : sids) {
                Shard s = shardManager.getShard(id);
                Assert.assertNotNull(s);
                Assert.assertTrue(s.getGroupIds().isEmpty());
            }
        }
    }

    @Test
    public void testShardManagerListShardGroup() {
        List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
        int originBatchSize = Config.LIST_SHARD_GROUP_BATCH_SIZE;
        Config.LIST_SHARD_GROUP_BATCH_SIZE = 5;
        for (int i = 0; i < 10; ++i) {
            createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
        }

        List<ShardGroupInfo> shardGroupInfos = shardManager.createShardGroup(createShardGroupInfos);
        List<Long> groupIds = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());
        Assert.assertEquals(10, shardGroupInfos.size());

        List<ShardGroupInfo> shardGroupInfos2 = new ArrayList<>();
        long startGroupId = 0;
        // list the first 5 groups
        {
            Pair<List<ShardGroupInfo>, Long> pair = shardManager.listShardGroupInfo(false /* includeAnonymousGroup */,
                    startGroupId);
            Assert.assertEquals(Config.LIST_SHARD_GROUP_BATCH_SIZE, pair.getKey().size());
            shardGroupInfos2.addAll(pair.getKey());
            Assert.assertNotEquals((Long) 0L, pair.getValue());
            startGroupId = pair.getValue();
        }

        // list the second 5 groups
        {
            Pair<List<ShardGroupInfo>, Long> pair = shardManager.listShardGroupInfo(false /* includeAnonymousGroup */,
                    startGroupId);
            Assert.assertEquals(Config.LIST_SHARD_GROUP_BATCH_SIZE, pair.getKey().size());
            shardGroupInfos2.addAll(pair.getKey());
            Assert.assertNotEquals((Long) 0L, pair.getValue());
            startGroupId = pair.getValue();
        }

        // list the last 1 group
        {
            Pair<List<ShardGroupInfo>, Long> pair = shardManager.listShardGroupInfo(false /* includeAnonymousGroup */,
                    startGroupId);
            Assert.assertEquals(1, pair.getKey().size());
            shardGroupInfos2.addAll(pair.getKey());
            Assert.assertEquals((Long) 0L, pair.getValue());
        }

        List<Long> groupIds2 = shardGroupInfos2.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());
        Assert.assertEquals(shardGroupInfos.size() + 1 /* plus default group */, shardGroupInfos2.size());
        for (int i = 0, j = 0; i < groupIds.size();) {
            if (groupIds2.get(j) == Constant.DEFAULT_ID) {
                ++j;
                continue;
            }
            Assert.assertEquals(groupIds.get(i), groupIds2.get(j));
            ++i;
            ++j;
        }
        Config.LIST_SHARD_GROUP_BATCH_SIZE = originBatchSize;
    }

    @Test
    public void testShardManagerGetShardGroup() {
        List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            createShardGroupInfos.add(CreateShardGroupInfo.newBuilder()
                    .putProperties(String.valueOf(i), String.valueOf(i)).build());
        }
        List<ShardGroupInfo> shardGroupInfos = shardManager.createShardGroup(createShardGroupInfos);
        List<Long> groupIds = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());
        Assert.assertEquals(10, shardGroupInfos.size());

        // normal
        List<ShardGroupInfo> shardGroupInfos2 = shardManager.getShardGroupInfo(groupIds);
        for (int i = 0; i < 10; ++i) {
            Assert.assertEquals(shardGroupInfos.get(i).getGroupId(), shardGroupInfos2.get(i).getGroupId());
            Assert.assertEquals(shardGroupInfos.get(i).getPropertiesMap(), shardGroupInfos2.get(i).getPropertiesMap());
        }

        // shard group not exist
        try {
            groupIds.add(123456789L);
            shardManager.getShardGroupInfo(groupIds);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    // prepare 3 groups
    // first 2 groups have 3 shards, the third has 2 shards, the last has 0 shard
    private List<Long> prepareForMetaGroupTest() {
        List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());

        List<ShardGroupInfo> shardGroupInfos = shardManager.createShardGroup(createShardGroupInfos);
        List<Long> groupIds = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());

        List<CreateShardInfo> createShardInfos = new ArrayList<>();
        List<Long> groupIds2 = new ArrayList<>();
        groupIds2.add(groupIds.get(0));
        List<Long> groupIds3 = new ArrayList<>();
        groupIds3.add(groupIds.get(1));
        List<Long> groupIds4 = new ArrayList<>();
        groupIds4.add(groupIds.get(2));
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds2).build());
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds2).build());
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds2).build());
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds3).build());
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds3).build());
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds3).build());
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds4).build());
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds4).build());
        shardManager.createShard(createShardInfos, fsMgr);
        return groupIds;
    }

    @Test
    public void testMetaGroup() {
        List<Long> groupIds = prepareForMetaGroupTest();

        // test invalid placement policy
        try {
            CreateMetaGroupInfo info = CreateMetaGroupInfo.newBuilder()
                    .setPlacementPolicy(PlacementPolicy.NONE)
                    .build();
            shardManager.createMetaGroup(info);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test non-exist shard group
        try {
            List<Long> badGroupIds = new ArrayList<>();
            badGroupIds.add(987654321L);
            CreateMetaGroupInfo info = CreateMetaGroupInfo.newBuilder()
                    .addAllShardGroupIds(badGroupIds)
                    .setPlacementPolicy(PlacementPolicy.PACK)
                    .build();
            shardManager.createMetaGroup(info);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test mismatch shard group size
        try {
            List<Long> groupIds5 = new ArrayList<>();
            groupIds5.add(groupIds.get(0));
            groupIds5.add(groupIds.get(2));
            CreateMetaGroupInfo info = CreateMetaGroupInfo.newBuilder()
                    .addAllShardGroupIds(groupIds5)
                    .setPlacementPolicy(PlacementPolicy.PACK)
                    .build();
            shardManager.createMetaGroup(info);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test empty shard group
        try {
            List<Long> groupIds6 = new ArrayList<>();
            groupIds6.add(groupIds.get(3));
            CreateMetaGroupInfo info = CreateMetaGroupInfo.newBuilder()
                    .addAllShardGroupIds(groupIds6)
                    .setPlacementPolicy(PlacementPolicy.PACK)
                    .build();
            shardManager.createMetaGroup(info);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test create with initial empty shard group
        {
            CreateMetaGroupInfo info = CreateMetaGroupInfo.newBuilder()
                    .setPlacementPolicy(PlacementPolicy.PACK)
                    .build();
            MetaGroupInfo metaGroupInfo = shardManager.createMetaGroup(info);
            Assert.assertEquals(metaGroupInfo.getServiceId(), shardManager.getServiceId());
            Assert.assertEquals(metaGroupInfo.getShardGroupIdsList().size(), 0);
            Assert.assertEquals(metaGroupInfo.getPlacementPolicy(), PlacementPolicy.PACK);
        }

        // test create with initial non-empty shard group
        List<Long> groupIds7 = new ArrayList<>();
        groupIds7.add(groupIds.get(0));
        groupIds7.add(groupIds.get(1));
        CreateMetaGroupInfo info = CreateMetaGroupInfo.newBuilder()
                .addAllShardGroupIds(groupIds7)
                .setPlacementPolicy(PlacementPolicy.PACK)
                .build();
        MetaGroupInfo metaGroupInfo = shardManager.createMetaGroup(info);
        Assert.assertEquals(metaGroupInfo.getShardGroupIdsList().size(), 3);

        // test list anonymous group
        Assert.assertNotEquals(shardManager.listShardGroupInfo(false, 0).getKey(),
                shardManager.listShardGroupInfo(true, 0).getKey());

        verifyMetaGroupAfterAdd(shardManager, metaGroupInfo, groupIds);

        // test meta group already exists
        try {
            CreateMetaGroupInfo existInfo = CreateMetaGroupInfo.newBuilder()
                    .setMetaGroupId(metaGroupInfo.getMetaGroupId())
                    .setPlacementPolicy(PlacementPolicy.PACK)
                    .build();
            shardManager.createMetaGroup(existInfo);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.ALREADY_EXIST);
        }

        // test delete non-exist meta group
        shardManager.deleteMetaGroup(987654321L);
        // test delete meta group
        shardManager.deleteMetaGroup(metaGroupInfo.getMetaGroupId());
        // test list anonymous group, should be same after meta group is deleted
        Assert.assertEquals(shardManager.listShardGroupInfo(false, 0).getKey(),
                shardManager.listShardGroupInfo(true, 0).getKey());
        try {
            shardManager.getMetaGroupInfo(metaGroupInfo.getMetaGroupId());
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        verifyMetaGroupAfterDelete(shardManager, metaGroupInfo.getShardGroupIdsList(), groupIds);
    }

    @Test
    public void testUpdateMetaGroup() {
        List<Long> groupIds = prepareForMetaGroupTest();

        // test info not set
        try {
            UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                    .build();
            shardManager.updateMetaGroup(updateInfo);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test invalid src meta group
        try {
            QuitMetaGroupInfo quitInfo = QuitMetaGroupInfo.newBuilder().setMetaGroupId(987654321).build();
            UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                    .setQuitInfo(quitInfo)
                    .build();
            shardManager.updateMetaGroup(updateInfo);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
        try {
            QuitMetaGroupInfo quitInfo = QuitMetaGroupInfo.newBuilder().build();
            UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                    .setQuitInfo(quitInfo)
                    .build();
            shardManager.updateMetaGroup(updateInfo);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test invalid dst meta group
        try {
            JoinMetaGroupInfo joinInfo = JoinMetaGroupInfo.newBuilder().setMetaGroupId(987654321).build();
            UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                    .setJoinInfo(joinInfo)
                    .build();
            shardManager.updateMetaGroup(updateInfo);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
        try {
            JoinMetaGroupInfo joinInfo = JoinMetaGroupInfo.newBuilder().build();
            UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                    .setJoinInfo(joinInfo)
                    .build();
            shardManager.updateMetaGroup(updateInfo);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // create meta group
        List<Long> groupIdsToCreate = new ArrayList<>();
        groupIdsToCreate.add(groupIds.get(0));
        groupIdsToCreate.add(groupIds.get(1));
        CreateMetaGroupInfo info = CreateMetaGroupInfo.newBuilder()
                .setPlacementPolicy(PlacementPolicy.PACK)
                .addAllShardGroupIds(groupIdsToCreate)
                .build();
        MetaGroupInfo metaGroupInfo = shardManager.createMetaGroup(info);

        // test empty shard group
        try {
            QuitMetaGroupInfo quitInfo = QuitMetaGroupInfo.newBuilder()
                    .setMetaGroupId(metaGroupInfo.getMetaGroupId())
                    .build();
            UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                    .setQuitInfo(quitInfo)
                    .build();
            shardManager.updateMetaGroup(updateInfo);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }
        // test non-exist shard group
        try {
            QuitMetaGroupInfo quitInfo = QuitMetaGroupInfo.newBuilder()
                    .setMetaGroupId(metaGroupInfo.getMetaGroupId())
                    .build();
            List<Long> badGroupIds = new ArrayList<>();
            badGroupIds.add(987654321L);
            UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                    .setQuitInfo(quitInfo)
                    .addAllShardGroupIds(badGroupIds)
                    .build();
            shardManager.updateMetaGroup(updateInfo);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test src anonymous group size mismatch shard group size
        try {
            QuitMetaGroupInfo quitInfo = QuitMetaGroupInfo.newBuilder()
                    .setMetaGroupId(metaGroupInfo.getMetaGroupId())
                    .build();
            List<Long> badGroupIds = new ArrayList<>();
            badGroupIds.add(groupIds.get(2));
            UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                    .setQuitInfo(quitInfo)
                    .addAllShardGroupIds(badGroupIds)
                    .build();
            shardManager.updateMetaGroup(updateInfo);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }
        // test dst anonymous group size mismatch shard group size, empty meta group
        try {
            CreateMetaGroupInfo info1 = CreateMetaGroupInfo.newBuilder()
                    .setPlacementPolicy(PlacementPolicy.PACK)
                    .build();
            shardManager.createMetaGroup(info1);
            JoinMetaGroupInfo joinInfo = JoinMetaGroupInfo.newBuilder()
                    .setMetaGroupId(metaGroupInfo.getMetaGroupId())
                    .build();
            List<Long> badGroupIds = new ArrayList<>();
            badGroupIds.add(groupIds.get(2));
            badGroupIds.add(groupIds.get(1));
            UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                    .setJoinInfo(joinInfo)
                    .addAllShardGroupIds(badGroupIds)
                    .build();
            shardManager.updateMetaGroup(updateInfo);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }
        // test dst anonymous group size mismatch shard group size, not empty meta group
        try {
            JoinMetaGroupInfo joinInfo = JoinMetaGroupInfo.newBuilder()
                    .setMetaGroupId(metaGroupInfo.getMetaGroupId())
                    .build();
            List<Long> badGroupIds = new ArrayList<>();
            badGroupIds.add(groupIds.get(2));
            UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                    .setJoinInfo(joinInfo)
                    .addAllShardGroupIds(badGroupIds)
                    .build();
            shardManager.updateMetaGroup(updateInfo);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.INVALID_ARGUMENT);
        }

        // test remove from meta group
        QuitMetaGroupInfo quitInfo = QuitMetaGroupInfo.newBuilder()
                .setMetaGroupId(metaGroupInfo.getMetaGroupId())
                .build();
        UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                .setQuitInfo(quitInfo)
                .addAllShardGroupIds(groupIdsToCreate)
                .build();
        shardManager.updateMetaGroup(updateInfo);
        verifyMetaGroupAfterRemove(metaGroupInfo.getShardGroupIdsList());
        // verify shard after remove
        for (Long groupId : groupIdsToCreate) {
            ShardGroup sg = shardManager.getShardGroup(groupId);
            Assert.assertEquals(sg.getShardIds().size(), 3);
            for (Long shardId : sg.getShardIds()) {
                Shard s = shardManager.getShard(shardId);
                Assert.assertEquals(s.getGroupIds().size(), 1);
                Assert.assertEquals(s.getGroupIds().get(0), (Long) sg.getGroupId());
            }
        }

        // test add to meta group
        CreateMetaGroupInfo info2 = CreateMetaGroupInfo.newBuilder()
                .setPlacementPolicy(PlacementPolicy.PACK)
                .build();
        MetaGroupInfo metaGroupInfo2 = shardManager.createMetaGroup(info2);
        JoinMetaGroupInfo joinInfo = JoinMetaGroupInfo.newBuilder()
                .setMetaGroupId(metaGroupInfo2.getMetaGroupId())
                .build();
        updateInfo = UpdateMetaGroupInfo.newBuilder()
                .setJoinInfo(joinInfo)
                .addAllShardGroupIds(groupIdsToCreate)
                .build();
        shardManager.updateMetaGroup(updateInfo);
        metaGroupInfo2 = shardManager.getMetaGroupInfo(metaGroupInfo2.getMetaGroupId());
        verifyMetaGroupAfterAdd(shardManager, metaGroupInfo2, groupIdsToCreate);

        // test transfer meta group
        TransferMetaGroupInfo transferInfo = TransferMetaGroupInfo.newBuilder()
                .setSrcMetaGroupId(metaGroupInfo2.getMetaGroupId())
                .setDstMetaGroupId(metaGroupInfo.getMetaGroupId())
                .build();
        updateInfo = UpdateMetaGroupInfo.newBuilder()
                .setTransferInfo(transferInfo)
                .addAllShardGroupIds(groupIdsToCreate)
                .build();
        shardManager.updateMetaGroup(updateInfo);
        metaGroupInfo = shardManager.getMetaGroupInfo(metaGroupInfo.getMetaGroupId());
        verifyMetaGroupAfterRemove(metaGroupInfo2.getShardGroupIdsList());
        verifyMetaGroupAfterAdd(shardManager, metaGroupInfo, groupIdsToCreate);
    }

    @Test
    public void testUpdateMetaGroupWithDeletion() {
        List<Long> groupIds = prepareForMetaGroupTest();

        List<Long> groupIdsToCreate = new ArrayList<>();
        groupIdsToCreate.add(groupIds.get(0));
        groupIdsToCreate.add(groupIds.get(1));
        CreateMetaGroupInfo info = CreateMetaGroupInfo.newBuilder()
                .setPlacementPolicy(PlacementPolicy.PACK)
                .addAllShardGroupIds(groupIdsToCreate)
                .build();
        MetaGroupInfo metaGroupInfo = shardManager.createMetaGroup(info);

        QuitMetaGroupInfo quitInfo = QuitMetaGroupInfo.newBuilder()
                .setMetaGroupId(metaGroupInfo.getMetaGroupId())
                .setDeleteMetaGroupIfEmpty(true)
                .build();
        UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                .setQuitInfo(quitInfo)
                .addAllShardGroupIds(groupIdsToCreate)
                .build();
        shardManager.updateMetaGroup(updateInfo);

        // meta group should be deleted
        StarException thrown = Assert.assertThrows(StarException.class, () -> {
            shardManager.getMetaGroupInfo(metaGroupInfo.getMetaGroupId());
        });
        Assert.assertEquals(ExceptionCode.NOT_EXIST, thrown.getExceptionCode());
    }

    @Test
    public void testListMetaGroup() {
        // before creation
        List<MetaGroupInfo> metaGroupInfos1 = shardManager.listMetaGroupInfo();
        Assert.assertEquals(metaGroupInfos1.size(), 0);

        CreateMetaGroupInfo info = CreateMetaGroupInfo.newBuilder()
                .setPlacementPolicy(PlacementPolicy.PACK)
                .build();
        shardManager.createMetaGroup(info);

        // after creation
        List<MetaGroupInfo> metaGroupInfos2 = shardManager.listMetaGroupInfo();
        Assert.assertEquals(metaGroupInfos2.size(), 1);

        List<Long> metaGroupIds = shardManager.getAllMetaGroupIds();
        Assert.assertEquals(metaGroupIds.size(), 1);

        MetaGroup metaGroup = shardManager.getMetaGroup(metaGroupIds.get(0));
        Assert.assertNotNull(metaGroup);
    }

    private void verifyMetaGroupAfterRemove(List<Long> anonymousShardGroupIds) {
        for (Long groupId : anonymousShardGroupIds) {
            ShardGroup asg = shardManager.getShardGroup(groupId);
            Assert.assertEquals(asg.getShardIds().size(), 0);
        }
    }

    public static void verifyMetaGroupAfterAdd(ShardManager shardManager, MetaGroupInfo metaGroupInfo, List<Long> shardGroupIds) {
        List<Long> anonymousShardGroupIds = metaGroupInfo.getShardGroupIdsList();
        //        sg1    sg2
        // asg1   s1     s4
        // asg2   s2     s5
        // asg3   s3     s6
        ShardGroup sg1 = shardManager.getShardGroup(shardGroupIds.get(0));
        ShardGroup sg2 = shardManager.getShardGroup(shardGroupIds.get(1));
        ShardGroup asg1 = shardManager.getShardGroup(anonymousShardGroupIds.get(0));
        ShardGroup asg2 = shardManager.getShardGroup(anonymousShardGroupIds.get(1));
        ShardGroup asg3 = shardManager.getShardGroup(anonymousShardGroupIds.get(2));
        Assert.assertTrue(asg1.isAnonymous());
        Assert.assertTrue(asg2.isAnonymous());
        Assert.assertTrue(asg3.isAnonymous());
        Assert.assertEquals(asg1.getMetaGroupId(), metaGroupInfo.getMetaGroupId());
        Assert.assertEquals(asg2.getMetaGroupId(), metaGroupInfo.getMetaGroupId());
        Assert.assertEquals(asg3.getMetaGroupId(), metaGroupInfo.getMetaGroupId());
        Assert.assertEquals(asg1.getShardIds().size(), 2);
        Assert.assertEquals(asg2.getShardIds().size(), 2);
        Assert.assertEquals(asg3.getShardIds().size(), 2);
        Assert.assertEquals(asg1.getShardIds().get(0), sg1.getShardIds().get(0));
        Assert.assertEquals(asg2.getShardIds().get(0), sg1.getShardIds().get(1));
        Assert.assertEquals(asg3.getShardIds().get(0), sg1.getShardIds().get(2));
        Assert.assertEquals(asg1.getShardIds().get(1), sg2.getShardIds().get(0));
        Assert.assertEquals(asg2.getShardIds().get(1), sg2.getShardIds().get(1));
        Assert.assertEquals(asg3.getShardIds().get(1), sg2.getShardIds().get(2));
        // verify shard
        for (int i = 0; i < sg1.getShardIds().size(); ++i) {
            // s1 - s3
            Shard s = shardManager.getShard(sg1.getShardIds().get(i));
            Assert.assertEquals(s.getGroupIds().size(), 2);
            Assert.assertEquals(s.getGroupIds().get(0), (Long) sg1.getGroupId());
            Assert.assertEquals(s.getGroupIds().get(1), anonymousShardGroupIds.get(i));
        }
        for (int i = 0; i < sg2.getShardIds().size(); ++i) {
            // s4 - s6
            Shard s = shardManager.getShard(sg2.getShardIds().get(i));
            Assert.assertEquals(s.getGroupIds().size(), 2);
            Assert.assertEquals(s.getGroupIds().get(0), (Long) sg2.getGroupId());
            Assert.assertEquals(s.getGroupIds().get(1), anonymousShardGroupIds.get(i));
        }
    }

    // meta group has 3 anonymous groups before delete
    public static void verifyMetaGroupAfterDelete(ShardManager shardManager, List<Long> anonymousShardGroupIds,
                                                  List<Long> shardGroupIds) {
        // verify shard group
        ShardGroup asg1 = shardManager.getShardGroup(anonymousShardGroupIds.get(0));
        ShardGroup asg2 = shardManager.getShardGroup(anonymousShardGroupIds.get(1));
        ShardGroup asg3 = shardManager.getShardGroup(anonymousShardGroupIds.get(2));
        Assert.assertNull(asg1);
        Assert.assertNull(asg2);
        Assert.assertNull(asg3);
        // verify shard
        for (Long groupId : shardGroupIds) {
            for (Long shardId : shardManager.getShardGroup(groupId).getShardIds()) {
                Shard s = shardManager.getShard(shardId);
                Assert.assertEquals(s.getGroupIds().size(), 1);
                Assert.assertEquals(s.getGroupIds().get(0), groupId);
            }
        }
    }

    @Test
    public void testShardManagerSerializeDeserializeShardAndGroupPreserveOrder() throws IOException {
        /*
        Create two shards: shard0, shard1
        Create two groups: group0, group1
            group0.addShard(shard0);
            group0.addShard(shard1);
            group1.addShard(shard1);
            group1.addShard(shard0);

        Do serialization and deserialization, both groups can preserve the order of its shards.
         */
        String serviceId = "service-shard-manager-serialization";

        List<Shard> shards = new ArrayList<>();
        List<ShardGroup> groups = new ArrayList<>();
        { // create shards
            int shardCount = 5;
            List<CreateShardInfo> createShardInfos = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; ++i) {
                createShardInfos.add(CreateShardInfo.newBuilder().build());
            }

            List<ShardInfo> shardInfos = shardManager.createShard(createShardInfos, fsMgr);
            Assert.assertEquals(shardInfos.size(), shardCount);
            shardInfos.forEach(x -> {
                Shard s = shardManager.getShard(x.getShardId());
                Assert.assertTrue(s.getGroupIds().isEmpty()); // empty group id
                shards.add(s);
            });
        }

        { // create shard groups
            List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
            int shardGroupCount = 2;
            for (int i = 0; i < shardGroupCount; ++i) {
                createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
            }
            List<ShardGroupInfo> shardGroupResult = shardManager.createShardGroup(createShardGroupInfos);
            Assert.assertEquals(shardGroupResult.size(), shardGroupCount);
            shardGroupResult.forEach(x -> {
                ShardGroup g = shardManager.getShardGroup(x.getGroupId());
                Assert.assertTrue(g.getShardIds().isEmpty());
                groups.add(g);
            });
        }

        Shard shard0 = shards.get(0);
        Shard shard1 = shards.get(1);
        ShardGroup group0 = groups.get(0);
        ShardGroup group1 = groups.get(1);
        // group0: shard0, shard1
        group0.addShardId(shard0.getShardId());
        group0.addShardId(shard1.getShardId());
        // group1: shard1, shard0
        group1.addShardId(shard1.getShardId());
        group1.addShardId(shard0.getShardId());

        // dumpMeta
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        shardManager.dumpMeta(os);
        ByteArrayInputStream in = new ByteArrayInputStream(os.toByteArray());

        ShardManager shardMgr2 = new ShardManager(serviceId, new DummyJournalSystem(), new IdGenerator(null), null);
        shardMgr2.loadMeta(in);

        // now check shardMgr2 data
        Assert.assertEquals(shardMgr2.getShardCount(), shardManager.getShardCount());
        Assert.assertEquals(shardMgr2.getShardGroupCount(), shardManager.getShardGroupCount());

        // check that all shards shares the same fileStore object
        List<FileStore> fileStores =
                shardMgr2.getAllShardIds().stream().map(x -> shardMgr2.getShard(x).getFilePath().fs)
                        .collect(Collectors.toList());
        Assert.assertEquals(shardMgr2.getShardCount(), fileStores.size());
        Assert.assertTrue(fileStores.size() > 1);

        FileStore fs = fileStores.get(0);
        for (int i = 1; i < fileStores.size(); ++i) {
            FileStore other = fileStores.get(i);
            Assert.assertSame(fs, other);
        }

        ShardGroup group00 = shardMgr2.getShardGroup(group0.getGroupId());
        ShardGroup group11 = shardMgr2.getShardGroup(group1.getGroupId());

        // identical shard id list before and after
        Assert.assertEquals(group0.getShardIds(), group00.getShardIds());
        Assert.assertEquals(group1.getShardIds(), group11.getShardIds());

        { // dump shardMgr2 again, get the same dump result
            ByteArrayOutputStream os2 = new ByteArrayOutputStream();
            shardMgr2.dumpMeta(os2);
            Assert.assertArrayEquals(os.toByteArray(), os2.toByteArray());
        }
    }

    private void gatherAndValidateMetrics(String serviceId, double expectedNumShards, double expectedNumShardGroups)
            throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        MetricsSystem.exportMetricsTextFormat(os);

        List<String> expectedStrings = new ArrayList<>();
        expectedStrings.add(String.format("starmgr_num_shards{serviceId=\"%s\"} %.1f", serviceId, expectedNumShards));
        expectedStrings.add(
                String.format("starmgr_num_shard_groups{serviceId=\"%s\"} %.1f", serviceId, expectedNumShardGroups));
        String content = os.toString();
        String [] lines = content.split("\n");
        for (String str : expectedStrings) {
            boolean found = false;
            for (String line : lines) {
                if (line.equals(str)) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue(String.format("Expect find string '%s' from content:[%s]", str, content), found);
        }
    }

    @Test
    public void testShardManagerMetrics() throws IOException {
        String serviceId = helper.bootstrapService("shard_manager_metrics");

        double expectedNumShards = 0;
        double expectedNumShardGroups = 1;

        gatherAndValidateMetrics(serviceId, expectedNumShards, expectedNumShardGroups);

        ShardManager shardManager1 = helper.getServiceManager().getShardManager(serviceId);
        CreateShardGroupInfo createShardGroupRequest = CreateShardGroupInfo.newBuilder()
                .setPolicy(PlacementPolicy.SPREAD)
                .build();
        // test normal
        {
            // random number of shard groups
            long nGroups = System.currentTimeMillis() % 13 + 1;
            shardManager1.createShardGroup(Collections.nCopies((int) nGroups, createShardGroupRequest));
            expectedNumShardGroups += nGroups;

            gatherAndValidateMetrics(serviceId, expectedNumShards, expectedNumShardGroups);
        }

        // test normal
        long shardGroupId =
                shardManager1.createShardGroup(Collections.singletonList(createShardGroupRequest)).get(0)
                        .getGroupId();
        expectedNumShardGroups++;
        // random number of shards
        long nShards = System.currentTimeMillis() % 37 + 1;
        {
            CreateShardInfo.Builder builder = CreateShardInfo.newBuilder().setReplicaCount(1)
                    .addGroupIds(shardGroupId);

            List<ShardInfo> shardInfoList =
                    shardManager1.createShard(Collections.nCopies((int) nShards, builder.build()),
                            helper.getServiceManager().getFileStoreMgr(serviceId));
            expectedNumShards += nShards;

            gatherAndValidateMetrics(serviceId, expectedNumShards, expectedNumShardGroups);
        }

        // test dump
        {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            shardManager1.dumpMeta(os);
            ByteArrayInputStream in = new ByteArrayInputStream(os.toByteArray());
            ShardManager shardManager2 =
                    new ShardManager(serviceId + "aaa", new DummyJournalSystem(), new IdGenerator(null), null);
            shardManager2.loadMeta(in);

            gatherAndValidateMetrics(serviceId + "aaa", expectedNumShards, expectedNumShardGroups);
        }

        // test delete
        {
            shardManager1.deleteShardGroup(Collections.singletonList(shardGroupId), true /* deleteShards */);
            expectedNumShards -= nShards;
            expectedNumShardGroups--;

            gatherAndValidateMetrics(serviceId, expectedNumShards, expectedNumShardGroups);
        }
    }

    @Test
    public void testFileStoreUpdate() {
        helper.setDefaultWorkerGroupReplicaNumber(5);
        ServiceManager serviceManager = helper.getServiceManager();
        FileStoreMgr fsMgr = serviceManager.getFileStoreMgr(serviceId);
        fsMgr.clear();
        AwsCredential credential = AwsCredentialMgr.getCredentialFromConfig();
        FileStore s3fs = new S3FileStore("test-fskey", "test-fsname",
                "test-bucket", "test-region", "test-endpoint", credential, "");
        fsMgr.addFileStore(s3fs);

        long shardId1 = helper.createTestShard(0);
        long shardId2 = helper.createTestShard(0);

        Shard shard1 = helper.getDefaultShardManager().getShard(shardId1);
        Shard shard2 = helper.getDefaultShardManager().getShard(shardId2);
        Assert.assertEquals(0, shard1.getFilePath().fs.getVersion());
        Assert.assertEquals(0, shard2.getFilePath().fs.getVersion());

        s3fs.setEnabled(false);
        serviceManager.updateFileStore(serviceId, s3fs.toProtobuf());

        // version should be updated
        Assert.assertEquals(1, shard1.getFilePath().fs.getVersion());
        Assert.assertEquals(1, shard2.getFilePath().fs.getVersion());

        // test properties update correctly
        Map<String, String> properties = new HashMap<>();
        properties.put("test-key", "test-value");
        FileStore s3fsCopied = FileStore.fromProtobuf(s3fs.toProtobuf());
        FileStoreInfo.Builder builder = s3fsCopied.toProtobuf().toBuilder().putAllProperties(properties);
        serviceManager.updateFileStore(serviceId, builder.build());

        FileStore fileStore = serviceManager.getFileStoreMgr(serviceId).getFileStore("test-fskey");
        Assert.assertNotNull(fileStore.getPropertiesMap());
        Assert.assertEquals("test-value", fileStore.getPropertiesMap().get("test-key"));
    }

    @Test
    public void testFileStoreReplace() {
        helper.setDefaultWorkerGroupReplicaNumber(5);
        ServiceManager serviceManager = helper.getServiceManager();
        FileStoreMgr fsMgr = serviceManager.getFileStoreMgr(serviceId);
        fsMgr.clear();
        AwsCredential credential = AwsCredentialMgr.getCredentialFromConfig();
        FileStore s3fs = new S3FileStore("test-fskey", "test-fsname",
                "test-bucket", "test-region", "test-endpoint", credential, "");
        fsMgr.addFileStore(s3fs);

        long shardId1 = helper.createTestShard(0);
        long shardId2 = helper.createTestShard(0);

        Shard shard1 = helper.getDefaultShardManager().getShard(shardId1);
        Shard shard2 = helper.getDefaultShardManager().getShard(shardId2);
        Assert.assertEquals(0, shard1.getFilePath().fs.getVersion());
        Assert.assertEquals(0, shard2.getFilePath().fs.getVersion());

        s3fs.setEnabled(false);
        serviceManager.replaceFileStore(serviceId, s3fs.toProtobuf());

        // version should be updated
        Assert.assertEquals(1, shard1.getFilePath().fs.getVersion());
        Assert.assertEquals(1, shard2.getFilePath().fs.getVersion());
    }

    @Test
    public void testShardManageBatchUpdateShardReplicas() {
        MemoryJournalSystem journalSystem = new MemoryJournalSystem();
        try {
            // force replacing the JournalSystem in ShardManager to our own MemoryJournalSystem
            FieldUtils.writeField(shardManager, "journalSystem", journalSystem, true);
        } catch (IllegalAccessException exception) {
            Assert.fail(exception.getMessage());
        }

        // create two shards in group 0
        List<Long> shardIds = helper.createTestShards(2, 0);
        Shard shard1 = helper.getDefaultShardManager().getShard(shardIds.get(0));
        Shard shard2 = helper.getDefaultShardManager().getShard(shardIds.get(1));
        Assert.assertEquals(0L, shard1.getReplicaSize());
        Assert.assertEquals(0L, shard2.getReplicaSize());
        journalSystem.clearJournals();

        long numOfJournals = 0;
        long workerId = 10;
        shardManager.addShardReplicas(shardIds, workerId, false);
        Assert.assertEquals(++numOfJournals, journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_SHARD));

        Assert.assertEquals(1L, shard1.getReplicaSize());
        Assert.assertEquals(1L, shard2.getReplicaSize());

        shardManager.scaleInShardReplicas(shardIds, workerId);
        Assert.assertEquals(++numOfJournals, journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_SHARD));

        Assert.assertEquals(1L, shard1.getReplicaSize());
        Assert.assertEquals(1L, shard2.getReplicaSize());
        Assert.assertEquals(workerId, shard1.getReplica().get(0).getWorkerId());
        Assert.assertEquals(ReplicaState.REPLICA_SCALE_IN, shard1.getReplica().get(0).getState());
        Assert.assertEquals(workerId, shard2.getReplica().get(0).getWorkerId());
        Assert.assertEquals(ReplicaState.REPLICA_SCALE_IN, shard2.getReplica().get(0).getState());

        {
            workerId += 100;
            shardManager.scaleOutShardReplicas(shardIds, workerId, false);
            Assert.assertEquals(++numOfJournals, journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_SHARD));
            Assert.assertEquals(2L, shard1.getReplicaSize());
            Assert.assertEquals(2L, shard2.getReplicaSize());
            Assert.assertEquals(workerId, shard1.getReplica().get(1).getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, shard1.getReplica().get(1).getState());
            Assert.assertEquals(workerId, shard2.getReplica().get(1).getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, shard2.getReplica().get(1).getState());

            shardManager.scaleOutShardReplicasDone(shardIds, workerId);
            Assert.assertEquals(++numOfJournals, journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_SHARD));
            Assert.assertEquals(2L, shard1.getReplicaSize());
            Assert.assertEquals(2L, shard2.getReplicaSize());
            Assert.assertEquals(workerId, shard1.getReplica().get(1).getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_OK, shard1.getReplica().get(1).getState());
            Assert.assertEquals(workerId, shard2.getReplica().get(1).getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_OK, shard2.getReplica().get(1).getState());

            // No update at all, no journal log
            shardManager.scaleOutShardReplicasDone(shardIds, workerId);
            Assert.assertEquals(numOfJournals, journalSystem.getJournalTypeCount(OperationType.OP_UPDATE_SHARD));
        }
    }

    @Test
    public void testUpdateShardReplicaInfo() {
        // create two shards in group 0
        List<Long> shardIds = helper.createTestShards(3, 0);
        ShardManager shardManager1 = helper.getDefaultShardManager();
        Shard shard1 = shardManager1.getShard(shardIds.get(0));
        Shard shard2 = shardManager1.getShard(shardIds.get(1));
        Shard shard3 = shardManager1.getShard(shardIds.get(2));
        long workerId = 10086;

        // Create a replica (SCALE_OUT) for each shard
        shardManager1.scaleOutShardReplicas(shardIds, workerId, false);
        for (long id : shardIds) {
            Shard shard = shardManager1.getShard(id);
            Assert.assertEquals(1L, shard.getReplicaSize());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, shard.getReplica().get(0).getState());
        }

        {
            List<ReplicaUpdateInfo> updateInfos = new ArrayList<>();
            // update shard1 & shard2
            updateInfos.add(ReplicaUpdateInfo.newBuilder().setShardId(shard1.getShardId())
                    .setReplicaState(ReplicaState.REPLICA_OK).build());
            updateInfos.add(ReplicaUpdateInfo.newBuilder().setShardId(shard2.getShardId())
                    .setReplicaState(ReplicaState.REPLICA_OK).build());

            // workerId not exist
            shardManager1.updateShardReplicaInfo(workerId + 1, updateInfos);

            for (long id : shardIds) {
                Shard shard = shardManager1.getShard(id);
                Assert.assertEquals(1L, shard.getReplicaSize());
                Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, shard.getReplica().get(0).getState());
            }

            shardManager1.updateShardReplicaInfo(workerId, updateInfos);
            // shard1 & shard2 updated
            Assert.assertEquals(1L, shard1.getReplicaSize());
            Assert.assertEquals(ReplicaState.REPLICA_OK, shard1.getReplica().get(0).getState());
            Assert.assertEquals(1L, shard2.getReplicaSize());
            Assert.assertEquals(ReplicaState.REPLICA_OK, shard2.getReplica().get(0).getState());
            Assert.assertEquals(1L, shard3.getReplicaSize());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, shard3.getReplica().get(0).getState());
        }

        {
            List<ReplicaUpdateInfo> updateInfos = new ArrayList<>();
            // update shard1 & shard3 & non-exist-shard
            updateInfos.add(ReplicaUpdateInfo.newBuilder().setShardId(shard1.getShardId())
                    .setReplicaState(ReplicaState.REPLICA_OK).build());
            updateInfos.add(ReplicaUpdateInfo.newBuilder().setShardId(shard3.getShardId())
                    .setReplicaState(ReplicaState.REPLICA_OK).build());
            // not exist
            updateInfos.add(ReplicaUpdateInfo.newBuilder().setShardId(shard3.getShardId() + 1)
                    .setReplicaState(ReplicaState.REPLICA_OK).build());

            shardManager1.updateShardReplicaInfo(workerId, updateInfos);

            // shard3 updated
            Assert.assertEquals(1L, shard1.getReplicaSize());
            Assert.assertEquals(ReplicaState.REPLICA_OK, shard1.getReplica().get(0).getState());
            Assert.assertEquals(1L, shard2.getReplicaSize());
            Assert.assertEquals(ReplicaState.REPLICA_OK, shard2.getReplica().get(0).getState());
            Assert.assertEquals(1L, shard3.getReplicaSize());
            Assert.assertEquals(ReplicaState.REPLICA_OK, shard3.getReplica().get(0).getState());
        }
    }
}
