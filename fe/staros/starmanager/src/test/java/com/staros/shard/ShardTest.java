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

import com.google.common.collect.Lists;
import com.staros.credential.AwsSimpleCredential;
import com.staros.filecache.FileCache;
import com.staros.filestore.FilePath;
import com.staros.filestore.S3FileStore;
import com.staros.proto.AddShardInfo;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsDefaultCredentialInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.ReplicaInfo;
import com.staros.proto.ReplicaRole;
import com.staros.proto.ReplicaState;
import com.staros.proto.S3FileStoreInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.ShardState;
import com.staros.replica.Replica;
import com.staros.util.Constant;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardTest {
    public static Shard getTestShard(String serviceId, long groupId, long shardId) {
        List<Long> groupIds = new ArrayList<>();
        groupIds.add(groupId);
        S3FileStore fs =
                new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG, "bucket", "region",
                        "endpoint", new AwsSimpleCredential("ak", "sk"), "");
        FilePath path = new FilePath(fs, "suffix");
        FileCache cache = new FileCache();
        Shard shard = new Shard(serviceId, groupIds, shardId, path, cache);

        Replica replica = new Replica(123);
        List<Replica> replicas = new ArrayList<>();
        replicas.add(replica);
        shard.setReplicas(replicas);

        shard.setExpectedReplicaNum(111);

        Map<String, String> properties = new HashMap<>();
        properties.put("hello", "world");
        shard.setProperties(properties);
        return shard;
    }

    @Test
    public void testShard() {
        String serviceId = "1";
        {
            long groupId = 2;
            long shardId = 3;
            Shard shard = getTestShard(serviceId, groupId, shardId);

            List<Long> groupIds = new ArrayList<>();
            groupIds.add(groupId);
            Assert.assertEquals(shard.getServiceId(), serviceId);
            Assert.assertEquals(shard.getGroupIds(), groupIds);
            Assert.assertEquals(shard.getShardId(), shardId);
            Assert.assertEquals(shard.getState(), ShardState.NORMAL);
            Assert.assertNotEquals(shard.hashCode(), 0);
        }

        {
            long shardId = 12345L;
            final List<Long> groups = Arrays.asList(1L, 2L, 3L);
            // empty group list
            Shard shard  = new Shard(serviceId, new ArrayList<>(), shardId);
            Assert.assertTrue(shard.getGroupIds().isEmpty());

            // now join in all groups
            for (Long gid : groups) {
                Assert.assertTrue(shard.joinGroup(gid));
            }
            Assert.assertEquals(shard.getGroupIds().size(), groups.size());
            Assert.assertEquals(shard.getGroupIds(), groups);

            // shard not in the group 0
            Assert.assertFalse(shard.quitGroup(0L));
            Assert.assertEquals(shard.getGroupIds().size(), 3);
            // shard is in group 1, quit group 1, remain in group 2, 3
            Assert.assertTrue(shard.quitGroup(1L));
            Assert.assertEquals(shard.getGroupIds().size(), 2);
            Assert.assertTrue(shard.getGroupIds().contains(2L));
            Assert.assertTrue(shard.getGroupIds().contains(3L));
        }
    }

    @Test
    public void testSerialization() {
        String serviceId = "1";
        long groupId = 2;
        long shardId = 3;
        Shard shard1 = getTestShard(serviceId, groupId, shardId);

        // serialization
        ShardInfo info = shard1.toProtobuf();
        Assert.assertEquals(info.getFilePath().getFsInfo().getS3FsInfo().getCredential().getCredentialCase(),
                AwsCredentialInfo.CredentialCase.SIMPLE_CREDENTIAL);

        ShardInfo info2 = shard1.toDebugProtobuf();
        Assert.assertEquals(info2.getFilePath().getFsInfo().getS3FsInfo().getCredential().getCredentialCase(),
                AwsCredentialInfo.CredentialCase.CREDENTIAL_NOT_SET);

        ShardInfo info3 = shard1.toProtobuf(true /* withoutReplicaInfo */);
        Assert.assertEquals(info3.getReplicaInfoList().size(), 0);
        ShardInfo info4 = shard1.toProtobuf(false /* withoutReplicaInfo */);
        Assert.assertEquals(info4.getReplicaInfoList().size(), 1);

        // deserialization
        Shard shard2 = Shard.fromProtobuf(info);

        Assert.assertEquals(shard1.getServiceId(), shard2.getServiceId());
        Assert.assertEquals(shard1.getGroupIds(), shard2.getGroupIds());
        Assert.assertEquals(shard1.getShardId(), shard2.getShardId());
        Assert.assertEquals(shard1.getState(), shard2.getState());
        Assert.assertEquals(shard1.getReplicaWorkerIds().size(), 1);
        Assert.assertEquals(shard1.getReplicaWorkerIds().get(0), shard2.getReplicaWorkerIds().get(0));
        Assert.assertEquals(shard1.getProperties(), shard2.getProperties());
    }

    @Test
    public void testShardReplica() {
        String serviceId = "1";
        long groupId = 2;
        List<Long> groupIds = new ArrayList<>();
        groupIds.add(groupId);
        long shardId = 3;
        Shard shard = new Shard(serviceId, groupIds, shardId);

        long workerId1 = 100;
        long workerId2 = 101;
        long workerId3 = 102;
        long workerId4 = 103;

        // test add
        {
            Assert.assertTrue(shard.addReplica(workerId1));
            Assert.assertFalse(shard.addReplica(workerId1));
            List<ReplicaInfo> replicas = shard.toProtobuf().getReplicaInfoList();
            Assert.assertEquals(replicas.size(), 1);
            Assert.assertEquals(replicas.get(0).getReplicaRole(), ReplicaRole.PRIMARY);
        }
        {
            Assert.assertTrue(shard.addReplica(workerId2));
            Assert.assertFalse(shard.addReplica(workerId2));
            List<ReplicaInfo> replicas = shard.toProtobuf().getReplicaInfoList();
            Assert.assertEquals(replicas.size(), 2);
            Assert.assertEquals(replicas.get(0).getReplicaRole(), ReplicaRole.PRIMARY);
            Assert.assertEquals(replicas.get(1).getReplicaRole(), ReplicaRole.PRIMARY);
        }
        {
            Assert.assertTrue(shard.addReplica(workerId3));
            Assert.assertFalse(shard.addReplica(workerId3));
            List<ReplicaInfo> replicas = shard.toProtobuf().getReplicaInfoList();
            Assert.assertEquals(replicas.size(), 3);
            // every one is PRIMARY
            Assert.assertEquals(replicas.get(0).getReplicaRole(), ReplicaRole.PRIMARY);
            Assert.assertEquals(replicas.get(1).getReplicaRole(), ReplicaRole.PRIMARY);
            Assert.assertEquals(replicas.get(2).getReplicaRole(), ReplicaRole.PRIMARY);
        }
        {
            Assert.assertTrue(shard.addTempReplica(workerId4));
            Assert.assertFalse(shard.addTempReplica(workerId4));
            List<ReplicaInfo> replicas = shard.toProtobuf().getReplicaInfoList();
            Assert.assertEquals(replicas.size(), 4);
            // temp replica added
            Assert.assertTrue(shard.getReplica().get(3).getTempFlag());

            // remove the temp replica directly
            Assert.assertTrue(shard.removeReplica(workerId4));
            Assert.assertFalse(shard.removeReplica(workerId4));
        }

        // test remove
        {
            Assert.assertTrue(shard.removeReplica(workerId2));
            Assert.assertFalse(shard.removeReplica(workerId2));
            List<ReplicaInfo> replicas = shard.toProtobuf().getReplicaInfoList();
            Assert.assertEquals(replicas.size(), 2);
            Assert.assertEquals(replicas.get(0).getReplicaRole(), ReplicaRole.PRIMARY);
            Assert.assertEquals(replicas.get(1).getReplicaRole(), ReplicaRole.PRIMARY);
        }
        {
            Assert.assertTrue(shard.removeReplica(workerId1));
            Assert.assertFalse(shard.removeReplica(workerId1));
            List<ReplicaInfo> replicas = shard.toProtobuf().getReplicaInfoList();
            Assert.assertEquals(replicas.size(), 1);
            Assert.assertEquals(replicas.get(0).getReplicaRole(), ReplicaRole.PRIMARY);
        }
        {
            Assert.assertTrue(shard.removeReplica(workerId3));
            Assert.assertFalse(shard.removeReplica(workerId3));
            List<ReplicaInfo> replicas = shard.toProtobuf().getReplicaInfoList();
            Assert.assertEquals(replicas.size(), 0);
        }
    }

    @Test
    public void testUpdateFileStore() {
        String serviceId = "1";
        long groupId = 2;
        long shardId = 3;
        Shard shard1 = getTestShard(serviceId, groupId, shardId);
        AwsCredentialInfo awsCredentialInfo = AwsCredentialInfo.newBuilder()
                .setDefaultCredential(AwsDefaultCredentialInfo.newBuilder().build()).build();
        S3FileStoreInfo s3fs = S3FileStoreInfo.newBuilder()
                .setBucket("bucket").setPathPrefix("prefix").setCredential(awsCredentialInfo).build();
        FileStoreInfo fsInfo = FileStoreInfo.newBuilder()
                .setFsName(Constant.S3_FSNAME_FOR_CONFIG)
                .setS3FsInfo(s3fs)
                .setFsKey(Constant.S3_FSKEY_FOR_CONFIG)
                .setFsType(FileStoreType.S3)
                .addAllLocations(new ArrayList<>())
                .setVersion(0).build();
        Assert.assertFalse(shard1.updateFileStore(fsInfo));
        Assert.assertEquals(0, shard1.getFilePath().fs.getVersion());

        fsInfo = fsInfo.toBuilder().setVersion(1).build();
        Assert.assertTrue(shard1.updateFileStore(fsInfo));
        Assert.assertEquals(1, shard1.getFilePath().fs.getVersion());

        fsInfo = fsInfo.toBuilder().setFsKey("test-fskey").setVersion(2).build();
        Assert.assertFalse(shard1.updateFileStore(fsInfo));
        Assert.assertEquals(1, shard1.getFilePath().fs.getVersion());

        fsInfo = fsInfo.toBuilder().setFsKey(new String(Constant.S3_FSKEY_FOR_CONFIG)).setVersion(2).build();
        Assert.assertTrue(shard1.updateFileStore(fsInfo));
        Assert.assertEquals(2, shard1.getFilePath().fs.getVersion());
    }

    @Test
    public void testImmutableReplicaList() {
        Shard testShard = new Shard("serviceId", Lists.newArrayList(1L), 123);
        List<Replica> replicas1 = testShard.getReplica();
        Assert.assertTrue(replicas1.isEmpty());
        Assert.assertThrows(UnsupportedOperationException.class, () -> replicas1.add(new Replica(2L)));

        // Add two new replicas
        long workerId1 = 11;
        long workerId2 = 22L;
        Assert.assertTrue(testShard.addReplica(workerId1));
        Assert.assertTrue(testShard.addReplica(workerId2));

        List<Replica> replicas2 = testShard.getReplica();
        Assert.assertEquals(2L, replicas2.size());
        Assert.assertEquals(workerId1, replicas2.get(0).getWorkerId());
        Assert.assertEquals(workerId2, replicas2.get(1).getWorkerId());
        // Can't modify the list outside the Shard
        Assert.assertThrows(UnsupportedOperationException.class, () -> replicas2.remove(0));

        // replicas1 still empty
        Assert.assertTrue(replicas1.isEmpty());

        Assert.assertTrue(testShard.removeReplica(workerId1));
        List<Replica> replicas3 = testShard.getReplica();
        Assert.assertEquals(1L, replicas3.size());
        Assert.assertEquals(workerId2, replicas3.get(0).getWorkerId());

        // replicas1 and replicas2 not affected
        Assert.assertTrue(replicas1.isEmpty());
        Assert.assertEquals(2L, replicas2.size());
    }

    @Test
    public void testGetAddShardInfo() {
        Shard shard = getTestShard("serviceId", 1, 10086);
        shard.setReplicas(Collections.emptyList());
        // no replica at all.
        Assert.assertTrue(shard.getReplica().isEmpty());

        long workerId1 = 10000;
        long workerId2 = 10010;
        List<Long> workerIds = Lists.newArrayList(workerId1, workerId2);

        AddShardInfo info;
        {
            info = shard.getAddShardInfo();
            Assert.assertEquals(0L, info.getReplicaInfoCount());

            info = shard.getAddShardInfo(workerIds, false);
            Assert.assertEquals(0L, info.getReplicaInfoCount());
        }

        // Add two replicas
        Assert.assertTrue(shard.addReplica(workerId1));
        Assert.assertTrue(shard.addReplica(workerId2));

        {
            // doesn't contain replicaInfo if the worker id list is not provided.
            info = shard.getAddShardInfo();
            Assert.assertEquals(0L, info.getReplicaInfoCount());

            // have the two replicas
            info = shard.getAddShardInfo(workerIds, false);
            Assert.assertEquals(2L, info.getReplicaInfoCount());
            Assert.assertTrue(workerIds.contains(info.getReplicaInfo(0).getWorkerId()));
            Assert.assertTrue(workerIds.contains(info.getReplicaInfo(1).getWorkerId()));

            // no-exists replicas
            List<Long> notExistReplicaWorkerIds = Lists.newArrayList(200000L, 2000001L);
            info = shard.getAddShardInfo(notExistReplicaWorkerIds, false);
            Assert.assertEquals(0L, info.getReplicaInfoCount());

            List<Long> workerIdsWithNotExistReplica = new ArrayList<>(workerIds);
            long notExistWorkerId = 30000L;
            workerIdsWithNotExistReplica.add(notExistWorkerId);
            info = shard.getAddShardInfo(workerIdsWithNotExistReplica, false);
            Assert.assertEquals(2L, info.getReplicaInfoCount());
            Assert.assertTrue(info.getReplicaInfoList().stream().noneMatch(x -> x.getWorkerId() == notExistWorkerId));
        }
    }

    @Test
    public void testShardReplicaOperations() {
        Shard shard = getTestShard("serviceId", 1, 10088);
        long workerId = 3;
        {
            shard.setReplicas(Collections.emptyList());
            Assert.assertTrue(shard.addReplica(workerId));
            Assert.assertEquals(1L, shard.getReplicaSize());
            Assert.assertEquals(workerId, shard.getReplica().get(0).getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_OK, shard.getReplica().get(0).getState());

            // can't add the same workerId with different replica state
            Assert.assertFalse(shard.addReplica(workerId, ReplicaState.REPLICA_OK));
            Assert.assertFalse(shard.addReplica(workerId, ReplicaState.REPLICA_SCALE_OUT));
            Assert.assertFalse(shard.addReplica(workerId, ReplicaState.REPLICA_SCALE_IN));
        }
        {
            shard.setReplicas(Collections.emptyList());
            ++workerId;
            Assert.assertTrue(shard.addReplica(workerId, ReplicaState.REPLICA_SCALE_IN));
            Assert.assertEquals(1L, shard.getReplicaSize());
            Assert.assertEquals(workerId, shard.getReplica().get(0).getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_IN, shard.getReplica().get(0).getState());
        }
        {
            shard.setReplicas(Collections.emptyList());
            ++workerId;
            Assert.assertTrue(shard.addReplica(workerId, ReplicaState.REPLICA_SCALE_OUT));
            Assert.assertEquals(1L, shard.getReplicaSize());
            Assert.assertEquals(workerId, shard.getReplica().get(0).getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, shard.getReplica().get(0).getState());
        }
        {
            shard.setReplicas(Collections.emptyList());
            ++workerId;
            Assert.assertTrue(shard.scaleOutReplica(workerId));
            Assert.assertEquals(1L, shard.getReplicaSize());
            Assert.assertEquals(workerId, shard.getReplica().get(0).getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, shard.getReplica().get(0).getState());

            Assert.assertTrue(shard.scaleOutReplicaDone(workerId));
            Assert.assertEquals(1L, shard.getReplicaSize());
            Assert.assertEquals(workerId, shard.getReplica().get(0).getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_OK, shard.getReplica().get(0).getState());

            // replica state not match
            Assert.assertFalse(shard.scaleOutReplicaDone(workerId));
            // workerId not exist
            Assert.assertFalse(shard.scaleOutReplicaDone(workerId + 1));
        }
        {
            shard.setReplicas(Collections.emptyList());
            ++workerId;
            Assert.assertFalse(shard.scaleInReplica(workerId));
            Assert.assertEquals(0L, shard.getReplicaSize());

            Assert.assertTrue(shard.addReplica(workerId));
            Assert.assertEquals(workerId, shard.getReplica().get(0).getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_OK, shard.getReplica().get(0).getState());

            Assert.assertTrue(shard.scaleInReplica(workerId));
            Assert.assertEquals(workerId, shard.getReplica().get(0).getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_IN, shard.getReplica().get(0).getState());
        }
        {
            shard.setReplicas(Collections.emptyList());
            ++workerId;
            Assert.assertTrue(shard.scaleOutTempReplica(workerId));
            Assert.assertEquals(1L, shard.getReplicaSize());
            Replica replica = shard.getReplica().get(0);
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, replica.getState());
            Assert.assertTrue(replica.getTempFlag());

            Assert.assertTrue(shard.convertTempReplicaToNormal(workerId));
            Assert.assertFalse(replica.getTempFlag());

            Assert.assertFalse(shard.convertTempReplicaToNormal(workerId));
        }
        {
            shard.setReplicas(Collections.emptyList());
            ++workerId;
            Assert.assertTrue(shard.scaleOutReplica(workerId));
            Assert.assertEquals(1L, shard.getReplicaSize());
            Replica replica = shard.getReplica().get(0);
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, replica.getState());
            Assert.assertFalse(replica.getTempFlag());

            Assert.assertTrue(shard.scaleOutReplicaDone(workerId));
            Assert.assertEquals(ReplicaState.REPLICA_OK, replica.getState());

            // REPLICA_OK, can't do cancel ScaleIn
            Assert.assertFalse(shard.cancelScaleInReplica(workerId));
            Assert.assertTrue(shard.scaleInReplica(workerId));
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_IN, replica.getState());

            // REPLICA_SCALE_IN, can cancel the scale-in, return to REPLICA_OK
            Assert.assertTrue(shard.cancelScaleInReplica(workerId));
            Assert.assertEquals(ReplicaState.REPLICA_OK, replica.getState());
        }
    }

    @Test
    public void testShardHashCodeNotChangeRelatedToReplica() {
        // hashCode won't be affected by replica change
        Shard shard = getTestShard("serviceId", 1, 10088);
        shard.setReplicas(Collections.emptyList());

        int hashCode = shard.hashCode();

        shard.addReplica(1);
        Assert.assertEquals(hashCode, shard.hashCode());

        shard.scaleOutReplica(2);
        Assert.assertEquals(hashCode, shard.hashCode());

        shard.scaleOutReplicaDone(2);
        Assert.assertEquals(hashCode, shard.hashCode());

        shard.scaleInReplica(2);
        Assert.assertEquals(hashCode, shard.hashCode());
    }

    @Test
    public void testShardHashCodeWithReplica() {
        Shard shard = getTestShard("serviceId", 1, 10088);
        shard.setReplicas(Collections.emptyList());

        List<Long> workerIds = new ArrayList<>();
        int hashCode = shard.hashCodeWithReplicas(workerIds);
        // no replica at all
        Assert.assertEquals(hashCode, shard.hashCode());

        workerIds.add(1L);
        shard.addReplica(1);
        hashCode = shard.hashCodeWithReplicas(workerIds);
        Assert.assertNotEquals(hashCode, shard.hashCode());

        hashCode = shard.hashCodeWithReplicas(workerIds);
        shard.scaleOutReplica(2);
        workerIds.add(2L);
        // hashCode changed by scaling new replica
        Assert.assertNotEquals(hashCode, shard.hashCodeWithReplicas(workerIds));

        hashCode = shard.hashCodeWithReplicas(workerIds);
        shard.scaleOutReplicaDone(2);
        // hashCode changed by scaling replica done
        Assert.assertNotEquals(hashCode, shard.hashCodeWithReplicas(workerIds));

        hashCode = shard.hashCodeWithReplicas(workerIds);
        shard.scaleInReplica(2);
        // hashCode changed by scaling in replica
        Assert.assertNotEquals(hashCode, shard.hashCodeWithReplicas(workerIds));
    }

    @Test
    public void testShardHashCodeWithSupplReplica() {
        Shard shard = getTestShard("serviceId", 1, 10088);
        shard.setReplicas(Collections.emptyList());

        List<Long> workerIds = new ArrayList<>();
        workerIds.add(1L);
        shard.addReplica(1L);

        Replica replica = new Replica(2, ReplicaState.REPLICA_SCALE_OUT, 0, false);
        AddShardInfo info = shard.getAddShardInfo(workerIds, replica, true);

        shard.scaleOutReplica(2);
        workerIds.add(2L);

        // the hashCode in AddShardInfo is calculated as if the replica were added to the shard
        Assert.assertEquals(shard.hashCodeWithReplicas(workerIds), info.getHashCode());
    }
}
