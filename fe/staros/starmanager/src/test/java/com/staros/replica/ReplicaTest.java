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


package com.staros.replica;

import com.staros.proto.ReplicaInfo;
import com.staros.proto.ReplicaInfoLite;
import com.staros.proto.ReplicaRole;
import com.staros.proto.ReplicaState;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ReplicaTest {
    @Test
    public void testReplica() {
        long workerId = 1234;
        Replica replica = new Replica(workerId);
        Assert.assertEquals(replica.getRole(), ReplicaRole.PRIMARY);
        replica.setRole(ReplicaRole.SECONDARY);
        Assert.assertEquals(replica.getRole(), ReplicaRole.SECONDARY);
        Assert.assertEquals(ReplicaState.REPLICA_OK, replica.getState());
        Assert.assertFalse(replica.getTempFlag());
    }

    @Test
    public void testSerialization() {
        List<Replica> replicas = new ArrayList<>();
        {
            long workerId = 1234;
            Replica replica1 = new Replica(workerId);
            replica1.setRole(ReplicaRole.SECONDARY);
            replica1.setReplicaState(ReplicaState.REPLICA_SCALE_OUT);
            replicas.add(replica1);

            Replica replica2 = new Replica(workerId, ReplicaState.REPLICA_SCALE_IN, System.currentTimeMillis(), true);
            replicas.add(replica2);
        }

        for (Replica replica : replicas) {
            // serialization
            ReplicaInfo info = replica.toProtobuf();
            // deserialization
            Replica replica2 = Replica.fromProtobuf(info);

            Assert.assertEquals(replica.getWorkerId(), replica2.getWorkerId());
            Assert.assertEquals(replica.getRole(), replica2.getRole());
            Assert.assertEquals(replica.getState(), replica2.getState());
            Assert.assertEquals(replica.getTempFlag(), replica2.getTempFlag());

            ReplicaInfoLite infoLite = replica2.toReplicaInfoLite();
            Assert.assertEquals(infoLite.getWorkerId(), replica2.getWorkerId());
            Assert.assertEquals(infoLite.getReplicaState(), replica2.getState());
        }
    }

    @Test
    public void testSetReplicaState() {
        Replica replica = new Replica(10086);

        long t1 = System.currentTimeMillis();
        replica.setReplicaState(ReplicaState.REPLICA_SCALE_OUT);
        long t2 = System.currentTimeMillis();

        ReplicaInfo info = replica.toProtobuf();
        Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, info.getReplicaState());
        Assert.assertTrue(t1 <= info.getReplicaStateTimestamp());
        Assert.assertTrue(info.getReplicaStateTimestamp() <= t2);
    }

    @Test
    public void testReplicaHashCode() {
        Replica replica = new Replica(1L, ReplicaState.REPLICA_OK, 1000, false);
        int hashCode1 = replica.hashCode();

        {
            Replica replica2 = new Replica(1L, ReplicaState.REPLICA_OK, 1001, false);
            Assert.assertEquals(hashCode1, replica2.hashCode());
        }

        replica.setReplicaState(ReplicaState.REPLICA_SCALE_IN);
        int hashCode2 = replica.hashCode();
        Assert.assertNotEquals(hashCode1, hashCode2);

        replica.setReplicaState(ReplicaState.REPLICA_SCALE_OUT);
        int hashCode3 = replica.hashCode();
        Assert.assertNotEquals(hashCode1, hashCode3);
        Assert.assertNotEquals(hashCode2, hashCode3);

        replica.setReplicaState(ReplicaState.REPLICA_OK);
        int hashCode4 = replica.hashCode();
        Assert.assertEquals(hashCode1, hashCode4); // hashCode back to the same as the initial status
        Assert.assertNotEquals(hashCode2, hashCode4);
        Assert.assertNotEquals(hashCode3, hashCode4);
    }

    @Test
    public void testTempReplica() {
        Replica replica = new Replica(1L, ReplicaState.REPLICA_OK, 1L, true);
        Assert.assertTrue(replica.getTempFlag());
        replica.clearTempFlag();
        Assert.assertFalse(replica.getTempFlag());
    }
}
