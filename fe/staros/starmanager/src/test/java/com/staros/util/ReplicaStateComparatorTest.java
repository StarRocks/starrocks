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

package com.staros.util;

import com.staros.proto.ReplicaState;
import org.junit.Assert;
import org.junit.Test;

public class ReplicaStateComparatorTest {
    @Test
    public void testReplicaStateComparator() {
        ReplicaState r11 = ReplicaState.REPLICA_OK;
        ReplicaState r12 = ReplicaState.REPLICA_OK;
        ReplicaState r21 = ReplicaState.REPLICA_SCALE_OUT;
        ReplicaState r22 = ReplicaState.REPLICA_SCALE_OUT;
        ReplicaState r31 = ReplicaState.REPLICA_SCALE_IN;
        ReplicaState r32 = ReplicaState.REPLICA_SCALE_IN;

        // OK < SCALE_IN < SCALE_OUT by ReplicaStateComparator
        Assert.assertEquals(0, ReplicaStateComparator.compare(r11, r12));
        Assert.assertEquals(0, ReplicaStateComparator.compare(r21, r22));
        Assert.assertEquals(0, ReplicaStateComparator.compare(r31, r32));
        Assert.assertEquals(0, ReplicaStateComparator.compare(r12, r11));
        Assert.assertEquals(0, ReplicaStateComparator.compare(r22, r21));
        Assert.assertEquals(0, ReplicaStateComparator.compare(r32, r31));

        Assert.assertEquals(-1, ReplicaStateComparator.compare(r11, r21));
        Assert.assertEquals(-1, ReplicaStateComparator.compare(r11, r31));
        Assert.assertEquals(1, ReplicaStateComparator.compare(r21, r11));
        Assert.assertEquals(1, ReplicaStateComparator.compare(r31, r11));

        Assert.assertEquals(1, ReplicaStateComparator.compare(r21, r31));
        Assert.assertEquals(-1, ReplicaStateComparator.compare(r31, r21));
    }
}
