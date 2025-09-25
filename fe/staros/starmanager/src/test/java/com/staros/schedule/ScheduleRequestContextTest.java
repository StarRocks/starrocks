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

package com.staros.schedule;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

public class ScheduleRequestContextTest {
    @Test
    public void testScheduleRequestContextHashCode() {
        {
            // only shardId, workGroupId  matters
            ScheduleRequestContext ctx1 = new ScheduleRequestContext("1", 1, 2, null);
            ScheduleRequestContext ctx2 = new ScheduleRequestContext("2", 1, 2, null);
            Assert.assertEquals(ctx1.hashCode(), ctx2.hashCode());
        }
        {
            // only shardId, workGroupId  matters
            ScheduleRequestContext ctx1 = new ScheduleRequestContext("1", 1, 2, null);
            ScheduleRequestContext ctx2 = new ScheduleRequestContext("2", 1, 2, new CountDownLatch(1));
            Assert.assertEquals(ctx1.hashCode(), ctx2.hashCode());
        }
        {
            // only shardId, workGroupId  matters
            ScheduleRequestContext ctx1 = new ScheduleRequestContext("1", 1, 2, null);
            ScheduleRequestContext ctx2 = new ScheduleRequestContext("1", 2, 2, null);
            Assert.assertNotEquals(ctx1.hashCode(), ctx2.hashCode());
        }
        {
            // only shardId, workGroupId  matters
            ScheduleRequestContext ctx1 = new ScheduleRequestContext("1", 1, 2, null);
            ScheduleRequestContext ctx2 = new ScheduleRequestContext("1", 1, 3, null);
            Assert.assertNotEquals(ctx1.hashCode(), ctx2.hashCode());
        }
    }

    @Test
    public void testScheduleRequestContextEquals() {
        { // different object equals
            ScheduleRequestContext ctx = new ScheduleRequestContext("1", 1, 1, null);
            Assert.assertEquals(ctx, ctx);
            Assert.assertNotEquals(ctx, null);
            Assert.assertNotEquals(null, ctx);
            Assert.assertFalse(Objects.equals(ctx, 1L));
        }
        { // only serviceId, shardId, workerGroupId matters
            ScheduleRequestContext ctx1 = new ScheduleRequestContext("1", 1, 2, null);
            ScheduleRequestContext ctx2 = new ScheduleRequestContext("1", 1, 2, new CountDownLatch(1));
            Assert.assertEquals(ctx1, ctx2);
            Assert.assertFalse(ctx1.isWaited());
            Assert.assertTrue(ctx2.isWaited());

            ctx2.setWorkerIds(Collections.nCopies(3, 5L));
            // still equals
            Assert.assertEquals(ctx1, ctx2);
        }
        { // only serviceId, shardId, workerGroupId matters
            ScheduleRequestContext ctx1 = new ScheduleRequestContext("1", 1, 2, null);
            ScheduleRequestContext ctx2 = new ScheduleRequestContext("1", 1, 3, null);
            Assert.assertNotEquals(ctx1, ctx2);
        }
        { // only serviceId, shardId, workerGroupId matters
            ScheduleRequestContext ctx1 = new ScheduleRequestContext("1", 1, 2, null);
            ScheduleRequestContext ctx2 = new ScheduleRequestContext("1", 2, 2, null);
            Assert.assertNotEquals(ctx1, ctx2);
        }
        { // only serviceId, shardId, workerGroupId matters
            ScheduleRequestContext ctx1 = new ScheduleRequestContext("1", 1, 2, null);
            ScheduleRequestContext ctx2 = new ScheduleRequestContext("2", 1, 2, null);
            Assert.assertNotEquals(ctx1, ctx2);
            // even though the two objects are not equal, their hashcode can be equal.
            Assert.assertEquals(ctx1.hashCode(), ctx2.hashCode());
        }
    }

    @Test
    public void testScheduleRequestContextSort() {
        // MSB -> LSB, shardId / workerGroupId / serviceId
        ScheduleRequestContext ctx0 = new ScheduleRequestContext("1", 10, 5, null);
        ScheduleRequestContext ctx1 = new ScheduleRequestContext("1", 8, 5, null);
        ScheduleRequestContext ctx2 = new ScheduleRequestContext("1", 10, 4, null);
        ScheduleRequestContext ctx3 = new ScheduleRequestContext("2", 8, 5, null);
        ScheduleRequestContext ctx4 = new ScheduleRequestContext("1", 10, 4, null);
        // ctx1 < ctx3 < ctx2 == ctx4 < ctx0
        List<ScheduleRequestContext> list = new ArrayList<>();
        list.add(ctx0);
        list.add(ctx1);
        list.add(ctx2);
        list.add(ctx3);
        list.add(ctx4);

        // before sort, elements are in the order of its adding precedence
        Assert.assertEquals(list.get(0), ctx0);
        Assert.assertEquals(list.get(1), ctx1);
        Assert.assertEquals(list.get(2), ctx2);
        Assert.assertEquals(list.get(3), ctx3);
        Assert.assertEquals(list.get(4), ctx4);
        // stable sort
        Collections.sort(list);
        // after sort
        Assert.assertEquals(list.get(0), ctx1);
        Assert.assertEquals(list.get(1), ctx3);
        Assert.assertEquals(list.get(2), ctx2);
        Assert.assertEquals(list.get(3), ctx4);
        Assert.assertEquals(list.get(4), ctx0);
    }
}
