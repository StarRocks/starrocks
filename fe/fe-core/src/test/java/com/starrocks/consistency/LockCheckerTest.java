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

package com.starrocks.consistency;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LockCheckerTest {

    private static List<Thread> makeThreads(int n) {
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            threads.add(new Thread(() -> {
            }, "waiter-" + i));
        }
        return threads;
    }

    @Test
    public void testWaiterArrayWithoutCap() {
        List<Thread> waiters = makeThreads(5);
        JsonArray arr = LockChecker.getLockWaiterInfoJsonArray(waiters);
        Assertions.assertEquals(5, arr.size());
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.get(i).getAsJsonObject();
            Assertions.assertTrue(obj.has("threadId"));
            Assertions.assertTrue(obj.has("threadName"));
            Assertions.assertFalse(obj.has("omitted"));
        }
    }

    @Test
    public void testWaiterArrayCapZeroDisablesCap() {
        List<Thread> waiters = makeThreads(7);
        JsonArray arr = LockChecker.getLockWaiterInfoJsonArray(waiters, 0);
        Assertions.assertEquals(7, arr.size());
        for (int i = 0; i < arr.size(); i++) {
            Assertions.assertFalse(arr.get(i).getAsJsonObject().has("omitted"));
        }
    }

    @Test
    public void testWaiterArrayCapNegativeDisablesCap() {
        List<Thread> waiters = makeThreads(4);
        JsonArray arr = LockChecker.getLockWaiterInfoJsonArray(waiters, -1);
        Assertions.assertEquals(4, arr.size());
    }

    @Test
    public void testWaiterArrayCapNotExceeded() {
        List<Thread> waiters = makeThreads(3);
        JsonArray arr = LockChecker.getLockWaiterInfoJsonArray(waiters, 5);
        Assertions.assertEquals(3, arr.size(),
                "cap above total — should list all without trailer");
        for (int i = 0; i < arr.size(); i++) {
            Assertions.assertFalse(arr.get(i).getAsJsonObject().has("omitted"));
        }
    }

    @Test
    public void testWaiterArrayCapEqualsTotal() {
        List<Thread> waiters = makeThreads(4);
        JsonArray arr = LockChecker.getLockWaiterInfoJsonArray(waiters, 4);
        Assertions.assertEquals(4, arr.size(),
                "cap equals total — no trailer");
        Assertions.assertFalse(arr.get(3).getAsJsonObject().has("omitted"));
    }

    @Test
    public void testWaiterArrayCapTruncates() {
        List<Thread> waiters = makeThreads(10);
        JsonArray arr = LockChecker.getLockWaiterInfoJsonArray(waiters, 3);
        // 3 individual + 1 trailer
        Assertions.assertEquals(4, arr.size());
        for (int i = 0; i < 3; i++) {
            JsonObject obj = arr.get(i).getAsJsonObject();
            Assertions.assertTrue(obj.has("threadName"));
            Assertions.assertFalse(obj.has("omitted"));
        }
        JsonObject trailer = arr.get(3).getAsJsonObject();
        Assertions.assertTrue(trailer.has("omitted"));
        Assertions.assertEquals("remain 7 waiters omitted",
                trailer.get("omitted").getAsString());
    }

    @Test
    public void testWaiterArrayNullSource() {
        JsonArray arr = LockChecker.getLockWaiterInfoJsonArray(null, 3);
        Assertions.assertEquals(0, arr.size());
    }

    @Test
    public void testWaiterArrayEmptySource() {
        JsonArray arr = LockChecker.getLockWaiterInfoJsonArray(Collections.emptyList(), 3);
        Assertions.assertEquals(0, arr.size());
    }

    @Test
    public void testWaiterArrayNullEntriesSkipped() {
        List<Thread> waiters = new ArrayList<>();
        waiters.add(new Thread(() -> { }, "t1"));
        waiters.add(null);
        waiters.add(new Thread(() -> { }, "t2"));
        JsonArray arr = LockChecker.getLockWaiterInfoJsonArray(waiters, 0);
        // null skipped, no trailer
        Assertions.assertEquals(2, arr.size());
        Assertions.assertEquals("t1", arr.get(0).getAsJsonObject().get("threadName").getAsString());
        Assertions.assertEquals("t2", arr.get(1).getAsJsonObject().get("threadName").getAsString());
    }
}
