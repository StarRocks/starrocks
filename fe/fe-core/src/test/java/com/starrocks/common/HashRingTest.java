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


package com.starrocks.common;

import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.starrocks.common.util.ConsistentHashRing;
import com.starrocks.common.util.HashRing;
import com.starrocks.common.util.PlainHashRing;
import com.starrocks.common.util.RendezvousHashRing;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashRingTest {
    final int kNodeSize = 5;
    final int kKeySize = 2000;
    final int kVirtualNumber = 100;
    Funnel<String> funnel = new Funnel<String>() {
        @Override
        public void funnel(String from, PrimitiveSink into) {
            into.putBytes(from.getBytes(StandardCharsets.UTF_8));
        }
    };

    public List<String> generateNodes(int nodeSize) {
        List<String> nodes = new ArrayList<>();
        for (int i = 0; i < nodeSize; i++) {
            nodes.add(String.format("Host%d", i));
        }
        return nodes;
    }

    public List<String> generateKeys(int keySize) {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < keySize; i++) {
            keys.add(String.format("Key%02d", i));
        }
        return keys;
    }

    public void testWithHashRing(HashRing<String, String> hashRing, List<String> nodes) {
        List<String> keys = generateKeys(kKeySize);
        System.out.println("======================================");
        System.out.printf("Test Policy: %s. # of Keys = %d, # of Hosts = %d\n", hashRing.policy(),
                keys.size(), nodes.size());

        Map<String, String> assign = new HashMap<>();
        Map<String, Integer> hostLoad = new HashMap<>();
        for (String key : keys) {
            List<String> hosts = hashRing.get(key, 1);
            String host = hosts.get(0);
            assign.put(key, host);
            hostLoad.put(host, hostLoad.getOrDefault(host, 0) + 1);
        }

        List<Integer> loads = new ArrayList<>(hostLoad.values());
        float avg = keys.size() * 1.0f / nodes.size();
        Collections.sort(loads);
        double t = 0;
        for (Integer x : loads) {
            t += (x - avg) * (x - avg);
        }
        t = Math.sqrt(t / loads.size());

        System.out.printf("Load: min = %d, max = %d, median = %d, stddev = %.2f\n", loads.get(0),
                loads.get(loads.size() - 1), loads.get(loads.size() / 2), t);

        // test to remove 0
        {
            hashRing.removeNode("Host0");
            int changed = 0;
            for (String key : keys) {
                List<String> hosts = hashRing.get(key, 1);
                String host = hosts.get(0);
                String before = assign.get(key);
                if (!host.equals(before)) {
                    changed += 1;
                }
            }
            float moveRatio = changed * 1.0f / keys.size();
            float perfectRatio = 1.0f / nodes.size();
            System.out.printf("Remove Host0: move ratio = %.2f, perfect ratio = %.2f\n", moveRatio, perfectRatio);
        }
        // test to add 100
        {
            hashRing.addNode("Host100");
            int changed = 0;
            for (String key : keys) {
                List<String> hosts = hashRing.get(key, 1);
                String host = hosts.get(0);
                String before = assign.get(key);
                if (!host.equals(before)) {
                    changed += 1;
                }
            }
            float moveRatio = changed * 1.0f / keys.size();
            float perfectRatio = 1.0f / nodes.size();
            System.out.printf("Add Host100: move ratio = %.2f, perfect ratio = %.2f\n", moveRatio, perfectRatio);
        }
    }

    @Test
    public void testConsistentHashRing() {
        List<String> nodes = generateNodes(kNodeSize);
        ConsistentHashRing hashRing =
                new ConsistentHashRing(Hashing.murmur3_128(), funnel, funnel, nodes, kVirtualNumber);
        testWithHashRing(hashRing, nodes);
    }

    @Test
    public void testRendezvousHashRing() {
        List<String> nodes = generateNodes(kNodeSize);
        RendezvousHashRing hashRing = new RendezvousHashRing<>(Hashing.murmur3_128(), funnel, funnel, nodes);
        testWithHashRing(hashRing, nodes);
    }

    @Test
    public void testPlainHashRing() {
        List<String> nodes = generateNodes(kNodeSize);
        PlainHashRing hashRing = new PlainHashRing(Hashing.murmur3_128(), funnel, nodes);
        testWithHashRing(hashRing, nodes);
    }
}
