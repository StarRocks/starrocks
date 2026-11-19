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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    public void testConsistentHashRingAskMoreNodesThanRing() {
        List<String> nodes = generateNodes(2);
        ConsistentHashRing<String, String> hashRing =
                new ConsistentHashRing<>(Hashing.murmur3_128(), funnel, funnel, nodes, kVirtualNumber);
        for (String key : generateKeys(100)) {
            List<String> hosts = hashRing.get(key, 3);
            Assertions.assertEquals(2, hosts.size());
            Assertions.assertEquals(hosts.get(0), hashRing.get(key, 1).get(0));
            Assertions.assertTrue(hosts.containsAll(nodes));
        }

        hashRing.removeNode("Host0");
        Assertions.assertEquals(List.of("Host1"), hashRing.get("Key00", 3));
        hashRing.removeNode("Host1");
        Assertions.assertTrue(hashRing.get("Key00", 3).isEmpty());
        hashRing.addNode("Host2");
        Assertions.assertEquals(List.of("Host2"), hashRing.get("Key00", 3));
    }

    @Test
    public void testConsistentHashRingArrayLookupMatchesRingWalk() {
        // Asking for all 20 nodes takes the TreeMap walk; its first k nodes are what the array lookup must return.
        List<String> nodes = generateNodes(20);
        ConsistentHashRing<String, String> hashRing =
                new ConsistentHashRing<>(Hashing.murmur3_128(), funnel, funnel, nodes, kVirtualNumber);
        List<String> keys = generateKeys(kKeySize);
        for (int round = 0; round < 3; round++) {
            int nodeCount = 20 - round;
            for (String key : keys) {
                List<String> walk = hashRing.get(key, nodeCount);
                Assertions.assertEquals(nodeCount, walk.size());
                for (int k = 1; k <= 8; k++) {
                    Assertions.assertEquals(walk.subList(0, k), hashRing.get(key, k), key + " k=" + k);
                }
            }
            // the array snapshot must follow ring changes
            hashRing.removeNode("Host" + round);
        }
    }

    @Test
    public void testConsistentHashRingArrayLookupAtVirtualNodeHashes() {
        // A key funneled as (node bytes, index) hashes exactly like that virtual node, so these keys land on ring
        // positions: the lookup starts at the node itself (inclusive), and the largest hash must wrap around.
        Funnel<Map.Entry<String, Integer>> vnodeKeyFunnel = (key, into) -> {
            into.putBytes(key.getKey().getBytes(StandardCharsets.UTF_8));
            into.putInt(key.getValue());
        };
        List<String> nodes = generateNodes(12);
        ConsistentHashRing<Map.Entry<String, Integer>, String> hashRing =
                new ConsistentHashRing<>(Hashing.murmur3_128(), vnodeKeyFunnel, funnel, nodes, kVirtualNumber);

        List<Map.Entry<String, Integer>> keys = new ArrayList<>();
        Map.Entry<String, Integer> largest = null;
        Map.Entry<String, Integer> smallest = null;
        long largestHash = Long.MIN_VALUE;
        long smallestHash = Long.MAX_VALUE;
        for (String node : nodes) {
            for (int i = 0; i < kVirtualNumber; i++) {
                Map.Entry<String, Integer> key = Map.entry(node, i);
                long hash = Hashing.murmur3_128().newHasher().putObject(key, vnodeKeyFunnel).hash().asLong();
                if (hash > largestHash) {
                    largestHash = hash;
                    largest = key;
                }
                if (hash < smallestHash) {
                    smallestHash = hash;
                    smallest = key;
                }
                if (i % 10 == 0) {
                    keys.add(key);
                }
            }
        }
        keys.add(largest);
        keys.add(smallest);

        for (Map.Entry<String, Integer> key : keys) {
            List<String> walk = hashRing.get(key, nodes.size());
            Assertions.assertEquals(key.getKey(), walk.get(0), key.toString());
            for (int k = 1; k <= 8; k++) {
                Assertions.assertEquals(walk.subList(0, k), hashRing.get(key, k), key + " k=" + k);
            }
        }
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
