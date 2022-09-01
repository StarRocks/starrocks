// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common;

import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.starrocks.common.util.ConsistentHashRing;
import com.starrocks.common.util.HashRing;
import com.starrocks.common.util.RendezvousHashRing;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashRingTest {
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
        System.out.printf("test policy: %s\n", hashRing.policy());
        List<String> keys = generateKeys(2000);
        Map<String, String> assign = new HashMap<>();
        for (String key : keys) {
            List<String> hosts = hashRing.get(key, 1);
            assign.put(key, hosts.get(0));
        }

        // test to remove 0
        {
            System.out.printf("# of Keys = %d, # of Hosts = %d, remove Host0\n", keys.size(), nodes.size());
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
            System.out.printf("Remove Host0: move ratio = %.2f\n", moveRatio);
        }
        // test to add 100
        {
            System.out.printf("# of Keys = %d, # of Hosts = %d, Add Host100\n", keys.size(), nodes.size());
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
            System.out.printf("Add Host100: move ratio = %.2f\n", moveRatio);
        }
    }

    @Test
    public void testConsistentHashRing() {
        List<String> nodes = generateNodes(10);
        ConsistentHashRing hashRing = new ConsistentHashRing(Hashing.murmur3_128(), funnel, funnel, nodes, 10);
        testWithHashRing(hashRing, nodes);
    }

    @Test
    public void testRendezvousHashRing() {
        List<String> nodes = generateNodes(10);
        RendezvousHashRing hashRing = new RendezvousHashRing<>(Hashing.murmur3_128(), funnel, funnel, nodes);
        testWithHashRing(hashRing, nodes);
    }
}
