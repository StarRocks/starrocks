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


package com.starrocks.common.util;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashRing<K, N> implements HashRing<K, N> {
    // Lookups with at most this many distinct nodes walk a sorted array snapshot of the ring instead of the TreeMap.
    private static final int MAX_ARRAY_LOOKUP_DISTINCT_NUMBER = 8;

    HashFunction hashFunction;

    TreeMap<Long, VNode> hashRing = new TreeMap<>();
    // distinct physical nodes on the ring, to stop a lookup once all of them are found
    Set<N> nodes = new HashSet<>();
    Funnel<K> keyFunnel;
    Funnel<N> nodeFunnel;
    int virtualNumber;
    long vNodeHashDelta;
    final boolean vNodeEvenlyDistributed = false;
    // The ring as parallel arrays sorted by hash; rebuilt on the first lookup after the ring changes.
    private volatile Snapshot snapshot = null;

    private record Snapshot(long[] hashes, Object[] nodes) {
    }

    class VNode {
        N node;
        int index;

        VNode(N node, int index) {
            this.node = node;
            this.index = index;
        }

        long hashValue() {
            Hasher hasher = hashFunction.newHasher();
            hasher.putObject(node, nodeFunnel);
            long hash = 0;
            if (vNodeEvenlyDistributed) {
                long baseHash = hasher.hash().asLong();
                hash = baseHash + vNodeHashDelta * index;
            } else {
                hasher.putInt(index);
                hash = hasher.hash().asLong();
            }
            return hash;
        }
    }

    public int getVirtualNumber() {
        return virtualNumber;
    }

    @Override
    public String policy() {
        return "ConsistentHash";
    }

    public ConsistentHashRing(HashFunction hashFunction, Funnel<K> keyFunnel, Funnel<N> nodeFunnel,
                              Collection<N> nodes,
                              int virtualNumber) {
        this.hashFunction = hashFunction;
        this.keyFunnel = keyFunnel;
        this.nodeFunnel = nodeFunnel;
        this.virtualNumber = virtualNumber;
        vNodeHashDelta = (Long.MAX_VALUE / virtualNumber) * 2;
        for (N node : nodes) {
            addNode(node);
        }
    }

    @Override
    public void addNode(N node) {
        snapshot = null;
        nodes.add(node);
        for (int i = 0; i < virtualNumber; i++) {
            VNode vnode = new VNode(node, i);
            long hash = vnode.hashValue();
            if (!hashRing.containsKey(hash)) {
                hashRing.put(hash, vnode);
            }
        }
    }

    @Override
    public void removeNode(N node) {
        snapshot = null;
        nodes.remove(node);
        for (int i = 0; i < virtualNumber; i++) {
            VNode vnode = new VNode(node, i);
            long hash = vnode.hashValue();
            if (hashRing.containsKey(hash)) {
                hashRing.remove(hash);
            }
        }
    }

    private void collectNodes(SortedMap<Long, VNode> map, List<N> ans, int distinctNumber) {
        Set<N> visited = new HashSet<>(ans);
        for (Map.Entry<Long, VNode> entry : map.entrySet()) {
            VNode vnode = entry.getValue();
            if (visited.contains(vnode.node)) {
                continue;
            }
            visited.add(vnode.node);
            ans.add(vnode.node);
            if (ans.size() == distinctNumber) {
                break;
            }
        }
    }

    private Snapshot getSnapshot() {
        Snapshot current = snapshot;
        if (current == null) {
            long[] hashes = new long[hashRing.size()];
            Object[] ringNodes = new Object[hashRing.size()];
            int i = 0;
            for (Map.Entry<Long, VNode> entry : hashRing.entrySet()) {
                hashes[i] = entry.getKey();
                ringNodes[i] = entry.getValue().node;
                i++;
            }
            current = new Snapshot(hashes, ringNodes);
            snapshot = current;
        }
        return current;
    }

    // Walks the ring once, clockwise from the first virtual node whose hash is >= `hash`: the same order as the
    // tail-then-whole-ring walk below, without a TreeMap view and a visited set per lookup.
    @SuppressWarnings("unchecked")
    private void collectNodes(Snapshot ring, long hash, List<N> ans, int distinctNumber) {
        int size = ring.hashes().length;
        int start = Arrays.binarySearch(ring.hashes(), hash);
        if (start < 0) {
            start = -(start + 1);
        }
        for (int step = 0; step < size && ans.size() < distinctNumber; step++) {
            int index = start + step;
            if (index >= size) {
                index -= size;
            }
            N node = (N) ring.nodes()[index];
            if (!ans.contains(node)) {
                ans.add(node);
            }
        }
    }

    @Override
    public List<N> get(K key, int distinctNumber) {
        List<N> ans = new ArrayList<>();
        // asking for more nodes than the ring has would walk every virtual node twice; the result is the same
        if (distinctNumber > nodes.size()) {
            distinctNumber = nodes.size();
        }
        Hasher hasher = hashFunction.newHasher();
        long hash = hasher.putObject(key, keyFunnel).hash().asLong();

        if (distinctNumber > 0 && distinctNumber <= MAX_ARRAY_LOOKUP_DISTINCT_NUMBER) {
            collectNodes(getSnapshot(), hash, ans, distinctNumber);
            return ans;
        }

        // search from `hash` to end.
        collectNodes(hashRing.tailMap(hash, true), ans, distinctNumber);
        if (ans.size() < distinctNumber) {
            // search from begin to end.
            // so we walk this ring twice at most.
            collectNodes(hashRing, ans, distinctNumber);
        }
        return ans;
    }
}