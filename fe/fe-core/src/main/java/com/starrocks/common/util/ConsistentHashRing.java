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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashRing<K, N> implements HashRing<K, N> {
    HashFunction hashFunction;

    TreeMap<Long, VNode> hashRing = new TreeMap<>();
    // Physical nodes currently on the ring. A node is spread across `virtualNumber`
    // virtual nodes, so the ring can never hand back more distinct nodes than this set
    // holds; `get` uses its size to stop walking instead of scanning for nodes that do
    // not exist.
    Set<N> physicalNodes = new HashSet<>();
    Funnel<K> keyFunnel;
    Funnel<N> nodeFunnel;
    int virtualNumber;
    long vNodeHashDelta;
    final boolean vNodeEvenlyDistributed = false;

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
        physicalNodes.add(node);
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
        physicalNodes.remove(node);
        for (int i = 0; i < virtualNumber; i++) {
            VNode vnode = new VNode(node, i);
            long hash = vnode.hashValue();
            if (hashRing.containsKey(hash)) {
                hashRing.remove(hash);
            }
        }
    }

    private void collectNodes(SortedMap<Long, VNode> map, List<N> ans, Set<N> visited, int target) {
        for (Map.Entry<Long, VNode> entry : map.entrySet()) {
            VNode vnode = entry.getValue();
            if (!visited.add(vnode.node)) {
                continue;
            }
            ans.add(vnode.node);
            if (ans.size() == target) {
                return;
            }
        }
    }

    @Override
    public List<N> get(K key, int distinctNumber) {
        List<N> ans = new ArrayList<>();
        // Never ask for more distinct nodes than the ring actually has: with a larger
        // target the walk can never reach it and scans every virtual node in vain.
        int target = Math.min(distinctNumber, physicalNodes.size());
        if (target <= 0) {
            return ans;
        }

        Hasher hasher = hashFunction.newHasher();
        long hash = hasher.putObject(key, keyFunnel).hash().asLong();

        // Walk the ring clockwise once, wrapping around: [hash, end] then [begin, hash).
        // `visited` is shared so the wrap-around segment does not re-scan the tail and a
        // node selected in the first segment is not added twice.
        Set<N> visited = new HashSet<>();
        collectNodes(hashRing.tailMap(hash, true), ans, visited, target);
        if (ans.size() < target) {
            collectNodes(hashRing.headMap(hash, false), ans, visited, target);
        }
        return ans;
    }
}