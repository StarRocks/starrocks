// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.util;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

public class StaticPartitionedHashRing<K, N extends Comparable<N>> implements HashRing<K, N> {
    HashFunction hashFunction;

    TreeMap<Long, N> hashRing = new TreeMap<>();
    Funnel<K> keyFunnel;
    List<Long> staticHashValues = new ArrayList<>();
    TreeSet<N> nodes;

    @Override
    public String policy() {
        return "StaticPartitionedHash";
    }

    public StaticPartitionedHashRing(HashFunction hashFunction, Funnel<K> keyFunnel,
                                     Collection<N> nodes,
                                     int partitionedSize) {
        this.hashFunction = hashFunction;
        this.keyFunnel = keyFunnel;
        this.nodes = new TreeSet<>(nodes);
        initStaticHashValues(partitionedSize);
        for (N node : nodes) {
            this.nodes.add(node);
        }
        rehash();
    }

    @Override
    public void addNode(N node) {
        nodes.add(node);
        rehash();
    }

    @Override
    public void removeNode(N node) {
        nodes.remove(node);
        rehash();
    }

    private void initStaticHashValues(int size) {
        long delta = (Long.MAX_VALUE / size) * 2;
        long now = Long.MIN_VALUE;
        for (int i = 0; i < size; i++) {
            staticHashValues.add(now);
            now += delta;
        }
    }

    private void rehash() {
        hashRing.clear();
        Iterator<N> iterator = nodes.iterator();
        for (long value : staticHashValues) {
            if (!iterator.hasNext()) {
                iterator = nodes.iterator();
            }
            N node = iterator.next();
            hashRing.put(value, node);
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, N> entry : hashRing.entrySet()) {
            sb.append(String.format("%d -> %s\n", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }

    private void collectNodes(SortedMap<Long, N> map, List<N> ans, int distinctNumber) {
        Set<N> visited = new HashSet<>(ans);
        for (Map.Entry<Long, N> entry : map.entrySet()) {
            N node = entry.getValue();
            if (visited.contains(node)) {
                continue;
            }
            visited.add(node);
            ans.add(node);
            if (ans.size() == distinctNumber) {
                break;
            }
        }
    }

    @Override
    public List<N> get(K key, int distinctNumber) {
        List<N> ans = new ArrayList<>();
        Hasher hasher = hashFunction.newHasher();
        long hash = hasher.putObject(key, keyFunnel).hash().asLong();

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