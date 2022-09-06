// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.util;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PlainHashRing<K, N> implements HashRing<K, N> {
    HashFunction hashFunction;
    Funnel<K> keyFunnel;
    List<N> nodes = new ArrayList<>();

    @Override
    public String policy() {
        return "PlainHash";
    }

    public PlainHashRing(HashFunction hashFunction, Funnel<K> keyFunnel,
                         Collection<N> nodes) {
        this.hashFunction = hashFunction;
        this.keyFunnel = keyFunnel;
        for (N node : nodes) {
            addNode(node);
        }
    }

    @Override
    public void addNode(N node) {
        nodes.add(node);
    }

    @Override
    public void removeNode(N node) {
        nodes.remove(node);
    }

    @Override
    public List<N> get(K key, int distinctNumber) {
        List<N> ans = new ArrayList<>();
        Hasher hasher = hashFunction.newHasher();
        long hash = hasher.putObject(key, keyFunnel).hash().asLong();
        if (hash == Long.MIN_VALUE) {
            hash += 1;
        }
        int index = (int) (Math.abs(hash) % nodes.size());
        distinctNumber = Math.min(distinctNumber, nodes.size());
        for (int i = 0; i < distinctNumber; i++) {
            index = (index + i) % nodes.size();
            ans.add(nodes.get(index));
        }
        return ans;
    }
}