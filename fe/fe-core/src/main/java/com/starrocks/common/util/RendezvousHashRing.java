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
import java.util.Collections;
import java.util.List;

public class RendezvousHashRing<K, N> implements HashRing<K, N> {
    HashFunction hashFunction;
    Funnel<K> keyFunnel;
    Funnel<N> nodeFunnel;

    class VNode {
        N node;
        float weight;

        VNode(N node) {
            this.node = node;
            this.weight = 1.0f;
        }
    }

    List<VNode> vNodes = new ArrayList<>();

    @Override
    public String policy() {
        return "RendezvousHash";
    }

    public RendezvousHashRing(HashFunction hashFunction, Funnel<K> keyFunnel, Funnel<N> nodeFunnel,
                              Collection<N> nodes) {
        this.hashFunction = hashFunction;
        this.keyFunnel = keyFunnel;
        this.nodeFunnel = nodeFunnel;
        for (N node : nodes) {
            addNode(node);
        }
    }

    @Override
    public void addNode(N node) {
        VNode vnode = new VNode(node);
        vNodes.add(vnode);
    }

    @Override
    public void removeNode(N node) {
        for (int i = 0; i < vNodes.size(); i++) {
            if (vNodes.get(i).node.equals(node)) {
                vNodes.remove(i);
                return;
            }
        }
    }

    @Override
    public List<N> get(K key, int distinctNumber) {
        class Affinity implements Comparable<Affinity> {
            float weight;
            N node;

            @Override
            public int compareTo(Affinity x) {
                if (weight < x.weight) {
                    return 1;
                } else if (weight > x.weight) {
                    return -1;
                }
                return 0;
            }
        }
        List<Affinity> affinities = new ArrayList<>();
        for (int i = 0; i < vNodes.size(); i++) {
            Hasher hasher = hashFunction.newHasher();
            hasher.putObject(key, keyFunnel);
            hasher.putObject(vNodes.get(i).node, nodeFunnel);
            long hash = hasher.hash().asLong();
            if (hash == Long.MIN_VALUE) {
                hash += 1;
            }
            float w = vNodes.get(i).weight * (65.0f - (float) Math.log1p(Math.abs(hash)));
            Affinity aff = new Affinity();
            aff.weight = w;
            aff.node = vNodes.get(i).node;
            affinities.add(aff);
        }
        Collections.sort(affinities);

        List<N> ans = new ArrayList<>();
        for (int i = 0; i < Math.min(distinctNumber, affinities.size()); i++) {
            ans.add(affinities.get(i).node);
        }
        return ans;
    }
}
