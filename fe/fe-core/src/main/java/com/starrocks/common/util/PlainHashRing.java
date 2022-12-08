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