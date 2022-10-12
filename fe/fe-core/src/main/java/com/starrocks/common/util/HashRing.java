// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common.util;

import java.util.List;

public interface HashRing<K, N> {
    void addNode(N node);

    void removeNode(N node);

    // To get `distinctNumber` distinct nodes with high affinity with `key`
    List<N> get(K key, int distinctNumber);

    String policy();
}
