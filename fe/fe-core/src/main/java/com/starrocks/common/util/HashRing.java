package com.starrocks.common.util;

import java.util.List;

public interface HashRing<K, N> {
    void addNode(N node);

    void removeNode(N node);

    List<N> get(K key, int distinctNumber);

    String policy();
}
