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

package com.starrocks.connector.partitiontraits;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * External base tables' partition infos ({@link DefaultTraits#getPartitionNameWithPartitionInfo()}), fetched
 * before a caller takes an FE metadata lock and handed to the same call made while the lock is held.
 *
 * <p><b>Why only the fetch moves.</b> {@link DefaultTraits#getUpdatedPartitionNames} fetches the connector's
 * partitions and then compares them against the MV's refresh state. The comparison reads state the lock
 * protects and is cheap, so it stays under the lock and always sees the current MV state. The fetch depends
 * only on the table and the snapshot it reads, never on MV state, and the lock never covered the external
 * table. A result fetched just before the lock is therefore the same as a connector that changed right after
 * the read, so no "still current" check is needed.
 *
 * <p><b>A pure cache.</b> An entry is keyed by the table, the snapshot the traits read
 * ({@link DefaultTraits#partitionInfoSnapshot()}) and the query-rewrite flag. A lookup that misses, including
 * any lookup outside an open scope, fetches from the connector exactly as before. So correctness never depends
 * on what was prefetched, only on the key covering every input of the fetch. A failed prefetch records nothing,
 * and the call under the lock then fails or answers the way it always has.
 *
 * <p>The scope is confined to the thread that opened it and closed with try-with-resources.
 */
public final class PrefetchedPartitionInfos implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(PrefetchedPartitionInfos.class);
    private static final ThreadLocal<PrefetchedPartitionInfos> CURRENT = new ThreadLocal<>();

    private record Key(Table table, Object snapshot, boolean queryMVRewrite) {
    }

    private final PrefetchedPartitionInfos outer;
    private final Map<Key, Map<String, PartitionInfo>> byKey = Maps.newHashMap();

    private PrefetchedPartitionInfos(PrefetchedPartitionInfos outer) {
        this.outer = outer;
    }

    /**
     * Open a scope on the current thread. Lookups made on this thread until {@link #close()} consult it.
     */
    public static PrefetchedPartitionInfos open() {
        PrefetchedPartitionInfos scope = new PrefetchedPartitionInfos(CURRENT.get());
        CURRENT.set(scope);
        return scope;
    }

    /**
     * Fetch the partition infos a refresh-time call for {@code table} would fetch, and keep them for this
     * scope. Nothing is fetched when no call would: when the traits detect updates without partition infos
     * and the MV tracks no partition version of the table (the dropped-partition check is then a no-op too).
     *
     * @param pinnedVersionRange       the snapshot the calls under the lock will read, null for the live one
     * @param tracksPartitionVersions  whether the MV holds partition versions for this table
     * @return whether an entry for the table is now held
     */
    public boolean prefetch(Table table, TvrVersionRange pinnedVersionRange, boolean tracksPartitionVersions) {
        try {
            if (!(ConnectorPartitionTraits.buildWithoutCache(table) instanceof DefaultTraits traits)) {
                return false;
            }
            if (!traits.readsPartitionInfoToDetectUpdates() && !tracksPartitionVersions) {
                return false;
            }
            if (pinnedVersionRange != null) {
                traits.setPinnedVersionRange(pinnedVersionRange);
            }
            Key key = keyOf(traits);
            if (!byKey.containsKey(key)) {
                byKey.put(key, traits.fetchPartitionNameWithPartitionInfo());
            }
            return true;
        } catch (Exception e) {
            // Leave it to the call under the lock, which then fails or answers exactly as before.
            LOG.debug("Failed to prefetch partition infos of table {}, fetching them on demand: {}",
                    table.getName(), e.getMessage());
            return false;
        }
    }

    /**
     * @return a copy of the partition infos prefetched for {@code traits}, or null when there is no open scope
     *         or it holds nothing for this key
     */
    static Map<String, PartitionInfo> lookup(DefaultTraits traits) {
        PrefetchedPartitionInfos scope = CURRENT.get();
        if (scope == null || scope.byKey.isEmpty()) {
            return null;
        }
        Map<String, PartitionInfo> prefetched = scope.byKey.get(keyOf(traits));
        return prefetched == null ? null : Maps.newHashMap(prefetched);
    }

    private static Key keyOf(DefaultTraits traits) {
        return new Key(traits.getTable(), traits.partitionInfoSnapshot(), traits.isQueryMVRewrite());
    }

    @Override
    public void close() {
        if (CURRENT.get() == this) {
            if (outer == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(outer);
            }
        }
    }
}
