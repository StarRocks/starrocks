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

package org.apache.iceberg;

import com.starrocks.connector.ConnectorMetadataCache;
import com.starrocks.connector.ConnectorMetadataCache.Kind;
import com.starrocks.connector.iceberg.CachingIcebergCatalog.IcebergTableName;

import java.util.List;
import java.util.Set;
import java.util.function.ToLongFunction;

// Built DeleteFileIndex objects shared across scans, so that a table with many position deletes does not
// re-sort the same delete files on every query. Only filter-free indexes of position deletes and DVs are
// stored (see StarRocksIcebergTableScan#reuseCachedDeleteFileIndex); their delete files are the objects
// held by the delete file cache, so an entry mostly adds the index arrays and maps on top of them.
// One catalog's view (scope) of the Iceberg metadata cache all catalogs share.
public final class DeleteFileIndexCache {
    // Delete manifests are immutable, so the same manifest set always yields the same index. The scan schema
    // and the table's spec ids are part of the key because the index binds partitions through the specs.
    record Key(IcebergTableName table, int schemaId, Set<Integer> specIds, Set<String> deleteManifestPaths) {
    }

    // An entry weighs what it keeps alive once the delete file cache has evicted its delete files.
    private record Entry(DeleteFileIndex index, long weight) {
    }

    static {
        ConnectorMetadataCache.registerWeigher(Kind.DELETE_FILE_INDEX,
                (key, entry) -> ((Entry) entry).weight());
    }

    private final ConnectorMetadataCache.Store store;
    private final String scope;
    private final ToLongFunction<Set<DeleteFile>> weigher;

    // weigher estimates the memory of one delete manifest's cached files; an entry weighs the sum over its
    // manifests.
    public DeleteFileIndexCache(ConnectorMetadataCache.Store store, String scope,
                                ToLongFunction<Set<DeleteFile>> weigher) {
        this.store = store;
        this.scope = scope;
        this.weigher = weigher;
    }

    DeleteFileIndex get(Key key) {
        Entry entry = (Entry) store.getIfPresent(scope, Kind.DELETE_FILE_INDEX, key);
        return entry == null ? null : entry.index();
    }

    void put(Key key, DeleteFileIndex index, List<Set<DeleteFile>> deleteFiles) {
        long weight = 0;
        for (Set<DeleteFile> files : deleteFiles) {
            weight += weigher.applyAsLong(files);
        }
        store.put(scope, Kind.DELETE_FILE_INDEX, key, new Entry(index, weight));
    }

    public void invalidate(IcebergTableName table) {
        store.invalidateIf(scope, Kind.DELETE_FILE_INDEX, key -> isKeyOf(key, table));
    }

    // Whether a key of this cache belongs to the table.
    public static boolean isKeyOf(Object key, IcebergTableName table) {
        return ((Key) key).table().equals(table);
    }

    public long estimatedSize() {
        return store.entries(scope, Kind.DELETE_FILE_INDEX).size();
    }
}
