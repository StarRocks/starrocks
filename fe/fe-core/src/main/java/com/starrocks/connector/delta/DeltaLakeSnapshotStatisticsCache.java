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

package com.starrocks.connector.delta;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.math.LongMath;
import com.starrocks.catalog.DeltaLakeTable;
import io.delta.kernel.internal.SnapshotImpl;

import java.util.concurrent.TimeUnit;

// Catalog-scoped, bounded summaries only. Never retain query operators, file tasks, or partial scans.
public class DeltaLakeSnapshotStatisticsCache {
    private record Key(String location, String tableId, long version) { }

    private final Cache<Key, Long> rowCounts = CacheBuilder.newBuilder()
            .maximumSize(1000).expireAfterAccess(1, TimeUnit.HOURS).build();

    private static Key key(DeltaLakeTable table) {
        return new Key(table.getTableLocation(), table.getDeltaMetadata().getId(),
                ((SnapshotImpl) table.getDeltaSnapshot()).getVersion());
    }

    public Long getRowCount(DeltaLakeTable table) {
        return rowCounts.getIfPresent(key(table));
    }

    public Collector newCollector(DeltaLakeTable table) {
        return new Collector(key(table));
    }

    public void invalidateAll() {
        rowCounts.invalidateAll();
    }

    public final class Collector {
        private final Key key;
        private long rows;
        private boolean valid = true;

        private Collector(Key key) {
            this.key = key;
        }

        public void add(long records) {
            valid &= records >= 0;
            rows = LongMath.saturatedAdd(rows, Math.max(0, records));
        }

        public void abort() {
            valid = false;
        }

        // Called only after the unfiltered iterator reports EOF. close/cancel must never publish.
        public void complete() {
            if (valid) {
                rowCounts.put(key, rows);
            }
        }
    }
}
