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
package com.starrocks.sql.optimizer.statistics;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;

import java.util.Optional;

public interface IMinMaxStatsMgr {
    record ColumnMinMax(String minValue, String maxValue) {}

    Optional<ColumnMinMax> getStats(ColumnIdentifier identifier, StatsVersion version);

    Optional<ColumnMinMax> getStatsSync(ColumnIdentifier identifier, StatsVersion version);

    void removeStats(ColumnIdentifier identifier);

    /**
     * Drop every cached min/max entry of {@code table}.
     *
     * <p>A cached entry is validated against the table-level {@code max(visibleVersionTime)} under
     * a "cached is at least as new as requested" test, which assumes that stamp only ever grows.
     * It does not: partition-set changes move whole {@link com.starrocks.catalog.Partition}
     * objects -- each carrying its own visible-version time -- in and out of the formal list, and a
     * schema change need not touch it at all. So it is the callers of this method, not the version
     * check, that keep the cache honest across DDL. See {@code OlapTable#invalidateMinMaxStats} for
     * where it is hooked and why those points cover the follower replay path too.
     */
    static void invalidateTable(OlapTable table) {
        IMinMaxStatsMgr mgr = internalInstance();
        for (Column column : table.getColumns()) {
            mgr.removeStats(new ColumnIdentifier(table.getId(), column.getColumnId()));
        }
    }

    static IMinMaxStatsMgr internalInstance() {
        return ColumnMinMaxMgr.getInstance();
    }

    static IMinMaxStatsMgr icebergInstance() {
        return IcebergColumnMinMaxMgr.getInstance();
    }
}
