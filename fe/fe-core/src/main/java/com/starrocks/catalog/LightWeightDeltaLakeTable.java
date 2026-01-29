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
// 

package com.starrocks.catalog;

import com.starrocks.catalog.DeltaLakeTable;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Metadata;

/**
 * Lightweight Iceberg table now is used inside mv plan cache to avoid hold large memory usage.
 *
 * When we serialize logical plans of materialized views into the MV plan cache, keeping the full
 * snapshots would retain large snapshot metadata,
 * quickly blowing up cache size and heap. 
 * 
 * LightWeightDeltaLakeTable drops the snapshots and keeps only
 * lightweight identifiers. It can be used to compare whether two tables are the same,
 * but no more metadata related operations are supported.
 */

public class LightWeightDeltaLakeTable extends DeltaLakeTable {

    //used for comparasion
    private String tableIdentifier;

    public LightWeightDeltaLakeTable(DeltaLakeTable table) {
        super(table.getId(), table.getCatalogName(), table.getCatalogDBName(), table.getCatalogTableName(),
                table.getBaseSchema(), table.getPartitionColumnNames(), null, null, table.getMetastoreTable());
        this.tableIdentifier = table.getTableIdentifier();
    }

    @Override
    public Snapshot getDeltaSnapshot() {
        throw new UnsupportedOperationException("LightWeightDeltaLakeTable does not support getDeltaSnapshot");
    }

    @Override
    public Engine getDeltaEngine() {
        throw new UnsupportedOperationException("LightWeightDeltaLakeTable does not support getDeltaEngine");
    }

    @Override
    public Metadata getDeltaMetadata() {
        throw new UnsupportedOperationException("LightWeightDeltaLakeTable does not support getDeltaMetadata");
    }

    @Override
    public String getTableIdentifier() {
        return tableIdentifier;
    }
}