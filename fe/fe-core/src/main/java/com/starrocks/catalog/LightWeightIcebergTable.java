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

import com.starrocks.catalog.IcebergTable;

import java.util.List;

/**
 * Lightweight Iceberg table now is used inside mv plan cache to avoid hold large memory usage.
 *
 * When we serialize logical plans of materialized views into the MV plan cache, keeping the full
 * org.apache.iceberg.Table would retain large snapshot metadata,
 * quickly blowing up cache size and heap. 
 * 
 * LightIcebergTable drops the nativeTable and keeps only
 * lightweight identifiers. It can be used to compare whether two Iceberg tables are the same,
 * but no more metadata related operations are supported.
 */

public class LightWeightIcebergTable extends IcebergTable {

    //used for comparasion
    private String tableIdentifier;

    public LightWeightIcebergTable(IcebergTable table) {
        super(table.getId(), table.getName(), table.getCatalogName(), table.getResourceName(),
                table.getCatalogDBName(), table.getCatalogTableName(), table.getComment(),
                table.getBaseSchema(), null, table.getIcebergProperties());
        this.tableIdentifier = table.getTableIdentifier();
    }

    @Override
    public org.apache.iceberg.Table getNativeTable() {
        throw new UnsupportedOperationException("LightWeightIcebergTable does not support getNativeTable");
    }

    @Override
    public String getTableIdentifier() {
        return tableIdentifier;
    }

    @Override
    public List<Column> getPartitionColumns() {
        throw new UnsupportedOperationException("LightWeightIcebergTable does not support getPartitionColumns");
    }

    @Override
    public String getUUID() {
        throw new UnsupportedOperationException("LightWeightIcebergTable does not support getUUID");
    }

    @Override
    public boolean isUnPartitioned() {
        throw new UnsupportedOperationException("LightWeightIcebergTable does not support isUnPartitioned");
    }
}