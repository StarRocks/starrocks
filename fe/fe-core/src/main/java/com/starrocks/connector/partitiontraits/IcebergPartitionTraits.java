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

import com.google.common.collect.Lists;
import com.starrocks.catalog.IcebergPartitionKey;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.server.GlobalStateMgr;
import org.apache.iceberg.Snapshot;

import java.util.List;
import java.util.Optional;

public class IcebergPartitionTraits extends DefaultTraits {

    @Override
    public boolean supportPartitionRefresh() {
        // TODO: refine the check
        return true;
    }

    @Override
    public String getDbName() {
        return ((IcebergTable) table).getRemoteDbName();
    }

    @Override
    public boolean isSupportPCTRefresh() {
        return true;
    }

    @Override
    public String getTableName() {
        return ((IcebergTable) table).getRemoteTableName();
    }

    @Override
    public PartitionKey createEmptyKey() {
        return new IcebergPartitionKey();
    }

    @Override
    public List<PartitionInfo> getPartitions(List<String> partitionNames) {
        IcebergTable icebergTable = (IcebergTable) table;
        return GlobalStateMgr.getCurrentState().getMetadataMgr().
                getPartitions(icebergTable.getCatalogName(), table, partitionNames);
    }

    @Override
    public Optional<Long> maxPartitionRefreshTs() {
        IcebergTable icebergTable = (IcebergTable) table;
        return icebergTable.getSnapshot().map(Snapshot::timestampMillis);
    }

    @Override
    public List<String> getPartitionNames() {
        if (table.isUnPartitioned()) {
            return Lists.newArrayList(table.getName());
        }

        return GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                table.getCatalogName(), getDbName(), getTableName());
    }
}

