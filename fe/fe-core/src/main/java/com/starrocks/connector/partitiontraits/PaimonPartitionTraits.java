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

import com.starrocks.catalog.PaimonPartitionKey;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PaimonPartitionTraits extends DefaultTraits {

    @Override
    public String getDbName() {
        return ((PaimonTable) table).getDbName();
    }

    @Override
    public boolean isSupportPCTRefresh() {
        return true;
    }

    @Override
    public PartitionKey createEmptyKey() {
        return new PaimonPartitionKey();
    }

    @Override
    public List<PartitionInfo> getPartitions(List<String> partitionNames) {
        PaimonTable paimonTable = (PaimonTable) table;
        return GlobalStateMgr.getCurrentState().getMetadataMgr().
                getPartitions(paimonTable.getCatalogName(), table, partitionNames);
    }

    @Override
    public Optional<Long> maxPartitionRefreshTs() {
        Map<String, PartitionInfo> partitionNameWithPartition =
                getPartitionNameWithPartitionInfo();
        return partitionNameWithPartition.values().stream()
                .map(com.starrocks.connector.PartitionInfo::getModifiedTime)
                .max(Long::compareTo);
    }
}