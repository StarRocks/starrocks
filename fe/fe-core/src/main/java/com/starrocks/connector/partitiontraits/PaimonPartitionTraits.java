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
import com.starrocks.catalog.Table;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.server.GlobalStateMgr;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PaimonPartitionTraits extends DefaultTraits {
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

    @Override
    public LocalDateTime getTableLastUpdateTime(int extraSeconds) {
        PaimonTable paimonTable = (PaimonTable) table;
        long lastModifiedTime = queryLastModifiedTime(paimonTable.getCatalogName(), paimonTable.getCatalogDBName(),
                paimonTable.getCatalogTableName(), paimonTable);
        if (lastModifiedTime != 0L) {
            // paimon lastModifiedTime is milli timestamp
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(lastModifiedTime).plusSeconds(extraSeconds),
                    Clock.systemDefaultZone().getZone());
        }
        return null;
    }

    private static long queryLastModifiedTime(String catalogName, String dbName, String tableName,
                                              Table table) {
        List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                catalogName, dbName, tableName, null);
        List<PartitionInfo> partitions = GlobalStateMgr.getCurrentState().getMetadataMgr().
                getPartitions(catalogName, table, partitionNames);
        return partitions.stream().map(PartitionInfo::getModifiedTime).max(Long::compareTo).
                orElse(0L);
    }
}