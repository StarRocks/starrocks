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

import com.starrocks.catalog.ADBCPartitionKey;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.connector.PartitionInfo;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

// TODO: implement ADBC partition support when drivers expose partitioning metadata.
//       Reference: JDBCPartitionTraits.java for the JDBC partition implementation.
public class ADBCPartitionTraits extends DefaultTraits {
    @Override
    public String getTableName() {
        return table.getCatalogTableName();
    }

    @Override
    public boolean isSupportPCTRefresh() {
        return false;  // Full refresh only
    }

    @Override
    public List<PartitionInfo> getPartitions(List<String> partitionNames) {
        // Unpartitioned tables still have one synthetic partition. ADBC does not
        // expose a change token, so its version and modified time remain unknown.
        return Collections.nCopies(partitionNames.size(), () -> -1L);
    }

    @Override
    public Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                MaterializedView.AsyncRefreshContext context) {
        // Unknown freshness requires a full refresh in checked MV rewrite mode.
        return null;
    }

    @Override
    public Set<String> getUpdatedPartitionNames(LocalDateTime checkTime, int extraSeconds) {
        return null;
    }

    @Override
    public LocalDateTime getTableLastUpdateTime(int extraSeconds) {
        return null;
    }

    @Override
    public Optional<Long> maxPartitionRefreshTs() {
        return Optional.empty();
    }

    @Override
    public PartitionKey createEmptyKey() {
        return new ADBCPartitionKey();
    }
}
