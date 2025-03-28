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

import com.google.common.collect.Range;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCPartitionKey;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class JDBCPartitionTraits extends DefaultTraits {
    @Override
    public String getTableName() {
        return table.getCatalogTableName();
    }

    @Override
    public boolean isSupportPCTRefresh() {
        return true;
    }

    @Override
    public List<PartitionInfo> getPartitions(List<String> partitionNames) {
        JDBCTable jdbcTable = (JDBCTable) table;
        return GlobalStateMgr.getCurrentState().getMetadataMgr().
                getPartitions(jdbcTable.getCatalogName(), table, partitionNames);
    }

    @Override
    public Map<String, Range<PartitionKey>> getPartitionKeyRange(Column partitionColumn, Expr partitionExpr)
            throws AnalysisException {
        return PartitionUtil.getRangePartitionMapOfJDBCTable(
                table, partitionColumn, getPartitionNames(), partitionExpr);
    }

    @Override
    public PartitionKey createEmptyKey() {
        return new JDBCPartitionKey();
    }

    @Override
    public Optional<Long> maxPartitionRefreshTs() {
        Map<String, com.starrocks.connector.PartitionInfo> partitionNameWithPartition =
                getPartitionNameWithPartitionInfo();
        return partitionNameWithPartition.values().stream()
                .map(com.starrocks.connector.PartitionInfo::getModifiedTime)
                .max(Long::compareTo);
    }

    @Override
    public Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                MaterializedView.AsyncRefreshContext context) {

        try {
            return super.getUpdatedPartitionNames(baseTables, context);
        } catch (Exception e) {
            // some external table traits do not support getPartitionNameWithPartitionInfo, will throw exception,
            // just return null
            return null;
        }
    }
}

