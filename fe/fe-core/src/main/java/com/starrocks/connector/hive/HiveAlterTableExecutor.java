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

package com.starrocks.connector.hive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.HiveTable;
import com.starrocks.common.Version;
import com.starrocks.connector.ConnectorAlterTableExecutor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.connector.hive.HiveMetadata.STARROCKS_QUERY_ID;

public class HiveAlterTableExecutor extends ConnectorAlterTableExecutor {
    private HiveTable table;
    private final HiveMetastoreOperations hmsOps;

    public HiveAlterTableExecutor(AlterTableStmt stmt,
                                  HiveTable hiveTable,
                                  ConnectContext context,
                                  HiveMetastoreOperations hmsOps) {
        super(stmt, context);
        this.table = hiveTable;
        this.hmsOps = hmsOps;
    }

    @Override
    public Void visitAddPartitionClause(AddPartitionClause clause, ConnectContext context) {
        actions.add(() -> {
            addPartition(clause);
        });
        return null;
    }

    private void addPartition(AlterClause alterClause) {
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterClause;
        List<String> partitionColumns = table.getPartitionColumnNames();
        // now do not support to specify location of hive partition in add partition
        if (!(addPartitionClause.getPartitionDesc() instanceof SingleItemListPartitionDesc)) {
            return;
        }
        SingleItemListPartitionDesc partitionDesc = (SingleItemListPartitionDesc) addPartitionClause.getPartitionDesc();
        String tablePath = table.getTableLocation();
        String partitionString = partitionColumns.get(0) + "=" + partitionDesc.getValues().get(0);
        String partitionPath = tablePath + "/" + partitionString;
        HivePartition hivePartition = HivePartition.builder()
                .setDatabaseName(table.getDbName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumnNames().stream()
                        .map(table::getColumn)
                        .collect(Collectors.toList()))
                .setValues(partitionDesc.getValues())
                .setParameters(ImmutableMap.<String, String>builder()
                        .put("starrocks_version", Version.STARROCKS_VERSION + "-" + Version.STARROCKS_COMMIT_HASH)
                        .put(STARROCKS_QUERY_ID, ConnectContext.get().getQueryId().toString())
                        .buildOrThrow())
                .setStorageFormat(table.getStorageFormat())
                .setLocation(partitionPath)
                .build();
        HivePartitionWithStats partitionWithStats =
                new HivePartitionWithStats(partitionString, hivePartition, HivePartitionStats.empty());
        hmsOps.addPartitions(table.getDbName(), table.getTableName(), Lists.newArrayList(partitionWithStats));
    }
}
