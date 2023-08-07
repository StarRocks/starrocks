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

package com.starrocks.server;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.PartitionDesc;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

public class ElasticSearchTableFactory implements AbstractTableFactory {

    public static final ElasticSearchTableFactory INSTANCE = new ElasticSearchTableFactory();

    private ElasticSearchTableFactory() {

    }

    @Override
    @NotNull
    public Table createTable(LocalMetastore metastore, Database database, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();

        // create columns
        List<Column> baseSchema = stmt.getColumnDefs().stream()
                .map(ref -> new Column(ref.getName(),
                        ref.getType(),
                        ref.isKey(),
                        ref.getAggregateType(),
                        ref.isAllowNull(),
                        ref.getDefaultValueDef(),
                        "by es comment"))
                .collect(Collectors.toList());
        // metastore is null when external table
        if (null != metastore) {
            metastore.validateColumns(baseSchema);
        }

        // create partition info
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        PartitionInfo partitionInfo = null;
        Map<String, Long> partitionNameToId = Maps.newHashMap();
        if (partitionDesc != null) {
            partitionInfo = partitionDesc.toPartitionInfo(baseSchema, partitionNameToId, false);
        } else if (null != metastore) {
            long partitionId = metastore.getNextId();
            // use table name as single partition name
            partitionNameToId.put(tableName, partitionId);
            partitionInfo = new SinglePartitionInfo();
        }

        long tableId = GlobalStateMgr.getCurrentState().getNextId();
        EsTable esTable = new EsTable(tableId, tableName, baseSchema, stmt.getProperties(), partitionInfo);
        esTable.setComment(stmt.getComment());
        return esTable;
    }
}
