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

package com.starrocks.sql.analyzer.mv;

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Set;

/**
 * Handler for JDBC tables.
 */
public class JDBCTablePartitionHandler implements MVBaseTablePartitionHandler {

    static final Set<JDBCTable.ProtocolType> SUPPORTED_JDBC_PARTITION_TYPE =
            ImmutableSet.of(JDBCTable.ProtocolType.MYSQL, JDBCTable.ProtocolType.MARIADB);

    @Override
    public void checkPartitionColumn(MVPartitionCheckContext context) {
        JDBCTable table = (JDBCTable) context.getTable();
        MVBaseTablePartitionHandler.checkPartitionColumnWithBaseTable(context.getSlotRef(), table);
        if (!SUPPORTED_JDBC_PARTITION_TYPE.contains(table.getProtocolType())) {
            throw new SemanticException(String.format("Materialized view PARTITION BY for JDBC %s is not " +
                    "supported, you could remove the PARTITION BY clause", table.getProtocolType()));
        }
    }
}
