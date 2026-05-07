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

import com.starrocks.catalog.Table;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry for {@link MVBaseTablePartitionHandler} implementations.
 * <p>
 * To add support for a new base table type, register its handler in the static block below.
 */
public class MVBaseTablePartitionHandlers {

    private static final Map<Table.TableType, MVBaseTablePartitionHandler> HANDLERS;

    static {
        HANDLERS = new HashMap<>();

        OlapTablePartitionHandler olap = new OlapTablePartitionHandler();
        HANDLERS.put(Table.TableType.OLAP, olap);
        HANDLERS.put(Table.TableType.MATERIALIZED_VIEW, olap);
        HANDLERS.put(Table.TableType.CLOUD_NATIVE, olap);
        HANDLERS.put(Table.TableType.CLOUD_NATIVE_MATERIALIZED_VIEW, olap);

        HMSTablePartitionHandler hms = new HMSTablePartitionHandler();
        HANDLERS.put(Table.TableType.HIVE, hms);
        HANDLERS.put(Table.TableType.HUDI, hms);
        HANDLERS.put(Table.TableType.ODPS, hms);

        HANDLERS.put(Table.TableType.ICEBERG, new IcebergTablePartitionHandler());
        HANDLERS.put(Table.TableType.JDBC, new JDBCTablePartitionHandler());
        HANDLERS.put(Table.TableType.PAIMON, new PaimonTablePartitionHandler());
    }

    /**
     * Get the handler for the given table, throwing if no handler is registered.
     */
    public static MVBaseTablePartitionHandler getHandler(Table table) {
        MVBaseTablePartitionHandler handler = HANDLERS.get(table.getType());
        if (handler == null) {
            throw new SemanticException(
                    "Materialized view with partition does not support base table type : " + table.getType());
        }
        return handler;
    }

}
