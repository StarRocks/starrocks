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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.ReplayConnectorMetadata;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.catalog.Table.TableType.HIVE;

/**
 * A {@link ConnectorMetadata} that serves hive external-catalog tables during ReplayFromDump. It synthesizes
 * a {@link HiveTable} from the declared column schema (keeping the real column types, unlike the legacy
 * resource-mapping replay which coerced every column to STRING) so the table can be planned entirely
 * offline. Row count and per-column statistics come from the dump (served by {@link ReplayConnectorMetadata});
 * partition names, when the dump captured them, drive partition pruning. Remote-file listing is mocked with a
 * single empty split so the scan can be built without touching storage.
 */
public class ReplayHiveCatalogMetadata extends ReplayConnectorMetadata {
    private static final List<RemoteFileInfo> MOCKED_FILES =
            ImmutableList.of(new RemoteFileInfo(null, ImmutableList.of(), null));

    public ReplayHiveCatalogMetadata(String catalogName) {
        super(catalogName);
    }

    public void registerTable(String dbName, String tableName, List<Column> columns,
                              List<String> dataColumnNames, List<String> partitionColumnNames,
                              long rowCount, Map<String, ColumnStatistic> columnStats,
                              List<String> partitionNames) {
        long tableId = GlobalStateMgr.getCurrentState().getNextId();
        HiveTable table = HiveTable.builder()
                .setId(tableId)
                .setTableName(tableName)
                .setCatalogName(catalogName)
                .setResourceName(catalogName)
                .setHiveDbName(dbName)
                .setHiveTableName(tableName)
                .setPartitionColumnNames(partitionColumnNames)
                .setDataColumnNames(dataColumnNames)
                .setFullSchema(columns)
                .setTableLocation("")
                .setCreateTime(0L)
                .build();
        register(dbName, tableName, table, rowCount, columnStats, partitionNames);
    }

    @Override
    public Table.TableType getTableType() {
        return HIVE;
    }

    // Filter the captured partition names by the requested per-column values (metastore form
    // "col=val/col2=val2"), mirroring MockedHiveMetadata so OptExternalPartitionPruner reproduces the pruned
    // partition count. An absent value (Optional.empty) matches any partition on that column.
    @Override
    public List<String> listPartitionNamesByValue(String databaseName, String tableName,
                                                   List<Optional<String>> partitionValues) {
        List<String> ret = new ArrayList<>();
        for (String p : listPartitionNames(databaseName, tableName, ConnectorMetadataRequestContext.DEFAULT)) {
            if (isPartitionNameValueMatched(p, partitionValues)) {
                ret.add(p);
            }
        }
        return ret;
    }

    private static boolean isPartitionNameValueMatched(String partitionName, List<Optional<String>> values) {
        String[] parts = partitionName.split("/");
        if (parts.length != values.size()) {
            return false;
        }
        for (int i = 0; i < parts.length; i++) {
            Optional<String> v = values.get(i);
            if (!v.isPresent()) {
                continue;
            }
            String[] kv = parts[i].split("=");
            if (kv.length != 2 || !kv[1].equals(v.get())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        return Lists.newArrayList(MOCKED_FILES);
    }

    @Override
    public RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params) {
        return new RemoteFileInfoDefaultSource(Lists.newArrayList(MOCKED_FILES));
    }
}
