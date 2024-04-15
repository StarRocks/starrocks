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

package com.starrocks.connector.delta;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.delta.cache.DeltaLakeTableName;
import com.starrocks.connector.delta.cost.DeltaLakeStatisticProvider;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.credential.CloudConfiguration;
import io.delta.standalone.internal.util.DeltaPartitionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeltaLakeMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeMetadata.class);
    private final String catalogName;
    private final HiveMetastoreOperations hmsOps;
    private final HdfsEnvironment hdfsEnvironment;
    private IHiveMetastore metastore;
    private final DeltaLakeStatisticProvider statisticProvider = new DeltaLakeStatisticProvider();

    public DeltaLakeMetadata(HdfsEnvironment hdfsEnvironment, String catalogName, HiveMetastoreOperations hmsOps) {
        this.hdfsEnvironment = hdfsEnvironment;
        this.catalogName = catalogName;
        this.hmsOps = hmsOps;
    }

    public DeltaLakeMetadata(HdfsEnvironment hdfsEnvironment,
                             IHiveMetastore metastore,
                             String catalogName,
                             HiveMetastoreOperations hmsOps) {
        this(hdfsEnvironment, catalogName, hmsOps);
        this.metastore = metastore;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.DELTALAKE;
    }

    @Override
    public List<String> listDbNames() {
        return hmsOps.getAllDatabaseNames();
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return hmsOps.getAllTableNames(dbName);
    }

    @Override
    public Database getDb(String dbName) {
        return hmsOps.getDb(dbName);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        if (metastore instanceof CachingDeltaLakeMetadata) {
            DeltaLakeTable table = (DeltaLakeTable) getTable(databaseName, tableName);
            String path = table.getDeltaLog().getPath().toString();
            DeltaLakeTableName tbl = new DeltaLakeTableName(catalogName, databaseName, tableName, path);
            try {
                final List<String> partitionColumns = table.getDeltaLog()
                        .snapshot()
                        .getMetadata()
                        .getPartitionColumns();
                final Set<Map<String, String>> allPartitions = ((CachingDeltaLakeMetadata) metastore)
                        .getAllPartitions(tbl);
                return new ArrayList<>(DeltaPartitionUtil.toHivePartitionFormat(partitionColumns, allPartitions));
            } catch (Exception e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }

        }
        return hmsOps.getPartitionKeys(databaseName, tableName);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        try {
            Table table = hmsOps.getTable(dbName, tblName);
            if (table == null) {
                return null;
            }
            HiveTable hiveTable = (HiveTable) table;
            String path = hiveTable.getTableLocation();
            long createTime = table.getCreateTime();
            return DeltaUtils.convertDeltaToSRTable(catalogName, dbName, tblName, path, hdfsEnvironment.getConfiguration(),
                    createTime, metastore);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            return null;
        }
    }

    @Override
    public boolean tableExists(String dbName, String tblName) {
        return hmsOps.tableExists(dbName, tblName);
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return hdfsEnvironment.getCloudConfiguration();
    }
}
