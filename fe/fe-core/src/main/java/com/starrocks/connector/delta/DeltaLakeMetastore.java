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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.IMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.toHivePartitionName;

public abstract class DeltaLakeMetastore implements IMetastore {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeMetastore.class);
    protected final String catalogName;
    protected final IMetastore delegate;
    protected final Configuration hdfsConfiguration;

    public DeltaLakeMetastore(String catalogName, IMetastore metastore, Configuration hdfsConfiguration) {
        this.catalogName = catalogName;
        this.delegate = metastore;
        this.hdfsConfiguration = hdfsConfiguration;
    }

    public List<String> getAllDatabaseNames() {
        return delegate.getAllDatabaseNames();
    }

    public List<String> getAllTableNames(String dbName) {
        return delegate.getAllTableNames(dbName);
    }

    public Database getDb(String dbName) {
        return delegate.getDb(dbName);
    }

    public DeltaLakeTable getTable(String dbName, String tableName) {
        MetastoreTable metastoreTable = getMetastoreTable(dbName, tableName);
        if (metastoreTable == null) {
            LOG.error("get metastore table failed. dbName: {}, tableName: {}", dbName, tableName);
            return null;
        }

        String path = metastoreTable.getTableLocation();
        long createTime = metastoreTable.getCreateTime();
        return DeltaUtils.convertDeltaToSRTable(catalogName, dbName, tableName, path, hdfsConfiguration, createTime);
    }

    public List<String> getPartitionKeys(String dbName, String tableName) {
        DeltaLakeTable deltaLakeTable = getTable(dbName, tableName);
        if (deltaLakeTable == null) {
            LOG.error("Table {}.{}.{} doesn't exist", catalogName, dbName, tableName);
            return Lists.newArrayList();
        }

        List<String> partitionKeys = Lists.newArrayList();
        Engine deltaEngine = deltaLakeTable.getDeltaEngine();
        List<String> partitionColumnNames = deltaLakeTable.getPartitionColumnNames();

        ScanBuilder scanBuilder = deltaLakeTable.getDeltaSnapshot().getScanBuilder(deltaEngine);
        Scan scan = scanBuilder.build();
        try (CloseableIterator<FilteredColumnarBatch> scanFilesAsBatches = scan.getScanFiles(deltaEngine)) {
            while (scanFilesAsBatches.hasNext()) {
                FilteredColumnarBatch scanFileBatch = scanFilesAsBatches.next();

                try (CloseableIterator<Row> scanFileRows = scanFileBatch.getRows()) {
                    while (scanFileRows.hasNext()) {
                        Row scanFileRow = scanFileRows.next();
                        Map<String, String> partitionValueMap = InternalScanFileUtils.getPartitionValues(scanFileRow);
                        List<String> partitionValues =
                                partitionColumnNames.stream().map(partitionValueMap::get).collect(
                                        Collectors.toList());
                        String partitionName = toHivePartitionName(partitionColumnNames, partitionValues);
                        partitionKeys.add(partitionName);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to get partition keys for table {}.{}.{}", catalogName, dbName, tableName, e);
            throw new StarRocksConnectorException(String.format("Failed to get partition keys for table %s.%s.%s",
                    catalogName, dbName, tableName), e);
        }

        return partitionKeys;
    }

    public boolean tableExists(String dbName, String tableName) {
        return delegate.tableExists(dbName, tableName);
    }
}
