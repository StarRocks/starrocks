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

package com.starrocks.connector.metadata.iceberg;

import com.starrocks.catalog.Table;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metadata.AbstractMetadataTableFactory;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IcebergMetadataTableFactory implements AbstractMetadataTableFactory {
    private static final Logger LOG = LogManager.getLogger(IcebergMetadataTableFactory.class);
    public static final IcebergMetadataTableFactory INSTANCE = new IcebergMetadataTableFactory();

    @Override
    public Table createTable(ConnectContext context, String catalogName, String dbName, String tableName,
                             MetadataTableType tableType) {
        switch (tableType) {
            case LOGICAL_ICEBERG_METADATA:
                return LogicalIcebergMetadataTable.create(catalogName, dbName, tableName);
            case REFS:
                return IcebergRefsTable.create(catalogName, dbName, tableName);
            case HISTORY:
                return IcebergHistoryTable.create(catalogName, dbName, tableName);
            case METADATA_LOG_ENTRIES:
                return IcebergMetadataLogEntriesTable.create(catalogName, dbName, tableName);
            case SNAPSHOTS:
                return IcebergSnapshotsTable.create(catalogName, dbName, tableName);
            case MANIFESTS:
                return IcebergManifestsTable.create(catalogName, dbName, tableName);
            case FILES:
                return IcebergFilesTable.create(catalogName, dbName, tableName);
            case PARTITIONS:
                return IcebergPartitionsTable.create(context, catalogName, dbName, tableName);
            default:
                LOG.error("Unrecognized iceberg metadata table type {}", tableType);
                throw new StarRocksConnectorException("Unrecognized iceberg metadata table type %s", tableType);
        }
    }
}
