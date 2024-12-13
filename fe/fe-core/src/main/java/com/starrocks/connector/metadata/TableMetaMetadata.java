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

package com.starrocks.connector.metadata;

import com.starrocks.catalog.Table;
<<<<<<< HEAD
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metadata.iceberg.LogicalIcebergMetadataTable;

=======
import com.starrocks.catalog.Type;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableVersion;
import com.starrocks.connector.TableVersionRange;

import java.util.Optional;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

// TODO(stephen): what's the pretty class name?
public class TableMetaMetadata implements ConnectorMetadata {
    public static final String METADATA_DB_NAME = "metadata_database";
<<<<<<< HEAD
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public static boolean isMetadataTable(String tableName) {
        return MetadataTableName.isMetadataTable(tableName);
    }

    private final String catalogName;
<<<<<<< HEAD

    public TableMetaMetadata(String catalogName) {
        this.catalogName = catalogName;
    }

=======
    private final String catalogType;

    public TableMetaMetadata(String catalogName, String catalogType) {
        this.catalogName = catalogName;
        this.catalogType = catalogType;
    }

    @Override
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public Table getTable(String dbName, String tblName) {
        MetadataTableName metadataTableName = MetadataTableName.from(tblName);
        MetadataTableType tableType = metadataTableName.getTableType();
        String tableName = metadataTableName.getTableName();
<<<<<<< HEAD

        switch (tableType) {
            case LOGICAL_ICEBERG_METADATA:
                return LogicalIcebergMetadataTable.create(catalogName, dbName, tableName);
            default:
                throw new StarRocksConnectorException("Unrecognized metadata table type {}", tableType);
=======
        AbstractMetadataTableFactory tableFactory = MetadataTableFactoryProvider.getFactory(catalogType);
        return tableFactory.createTable(catalogName, dbName, tableName, tableType);
    }

    @Override
    public TableVersionRange getTableVersionRange(String dbName, Table table,
                                                  Optional<ConnectorTableVersion> startVersion,
                                                  Optional<ConnectorTableVersion> endVersion) {
        if (endVersion.isPresent()) {
            Long snapshotId = endVersion.get().getConstantOperator().castTo(Type.BIGINT).get().getBigint();
            return TableVersionRange.withEnd(Optional.of(snapshotId));
        } else {
            return TableVersionRange.empty();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
    }

}