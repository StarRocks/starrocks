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


package com.starrocks.connector.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;


public class CachingHiveTableOps extends BaseHmsTableOps {
    private static final Logger LOG = LogManager.getLogger(CachingHiveTableOps.class);

    private static final String HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES =
            "iceberg.hive.metadata-refresh-max-retries";
    private static final int HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES_DEFAULT = 2;

    private final String fullName;
    private final String catalogName;
    private final String database;
    private final String tableName;
    private final Configuration conf;
    private final int metadataRefreshMaxRetries;
    private final FileIO fileIO;
    private final ClientPool<IMetaStoreClient, TException> metaClients;
    private final CachingBaseMetastoreCatalog catalog;

    public CachingHiveTableOps(
            CachingBaseMetastoreCatalog catalog,
            Configuration conf,
            ClientPool metaClients,
            FileIO fileIO,
            String catalogName,
            String database,
            String table) {
        this.catalog = catalog;
        this.conf = conf;
        this.metaClients = metaClients;
        this.fileIO = fileIO;
        this.fullName = catalogName + "." + database + "." + table;
        this.catalogName = catalogName;
        this.database = database;
        this.tableName = table;
        this.metadataRefreshMaxRetries =
                conf.getInt(
                        HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES,
                        HIVE_ICEBERG_METADATA_REFRESH_MAX_RETRIES_DEFAULT);
    }

    @Override
    protected String tableName() {
        return fullName;
    }

    @Override
    public FileIO io() {
        return fileIO;
    }

    @Override
    protected void doRefresh() {
        String metadataLocation = null;
        try {
            Table table = metaClients.run(client -> client.getTable(database, tableName));
            validateTableIsIceberg(table, fullName);

            metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);

        } catch (NoSuchObjectException e) {
            if (currentMetadataLocation() != null) {
                throw new NoSuchTableException("No such table: %s.%s", database, tableName);
            }

        } catch (TException e) {
            String errMsg =
                    String.format("Failed to get table info from metastore %s.%s", database, tableName);
            throw new RuntimeException(errMsg, e);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during refresh", e);
        }

        refreshFromMetadataLocation(metadataLocation, metadataRefreshMaxRetries);
    }

    @Override
    public void updateMetadata(TableMetadata tableMetadata) {
        catalog.updateMetadata(TableIdentifier.of(database, tableName), tableMetadata);
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
        CommitHiveTableOperations nativeTableOps = new CommitHiveTableOperations(
                conf, metaClients, fileIO, catalogName, database, tableName);
        nativeTableOps.doCommit(base, metadata);
    }

    static void validateTableIsIceberg(Table table, String fullName) {
        String tableType = table.getParameters().get(TABLE_TYPE_PROP);
        NoSuchIcebergTableException.check(
                tableType != null && tableType.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE),
                "Not an iceberg table: %s (type=%s)",
                fullName,
                tableType);
    }

    private class CommitHiveTableOperations extends HiveTableOperations {
        public CommitHiveTableOperations(
                Configuration conf,
                ClientPool metaClients,
                FileIO fileIO,
                String catalogName,
                String database,
                String table
        ) {
            super(conf, metaClients, fileIO, catalogName, database, table);
        }

        @Override
        public void doCommit(TableMetadata base, TableMetadata metadata) {
            super.doCommit(base, metadata);
        }
    }
}
