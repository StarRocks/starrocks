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

package com.starrocks.connector.iceberg.procedure;

import com.starrocks.catalog.Type;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.Procedure;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

public class RegisterTableProcedure extends Procedure {
    private static final String PROCEDURE_NAME = "register_table";
    private static final String SYSTEM_DATABASE = "system";

    private static final String DATABASE_NAME = "database_name";
    private static final String TABLE_NAME = "table_name";
    private static final String METADATA_FILE = "metadata_file";

    private final String catalogName;
    private final IcebergCatalog icebergCatalog;

    public RegisterTableProcedure(String catalogName, IcebergCatalog icebergCatalog) {
        super(
                SYSTEM_DATABASE,
                PROCEDURE_NAME,
                Arrays.asList(
                        new NamedArgument(DATABASE_NAME, Type.VARCHAR, false),
                        new NamedArgument(TABLE_NAME, Type.VARCHAR, true),
                        new NamedArgument(METADATA_FILE, Type.VARCHAR, true)
                )
        );
        this.catalogName = catalogName;
        this.icebergCatalog = icebergCatalog;
    }

    @Override
    public void execute(ConnectContext context, Map<String, ConstantOperator> args) {
        // Validate required arguments
        if (!args.containsKey(TABLE_NAME) || !args.containsKey(METADATA_FILE)) {
            throw new IllegalArgumentException("Missing required arguments: table_name, metadata_file");
        }

        // Get database name from argument or use current database as default
        String databaseName = args.containsKey(DATABASE_NAME) ?
                args.get(DATABASE_NAME).getVarchar() : context.getDatabase();
        String tableName = args.get(TABLE_NAME).getVarchar();
        String metadataFile = args.get(METADATA_FILE).getVarchar();
        
        // Validate argument values
        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new IllegalArgumentException("database_name cannot be null or empty");
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("table_name cannot be null or empty");
        }
        if (metadataFile == null || metadataFile.trim().isEmpty()) {
            throw new IllegalArgumentException("metadata_file cannot be null or empty");
        }

        // Validate metadata file format
        if (!metadataFile.endsWith("metadata.json") && !metadataFile.contains("/metadata/")) {
            throw new IllegalArgumentException("metadata_file should be a valid Iceberg table metadata file path");
        }

        // Get catalog metadata
        Optional<ConnectorMetadata> metadata = GlobalStateMgr.getCurrentState()
                .getMetadataMgr()
                .getOptionalMetadata(java.util.Optional.empty(), catalogName);

        if (metadata.isEmpty()) {
            throw new StarRocksConnectorException("Failed to get metadata for catalog '%s'", catalogName);
        }
        ConnectorMetadata icebergMetadata = metadata.get();

        // Validate database exists
        if (!icebergMetadata.dbExists(context, databaseName)) {
            throw new StarRocksConnectorException("Database '%s' does not exist in catalog '%s'",
                    databaseName, catalogName);
        }

        // Check if table already exists
        if (icebergMetadata.tableExists(context, databaseName, tableName)) {
            throw new StarRocksConnectorException("Table '%s.%s' already exists in catalog '%s'",
                    databaseName, tableName, catalogName);
        }

        try {
            // Register the table using Iceberg catalog
            icebergCatalog.registerTable(context, databaseName, tableName, metadataFile);
        } catch (Exception e) {
            // Wrap other exceptions
            throw new StarRocksConnectorException("Failed to register table '%s.%s' with metadata file '%s': %s",
                    databaseName, tableName, metadataFile, e.getMessage());
        }
    }
}
