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
import com.starrocks.connector.DatabaseTableName;
import com.starrocks.connector.Procedure;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hive.HiveCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RegisterTableProcedureTest {
    @Mocked
    private HiveCatalog hiveCatalog;

    private IcebergHiveCatalog icebergCatalog;
    private Procedure registerTableProcedure;
    private static final String CATALOG_NAME = "iceberg_catalog";

    @BeforeEach
    public void setUp() {
        icebergCatalog = new IcebergHiveCatalog(hiveCatalog, new Configuration());
        registerTableProcedure = new RegisterTableProcedure(CATALOG_NAME, icebergCatalog);
    }

    @Test
    public void testRegisterTableProcedureRegistry() {
        // Test that RegisterTableProcedure can be registered and found in IcebergProcedureRegistry
        IcebergProcedureRegistry registry = new IcebergProcedureRegistry();
        registry.register(registerTableProcedure);
        Procedure found = registry.find(DatabaseTableName.of(registerTableProcedure.getDatabaseName(),
                registerTableProcedure.getProcedureName()));
        Assertions.assertNotNull(found);
        Assertions.assertEquals(registerTableProcedure.getProcedureName(), found.getProcedureName());
    }

    @Test
    public void testRegisterTableProcedureExecuteSuccess(@Mocked ConnectContext context,
                                                         @Mocked GlobalStateMgr globalStateMgr,
                                                         @Mocked MetadataMgr metadataMgr,
                                                         @Mocked ConnectorMetadata connectorMetadata) {
        // Test successful table registration with all required arguments
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("test_db"));
        args.put("table_name", ConstantOperator.createVarchar("test_table"));
        args.put("metadata_file", ConstantOperator.createVarchar("/path/to/table/metadata.json"));

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getOptionalMetadata((Optional<String>) any, CATALOG_NAME);
                result = Optional.of(connectorMetadata);
                minTimes = 0;

                connectorMetadata.dbExists(context, "test_db");
                result = true;
                minTimes = 0;

                connectorMetadata.tableExists(context, "test_db", "test_table");
                result = false;
                minTimes = 0;

                icebergCatalog.registerTable(context, "test_db", "test_table", "/path/to/table/metadata.json");
                result = null;
                minTimes = 0;
            }
        };

        Assertions.assertDoesNotThrow(() -> registerTableProcedure.execute(context, args));
    }

    @Test
    public void testRegisterTableProcedureWithoutDatabaseName(@Mocked ConnectContext context) {
        // Test using current database when database_name is not provided
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("table_name", ConstantOperator.createVarchar("test_table"));
        args.put("metadata_file", ConstantOperator.createVarchar("/path/to/table/metadata.json"));

        Assertions.assertThrows(IllegalArgumentException.class, () -> registerTableProcedure.execute(context, args));
    }

    @Test
    public void testRegisterTableProcedureMissingTableName(@Mocked ConnectContext context) {
        // Test exception when table_name is missing
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("test_db"));
        args.put("metadata_file", ConstantOperator.createVarchar("/path/to/table/metadata.json"));

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> registerTableProcedure.execute(context, args));
        Assertions.assertTrue(exception.getMessage().contains("Missing required arguments"));
    }

    @Test
    public void testRegisterTableProcedureMissingMetadataFile(@Mocked ConnectContext context) {
        // Test exception when metadata_file is missing
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("test_db"));
        args.put("table_name", ConstantOperator.createVarchar("test_table"));

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> registerTableProcedure.execute(context, args));
        Assertions.assertTrue(exception.getMessage().contains("Missing required arguments"));
    }

    @Test
    public void testRegisterTableProcedureEmptyDatabaseName(@Mocked ConnectContext context) {
        // Test exception when database_name is empty
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar(""));
        args.put("table_name", ConstantOperator.createVarchar("test_table"));
        args.put("metadata_file", ConstantOperator.createVarchar("/path/to/table/metadata.json"));

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> registerTableProcedure.execute(context, args));
        Assertions.assertTrue(exception.getMessage().contains("database_name cannot be null or empty"));
    }

    @Test
    public void testRegisterTableProcedureEmptyTableName(@Mocked ConnectContext context) {
        // Test exception when table_name is empty
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("test_db"));
        args.put("table_name", ConstantOperator.createVarchar(""));
        args.put("metadata_file", ConstantOperator.createVarchar("/path/to/table/metadata.json"));

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> registerTableProcedure.execute(context, args));
        Assertions.assertTrue(exception.getMessage().contains("table_name cannot be null or empty"));
    }

    @Test
    public void testRegisterTableProcedureInvalidMetadataFile(@Mocked ConnectContext context) {
        // Test exception when metadata_file has invalid format
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("test_db"));
        args.put("table_name", ConstantOperator.createVarchar("test_table"));
        args.put("metadata_file", ConstantOperator.createVarchar("invalid_file.txt"));

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> registerTableProcedure.execute(context, args));
        Assertions.assertTrue(exception.getMessage().contains("should be a valid Iceberg table metadata file path"));
    }

    @Test
    public void testRegisterTableProcedureCatalogNotFound(@Mocked ConnectContext context,
                                                          @Mocked GlobalStateMgr globalStateMgr,
                                                          @Mocked MetadataMgr metadataMgr) {
        // Test exception when catalog metadata is not found
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("test_db"));
        args.put("table_name", ConstantOperator.createVarchar("test_table"));
        args.put("metadata_file", ConstantOperator.createVarchar("/path/to/table/metadata.json"));

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getOptionalMetadata((Optional<String>) any, CATALOG_NAME);
                result = Optional.empty();
                minTimes = 0;
            }
        };

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> registerTableProcedure.execute(context, args));
        Assertions.assertTrue(exception.getMessage().contains("Failed to get metadata for catalog"));
    }

    @Test
    public void testRegisterTableProcedureDatabaseNotExists(@Mocked ConnectContext context,
                                                            @Mocked GlobalStateMgr globalStateMgr,
                                                            @Mocked MetadataMgr metadataMgr,
                                                            @Mocked ConnectorMetadata connectorMetadata) {
        // Test exception when database does not exist
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("nonexistent_db"));
        args.put("table_name", ConstantOperator.createVarchar("test_table"));
        args.put("metadata_file", ConstantOperator.createVarchar("/path/to/table/metadata.json"));

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getOptionalMetadata((Optional<String>) any, CATALOG_NAME);
                result = Optional.of(connectorMetadata);
                minTimes = 0;

                connectorMetadata.dbExists(context, "nonexistent_db");
                result = false;
                minTimes = 0;
            }
        };

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> registerTableProcedure.execute(context, args));
        Assertions.assertTrue(exception.getMessage().contains("does not exist in catalog"));
    }

    @Test
    public void testRegisterTableProcedureTableAlreadyExists(@Mocked ConnectContext context,
                                                             @Mocked GlobalStateMgr globalStateMgr,
                                                             @Mocked MetadataMgr metadataMgr,
                                                             @Mocked ConnectorMetadata connectorMetadata) {
        // Test exception when table already exists
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("test_db"));
        args.put("table_name", ConstantOperator.createVarchar("existing_table"));
        args.put("metadata_file", ConstantOperator.createVarchar("/path/to/table/metadata.json"));

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getOptionalMetadata((Optional<String>) any, CATALOG_NAME);
                result = Optional.of(connectorMetadata);
                minTimes = 0;

                connectorMetadata.dbExists(context, "test_db");
                result = true;
                minTimes = 0;

                connectorMetadata.tableExists(context, "test_db", "existing_table");
                result = true;
                minTimes = 0;
            }
        };

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> registerTableProcedure.execute(context, args));
        Assertions.assertTrue(exception.getMessage().contains("already exists in catalog"));
    }

    @Test
    public void testRegisterTableProcedureRegistrationFailure(@Mocked ConnectContext context,
                                                              @Mocked GlobalStateMgr globalStateMgr,
                                                              @Mocked MetadataMgr metadataMgr,
                                                              @Mocked ConnectorMetadata connectorMetadata) {
        // Test exception when table registration fails
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("test_db"));
        args.put("table_name", ConstantOperator.createVarchar("test_table"));
        args.put("metadata_file", ConstantOperator.createVarchar("/path/to/table/metadata.json"));

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getMetadataMgr();
                result = metadataMgr;
                minTimes = 0;

                metadataMgr.getOptionalMetadata((Optional<String>) any, CATALOG_NAME);
                result = Optional.of(connectorMetadata);
                minTimes = 0;

                connectorMetadata.dbExists(context, "test_db");
                result = true;
                minTimes = 0;

                connectorMetadata.tableExists(context, "test_db", "test_table");
                result = false;
                minTimes = 0;

                icebergCatalog.registerTable(context, "test_db", "test_table", "/path/to/table/metadata.json");
                result = new RuntimeException("Registration failed");
                minTimes = 0;
            }
        };

        Exception exception = Assertions.assertThrows(Exception.class,
                () -> registerTableProcedure.execute(context, args));
        Assertions.assertTrue(exception.getMessage().contains("Failed to register table"));
    }

    @Test
    public void testRegisterTableProcedureNullArguments(@Mocked ConnectContext context) {
        // Test exception when arguments contain null values
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createNull(Type.VARCHAR));
        args.put("table_name", ConstantOperator.createVarchar("test_table"));
        args.put("metadata_file", ConstantOperator.createVarchar("/path/to/table/metadata.json"));

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> registerTableProcedure.execute(context, args));
        Assertions.assertTrue(exception.getMessage().contains("cannot be null or empty"));
    }
}
