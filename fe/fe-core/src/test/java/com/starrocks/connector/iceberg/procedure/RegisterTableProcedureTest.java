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
import com.starrocks.connector.DatabaseTableName;
import com.starrocks.connector.Procedure;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class RegisterTableProcedureTest {
    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @Test
    public void testRegisterTableProcedureRegistry() {
        // Test that RegisterTableProcedure can be registered and found in IcebergProcedureRegistry
        IcebergProcedureRegistry registry = new IcebergProcedureRegistry();
        Procedure proc = RegisterTableProcedure.getInstance();
        registry.register(proc);
        Procedure found = registry.find(DatabaseTableName.of(proc.getDatabaseName(), proc.getProcedureName()));
        Assertions.assertNotNull(found);
        Assertions.assertEquals(proc.getProcedureName(), found.getProcedureName());
    }

    @Test
    public void testRegisterTableProcedureExecute() {
        // Test that RegisterTableProcedure execute method works with valid arguments
        Procedure proc = RegisterTableProcedure.getInstance();
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("db1"));
        args.put("table_name", ConstantOperator.createVarchar("tbl1"));
        args.put("table_location", ConstantOperator.createVarchar("/path/to/table"));
        proc.execute(null, args);
        // No exception means success
    }

    @Test
    public void testProcedureMetadata() {
        // Test that RegisterTableProcedure has correct metadata
        Procedure proc = RegisterTableProcedure.getInstance();
        Assertions.assertEquals("system", proc.getDatabaseName());
        Assertions.assertEquals("register_table", proc.getProcedureName());
        Assertions.assertEquals(3, proc.getArguments().size());
    }

    @Test
    public void testProcedureArguments() {
        // Test that RegisterTableProcedure has correct argument definitions
        Procedure proc = RegisterTableProcedure.getInstance();
        var arguments = proc.getArguments();
        
        // Check database_name argument
        var databaseArg = arguments.get(0);
        Assertions.assertEquals("database_name", databaseArg.getName());
        Assertions.assertEquals(Type.VARCHAR, databaseArg.getType());
        Assertions.assertTrue(databaseArg.isRequired());
        
        // Check table_name argument
        var tableArg = arguments.get(1);
        Assertions.assertEquals("table_name", tableArg.getName());
        Assertions.assertEquals(Type.VARCHAR, tableArg.getType());
        Assertions.assertTrue(tableArg.isRequired());
        
        // Check table_location argument
        var locationArg = arguments.get(2);
        Assertions.assertEquals("table_location", locationArg.getName());
        Assertions.assertEquals(Type.VARCHAR, locationArg.getType());
        Assertions.assertTrue(locationArg.isRequired());
    }

    @Test
    public void testRegisterTableProcedureExecuteWithMinimalValidArgs() {
        // Test execute with minimal valid arguments
        Procedure proc = RegisterTableProcedure.getInstance();
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("test_db"));
        args.put("table_name", ConstantOperator.createVarchar("test_table"));
        args.put("table_location", ConstantOperator.createVarchar("s3://bucket/path"));
        
        // Should not throw exception
        proc.execute(null, args);
    }

    @Test
    public void testRegisterTableProcedureExecuteWithEmptyArgs() {
        // Test execute with empty arguments map (should not throw for now as implementation is stub)
        Procedure proc = RegisterTableProcedure.getInstance();
        Map<String, ConstantOperator> args = new HashMap<>();
        
        // Should not throw exception (implementation is stub)
        proc.execute(null, args);
    }

    @Test
    public void testRegisterTableProcedureExecuteWithPartialArgs() {
        // Test execute with only some arguments
        Procedure proc = RegisterTableProcedure.getInstance();
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("db1"));
        args.put("table_name", ConstantOperator.createVarchar("tbl1"));
        // Missing table_location
        
        // Should not throw exception (implementation is stub)
        proc.execute(null, args);
    }

    @Test
    public void testRegisterTableProcedureExecuteWithSpecialCharacters() {
        // Test execute with special characters in arguments
        Procedure proc = RegisterTableProcedure.getInstance();
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("test-db_123"));
        args.put("table_name", ConstantOperator.createVarchar("test.table$name"));
        args.put("table_location", ConstantOperator.createVarchar("hdfs://namenode:9000/path/with spaces/table"));
        
        // Should not throw exception
        proc.execute(null, args);
    }

    @Test
    public void testRegisterTableProcedureExecuteWithLongStrings() {
        // Test execute with very long strings
        Procedure proc = RegisterTableProcedure.getInstance();
        String longDbName = "a".repeat(1000);
        String longTableName = "b".repeat(1000);
        String longLocation = "c".repeat(1000);
        
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar(longDbName));
        args.put("table_name", ConstantOperator.createVarchar(longTableName));
        args.put("table_location", ConstantOperator.createVarchar(longLocation));
        
        // Should not throw exception
        proc.execute(null, args);
    }

    @Test
    public void testRegisterTableProcedureExecuteWithEmptyStringArgs() {
        // Test execute with empty string arguments
        Procedure proc = RegisterTableProcedure.getInstance();
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar(""));
        args.put("table_name", ConstantOperator.createVarchar(""));
        args.put("table_location", ConstantOperator.createVarchar(""));
        
        // Should not throw exception (implementation is stub)
        proc.execute(null, args);
    }

    @Test
    public void testRegisterTableProcedureExecuteWithExtraArgs() {
        // Test execute with extra arguments that are not defined
        Procedure proc = RegisterTableProcedure.getInstance();
        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("database_name", ConstantOperator.createVarchar("db1"));
        args.put("table_name", ConstantOperator.createVarchar("tbl1"));
        args.put("table_location", ConstantOperator.createVarchar("/path/to/table"));
        args.put("extra_arg", ConstantOperator.createVarchar("extra_value"));
        
        // Should not throw exception (implementation is stub)
        proc.execute(null, args);
    }

    @Test
    public void testRegisterTableProcedureSingleton() {
        // Test that getInstance returns the same instance
        Procedure proc1 = RegisterTableProcedure.getInstance();
        Procedure proc2 = RegisterTableProcedure.getInstance();
        Assertions.assertSame(proc1, proc2);
    }
}
