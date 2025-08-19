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
}
