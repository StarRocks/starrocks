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
import com.starrocks.connector.Procedure;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.Arrays;
import java.util.Map;

public class RegisterTableProcedure extends Procedure {
    private static final String PROCEDURE_NAME = "register_table";
    private static final String SYSTEM_DATABASE = "system";

    private static final String DATABASE_NAME = "database_name";
    private static final String TABLE_NAME = "table_name";
    private static final String TABLE_LOCATION = "table_location";

    private static final RegisterTableProcedure INSTANCE = new RegisterTableProcedure();

    public static RegisterTableProcedure getInstance() {
        return INSTANCE;
    }

    private RegisterTableProcedure() {
        super(
                SYSTEM_DATABASE,
                PROCEDURE_NAME,
                Arrays.asList(
                        new Argument(DATABASE_NAME, Type.VARCHAR, true),
                        new Argument(TABLE_NAME, Type.VARCHAR, true),
                        new Argument(TABLE_LOCATION, Type.VARCHAR, true)
                )
        );
    }

    @Override
    public void execute(ConnectContext context, Map<String, ConstantOperator> args) {
        // todo: Implement the logic to register an Iceberg table
    }
}
