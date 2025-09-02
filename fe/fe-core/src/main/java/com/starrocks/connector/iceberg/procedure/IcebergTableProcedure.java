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

import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.List;
import java.util.Map;

public abstract class IcebergTableProcedure {
    private final String procedureName;
    private final IcebergTableOperation operation;
    private final List<NamedArgument> arguments;


    public IcebergTableProcedure(String procedureName, List<NamedArgument> arguments, IcebergTableOperation operation) {
        this.procedureName = procedureName;
        this.arguments = arguments;
        this.operation = operation;
    }

    public String getProcedureName() {
        return this.procedureName;
    }

    public List<NamedArgument> getArguments() {
        return this.arguments;
    }

    public IcebergTableOperation getOperation() {
        return this.operation;
    }

    public abstract void execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args);
}