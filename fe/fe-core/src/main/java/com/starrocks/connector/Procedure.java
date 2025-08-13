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

package com.starrocks.connector;

import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.List;
import java.util.Map;

public abstract class Procedure {
    private final String databaseName;
    private final String procedureName;
    private final List<Argument> arguments;

    public Procedure(String databaseName, String procedureName, List<Argument> arguments) {
        this.databaseName = databaseName;
        this.procedureName = procedureName;
        this.arguments = arguments;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public String getProcedureName() {
        return this.procedureName;
    }

    public abstract void execute(ConnectContext context, Map<String, ConstantOperator> args);

    public List<Argument> getArguments() {
        return this.arguments;
    }

    public static class Argument {
        private final String name;
        private final Type type;
        private final boolean required;

        public Argument(String name, Type type, boolean required) {
            this.name = name;
            this.type = type;
            this.required = required;
        }

        public String getName() {
            return this.name;
        }

        public Type getType() {
            return this.type;
        }

        public boolean isRequired() {
            return this.required;
        }
    }
}

