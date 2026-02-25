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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IcebergProcedureRegistry {
    private final Map<DatabaseTableName, Procedure> procedures = new ConcurrentHashMap<>();

    public void register(Procedure procedure) {
        procedures.put(DatabaseTableName.of(procedure.getDatabaseName(), procedure.getProcedureName()), procedure);
    }

    public Procedure find(DatabaseTableName name) {
        return procedures.get(name);
    }

    public boolean exists(DatabaseTableName name) {
        return procedures.containsKey(name);
    }
}

