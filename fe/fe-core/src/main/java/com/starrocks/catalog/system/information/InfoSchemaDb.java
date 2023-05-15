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

package com.starrocks.catalog.system.information;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Information schema used for MySQL compatible.
public class InfoSchemaDb extends Database {
    public static final String DATABASE_NAME = "information_schema";

    public InfoSchemaDb() {
        super(SystemId.INFORMATION_SCHEMA_DB_ID, DATABASE_NAME);

        super.createTable(TablesSystemTable.create());
        super.createTable(PartitionsSystemTableSystemTable.create());
        super.createTable(TablePrivilegesSystemTable.create());
        super.createTable(ColumnPrivilegesSystemTable.create());
        super.createTable(ReferentialConstraintsSystemTable.create());
        super.createTable(KeyColumnUsageSystemTable.create());
        super.createTable(RoutinesSystemTable.create());
        super.createTable(SchemataSystemTable.create());
        super.createTable(SessionVariablesSystemTable.create());
        super.createTable(GlobalVariablesSystemTable.create());
        super.createTable(VerboseSessionVariablesSystemTable.create());
        super.createTable(ColumnsSystemTable.create());
        super.createTable(CharacterSetsSystemTable.create());
        super.createTable(CollationsSystemTable.create());
        super.createTable(TableConstraintsSystemTable.create());
        super.createTable(EnginesSystemTable.create());
        super.createTable(UserPrivilegesSystemTable.create());
        super.createTable(SchemaPrivilegesSystemTable.create());
        super.createTable(StatisticsSystemTable.create());
        super.createTable(TriggersSystemTable.create());
        super.createTable(EventsSystemTable.create());
        super.createTable(ViewsSystemTable.create());
        super.createTable(TasksSystemTable.create());
        super.createTable(TaskRunsSystemTable.create());
        super.createTable(MaterializedViewsSystemTable.create());
        super.createTable(LoadsSystemTable.create());
        super.createTable(LoadTrackingLogsSystemTable.create());
        super.createTable(TablesConfigSystemTable.create());
        super.createTable(BeCompactionsSystemTable.create());
        super.createTable(BeTabletsSystemTable.create());
        super.createTable(BeMetricsSystemTable.create());
        super.createTable(BeTxnsSystemTable.create());
        super.createTable(BeConfigsSystemTable.create());
        super.createTable(FeTabletSchedulesSystemTable.create());
        super.createTable(BeThreadsSystemTable.create());
        super.createTable(BeLogsSystemTable.create());
        super.createTable(BeBvarsSystemTable.create());
        super.createTable(BeCloudNativeCompactionsSystemTable.create());
    }

    @Override
    public boolean createTableWithLock(Table table, boolean isReplay) {
        return false;
    }

    @Override
    public boolean createTable(Table table) {
        // Do nothing.
        return false;
    }

    @Override
    public void dropTableWithLock(String name) {
        // Do nothing.
    }

    @Override
    public void dropTable(String name) {
        // Do nothing.
    }

    @Override
    public boolean createMaterializedWithLock(MaterializedView materializedView, boolean isReplay) {
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // Do nothing
    }

    public void readFields(DataInput in) throws IOException {
        throw new IOException("Not support.");
    }

    @Override
    public Table getTable(String name) {
        return super.getTable(name.toLowerCase());
    }

    public static boolean isInfoSchemaDb(String dbName) {
        if (dbName == null) {
            return false;
        }
        return DATABASE_NAME.equalsIgnoreCase(dbName);
    }
}
