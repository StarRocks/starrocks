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

        super.registerTableUnlock(TablesSystemTable.create());
        super.registerTableUnlock(PartitionsSystemTableSystemTable.create());
        super.registerTableUnlock(TablePrivilegesSystemTable.create());
        super.registerTableUnlock(ColumnPrivilegesSystemTable.create());
        super.registerTableUnlock(ReferentialConstraintsSystemTable.create());
        super.registerTableUnlock(KeyColumnUsageSystemTable.create());
        super.registerTableUnlock(RoutinesSystemTable.create());
        super.registerTableUnlock(SchemataSystemTable.create());
        super.registerTableUnlock(SessionVariablesSystemTable.create());
        super.registerTableUnlock(GlobalVariablesSystemTable.create());
        super.registerTableUnlock(VerboseSessionVariablesSystemTable.create());
        super.registerTableUnlock(ColumnsSystemTable.create());
        super.registerTableUnlock(CharacterSetsSystemTable.create());
        super.registerTableUnlock(CollationsSystemTable.create());
        super.registerTableUnlock(TableConstraintsSystemTable.create());
        super.registerTableUnlock(EnginesSystemTable.create());
        super.registerTableUnlock(UserPrivilegesSystemTable.create());
        super.registerTableUnlock(SchemaPrivilegesSystemTable.create());
        super.registerTableUnlock(StatisticsSystemTable.create());
        super.registerTableUnlock(TriggersSystemTable.create());
        super.registerTableUnlock(EventsSystemTable.create());
        super.registerTableUnlock(ViewsSystemTable.create());
        super.registerTableUnlock(TasksSystemTable.create());
        super.registerTableUnlock(TaskRunsSystemTable.create());
        super.registerTableUnlock(MaterializedViewsSystemTable.create());
        super.registerTableUnlock(LoadsSystemTable.create());
        super.registerTableUnlock(LoadTrackingLogsSystemTable.create());
        super.registerTableUnlock(RoutineLoadJobsSystemTable.create());
        super.registerTableUnlock(StreamLoadsSystemTable.create());
        super.registerTableUnlock(TablesConfigSystemTable.create());
        super.registerTableUnlock(BeCompactionsSystemTable.create());
        super.registerTableUnlock(BeTabletsSystemTable.create());
        super.registerTableUnlock(BeMetricsSystemTable.create());
        super.registerTableUnlock(BeTxnsSystemTable.create());
        super.registerTableUnlock(BeConfigsSystemTable.create());
        super.registerTableUnlock(FeTabletSchedulesSystemTable.create());
        super.registerTableUnlock(BeThreadsSystemTable.create());
        super.registerTableUnlock(BeLogsSystemTable.create());
        super.registerTableUnlock(BeBvarsSystemTable.create());
        super.registerTableUnlock(BeCloudNativeCompactionsSystemTable.create());
        super.registerTableUnlock(PipeFileSystemTable.create());
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
