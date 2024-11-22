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

package com.starrocks.statistic.columns;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;

import java.util.Objects;
import java.util.Optional;

public class ColumnFullId {

    @SerializedName("db_id")
    private final long dbId;
    @SerializedName("table_id")
    private final long tableId;
    @SerializedName("column_id")
    private final long columnUniqueId;

    public ColumnFullId(long dbId, long tableId, long columnUniqueId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.columnUniqueId = columnUniqueId;
    }

    public static ColumnFullId create(Database db, Table table, Column column) {
        return new ColumnFullId(db.getId(), table.getId(), column.getUniqueId());
    }

    public static Optional<ColumnFullId> lookup(TableName tableName, Column column) {
        LocalMetastore meta = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Optional<Database> db = meta.mayGetDb(tableName.getDb());
        Optional<Table> table = meta.mayGetTable(tableName.getDb(), tableName.getTbl());
        if (db.isPresent() && table.isPresent()) {
            return Optional.of(new ColumnFullId(db.get().getId(), table.get().getId(), column.getUniqueId()));
        }
        return Optional.empty();
    }

    public Optional<Pair<TableName, ColumnId>> toNames() {
        LocalMetastore meta = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Optional<Database> database = meta.mayGetDb(dbId);
        Optional<Table> table = meta.mayGetTable(dbId, tableId);
        Optional<Column> column = table.flatMap(x -> Optional.ofNullable(x.getColumnByUniqueId(columnUniqueId)));
        if (database.isEmpty() || table.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Pair.create(new TableName(database.get().getOriginName(), table.get().getName()),
                column.get().getColumnId()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnFullId that = (ColumnFullId) o;
        return dbId == that.dbId && tableId == that.tableId && columnUniqueId == that.columnUniqueId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, tableId, columnUniqueId);
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getColumnUniqueId() {
        return columnUniqueId;
    }
}
