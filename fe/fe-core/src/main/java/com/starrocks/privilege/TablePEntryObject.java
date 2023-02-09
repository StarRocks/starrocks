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


package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Objects;

public class TablePEntryObject implements PEntryObject {
    public static final String ALL_DATABASE_UUID = "ALL_DATABASES_UUID"; // represent all databases
    public static final String ALL_TABLES_UUID = "ALL_TABLES_UUID"; // represent all tables

    @SerializedName(value = "d")
    protected String databaseUUID;
    @SerializedName(value = "t")
    protected String tableUUID;

    public String getDatabaseUUID() {
        return databaseUUID;
    }

    public String getTableUUID() {
        return tableUUID;
    }

    public static TablePEntryObject generate(GlobalStateMgr mgr, List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 2) {
            throw new PrivilegeException("invalid object tokens, should have two: " + tokens);
        }
        String dbUUID;
        String tblUUID;

        if (tokens.get(0).equals("*")) {
            dbUUID = ALL_DATABASE_UUID;
            tblUUID = ALL_TABLES_UUID;
        } else {
            Database database = mgr.getDb(tokens.get(0));
            if (database == null) {
                throw new PrivObjNotFoundException("cannot find db: " + tokens.get(0));
            }
            dbUUID = database.getUUID();

            if (tokens.get(1).equals("*")) {
                tblUUID = ALL_TABLES_UUID;
            } else {
                Table table = database.getTable(tokens.get(1));
                if (table == null) {
                    throw new PrivObjNotFoundException("cannot find table " + tokens.get(1) + " in db " + tokens.get(0));
                }
                tblUUID = table.getUUID();
            }
        }

        return new TablePEntryObject(dbUUID, tblUUID);
    }

    protected TablePEntryObject(String databaseUUID, String tableUUID) {
        this.tableUUID = tableUUID;
        this.databaseUUID = databaseUUID;
    }

    /**
     * if the current table matches other table, including fuzzy matching.
     * <p>
     * this(db1.tbl1), other(db1.tbl1) -> true
     * this(db1.tbl1), other(db1.ALL) -> true
     * this(db1.ALL), other(db1.tbl1) -> false
     */
    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof TablePEntryObject)) {
            return false;
        }
        TablePEntryObject other = (TablePEntryObject) obj;
        if (Objects.equals(other.databaseUUID, ALL_DATABASE_UUID)) {
            return true;
        }
        if (Objects.equals(other.tableUUID, ALL_TABLES_UUID)) {
            return Objects.equals(databaseUUID, other.databaseUUID);
        }
        return Objects.equals(other.databaseUUID, databaseUUID) && Objects.equals(other.tableUUID, tableUUID);
    }

    @Override
    public boolean isFuzzyMatching() {
        return Objects.equals(databaseUUID, ALL_DATABASE_UUID) || Objects.equals(tableUUID, ALL_TABLES_UUID);
    }


    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        // TODO(yiming): change validation method for external catalog
        Database db = globalStateMgr.getDbIncludeRecycleBin(Long.parseLong(this.databaseUUID));
        if (db == null) {
            return false;
        }
        return globalStateMgr.getTableIncludeRecycleBin(db, Long.parseLong(this.tableUUID)) != null;
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof TablePEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }

        TablePEntryObject o = (TablePEntryObject) obj;
        // Always put the fuzzy matching object at the front of the privilege entry list
        // when sorting in ascendant order.
        if (Objects.equals(this.databaseUUID, o.databaseUUID)) {
            if (Objects.equals(this.tableUUID, o.tableUUID)) {
                return 0;
            } else if (Objects.equals(this.tableUUID, ALL_TABLES_UUID)) {
                return -1;
            } else if (Objects.equals(o.tableUUID, ALL_TABLES_UUID)) {
                return 1;
            } else {
                return this.tableUUID.compareTo(o.tableUUID);
            }
        } else if (Objects.equals(this.databaseUUID, ALL_DATABASE_UUID)) {
            return -1;
        } else if (Objects.equals(o.databaseUUID, ALL_DATABASE_UUID)) {
            return 1;
        } else {
            return this.databaseUUID.compareTo(o.databaseUUID);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TablePEntryObject that = (TablePEntryObject) o;
        return Objects.equals(databaseUUID, that.databaseUUID) && Objects.equals(tableUUID, that.tableUUID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseUUID, tableUUID);
    }

    @Override
    public PEntryObject clone() {
        return new TablePEntryObject(databaseUUID, tableUUID);
    }
}
