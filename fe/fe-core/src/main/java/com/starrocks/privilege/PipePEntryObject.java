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
import com.starrocks.load.pipe.Pipe;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.common.MetaNotFoundException;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class PipePEntryObject implements PEntryObject {

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "database")
    private String dbUUID;

    protected PipePEntryObject(String dbUUID, long id) {
        this.id = id;
        this.dbUUID = dbUUID;
    }

    public static PEntryObject generate(GlobalStateMgr mgr, List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 2) {
            throw new PrivilegeException("invalid object tokens, should have two: " + tokens);
        }
        String dbUUID;
        long pipeId;

        if (Objects.equals(tokens.get(0), "*")) {
            dbUUID = PrivilegeBuiltinConstants.ALL_DATABASES_UUID;
            pipeId = PrivilegeBuiltinConstants.ALL_PIPES_ID;
        } else {
            String dbName = tokens.get(0);
            Database database = mgr.getDb(dbName);
            if (database == null) {
                throw new PrivObjNotFoundException("cannot find db: " + dbName);
            }
            dbUUID = database.getUUID();

            if (Objects.equals(tokens.get(1), "*")) {
                pipeId = PrivilegeBuiltinConstants.ALL_PIPES_ID;
            } else {
                String name = tokens.get(1);
                Optional<Pipe> pipe = mgr.getPipeManager().mayGetPipe(new PipeName(dbName, name));

                pipe.orElseThrow(() ->
                        new PrivObjNotFoundException(
                                "cannot find pipe " + tokens.get(1) + " in db " + tokens.get(0))
                );
                pipeId = pipe.get().getId();
            }
        }

        return new PipePEntryObject(dbUUID, pipeId);
    }

    public long getId() {
        return id;
    }

    public String getDbUUID() {
        return dbUUID;
    }

    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof PipePEntryObject)) {
            return false;
        }
        PipePEntryObject other = (PipePEntryObject) obj;
        if (Objects.equals(other.getDbUUID(), PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
            return true;
        }
        if (Objects.equals(other.getId(), PrivilegeBuiltinConstants.ALL_PIPES_ID)) {
            return Objects.equals(getDbUUID(), other.getDbUUID());
        }
        return Objects.equals(getDbUUID(), other.getDbUUID()) &&
                Objects.equals(getId(), other.getId());
    }

    @Override
    public boolean isFuzzyMatching() {
        return Objects.equals(getDbUUID(), PrivilegeBuiltinConstants.ALL_DATABASES_UUID) ||
                Objects.equals(getId(), PrivilegeBuiltinConstants.ALL_PIPES_ID);
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        Database db = globalStateMgr.getDbIncludeRecycleBin(Long.parseLong(this.dbUUID));
        if (db == null) {
            return false;
        }
        return globalStateMgr.getPipeManager().mayGetPipe(getId()).isPresent();
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof PipePEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }
        PipePEntryObject o = (PipePEntryObject) obj;
        // other > all
        if (Objects.equals(this.dbUUID, o.getDbUUID())) {
            if (Objects.equals(this.getId(), o.getId())) {
                return 0;
            } else if (Objects.equals(getId(), PrivilegeBuiltinConstants.ALL_PIPES_ID)) {
                return -1;
            } else if (Objects.equals(o.getId(), PrivilegeBuiltinConstants.ALL_PIPES_ID)) {
                return 1;
            } else {
                return Long.compare(getId(), o.getId());
            }
        } else if (Objects.equals(getDbUUID(), PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
            return -1;
        } else if (Objects.equals(o.getDbUUID(), PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
            return 1;
        } else {
            return getDbUUID().compareTo(o.getDbUUID());
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
        PipePEntryObject that = (PipePEntryObject) o;
        return Objects.equals(id, that.getId()) && Objects.equals(dbUUID, that.dbUUID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), dbUUID);
    }

    @Override
    public PEntryObject clone() {
        return new PipePEntryObject(getDbUUID(), getId());
    }

    public Optional<Database> getDatabase() {
        try {
            if (getDbUUID().equals(PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
                return Optional.empty();
            }
            long dbId = Long.parseLong(getDbUUID());
            return GlobalStateMgr.getCurrentState().mayGetDb(dbId);
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    public Optional<Pipe> getPipe() {
        if (getId() == PrivilegeBuiltinConstants.ALL_PIPES_ID) {
            return Optional.empty();
        }
        return GlobalStateMgr.getCurrentState().getPipeManager().mayGetPipe(getId());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (Objects.equals(getDbUUID(), PrivilegeBuiltinConstants.ALL_DATABASES_UUID)) {
            sb.append("ALL ").append("DATABASES");
        } else {
            String dbName;
            Database database = GlobalStateMgr.getCurrentState().getDb(Long.parseLong(getDbUUID()));
            if (database == null) {
                throw new MetaNotFoundException("Cannot find database : " + getDbUUID());
            }
            dbName = database.getFullName();

            if (Objects.equals(getId(), PrivilegeBuiltinConstants.ALL_PIPES_ID)) {
                sb.append("ALL PIPES ").append(" IN DATABASE ").append(dbName);
            } else {
                Pipe pipe = getPipe().orElseThrow(() -> new MetaNotFoundException("pipe not found: " + getId()));
                sb.append(dbName).append(".").append(pipe.getName());
            }
        }

        return sb.toString();
    }
}
