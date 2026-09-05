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
// See the License for the specific

package com.starrocks.authorization;

import com.google.gson.annotations.SerializedName;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.common.MetaNotFoundException;
import com.starrocks.warehouse.Warehouse;

import java.util.List;
import java.util.Objects;

public class WarehousePEntryObject implements PEntryObject {

    @SerializedName(value = "i")
    private long id;

    public long getId() {
        return id;
    }

    public static WarehousePEntryObject generate(List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("invalid object tokens, should have only one, token: " + tokens);
        }
        String name = tokens.get(0);
        if (name.equals("*")) {
            return new WarehousePEntryObject(PrivilegeBuiltinConstants.ALL_WAREHOUSES_ID);
        } else {
            WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            Warehouse warehouse = warehouseMgr.getWarehouseAllowNull(name);
            if (warehouse == null) {
                throw new PrivObjNotFoundException("cannot find warehouse: " + name);
            }
            return new WarehousePEntryObject(warehouse.getId());
        }
    }

    protected WarehousePEntryObject(long id) {
        this.id = id;
    }

    /**
     * if the current warehouse matches other warehouse, including fuzzy matching.
     * <p>
     * this(warehouse1), other(warehouse1) -> true<p>
     * this(warehouse1), other(ALL) -> true<p>
     * this(ALL), other(warehouse1) -> false
     */
    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof WarehousePEntryObject)) {
            return false;
        }
        WarehousePEntryObject other = (WarehousePEntryObject) obj;
        if (other.id == PrivilegeBuiltinConstants.ALL_WAREHOUSES_ID) {
            return true;
        }
        return other.id == id;
    }

    @Override
    public boolean isFuzzyMatching() {
        return PrivilegeBuiltinConstants.ALL_WAREHOUSES_ID == id;
    }

    @Override
    public boolean validate() {
        return GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(id) != null;
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof WarehousePEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }
        WarehousePEntryObject o = (WarehousePEntryObject) obj;
        return Long.compare(this.id, o.id);
    }

    @Override
    public PEntryObject clone() {
        return new WarehousePEntryObject(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WarehousePEntryObject that = (WarehousePEntryObject) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        if (getId() == PrivilegeBuiltinConstants.ALL_WAREHOUSES_ID) {
            return "ALL WAREHOUSES";
        } else {
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(getId());
            if (warehouse == null) {
                throw new MetaNotFoundException("Can't find warehouse : " + id);
            }
            return warehouse.getName();
        }
    }
}
