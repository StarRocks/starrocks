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

package com.starrocks.authorization;

import com.google.gson.annotations.SerializedName;
import com.starrocks.context.ContextMgr;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Objects;

public class ContextBasePEntryObject implements PEntryObject {
    @SerializedName(value = "id")
    private final long id;

    protected ContextBasePEntryObject(long id) {
        this.id = id;
    }

    public static PEntryObject generate(List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("invalid object tokens, should have one: " + tokens);
        }
        String name = tokens.get(0);
        if (name.equals("*")) {
            return new ContextBasePEntryObject(PrivilegeBuiltinConstants.ALL_CONTEXTBASES_ID);
        }
        ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
        ContextMgr.ContextBaseMeta meta = mgr == null ? null : mgr.getContextBase(name);
        if (meta == null) {
            throw new PrivObjNotFoundException("cannot find contextbase: " + name);
        }
        return new ContextBasePEntryObject(meta.getId());
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof ContextBasePEntryObject)) {
            return false;
        }
        ContextBasePEntryObject other = (ContextBasePEntryObject) obj;
        if (other.id == PrivilegeBuiltinConstants.ALL_CONTEXTBASES_ID) {
            return true;
        }
        return other.id == id;
    }

    @Override
    public boolean isFuzzyMatching() {
        return id == PrivilegeBuiltinConstants.ALL_CONTEXTBASES_ID;
    }

    @Override
    public boolean validate() {
        if (id == PrivilegeBuiltinConstants.ALL_CONTEXTBASES_ID) {
            return true;
        }
        ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
        if (mgr == null) {
            return false;
        }
        for (ContextMgr.ContextBaseMeta meta : mgr.listContextBases()) {
            if (meta.getId() == id) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof ContextBasePEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }
        ContextBasePEntryObject o = (ContextBasePEntryObject) obj;
        if (id == PrivilegeBuiltinConstants.ALL_CONTEXTBASES_ID) {
            return o.id == PrivilegeBuiltinConstants.ALL_CONTEXTBASES_ID ? 0 : 1;
        }
        if (o.id == PrivilegeBuiltinConstants.ALL_CONTEXTBASES_ID) {
            return -1;
        }
        return Long.compare(id, o.id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContextBasePEntryObject that = (ContextBasePEntryObject) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public PEntryObject clone() {
        return new ContextBasePEntryObject(id);
    }

    @Override
    public String toString() {
        if (id == PrivilegeBuiltinConstants.ALL_CONTEXTBASES_ID) {
            return "ALL CONTEXTBASES";
        }
        ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
        if (mgr != null) {
            for (ContextMgr.ContextBaseMeta meta : mgr.listContextBases()) {
                if (meta.getId() == id) {
                    return meta.getName();
                }
            }
        }
        return "contextbase[" + id + "]";
    }
}
