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
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Objects;

public class ResourcePEntryObject implements PEntryObject {
    @SerializedName(value = "n")
    String name;  // can be null, means all resources

    public String getName() {
        return name;
    }

    public static ResourcePEntryObject generate(GlobalStateMgr mgr, List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("invalid object tokens, should have one: " + tokens);
        }
        String name = tokens.get(0);
        if (name.equals("*")) {
            return new ResourcePEntryObject(null);
        } else {
            if (!mgr.getResourceMgr().containsResource(name)) {
                throw new PrivObjNotFoundException("cannot find resource: " + tokens.get(0));
            }
            return new ResourcePEntryObject(name);
        }
    }

    protected ResourcePEntryObject(String name) {
        this.name = name;
    }

    /**
     * if the current resource matches other resource, including fuzzy matching.
     * <p>
     * this(hive0), other(hive0) -> true
     * this(hive0), other(ALL) -> true
     * this(ALL), other(hive0) -> false
     */
    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof ResourcePEntryObject)) {
            return false;
        }
        ResourcePEntryObject other = (ResourcePEntryObject) obj;
        if (other.name == null) {
            return true;
        }
        return other.name.equals(name);
    }

    @Override
    public boolean isFuzzyMatching() {
        return name == null;
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        return globalStateMgr.getResourceMgr().containsResource(name);
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof ResourcePEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }
        ResourcePEntryObject o = (ResourcePEntryObject) obj;
        // other > all
        if (name == null) {
            if (o.name == null) {
                return 0;
            } else {
                return 1;
            }
        }
        if (o.name == null) {
            return -1;
        }
        return name.compareTo(o.name);
    }

    @Override
    public PEntryObject clone() {
        return new ResourcePEntryObject(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourcePEntryObject that = (ResourcePEntryObject) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        if (name == null) {
            return "ALL RESOURCES";
        } else {
            return name;
        }
    }
}
