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
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaNotFoundException;

import java.util.List;
import java.util.Objects;

public class ResourceGroupPEntryObject implements PEntryObject {

    @SerializedName(value = "i")
    private long id;

    public long getId() {
        return id;
    }

    public static ResourceGroupPEntryObject generate(GlobalStateMgr mgr,
                                                     List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("invalid object tokens, should have only one, token: " + tokens);
        }
        String name = tokens.get(0);
        if (name.equals("*")) {
            return new ResourceGroupPEntryObject(PrivilegeBuiltinConstants.ALL_RESOURCE_GROUP_ID);
        } else {
            ResourceGroup resourceGroup = mgr.getResourceGroupMgr().getResourceGroup(name);
            if (resourceGroup == null) {
                throw new PrivObjNotFoundException("cannot find resource group: " + name);
            }
            return new ResourceGroupPEntryObject(resourceGroup.getId());
        }
    }

    protected ResourceGroupPEntryObject(long id) {
        this.id = id;
    }

    /**
     * if the current resource group matches other resource group, including fuzzy matching.
     * <p>
     * this(resource_group1), other(resource_group1) -> true<p>
     * this(resource_group1), other(ALL) -> true<p>
     * this(ALL), other(resource_group1) -> false
     */
    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof ResourceGroupPEntryObject)) {
            return false;
        }
        ResourceGroupPEntryObject other = (ResourceGroupPEntryObject) obj;
        if (other.id == PrivilegeBuiltinConstants.ALL_RESOURCE_GROUP_ID) {
            return true;
        }
        return other.id == id;
    }

    @Override
    public boolean isFuzzyMatching() {
        return PrivilegeBuiltinConstants.ALL_RESOURCE_GROUP_ID == id;
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        return globalStateMgr.getResourceGroupMgr().getResourceGroup(id) != null;
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof ResourceGroupPEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }
        ResourceGroupPEntryObject o = (ResourceGroupPEntryObject) obj;
        return Long.compare(this.id, o.id);
    }

    @Override
    public PEntryObject clone() {
        return new ResourceGroupPEntryObject(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceGroupPEntryObject that = (ResourceGroupPEntryObject) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        if (getId() == PrivilegeBuiltinConstants.ALL_RESOURCE_GROUP_ID) {
            return "ALL RESOURCE GROUPS";
        } else {
            ResourceGroup resourceGroup = GlobalStateMgr.getCurrentState().getResourceGroupMgr().getResourceGroup(getId());
            if (resourceGroup == null) {
                throw new MetaNotFoundException("Can't find resource group : " + id);
            }
            return resourceGroup.getName();
        }
    }
}
