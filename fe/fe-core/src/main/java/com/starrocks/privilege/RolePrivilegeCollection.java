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

import java.util.HashSet;
import java.util.Set;

/**
 * Deprecated class, can be removed in version 3.2
 */
@Deprecated
public class RolePrivilegeCollection extends PrivilegeCollection {
    // the name of the role
    @SerializedName(value = "n")
    private String name;
    // see RoleFlags
    @SerializedName(value = "ma")
    private long mask;
    @SerializedName(value = "p")
    private Set<Long> parentRoleIds;
    @SerializedName(value = "s")
    private Set<Long> subRoleIds;
    @SerializedName(value = "c")
    private String comment;

    public enum RoleFlags {
        MUTABLE(1),
        REMOVABLE(2);

        private long mask;

        RoleFlags(int m) {
            this.mask = 1L << m;
        }
    }

    // only when deserialized
    protected RolePrivilegeCollection() {
        this.name = "";
        this.mask = 0;
        this.parentRoleIds = new HashSet<>();
        this.subRoleIds = new HashSet<>();
        this.comment = "";
    }

    public RolePrivilegeCollection(String name, RolePrivilegeCollection.RoleFlags... flags) {
        this(name, null, flags);
    }

    public RolePrivilegeCollection(String name, String comment, RolePrivilegeCollection.RoleFlags... flags) {
        this.name = name;
        for (RolePrivilegeCollection.RoleFlags flag : flags) {
            this.mask |= flag.mask;
        }
        this.parentRoleIds = new HashSet<>();
        this.subRoleIds = new HashSet<>();
        this.comment = comment;
    }

    private void assertMutable() throws PrivilegeException {
        if (!checkFlag(RolePrivilegeCollection.RoleFlags.MUTABLE)) {
            throw new PrivilegeException("role " + name + " is not mutable!");
        }
    }

    public void disableMutable() {
        this.mask &= ~RolePrivilegeCollection.RoleFlags.MUTABLE.mask;
    }

    public boolean isRemovable() {
        return checkFlag(RolePrivilegeCollection.RoleFlags.REMOVABLE);
    }

    public boolean isMutable() {
        return checkFlag(RolePrivilegeCollection.RoleFlags.MUTABLE);
    }

    public String getName() {
        return name;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    private boolean checkFlag(RolePrivilegeCollection.RoleFlags flag) {
        return (this.mask & flag.mask) != 0;
    }

    public void addParentRole(long parentRoleId) throws PrivilegeException {
        assertMutable();
        parentRoleIds.add(parentRoleId);
    }

    public Set<Long> getParentRoleIds() {
        return parentRoleIds;
    }

    public void addSubRole(long subRoleId) {
        subRoleIds.add(subRoleId);
    }

    public Set<Long> getSubRoleIds() {
        return subRoleIds;
    }
}
