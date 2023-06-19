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
import java.util.List;
import java.util.Set;

public class RolePrivilegeCollectionV2 extends PrivilegeCollectionV2 {
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
    protected RolePrivilegeCollectionV2() {
        this.name = "";
        this.mask = 0;
        this.parentRoleIds = new HashSet<>();
        this.subRoleIds = new HashSet<>();
        this.comment = "";
    }

    public RolePrivilegeCollectionV2(String name, RoleFlags... flags) {
        this(name, null, flags);
    }

    public RolePrivilegeCollectionV2(String name, String comment, RoleFlags... flags) {
        this.name = name;
        for (RoleFlags flag : flags) {
            this.mask |= flag.mask;
        }
        this.parentRoleIds = new HashSet<>();
        this.subRoleIds = new HashSet<>();
        this.comment = comment;
    }

    private void assertMutable() throws PrivilegeException {
        if (!checkFlag(RoleFlags.MUTABLE)) {
            throw new PrivilegeException("role " + name + " is not mutable!");
        }
    }

    public void disableMutable() {
        this.mask &= ~RoleFlags.MUTABLE.mask;
    }

    public boolean isRemovable() {
        return checkFlag(RoleFlags.REMOVABLE);
    }

    public boolean isMutable() {
        return checkFlag(RoleFlags.MUTABLE);
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

    private boolean checkFlag(RoleFlags flag) {
        return (this.mask & flag.mask) != 0;
    }

    public void addParentRole(long parentRoleId) throws PrivilegeException {
        assertMutable();
        parentRoleIds.add(parentRoleId);
    }

    public void removeParentRole(long parentRoleId) throws PrivilegeException {
        assertMutable();
        parentRoleIds.remove(parentRoleId);
    }

    public Set<Long> getParentRoleIds() {
        return parentRoleIds;
    }

    public void addSubRole(long subRoleId) {
        subRoleIds.add(subRoleId);
    }

    public void removeSubRole(long subRoleId) {
        subRoleIds.remove(subRoleId);
    }

    public Set<Long> getSubRoleIds() {
        return subRoleIds;
    }

    @Override
    public void grant(ObjectType type, List<PrivilegeType> privilegeTypes, List<PEntryObject> objects, boolean isGrant)
            throws PrivilegeException {
        assertMutable();
        super.grant(type, privilegeTypes, objects, isGrant);
    }

    @Override
    public void revoke(ObjectType type, List<PrivilegeType> privilegeTypes, List<PEntryObject> objects)
            throws PrivilegeException {
        assertMutable();
        super.revoke(type, privilegeTypes, objects);
    }
}
