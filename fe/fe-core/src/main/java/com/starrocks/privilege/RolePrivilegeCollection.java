// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;

import java.util.HashSet;
import java.util.Set;

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

    enum RoleFlags {
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
    }

    public RolePrivilegeCollection(String name, RoleFlags... flags) {
        this.name = name;
        for (RoleFlags flag : flags) {
            this.mask |= flag.mask;
        }
        this.parentRoleIds = new HashSet<>();
        this.subRoleIds = new HashSet<>();
    }

    public boolean isMutable() {
        return checkFlag(RoleFlags.MUTABLE);
    }

    public boolean isRemovable() {
        return checkFlag(RoleFlags.REMOVABLE);
    }

    public String getName() {
        return name;
    }
    private boolean checkFlag(RoleFlags flag) {
        return (this.mask & flag.mask) != 0;
    }

    public void addParentRole(long parentRoleId) {
        parentRoleIds.add(parentRoleId);
    }

    public void removeParentRole(long parentRoleId) {
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
}
