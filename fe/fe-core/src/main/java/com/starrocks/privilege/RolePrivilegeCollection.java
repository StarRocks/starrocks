// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;

import java.util.HashSet;
import java.util.List;
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

    private void assertMutable() throws PrivilegeException {
        if (! checkFlag(RoleFlags.MUTABLE)) {
            throw new PrivilegeException("role " + name + " is not mutable!");
        }
    }

    public void disableMutable() {
        this.mask &= ~RoleFlags.MUTABLE.mask;
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

    public void addSubRole(long subRoleId) throws PrivilegeException {
        assertMutable();
        subRoleIds.add(subRoleId);
    }

    public void removeSubRole(long subRoleId) throws PrivilegeException {
        assertMutable();
        subRoleIds.remove(subRoleId);
    }

    public Set<Long> getSubRoleIds() {
        return subRoleIds;
    }

    @Override
    public void grant(short type, ActionSet actionSet, List<PEntryObject> objects, boolean isGrant)
            throws PrivilegeException {
        assertMutable();
        super.grant(type, actionSet, objects, isGrant);
    }

    @Override
    public void revoke(short type, ActionSet actionSet, List<PEntryObject> objects, boolean isGrant)
            throws PrivilegeException {
        assertMutable();
        super.revoke(type, actionSet, objects, isGrant);
    }
}
