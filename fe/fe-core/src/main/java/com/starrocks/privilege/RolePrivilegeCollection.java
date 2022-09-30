// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

public class RolePrivilegeCollection extends PrivilegeCollection {
    // the name of the role
    private String name;
    // see RoleFlags
    private long mask = 0;

    enum RoleFlags {
        MUTABLE(1),
        REMOVABLE(2),
        DEFAULT(3);

        private long mask;
        RoleFlags(int m) {
            this.mask = 1L << m;
        }
    }

    public RolePrivilegeCollection(String name) {
        this.name = name;
    }

    public RolePrivilegeCollection(String name, RoleFlags... flags) {
        this.name = name;
        for (RoleFlags flag : flags) {
            this.mask |= flag.mask;
        }
    }

    public boolean isDefault() {
        return checkFlag(RoleFlags.DEFAULT);
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
}
