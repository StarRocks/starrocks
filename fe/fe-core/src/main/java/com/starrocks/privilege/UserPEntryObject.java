// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Objects;

public class UserPEntryObject implements PEntryObject {
    @SerializedName(value = "u")
    private UserIdentity userIdentity;  // can be null, means all users
    protected UserPEntryObject(UserIdentity userIdentity) {
        this.userIdentity = userIdentity;
    }

    public static UserPEntryObject generate(GlobalStateMgr mgr, UserIdentity user) throws PrivilegeException {
        if (!mgr.getAuthenticationManager().doesUserExist(user)) {
            throw new PrivilegeException("cannot find user " + user);
        }
        return new UserPEntryObject(user);
    }

    public static UserPEntryObject generate(
            List<String> allTypes, String restrictType, String restrictName) throws PrivilegeException {
        // only support ON ALL USERS
        if (allTypes.size() != 1 || restrictType != null || restrictName != null) {
            throw new PrivilegeException("invalid ALL statement for user! only support ON ALL USERS");
        }
        return new UserPEntryObject(null);
    }

    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof UserPEntryObject)) {
            return false;
        }
        if (userIdentity == null) {
            return true; // this object is all
        }
        UserPEntryObject other = (UserPEntryObject) obj;
        return userIdentity.equals(other.userIdentity);
    }

    @Override
    public boolean isFuzzyMatching() {
        return userIdentity == null; // no fuzzy matching for user
    }

    /**
     * normally we check if a user exists by AuthenticationManager, but here we checked by PrivilegeManager to avoid deadlock.
     * lock order should always be:
     * AuthenticationManager.lock -> PrivilegeManager.userLock -> PrivilegeManager.roleLock
     * All validation are made in com.starrocks.privilege.PrivilegeManager#removeInvalidObject()
     */
    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        return globalStateMgr.getPrivilegeManager().getUserPrivilegeCollectionUnlockedAllowNull(userIdentity) != null;
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof UserPEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }
        UserPEntryObject o = (UserPEntryObject) obj;
        // other > all
        if (userIdentity == null) {
            if (o.userIdentity == null) {
                return 0;
            } else {
                return 1;
            }
        }
        if (o.userIdentity == null) {
            return -1;
        }
        return userIdentity.toString().compareTo(o.userIdentity.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserPEntryObject that = (UserPEntryObject) o;
        if (userIdentity == null) {
            return that.userIdentity == null;
        }
        if (that.userIdentity == null) {
            return false;
        }
        return userIdentity.equals(that.userIdentity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), userIdentity);
    }

    @Override
    public PEntryObject clone() {
        return new UserPEntryObject(userIdentity);
    }
}
