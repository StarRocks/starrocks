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
import com.starrocks.sql.ast.UserIdentity;

import java.util.Objects;

public class UserPEntryObject implements PEntryObject {
    @SerializedName(value = "u")
    private UserIdentity userIdentity;  // can be null, means all users

    protected UserPEntryObject(UserIdentity userIdentity) {
        this.userIdentity = userIdentity;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public static UserPEntryObject generate(GlobalStateMgr mgr, UserIdentity user) throws PrivilegeException {
        if (user == null) {
            return new UserPEntryObject(null);
        }

        if (!mgr.getAuthenticationManager().doesUserExist(user)) {
            throw new PrivObjNotFoundException("cannot find user " + user);
        }
        return new UserPEntryObject(user);
    }

    /**
     * if the current user matches other user, including fuzzy matching.
     * <p>
     * this(userx), other(userx) -> true
     * this(userx), other(ALL) -> true
     * this(ALL), other(userx) -> false
     */
    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof UserPEntryObject)) {
            return false;
        }
        UserPEntryObject other = (UserPEntryObject) obj;
        if (other.userIdentity == null) {
            return true; // this object is all
        }
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

    @Override
    public String toString() {
        if (userIdentity == null) {
            return "ALL USERS";
        } else {
            return userIdentity.toString();
        }
    }
}
