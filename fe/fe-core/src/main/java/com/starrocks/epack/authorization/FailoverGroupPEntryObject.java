// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.authorization;

import com.google.gson.annotations.SerializedName;
import com.starrocks.authorization.PEntryObject;
import com.starrocks.authorization.PrivObjNotFoundException;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaNotFoundException;

import java.util.List;
import java.util.Objects;

public class FailoverGroupPEntryObject implements PEntryObject {

    @SerializedName(value = "i")
    private long id;

    public long getId() {
        return id;
    }

    public static FailoverGroupPEntryObject generate(GlobalStateMgr mgr,
            List<String> tokens) throws PrivilegeException {
        if (tokens.size() != 1) {
            throw new PrivilegeException("invalid object tokens, should have only one, token: " + tokens);
        }
        String name = tokens.get(0);
        if (name.equals("*")) {
            return new FailoverGroupPEntryObject(PrivilegeBuiltinConstantsEPack.ALL_FAILOVER_GROUPS_ID);
        } else {
            FailoverGroup failoverGroup = mgr.getFailoverGroupMgr().getFailoverGroup(name);
            if (failoverGroup == null) {
                throw new PrivObjNotFoundException("cannot find failoverGroup: " + name);
            }
            return new FailoverGroupPEntryObject(failoverGroup.getId());
        }
    }

    protected FailoverGroupPEntryObject(long id) {
        this.id = id;
    }

    /**
     * if the current failover group matches other failover group, including fuzzy
     * matching.
     * this(failoverGroup1), other(failoverGroup1) -> true
     * this(failoverGroup1), other(ALL) -> true
     * this(ALL), other(failoverGroup1) -> false
     */
    @Override
    public boolean match(Object obj) {
        if (!(obj instanceof FailoverGroupPEntryObject)) {
            return false;
        }
        FailoverGroupPEntryObject other = (FailoverGroupPEntryObject) obj;
        if (other.id == PrivilegeBuiltinConstantsEPack.ALL_FAILOVER_GROUPS_ID) {
            return true;
        }
        return other.id == id;
    }

    @Override
    public boolean isFuzzyMatching() {
        return PrivilegeBuiltinConstantsEPack.ALL_FAILOVER_GROUPS_ID == id;
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        return globalStateMgr.getFailoverGroupMgr().getFailoverGroup(id) != null;
    }

    @Override
    public int compareTo(PEntryObject obj) {
        if (!(obj instanceof FailoverGroupPEntryObject)) {
            throw new ClassCastException("cannot cast " + obj.getClass().toString() + " to " + this.getClass());
        }
        FailoverGroupPEntryObject o = (FailoverGroupPEntryObject) obj;
        return Long.compare(this.id, o.id);
    }

    @Override
    public PEntryObject clone() {
        return new FailoverGroupPEntryObject(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FailoverGroupPEntryObject that = (FailoverGroupPEntryObject) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        if (getId() == PrivilegeBuiltinConstantsEPack.ALL_FAILOVER_GROUPS_ID) {
            return "ALL FAILOVER GROUPS";
        } else {
            FailoverGroup failoverGroup = GlobalStateMgr.getCurrentState().getFailoverGroupMgr()
                    .getFailoverGroup(getId());
            if (failoverGroup == null) {
                throw new MetaNotFoundException("Can't find failoverGroup : " + id);
            }
            return failoverGroup.getName();
        }
    }
}
