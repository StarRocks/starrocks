// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.lang.NotImplementedException;

/**
 * interface is hard to serialized/deserialized using GSON
 * to simplify the implementation, the base class PEntryObject contains one data field called `id`.
 */
public class PEntryObject implements Comparable<PEntryObject> {
    @SerializedName(value = "i")
    protected long id;

    protected PEntryObject(long id) {
        this.id = id;
    }

    /**
     * if the specific object match current object, including fuzzy matching.
     * the default behavior is simply check if id is identical.
     */
    public boolean match(Object obj) {
        if (!(obj instanceof PEntryObject)) {
            return false;
        }
        PEntryObject other = (PEntryObject) obj;
        return other.id == this.id;
    }

    /**
     * return true if current object can be used for fuzzy matching
     * e.g. GRANT SELECT ON ALL TABLES IN DATABASE db1
     */
    public boolean isFuzzyMatching() {
        throw new NotImplementedException();
    }

    /**
     * validate this object to see if still exists
     * the default behavior is to do nothing
     */
    public boolean validate(GlobalStateMgr globalStateMgr) {
        return true;
    }

    /**
     * used in Collections.sort() to accelerate lookup
     * make sure fuzzy matching is smaller than precise matching
     */
    @Override
    public int compareTo(PEntryObject o) {
        throw new NotImplementedException();
    }
}