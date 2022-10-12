// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.starrocks.server.GlobalStateMgr;

public interface PEntryObject extends Comparable<PEntryObject> {

    /**
     * if the specific object match current object, including fuzzy matching.
     * the default behavior is simply check if id is identical.
     */
    boolean match(Object obj);

    /**
     * return true if current object can be used for fuzzy matching
     * e.g. GRANT SELECT ON ALL TABLES IN DATABASE db1
     */
    boolean isFuzzyMatching();

    /**
     * validate this object to see if still exists
     */
    boolean validate(GlobalStateMgr globalStateMgr);

    /**
     * used in Collections.sort() to accelerate lookup
     * make sure fuzzy matching is smaller than precise matching
     */
    int compareTo(PEntryObject o);

    /**
     * used to find the exact entry when grant/revoke
     */
    boolean equals(Object o);

    int hashCode();
}