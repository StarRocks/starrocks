// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.privilege;

import com.starrocks.server.GlobalStateMgr;

/**
 * Each `GRANT`/`REVOKE` statement will create some privilege entry object
 * For example, `GRANT SELECT ON TABLE db1.tbl1 TO user1` will create an object like (db1, tbl1)
 * Thus when user1 executes `SELECT * FROM db1.tbl1`, `PrivilegeChecker` will try to find a matching one in all the table
 * object and check if SELECT action is granted on it.
 * If the GRANT statement contains `ALL`, for example, `GRANT SELECT ON ALL TABLES IN DATABASE db1 TO user1`, a slightly
 * different object like (db1, ALL) will be created to represent fuzzy matching.
 * Another scenario that will take usage of this fuzzy matching is when user1 execute statement like `USE DATABASE db1`.
 * We shall create a (db1, ALL) to check if there's any table in the database db1 that user1 have any privilege on.
 *
 * The matching rule on the above description can be simplified as this:
 * (db1, tbl1) matches (db1, tbl1)
 * (db1, ALL) matches (db1, tbl1)
 * (db1, tbl1) matches (db1, ALL)
 **/
public interface PEntryObject extends Comparable<PEntryObject>, Cloneable {

    /**
     * if the specific object matches current object, including fuzzy matching.
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

    /**
     * used to deep copy when merging Privilege collections
     **/
    Object clone();
}
