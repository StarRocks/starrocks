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

import com.starrocks.server.GlobalStateMgr;

/**
 * Each `GRANT`/`REVOKE` statement will create some privilege entry object
 * For example, `GRANT SELECT ON TABLE db1.tbl1 TO user1` will create an object like (db1, tbl1)
 * Thus when user1 executes `SELECT * FROM db1.tbl1`, `PrivilegeChecker` will try to find a matching one in all the table
 * object and check if SELECT action is granted on it.
 *
 * If the GRANT statement contains `ALL`, for example, `GRANT SELECT ON ALL TABLES IN DATABASE db1 TO user1`, a slightly
 * different object like (db1, ALL) will be created to represent fuzzy matching.
 *
 * Another scenario that will take usage of this fuzzy matching is when user1 execute statement like `USE DATABASE db1`.
 * We shall create a (db1, ALL) to check if there's any table in the database db1 that user1 have any privilege on.
 *
 * But things will get a bit confused on fuzzy matching when checking allow grant. For example, if we execute
 *   GRANT SELECT ON db1.tbl1 TO userx with grant option
 * Then `userx` should be denied to execute
 *   GRANT SELECT ON *.* TO usery
 * In other words, fuzzy matching should be one-way matching.
 *
 * The matching rule on the above description can be simplified as this:
 * (db1, tbl1) matches (db1, tbl1)
 * (db1, ALL) matches (db1, tbl1)
 * (db1, tbl1) doesn't match (db1, ALL)
 **/
public interface PEntryObject extends Comparable<PEntryObject> {

    /**
     * if the current object matches other object, including fuzzy matching.
     *
     * this(db1.tbl1), other(db1.tbl1) -> true
     * this(db1.tbl1), other(db1.ALL) -> true
     * this(db1.ALL), other(db1.tbl1) -> false
     */
    boolean match(Object other);

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
    PEntryObject clone();
}
