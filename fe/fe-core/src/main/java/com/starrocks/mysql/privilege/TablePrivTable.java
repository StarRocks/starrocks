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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/privilege/TablePrivTable.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.mysql.privilege;

import com.google.common.base.Preconditions;
import com.starrocks.common.io.Text;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/*
 * TablePrivTable saves all table level privs
 */
public class TablePrivTable extends PrivTable {

    /*
     * Return first priv which match current user on db.tbl The returned priv will
     * be saved in 'savedPrivs'.
     */
    public void getPrivs(UserIdentity currentUser, String db, String tbl, PrivBitSet savedPrivs) {
        List<PrivEntry> userPrivEntryList = map.get(currentUser);
        if (userPrivEntryList == null) {
            return;
        }

        TablePrivEntry matchedEntry = null;
        for (PrivEntry entry : userPrivEntryList) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) entry;
            // check db
            Preconditions.checkState(!tblPrivEntry.isAnyDb());
            if (!tblPrivEntry.getDbPattern().match(db)) {
                continue;
            }

            // check table
            if (!tblPrivEntry.getTblPattern().match(tbl)) {
                continue;
            }

            matchedEntry = tblPrivEntry;
            break;
        }
        if (matchedEntry == null) {
            return;
        }

        savedPrivs.or(matchedEntry.getPrivSet());
    }

    /*
     * Check if user has specified privilege on any table
     * TODO I can't think of any occasion that requires the privilege of ANY table,
     *      nor can I find such usage through all codes.
     *      I will try to remove this in another PR.
     */
    public boolean hasPriv(UserIdentity currentUser, PrivPredicate wanted) {
        Iterator<PrivEntry> iter = getReadOnlyIteratorByUser(currentUser);
        while (iter.hasNext()) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) iter.next();
            // check priv
            if (tblPrivEntry.privSet.satisfy(wanted)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasPrivsOfDb(UserIdentity currentUser, String db) {
        Iterator<PrivEntry> iter = getReadOnlyIteratorByUser(currentUser);
        while (iter.hasNext()) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) iter.next();
            // check db
            Preconditions.checkState(!tblPrivEntry.isAnyDb());
            if (!tblPrivEntry.getDbPattern().match(db)) {
                continue;
            }

            return true;
        }
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = TablePrivTable.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }

        super.write(out);
    }

    public boolean hasClusterPriv(ConnectContext ctx, String clusterName) {
        Iterator<PrivEntry> iter = this.getFullReadOnlyIterator();
        while (iter.hasNext()) {
            TablePrivEntry tblPrivEntry = (TablePrivEntry) iter.next();
            if (tblPrivEntry.getOrigDb().startsWith(clusterName)) {
                return true;
            }
        }
        return false;
    }
}
