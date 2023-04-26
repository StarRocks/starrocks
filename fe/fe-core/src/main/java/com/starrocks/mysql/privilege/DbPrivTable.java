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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/privilege/DbPrivTable.java

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

import com.starrocks.common.io.Text;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/*
 * DbPrivTable saves all database level privs
 */
public class DbPrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(DbPrivTable.class);

    /*
     * Return first priv which match the user@host on db.* The returned priv will be
     * saved in 'savedPrivs'.
     */
    public void getPrivs(UserIdentity currentUser, String db, PrivBitSet savedPrivs) {
        List<PrivEntry> userPrivEntryList = map.get(currentUser);
        if (userPrivEntryList == null) {
            return;
        }

        DbPrivEntry matchedEntry = null;
        for (PrivEntry entry : userPrivEntryList) {
            DbPrivEntry dbPrivEntry = (DbPrivEntry) entry;

            // check db
            if (!dbPrivEntry.isAnyDb() && !dbPrivEntry.getDbPattern().match(db)) {
                continue;
            }

            matchedEntry = dbPrivEntry;
            break;
        }
        if (matchedEntry == null) {
            return;
        }

        savedPrivs.or(matchedEntry.getPrivSet());
    }

    /*
     * Check if current user has specified privilege on any database
     */
    public boolean hasPriv(UserIdentity currentUser, PrivPredicate wanted) {
        Iterator<PrivEntry> iter = getReadOnlyIteratorByUser(currentUser);
        while (iter.hasNext()) {
            DbPrivEntry dbPrivEntry = (DbPrivEntry) iter.next();
            // check priv
            if (dbPrivEntry.privSet.satisfy(wanted)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = DbPrivTable.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }

        super.write(out);
    }
}
