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


package com.starrocks.mysql.privilege;

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.UserIdentity;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * ImpersonateUserPrivTable saves all impersonate user privileges
 */
public class ImpersonateUserPrivTable extends PrivTable {
    /**
     * Return first priv which match the user@host on securedUser
     * The returned priv will be saved in 'savedPrivs'.
     **/
    public void getPrivs(UserIdentity currentUser, UserIdentity securedUser, PrivBitSet savedPrivs) {
        List<PrivEntry> userPrivEntryList = map.get(currentUser);
        if (userPrivEntryList == null) {
            return;
        }

        ImpersonateUserPrivEntry matchedEntry = null;
        for (PrivEntry entry : userPrivEntryList) {
            ImpersonateUserPrivEntry impersonateUserEntry = (ImpersonateUserPrivEntry) entry;

            // check secured user
            if (!impersonateUserEntry.getSecuredUserIdentity().equals(securedUser)) {
                continue;
            }

            matchedEntry = impersonateUserEntry;
            break;
        }
        if (matchedEntry == null) {
            return;
        }

        savedPrivs.or(matchedEntry.getPrivSet());
    }

    /**
     * check if currentUser can EXECUTE AS toUser
     */
    public boolean canImpersonate(UserIdentity currentUser, UserIdentity toUser) {
        PrivBitSet privBitSet = new PrivBitSet();
        getPrivs(currentUser, toUser, privBitSet);
        return privBitSet.satisfy(PrivPredicate.IMPERSONATE);
    }

    protected void loadEntries(List<ImpersonateUserPrivEntry> dataEntries) throws AnalysisException {
        for (ImpersonateUserPrivEntry entry : dataEntries) {
            entry.analyse();
            UserIdentity newUser = entry.getUserIdent();
            List<PrivEntry> entries = this.map.computeIfAbsent(newUser, k -> new ArrayList<>());
            entries.add(entry);
        }
    }

    public List<ImpersonateUserPrivEntry> dumpEntries() {
        List<ImpersonateUserPrivEntry> dataEntries = new ArrayList<>();
        Iterator<PrivEntry> iter = this.getFullReadOnlyIterator();
        while (iter.hasNext()) {
            ImpersonateUserPrivEntry entry = (ImpersonateUserPrivEntry) iter.next();
            dataEntries.add(entry);
        }
        return dataEntries;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new IOException("not implement");
    }


}
