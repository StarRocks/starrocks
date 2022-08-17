// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.mysql.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * ImpersonateUserPrivTable saves all impersonate user privileges
 */
public class ImpersonateUserPrivTable extends PrivTable {
    private static final Logger LOG = LogManager.getLogger(ImpersonateUserPrivTable.class);

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

    public static ImpersonateUserPrivTable read(DataInput in) throws IOException {
        ImpersonateUserPrivTable table = new ImpersonateUserPrivTable();
        SerializeData data = GsonUtils.GSON.fromJson(Text.readString(in), SerializeData.class);
        for (ImpersonateUserPrivEntry entry : data.entries) {
            try {
                entry.analyse();
            } catch (AnalysisException e) {
                // TODO This is ugly, somewhere above AnalysisException should be allowed
                throw new IOException(e);
            }
            UserIdentity newUser = entry.getUserIdent();
            List<PrivEntry> entries = table.map.computeIfAbsent(newUser, k -> new ArrayList<>());
            entries.add(entry);
        }
        return table;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        SerializeData data = new SerializeData();
        Iterator<PrivEntry> iter = this.getFullReadOnlyIterator();
        while (iter.hasNext()) {
            ImpersonateUserPrivEntry entry = (ImpersonateUserPrivEntry) iter.next();
            data.entries.add(entry);
        }
        Text.writeString(out, GsonUtils.GSON.toJson(data));
    }

    private static class SerializeData {
        @SerializedName("entries")
        public List<ImpersonateUserPrivEntry> entries = new ArrayList<>();
    }
}
