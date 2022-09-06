// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.mysql.privilege;

import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;

public class ImpersonateUserPrivTableTest {
    private static final Logger LOG = LogManager.getLogger(ImpersonateUserPrivTableTest.class);

    /**
     * test serialized & deserialized
     */
    @Test
    public void testSerializedAndDeserialized() throws Exception {
        ImpersonateUserPrivTable table = new ImpersonateUserPrivTable();
        PrivPredicate ONLY_IMPERSONATE = PrivPredicate.of(PrivBitSet.of(Privilege.IMPERSONATE_PRIV), CompoundPredicate.Operator.AND);
        // Polyjuice Potion

        // 1. grant impersonate on Gregory to Harry
        // 1.1 grant
        UserIdentity harry = UserIdentity.createAnalyzedUserIdentWithIp("Harry", "%");
        UserIdentity gregory = UserIdentity.createAnalyzedUserIdentWithIp("Gregory", "%");
        ImpersonateUserPrivEntry entry = ImpersonateUserPrivEntry.create(harry, gregory);
        table.addEntry(entry, true, false);
        Assert.assertEquals(1, table.size());
        LOG.info("current table: {}", table);

        // 1.2 dump to entries
        List<ImpersonateUserPrivEntry> entries = table.dumpEntries();

        // 1.3 load from entries
        ImpersonateUserPrivTable loadTable = new ImpersonateUserPrivTable();
        loadTable.loadEntries(entries);
        LOG.info("load table: {}", loadTable);

        // 1.4 check & cleanup
        Assert.assertEquals(1, table.size());
        PrivBitSet privBitSet = new PrivBitSet();
        loadTable.getPrivs(harry, gregory, privBitSet);
        Assert.assertTrue(privBitSet.satisfy(ONLY_IMPERSONATE));
        Assert.assertTrue(loadTable.canImpersonate(harry, gregory));

        // 2. grant impersonate on Albert to Harry
        // 2.1 grant
        UserIdentity albert = UserIdentity.createAnalyzedUserIdentWithIp("Albert", "%");
        entry = ImpersonateUserPrivEntry.create(harry, albert);
        table.addEntry(entry, true, false);
        LOG.info("current table: {}", table);

        // 2.2. dump to entries
        entries = table.dumpEntries();

        // 2.3 load from entries
        loadTable = new ImpersonateUserPrivTable();
        loadTable.loadEntries(entries);
        LOG.info("load table: {}", loadTable);

        // 2.4 check & cleanup
        Assert.assertEquals(2, table.size());
        privBitSet = new PrivBitSet();
        loadTable.getPrivs(harry, gregory, privBitSet);
        Assert.assertTrue(privBitSet.satisfy(ONLY_IMPERSONATE));
        Assert.assertTrue(loadTable.canImpersonate(harry, gregory));
        privBitSet = new PrivBitSet();
        loadTable.getPrivs(harry, albert, privBitSet);
        Assert.assertTrue(privBitSet.satisfy(ONLY_IMPERSONATE));
        Assert.assertTrue(loadTable.canImpersonate(harry, albert));

        // 3. grant impersonate on Vincent to Ron
        // 3.1 grant
        UserIdentity vincent = UserIdentity.createAnalyzedUserIdentWithIp("Vincent", "%");
        UserIdentity ron = UserIdentity.createAnalyzedUserIdentWithIp("Ron", "%");
        entry = ImpersonateUserPrivEntry.create(ron, vincent);
        table.addEntry(entry, true, false);
        LOG.info("current table: {}", table);

        // 3.2. dump to entries
        entries = table.dumpEntries();

        // 3.3 load from entries
        loadTable = new ImpersonateUserPrivTable();
        loadTable.loadEntries(entries);
        LOG.info("load table: {}", loadTable);

        // 3.4 check & cleanup
        Assert.assertEquals(3, table.size());
        privBitSet = new PrivBitSet();
        loadTable.getPrivs(harry, gregory, privBitSet);
        Assert.assertTrue(privBitSet.satisfy(ONLY_IMPERSONATE));
        Assert.assertTrue(loadTable.canImpersonate(harry, gregory));
        privBitSet = new PrivBitSet();
        loadTable.getPrivs(harry, albert, privBitSet);
        Assert.assertTrue(privBitSet.satisfy(ONLY_IMPERSONATE));
        Assert.assertTrue(loadTable.canImpersonate(harry, albert));
        privBitSet = new PrivBitSet();
        loadTable.getPrivs(ron, vincent, privBitSet);
        Assert.assertTrue(privBitSet.satisfy(ONLY_IMPERSONATE));
        Assert.assertTrue(loadTable.canImpersonate(ron, vincent));
    }
}
