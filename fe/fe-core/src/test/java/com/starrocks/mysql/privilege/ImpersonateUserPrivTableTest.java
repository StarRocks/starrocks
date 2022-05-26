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

        // 1.2 dump to file
        File tempFile = File.createTempFile("ImpersonateUserPrivTableTest", ".image");
        LOG.info("dump to file {}", tempFile.getAbsolutePath());
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile));
        table.write(dos);
        dos.close();

        // 1.3 load from file
        DataInputStream dis = new DataInputStream(new FileInputStream(tempFile));
        ImpersonateUserPrivTable loadTable = ImpersonateUserPrivTable.read(dis);
        LOG.info("load table: {}", loadTable);

        // 1.4 check & cleanup
        Assert.assertEquals(1, table.size());
        PrivBitSet privBitSet = new PrivBitSet();
        loadTable.getPrivs(harry, gregory, privBitSet);
        Assert.assertTrue(privBitSet.satisfy(ONLY_IMPERSONATE));
        Assert.assertTrue(loadTable.canImpersonate(harry, gregory));
        tempFile.delete();

        // 2. grant impersonate on Albert to Harry
        // 2.1 grant
        UserIdentity albert = UserIdentity.createAnalyzedUserIdentWithIp("Albert", "%");
        entry = ImpersonateUserPrivEntry.create(harry, albert);
        table.addEntry(entry, true, false);
        LOG.info("current table: {}", table);

        // 2.2. dump to file
        tempFile = File.createTempFile("ImpersonateUserPrivTableTest", ".image");
        LOG.info("dump to file {}", tempFile.getAbsolutePath());
        dos = new DataOutputStream(new FileOutputStream(tempFile));
        table.write(dos);
        dos.close();

        // 2.3 load from file
        dis = new DataInputStream(new FileInputStream(tempFile));
        loadTable = ImpersonateUserPrivTable.read(dis);
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
        tempFile.delete();

        // 3. grant impersonate on Vincent to Ron
        // 3.1 grant
        UserIdentity vincent = UserIdentity.createAnalyzedUserIdentWithIp("Vincent", "%");
        UserIdentity ron = UserIdentity.createAnalyzedUserIdentWithIp("Ron", "%");
        entry = ImpersonateUserPrivEntry.create(ron, vincent);
        table.addEntry(entry, true, false);
        LOG.info("current table: {}", table);

        // 3.2 dump to file
        tempFile = File.createTempFile("ImpersonateUserPrivTableTest", ".image");
        LOG.info("dump to file {}", tempFile.getAbsolutePath());
        dos = new DataOutputStream(new FileOutputStream(tempFile));
        table.write(dos);
        dos.close();

        // 3.3 load from file
        dis = new DataInputStream(new FileInputStream(tempFile));
        loadTable = ImpersonateUserPrivTable.read(dis);
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
        tempFile.delete();
    }
}
