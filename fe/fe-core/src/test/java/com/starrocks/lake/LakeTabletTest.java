// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class LakeTabletTest {
    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Test
    public void testSerialization() throws Exception {
        new Expectations(globalStateMgr) {
            {
                GlobalStateMgr.getCurrentStateJournalVersion();
                minTimes = 0;
                result = FeConstants.meta_version;
            }
        };

        LakeTablet tablet = new LakeTablet(1L);
        tablet.setDataSize(3L);
        tablet.setRowCount(4L);

        // Serialize
        File file = new File("./LakeTabletSerializationTest");
        file.createNewFile();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file))) {
            tablet.write(dos);
            dos.flush();
        }

        // Deserialize
        LakeTablet newTablet = null;
        try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
            newTablet = LakeTablet.read(dis);
        }

        // Check
        Assert.assertNotNull(newTablet);
        Assert.assertEquals(1L, newTablet.getId());
        Assert.assertEquals(1L, newTablet.getShardId());
        Assert.assertEquals(3L, newTablet.getDataSize(true));
        Assert.assertEquals(4L, newTablet.getRowCount(0L));

        file.delete();
    }
}
