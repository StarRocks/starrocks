// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.starrocks.catalog.lake.ShardManager;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class ShardManagerTest {
    @Test
    public void test() {
        ShardManager info = new ShardManager();
        Assert.assertEquals(null, info.getShardDeleter());
    }

    @Test
    public void testLoadAndSaveShardDeleteInfo() throws Exception {
        ShardManager info = new ShardManager();
        File tempFile = File.createTempFile("ShardManagerTest", ".image");
        System.err.println("write image " + tempFile.getAbsolutePath());
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile));
        long checksum = 0;
        long saveChecksum = info.saveShardDeleteInfo(dos, checksum);
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream(tempFile));
        long loadChecksum = info.loadShardDeleteInfo(dis, checksum);
        Assert.assertEquals(saveChecksum, loadChecksum);
    }

}

