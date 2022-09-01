// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.ShardManager;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Set;
public class ShardManagerTest {
    @Test
    public void test() {
        ShardManager info = new ShardManager();
        Assert.assertNotNull(info.getShardDeleter());
    }

    @Test
    public void testLoadAndSaveShardDeleteInfo() throws Exception {
        ShardManager info = new ShardManager();
        File tempFile = File.createTempFile("ShardManagerTest", ".image");
        System.err.println("write image " + tempFile.getAbsolutePath());
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile));
        long checksum = 0;
        long saveChecksum = info.saveShardManager(dos, checksum);
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream(tempFile));
        long loadChecksum = GlobalStateMgr.getCurrentState().loadShardManager(dis, checksum);
        Assert.assertEquals(saveChecksum, loadChecksum);
        Set<Long> shardIds = Deencapsulation.getField(
                GlobalStateMgr.getCurrentState().getShardManager().getShardDeleter(), "shardIds");
        Assert.assertEquals(Deencapsulation.getField(info.getShardDeleter(), "shardIds"), shardIds);
    }

}

