// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import com.google.common.collect.Sets;
import com.starrocks.journal.JournalEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Set;

public class ShardInfoTest {

    private static final Logger LOG = LogManager.getLogger(ImpersonatePrivInfoTest.class);

    private String fileName = "./ShardInfoTest";

    @Test
    public void test() {
        ShardInfo info = new ShardInfo();
        Assert.assertNotNull(info.getShardIds());
    }

    @Test
    public void testSerialization() throws IOException {
        Set<Long> shardIds = Sets.newHashSet();
        shardIds.add(1L);
        shardIds.add(2L);

        ShardInfo info = new ShardInfo(shardIds);
        File file = new File(fileName);
        file.createNewFile();

        DataOutputStream out = new DataOutputStream(new FileOutputStream(fileName));
        info.write(out);
        out.close();

        DataInputStream in = new DataInputStream(new FileInputStream(fileName));
        info = info.read(in);
        in.close();

        Assert.assertEquals(info.getShardIds(), shardIds);

        // dump to file
        File tempFile = File.createTempFile("ShardInfoTest", ".image");
        LOG.info("dump to file {}", tempFile.getAbsolutePath());
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(tempFile));
        JournalEntity je = new JournalEntity();
        je.setData(info);
        je.write(dos);

        // load from file
        DataInputStream dis = new DataInputStream(new FileInputStream(tempFile));
        JournalEntity jeReader = new JournalEntity();
        jeReader.readFields(dis);
        info = (ShardInfo) jeReader.getData();
        Assert.assertEquals(info.getShardIds(), shardIds);


        Set<Long> shardIds2 = Sets.newHashSet();
        shardIds.add(3L);
        shardIds.add(4L);
        je.setData(new ShardInfo(shardIds2));
        je.write(dos);
        dos.close();

        jeReader.readFields(dis);
        info = (ShardInfo) jeReader.getData();
        Assert.assertEquals(info.getShardIds(), shardIds2);
    }
}
