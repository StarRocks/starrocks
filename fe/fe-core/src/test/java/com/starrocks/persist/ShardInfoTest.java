// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import com.google.common.collect.Sets;

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
    }
}
