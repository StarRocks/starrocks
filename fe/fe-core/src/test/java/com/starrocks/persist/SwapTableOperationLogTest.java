// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class SwapTableOperationLogTest {
    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./SwapTableOperationLogTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        SwapTableOperationLog log = new SwapTableOperationLog(1, 2, 3);
        log.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        SwapTableOperationLog readLog = SwapTableOperationLog.read(dis);
        Assert.assertTrue(readLog.getDbId() == log.getDbId());
        Assert.assertTrue(readLog.getNewTblId() == log.getNewTblId());
        Assert.assertTrue(readLog.getOrigTblId() == log.getOrigTblId());

        // 3. delete files
        dis.close();
        file.delete();
    }
}
