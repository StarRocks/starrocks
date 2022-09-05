// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class RenameMaterializedViewLogTest {

    private String fileName = "./RenameMaterializedViewLogTest";


    @After
    public void tearDownDrop() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testNormal() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(Files.newOutputStream(file.toPath()));
        String newMvName = "new_mv_name";
        RenameMaterializedViewLog renameMaterializedViewLog =
                new RenameMaterializedViewLog(1000, 100, newMvName);
        renameMaterializedViewLog.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(Files.newInputStream(file.toPath()));
        RenameMaterializedViewLog readRenameLog = RenameMaterializedViewLog.read(in);

        Assert.assertEquals(readRenameLog.getNewMaterializedViewName(), newMvName);
        Assert.assertEquals(readRenameLog.getId(), 1000);
        Assert.assertEquals(readRenameLog.getDbId(), 100);
        in.close();
    }

}