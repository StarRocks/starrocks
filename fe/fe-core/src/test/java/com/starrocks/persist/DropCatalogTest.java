// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.persist;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class DropCatalogTest {

    private String fileName = "./DropCatalogTest";


    @After
    public void tearDownDrop() throws Exception {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testNormal() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        DropCatalogLog dropCatalogLog =
                new DropCatalogLog("catalog_name");
        dropCatalogLog.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        DropCatalogLog readDropCatalogInfo = DropCatalogLog.read(in);
        Assert.assertEquals(readDropCatalogInfo.getCatalogName(), "catalog_name");
        in.close();
    }
}
