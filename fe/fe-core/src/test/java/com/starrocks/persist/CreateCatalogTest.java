// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.persist;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;

public class CreateCatalogTest {

    private String fileName = "./CreateCatalogTest";

    @After
    public void tearDownCreate() throws Exception {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testNormal() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        HashMap<String, String> properties = new HashMap<>();
        properties.put("type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        String comment = "external catalog for hive";
        CreateCatalogLog createCatalogLog =
                new CreateCatalogLog("catalog_name", comment, properties);
        createCatalogLog.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        CreateCatalogLog readCreateCatalogInfo = CreateCatalogLog.read(in);
        Assert.assertEquals(readCreateCatalogInfo.getCatalogName(), "catalog_name");
        Assert.assertEquals(readCreateCatalogInfo.getCatalogType(), "hive");
        Assert.assertEquals(readCreateCatalogInfo.getComment(), "external catalog for hive");
        Assert.assertEquals(readCreateCatalogInfo.getProperties(), properties);
        in.close();
    }
}
