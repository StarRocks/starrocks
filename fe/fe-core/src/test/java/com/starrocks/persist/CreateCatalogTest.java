// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.persist;

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ExternalCatalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
        Catalog catalog = new ExternalCatalog(1000, "catalog_name", comment, properties);
        catalog.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        Catalog readCreateCatalogInfo = Catalog.read(in);
        Assert.assertEquals(readCreateCatalogInfo.getName(), "catalog_name");
        Assert.assertEquals(readCreateCatalogInfo.getType(), "hive");
        Assert.assertEquals(readCreateCatalogInfo.getComment(), "external catalog for hive");
        Assert.assertEquals(readCreateCatalogInfo.getConfig(), properties);
        in.close();
    }
}
