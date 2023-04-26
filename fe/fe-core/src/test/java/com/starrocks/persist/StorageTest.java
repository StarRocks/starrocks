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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/persist/StorageTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.persist;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class StorageTest {
    private String meta = "storageTestDir/";

    public void mkdir() {
        File dir = new File(meta);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
        }
    }

    public void addFiles(int image, int edit) {
        File imageFile = new File(meta + "image." + image);
        try {
            imageFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 1; i <= edit; i++) {
            File editFile = new File(meta + "edits." + i);
            try {
                editFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        File current = new File(meta + "edits");
        try {
            current.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File version = new File(meta + "VERSION");
        try {
            version.createNewFile();
            String line1 = "#Mon Feb 02 13:59:54 CST 2015\n";
            String line2 = "clusterId=966271669";
            FileWriter fw = new FileWriter(version);
            fw.write(line1);
            fw.write(line2);
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteDir() {
        File dir = new File(meta);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }

            dir.delete();
        }
    }

    @Test
    public void testConstruct() {
        Storage storage1 = new Storage(1, "token", "test");
        Assert.assertEquals(1, storage1.getClusterID());
        Assert.assertEquals("test", storage1.getMetaDir());

        Storage storage2 = new Storage(1, "token", 2, "test");
        Assert.assertEquals(1, storage2.getClusterID());
        Assert.assertEquals(2, storage2.getImageJournalId());
        Assert.assertEquals("test", storage2.getMetaDir());
    }

    @Test
    public void testStorage() throws Exception {
        mkdir();
        addFiles(0, 10);

        Storage storage = new Storage("storageTestDir");
        Assert.assertEquals(966271669, storage.getClusterID());
        storage.setClusterID(1234);
        Assert.assertEquals(1234, storage.getClusterID());
        Assert.assertEquals(0, storage.getImageJournalId());

        Assert.assertTrue(storage.getCurrentImageFile().equals(new File("storageTestDir/image.0")));
        Assert.assertTrue(storage.getImageFile(0).equals(new File("storageTestDir/image.0")));
        Assert.assertTrue(Storage.getImageFile(new File("storageTestDir"), 0)
                .equals(new File("storageTestDir/image.0")));

        Assert.assertTrue(storage.getVersionFile().equals(new File("storageTestDir/VERSION")));

        storage.setImageJournalId(100);
        Assert.assertEquals(100, storage.getImageJournalId());

        Assert.assertEquals("storageTestDir", storage.getMetaDir());
        storage.setMetaDir("abcd");
        Assert.assertEquals("abcd", storage.getMetaDir());

        storage.setMetaDir("storageTestDir");
        storage.clear();
        File file = new File(storage.getMetaDir());
        Assert.assertEquals(0, file.list().length);

        deleteDir();
    }
}
