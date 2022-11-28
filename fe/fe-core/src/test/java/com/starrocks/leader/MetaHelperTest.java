// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.leader;

import com.starrocks.common.Config;
import com.starrocks.common.InvalidMetaDirException;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class MetaHelperTest {

    private String testDir = "meta_dir_test_" + UUID.randomUUID();

    @After
    public void teardown() {
        deleteDir(new File(testDir));
    }

    @Test(expected = InvalidMetaDirException.class)
    public void testHasTwoMetaDir() throws IOException,
            InvalidMetaDirException {
        Config.start_with_incomplete_meta = false;
        new MockUp<System>() {
            @Mock
            public String getenv(String name) {
                return testDir;
            }
        };

        mkdir(testDir + "/doris-meta/");
        mkdir(testDir + "/meta/");
        Config.meta_dir = testDir + "/meta";
        try {
            MetaHelper.checkMetaDir();
        } finally {
            deleteDir(new File(testDir));
        }
    }

    @Test
    public void testUseOldMetaDir() throws IOException,
            InvalidMetaDirException {
        Config.start_with_incomplete_meta = false;
        new MockUp<System>() {
            @Mock
            public String getenv(String name) {
                return testDir;
            }
        };

        mkdir(testDir + "/doris-meta/");
        Config.meta_dir = testDir + "/meta";
        try {
            MetaHelper.checkMetaDir();
        } finally {
            deleteDir(new File(testDir));
        }

        Assert.assertEquals(Config.meta_dir, testDir + "/doris-meta");
    }

    @Test(expected = InvalidMetaDirException.class)
    public void testImageExistBDBNotExist() throws IOException,
            InvalidMetaDirException {
        Config.start_with_incomplete_meta = false;
        Config.meta_dir = testDir + "/meta";
        mkdir(Config.meta_dir + "/image");
        File file = new File(Config.meta_dir + "/image/image.123");
        Assert.assertTrue(file.createNewFile());

        try {
            MetaHelper.checkMetaDir();
        } finally {
            deleteDir(new File(testDir + "/"));
        }
    }

    @Test
    public void testImageExistBDBNotExistWithConfig() throws IOException,
            InvalidMetaDirException {
        Config.start_with_incomplete_meta = true;
        Config.meta_dir = testDir + "/meta";
        mkdir(Config.meta_dir + "/image");
        File file = new File(Config.meta_dir + "/image/image.123");
        Assert.assertTrue(file.createNewFile());

        try {
            MetaHelper.checkMetaDir();
        } finally {
            deleteDir(new File(testDir + "/"));
        }
    }

    @Test
    public void testImageExistBDBExist() throws IOException,
            InvalidMetaDirException {
        Config.start_with_incomplete_meta = false;
        Config.meta_dir = testDir + "/meta";
        mkdir(Config.meta_dir + "/image");
        File fileImage = new File(Config.meta_dir + "/image/image.123");
        Assert.assertTrue(fileImage.createNewFile());
        mkdir(Config.meta_dir + "/bdb");
        File fileBDB = new File(Config.meta_dir + "/bdb/EF889.jdb");
        Assert.assertTrue(fileBDB.createNewFile());

        try {
            MetaHelper.checkMetaDir();
        } finally {
            deleteDir(new File(testDir + "/"));
        }
    }

    @Test
    public void testImageNotExistBDBExist() throws IOException,
            InvalidMetaDirException {
        Config.start_with_incomplete_meta = false;
        Config.meta_dir = testDir + "/meta";
        mkdir(Config.meta_dir + "/bdb");
        File file = new File(Config.meta_dir + "/bdb/EF889.jdb");
        Assert.assertTrue(file.createNewFile());

        try {
            MetaHelper.checkMetaDir();
        } finally {
            deleteDir(new File(testDir + "/"));
        }
    }

    private void mkdir(String targetDir) {
        File dir = new File(targetDir);
        if (dir.exists()) {
            deleteDir(dir);
        }
        dir.mkdirs();
    }

    private void deleteDir(File dir) {
        if (!dir.exists()) {
            return;
        }
        if (dir.isFile()) {
            dir.delete();
        } else {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        dir.delete();
    }
}
