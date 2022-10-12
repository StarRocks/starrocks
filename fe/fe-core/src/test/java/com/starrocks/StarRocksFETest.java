// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks;


import com.starrocks.common.Config;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class StarRocksFETest {

    private String testDir = "testDir";
    private boolean testFlag = false;

    @Test
    public void testHasTwoMateDir() throws NoSuchMethodException, 
                                           SecurityException, 
                                           IllegalArgumentException, 
                                           IllegalAccessException {
        testFlag = false;
        new MockUp<System>() {
            @Mock
            public void exit(int status) throws IllegalArgumentException {
                testFlag = true;
                throw new IllegalArgumentException();
            }
        };
        new MockUp<System>() {
            @Mock
            public String getenv(String name) {
                return testDir;
            }
        };
        
        mkdir(testDir + "/");
        mkdir(testDir + "/" + "doris-meta/");
        mkdir(testDir + "/" + "meta/");
        Config.meta_dir = "testDir/meta";
        Method method = StarRocksFE.class.getDeclaredMethod("checkMetaDir");
        method.setAccessible(true);
        try {
            method.invoke(StarRocksFE.class);
        } catch (InvocationTargetException e) {
        }
        Assert.assertTrue(testFlag);
        deleteDir(new File(testDir + "/"));
    }

    @Test
    public void testUseOldMateDir() throws NoSuchMethodException, 
                                           SecurityException, 
                                           IllegalArgumentException, 
                                           IllegalAccessException, 
                                           InvocationTargetException {    
        new MockUp<System>() {
            @Mock
            public String getenv(String name) {
                return testDir;
            }
        };
        
        mkdir(testDir + "/");
        mkdir(testDir + "/" + "doris-meta/");
        Method method = StarRocksFE.class.getDeclaredMethod("checkMetaDir");
        method.setAccessible(true);
        method.invoke(StarRocksFE.class);
        Assert.assertTrue(Config.meta_dir.equals("testDir/doris-meta"));
        deleteDir(new File(testDir + "/"));
    }


    private void mkdir(String targetDir) {
        File dir = new File(targetDir);
        if (dir.exists()) {
            deleteDir(dir);
        }
        dir.mkdir();
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
