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

package com.starrocks.fs;

import com.google.common.collect.Maps;
import com.starrocks.common.UserException;
import com.starrocks.fs.hdfs.HdfsFs;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.thrift.THdfsProperties;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestHdfsFsManager extends TestCase {

    private final String testHdfsHost = "hdfs://localhost:9000";

    private HdfsFsManager fileSystemManager;

    protected void setUp() throws Exception {
        fileSystemManager = new HdfsFsManager();
    }

    @Test
    public void testGetFileSystemSuccess() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("username", "user");
        properties.put("password", "passwd");
        try {
            HdfsFs fs = fileSystemManager.getFileSystem(testHdfsHost + "/data/abc/logs", properties, null);
            assertNotNull(fs);
            fs.getDFSFileSystem().close();
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGetFileSystemForS3aScheme() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("fs.s3a.access.key", "accessKey");
        properties.put("fs.s3a.secret.key", "secretKey");
        properties.put("fs.s3a.endpoint", "s3.test.com");
        try {
            HdfsFs fs = fileSystemManager.getFileSystem("s3a://testbucket/data/abc/logs", properties, null);
            assertNotNull(fs);
            fs.getDFSFileSystem().close();
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testS3GetRegionFromEndPoint1() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("fs.s3a.access.key", "accessKey");
        properties.put("fs.s3a.secret.key", "secretKey");
        properties.put("fs.s3a.endpoint", "s3.ap-southeast-1.amazonaws.com");
        THdfsProperties property = new THdfsProperties();
        try {
            HdfsFs fs = fileSystemManager.getFileSystem("s3a://testbucket/data/abc/logs", properties, property);
            assertNotNull(fs);
            Assert.assertEquals(property.region, "ap-southeast-1");
            fs.getDFSFileSystem().close();
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testS3GetRegionFromEndPoint2() throws IOException {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("fs.s3a.access.key", "accessKey");
        properties.put("fs.s3a.secret.key", "secretKey");
        properties.put("fs.s3a.endpoint", "s3-ap-southeast-1.amazonaws.com");
        THdfsProperties property = new THdfsProperties();
        try {
            HdfsFs fs = fileSystemManager.getFileSystem("s3a://testbucket/data/abc/logs", properties, property);
            assertNotNull(fs);
            Assert.assertEquals(property.region, "ap-southeast-1");
            fs.getDFSFileSystem().close();
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testList() throws UserException, IOException {
        HdfsFsManager hdfsFsManager = Mockito.spy(fileSystemManager);
        FileSystem fs = Mockito.mock(FileSystem.class);
        HdfsFs hdfs = Mockito.mock(HdfsFs.class);
        Mockito.when(hdfs.getDFSFileSystem()).thenReturn(fs);
        Mockito.when(fs.globStatus(new Path("not_found"))).thenThrow(new FileNotFoundException("not found"));
        Mockito.when(fs.globStatus(new Path("error"))).thenThrow(new RuntimeException("error"));
        FileStatus[] files = new FileStatus[] {
                new FileStatus(1, false, 1, 1, 1, new Path("file1"))
        };
        Mockito.when(fs.globStatus(new Path("s3a://dir/"))).thenReturn(files);

        // listFileMeta
        Assert.assertThrows(UserException.class,
                () -> hdfsFsManager.listFileMeta("not_found", Maps.newHashMap()));
        Assert.assertThrows(UserException.class,
                () -> hdfsFsManager.listFileMeta("error", Maps.newHashMap()));
        Assert.assertFalse(hdfsFsManager.listFileMeta("s3a://dir/", Maps.newHashMap()).isEmpty());

        // listPath
        Assert.assertEquals(1,
                hdfsFsManager.listPath("s3a://dir/", true, Maps.newHashMap()).size());
        Assert.assertEquals(1,
                hdfsFsManager.listPath("s3a://dir/", false, Maps.newHashMap()).size());
    }
}
