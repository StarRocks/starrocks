// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.starrocks.common.FeConstants;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemotePathKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.starrocks.connector.hive.MockedRemoteFileSystem.TEST_FILES;

public class HiveRemoteFileIOTest {
    @Test
    public void testGetRemoteFiles() {
        FileSystem fs = new MockedRemoteFileSystem(TEST_FILES);
        HiveRemoteFileIO fileIO = new HiveRemoteFileIO(new Configuration());
        fileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        String tableLocation = "hdfs://127.0.0.1:10000/hive.db/hive_tbl";
        RemotePathKey pathKey = RemotePathKey.of(tableLocation, false);
        Map<RemotePathKey, List<RemoteFileDesc>> remoteFileInfos = fileIO.getRemoteFiles(pathKey);
        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(pathKey);
        Assert.assertNotNull(fileDescs);
        Assert.assertEquals(1, fileDescs.size());
        RemoteFileDesc fileDesc = fileDescs.get(0);
        Assert.assertNotNull(fileDesc);
        Assert.assertEquals("000000_0", fileDesc.getFileName());
        Assert.assertEquals("", fileDesc.getCompression());
        Assert.assertEquals(20, fileDesc.getLength());
        Assert.assertEquals(1234567890, fileDesc.getModificationTime());
        Assert.assertFalse(fileDesc.isSplittable());
        Assert.assertNull(fileDesc.getTextFileFormatDesc());

        List<RemoteFileBlockDesc> blockDescs = fileDesc.getBlockDescs();
        Assert.assertEquals(1, blockDescs.size());
        RemoteFileBlockDesc blockDesc = blockDescs.get(0);
        Assert.assertEquals(0, blockDesc.getOffset());
        Assert.assertEquals(20, blockDesc.getLength());
        Assert.assertEquals(2, blockDesc.getReplicaHostIds().length);
    }

    @Test
    public void testPathContainsEmptySpace() {
        FileSystem fs = new MockedRemoteFileSystem(TEST_FILES);
        HiveRemoteFileIO fileIO = new HiveRemoteFileIO(new Configuration());
        fileIO.setFileSystem(fs);
        FeConstants.runningUnitTest = true;
        String tableLocation = "hdfs://127.0.0.1:10000/hive.db / hive_tbl";
        RemotePathKey pathKey = RemotePathKey.of(tableLocation, false);
        Map<RemotePathKey, List<RemoteFileDesc>> remoteFileInfos = fileIO.getRemoteFiles(pathKey);
        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(pathKey);
        Assert.assertNotNull(fileDescs);
        Assert.assertEquals(1, fileDescs.size());
    }

}
