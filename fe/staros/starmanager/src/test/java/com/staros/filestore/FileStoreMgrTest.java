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

package com.staros.filestore;

import com.staros.credential.AwsCredential;
import com.staros.credential.AwsDefaultCredential;
import com.staros.exception.InvalidArgumentStarException;
import com.staros.exception.NotExistStarException;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.util.Config;
import com.staros.util.Constant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class FileStoreMgrTest {
    @Before
    public void setUp() {
        Config.S3_BUCKET = "myTestBucketBalabala";
        Config.HDFS_URL = "url";
        Config.AZURE_BLOB_ENDPOINT = "xxx";
        Config.AZURE_BLOB_PATH = "xxx";
        Config.AZURE_ADLS2_ENDPOINT = "xxx";
        Config.AZURE_ADLS2_PATH = "xxx";
        Config.GCP_GS_ENDPOINT = "xxx";
        Config.GCP_GS_PATH = "xxx";
    }

    @Test
    public void testRemoveFileStore() {
        FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
        AwsCredential credential = new AwsDefaultCredential();
        Assert.assertThrows(NotExistStarException.class, () -> fsMgr.removeFileStore("bucket"));
        FileStore s3fs = new S3FileStore("test_fskey", "test_name", "bucket", "region",
                "endpoint", credential, "/");
        fsMgr.addFileStore(s3fs);
        fsMgr.removeFileStoreByName("test_name");
        Assert.assertNull(fsMgr.getFileStore("test_name"));

        Assert.assertThrows(InvalidArgumentStarException.class,
                () -> fsMgr.removeFileStoreByName(Constant.S3_FSNAME_FOR_CONFIG));
    }

    @Test
    public void testUpdateFileStore() {
        FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
        AwsCredential credential = new AwsDefaultCredential();
        Assert.assertThrows(NotExistStarException.class, () ->
                fsMgr.updateFileStore(
                        new S3FileStore("test_fskey", "test_name", "bucket", "region",
                                "endpoint", credential, "/")));
        FileStore fs = new S3FileStore("test_fskey", "test_name", "bucket", "region",
                "endpoint", credential, "/");
        fsMgr.addFileStore(fs);
        fs = new S3FileStore("test_fskey", "test_name", "bucket", "region", "endpoint", credential, "/");
        fsMgr.updateFileStore(fs);
        S3FileStore fileStore = (S3FileStore) fsMgr.getFileStore("test_fskey");
        Assert.assertEquals("s3://" + Paths.get("bucket", "/"), fileStore.rootPath());

        FileStore s3fs = fsMgr.loadS3FileStoreFromConfig();
        s3fs.setEnabled(false);
        Assert.assertThrows(InvalidArgumentStarException.class, () -> fsMgr.updateFileStore(s3fs));
    }

    @Test
    public void testReplaceFileStore() {
        FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
        AwsCredential credential = new AwsDefaultCredential();
        Assert.assertThrows(NotExistStarException.class, () ->
                fsMgr.replaceFileStore(
                        new S3FileStore("test_fskey", "test_name", "bucket", "region",
                                "endpoint", credential, "/")));
        FileStore fs = new S3FileStore("test_fskey", "test_name", "bucket", "region",
                "endpoint", credential, "/");
        fsMgr.addFileStore(fs);
        fs = new S3FileStore("test_fskey", "test_name", "bucket", "region", "endpoint", credential, "/aaa");
        fsMgr.replaceFileStore(fs);
        S3FileStore fileStore = (S3FileStore) fsMgr.getFileStore("test_fskey");
        Assert.assertEquals("s3://" + Paths.get("bucket", "/aaa"), fileStore.rootPath());

        FileStore s3fs = fsMgr.loadS3FileStoreFromConfig();
        s3fs.setEnabled(false);
        Assert.assertThrows(InvalidArgumentStarException.class, () -> fsMgr.replaceFileStore(s3fs));
    }

    @Test
    public void testListFileStore() {
        FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
        List<FileStore> fileStores = fsMgr.listFileStore(Constant.FS_NOT_SET);
        Assert.assertEquals(5, fileStores.size());
        AwsCredential credential = new AwsDefaultCredential();
        S3FileStore s3fs = new S3FileStore("test_fskey", "test_name", "bucket", "region",
                "endpoint", credential, "/");
        fsMgr.addFileStore(s3fs);
        fileStores = fsMgr.listFileStore(Constant.FS_NOT_SET);
        Assert.assertEquals(6, fileStores.size());
        HDFSFileStore hdfs = new HDFSFileStore("test_hdfskey", "test_name", "url");
        fsMgr.addFileStore(hdfs);
        fileStores = fsMgr.listFileStore(Constant.FS_NOT_SET);
        Assert.assertEquals(7, fileStores.size());
        fileStores = fsMgr.listFileStore(FileStoreType.S3);
        Assert.assertEquals(2, fileStores.size());
        fsMgr.removeFileStore("test_fskey");
        fileStores = fsMgr.listFileStore(Constant.FS_NOT_SET);
        Assert.assertEquals(6, fileStores.size());
        fsMgr.removeFileStore("test_hdfskey");
        fileStores = fsMgr.listFileStore(Constant.FS_NOT_SET);
        Assert.assertEquals(5, fileStores.size());
    }

    @Test
    public void testGetFileStore() {
        FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
        AwsCredential credential = new AwsDefaultCredential();
        S3FileStore s3fs = new S3FileStore("test_fskey", "test_name", "bucket", "region",
                "endpoint", credential, "/");
        Assert.assertNull(fsMgr.getFileStoreByName("test_name"));
        fsMgr.addFileStore(s3fs);
        Assert.assertEquals("test_fskey", fsMgr.getFileStoreByName("test_name").key());
    }

    @Test
    public void testFileStoreMgrMeta() throws IOException {
        AwsCredential credential = new AwsDefaultCredential();
        S3FileStore s3fs = new S3FileStore("test_fskey", "test_name", "bucket", "region",
                "endpoint", credential, "/");
        HDFSFileStore hdfs = new HDFSFileStore("test_hdfskey", "test_name", "url");

        // dumpMeta
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        FileStoreMgr fsMgr1 = FileStoreMgr.createFileStoreMgrForTest("");
        fsMgr1.addFileStore(s3fs);
        fsMgr1.updateFileStore(s3fs);
        fsMgr1.addFileStore(hdfs);
        fsMgr1.removeFileStoreByName(hdfs.name());
        fsMgr1.dumpMeta(os);

        FileStoreMgr fsMgr2 = FileStoreMgr.createFileStoreMgrForTest("");
        ByteArrayInputStream in = new ByteArrayInputStream(os.toByteArray());
        fsMgr2.loadMeta(in);

        // now check fsMgr2 data
        Assert.assertNotNull(fsMgr2.getFileStore(Constant.S3_FSKEY_FOR_CONFIG));
        Assert.assertNotNull(fsMgr2.getFileStore(Constant.HDFS_FSKEY_FOR_CONFIG));
        Assert.assertNotNull(fsMgr2.getFileStore(Constant.AZURE_BLOB_FSKEY_FOR_CONFIG));
        Assert.assertNotNull(fsMgr2.getFileStore(Constant.AZURE_ADLS2_FSKEY_FOR_CONFIG));
        Assert.assertNotNull(fsMgr2.getFileStore(s3fs.key()));
        Assert.assertEquals(s3fs.key(), fsMgr2.getFileStore(s3fs.key()).key());
        Assert.assertEquals(1, fsMgr2.getFileStore(s3fs.key()).getVersion());
        Assert.assertNull(fsMgr2.getFileStore(hdfs.key()));

        { // dump fsMgr2 again, get the same dump result
            ByteArrayOutputStream os2 = new ByteArrayOutputStream();
            fsMgr2.dumpMeta(os2);
            Assert.assertArrayEquals(os.toByteArray(), os2.toByteArray());
        }

        // Builtin file store will not be dumped.
        fsMgr1 = FileStoreMgr.createFileStoreMgrForTest("");
        os = new ByteArrayOutputStream();
        fsMgr1.dumpMeta(os);
        fsMgr2 = FileStoreMgr.createFileStoreMgrForTest("");
        fsMgr2.clear();
        in = new ByteArrayInputStream(os.toByteArray());
        fsMgr2.loadMeta(in);
        Assert.assertNull(fsMgr2.getFileStore(Constant.S3_FSKEY_FOR_CONFIG));

        // Invalid file store will not be loaded.
        fsMgr1 = FileStoreMgr.createFileStoreMgrForTest("");
        os = new ByteArrayOutputStream();
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsName("test-name")
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.INVALID)
                .addAllLocations(Arrays.asList("azblob://path")).build();
        FileStore invalidFileStore = FileStore.fromProtobuf(info);
        fsMgr1.addFileStore(invalidFileStore);
        fsMgr1.dumpMeta(os);
        fsMgr2 = FileStoreMgr.createFileStoreMgrForTest("");
        fsMgr2.clear();
        in = new ByteArrayInputStream(os.toByteArray());
        fsMgr2.loadMeta(in);
        Assert.assertEquals(0, fsMgr2.listFileStore(FileStoreType.INVALID).size());
    }

    @Test
    public void testAllocFileStore() {
        FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
        Assert.assertNotNull(fsMgr.allocFileStore(""));

        String fsKey = "test_fskey";
        AwsCredential credential = new AwsDefaultCredential();
        FileStore s3fs = new S3FileStore(fsKey, "test_name", "bucket", "region",
                "endpoint", credential, "/");
        fsMgr.addFileStore(s3fs);
        FileStore fs = fsMgr.allocFileStore(fsKey);
        Assert.assertEquals(fsKey, fs.key());

        s3fs.setEnabled(false);
        fsMgr.updateFileStore(s3fs);
        Assert.assertThrows(InvalidArgumentStarException.class, () -> fsMgr.allocFileStore(fsKey));
    }
}
