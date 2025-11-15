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

import com.staros.common.HijackConfig;
import com.staros.credential.ADLS2Credential;
import com.staros.credential.AwsDefaultCredential;
import com.staros.credential.AzBlobCredential;
import com.staros.credential.GSCredential;
import com.staros.exception.InvalidArgumentStarException;
import com.staros.exception.NotExistStarException;
import com.staros.proto.ADLS2CredentialInfo;
import com.staros.proto.ADLS2FileStoreInfo;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsCredentialType;
import com.staros.proto.AwsDefaultCredentialInfo;
import com.staros.proto.AzBlobCredentialInfo;
import com.staros.proto.AzBlobFileStoreInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.GSFileStoreInfo;
import com.staros.proto.HDFSFileStoreInfo;
import com.staros.proto.S3FileStoreInfo;
import com.staros.util.Config;
import com.staros.util.Constant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class FileStoreTest {

    @Before
    public void setUp() {
        Config.S3_BUCKET = "myTestBucketBalabala";
        Config.HDFS_URL = "hdfs://hdfs_endpoint";
        Config.AZURE_BLOB_ENDPOINT = "xxx";
        Config.AZURE_BLOB_PATH = "xxx";
        Config.AZURE_ADLS2_ENDPOINT = "xxx";
        Config.AZURE_ADLS2_PATH = "xxx";
        Config.GCP_GS_ENDPOINT = "xxx";
        Config.GCP_GS_PATH = "xxx";
    }

    @Test
    public void testS3FileStoreWithNoSubPath() {
        testS3FileStorePath("");
    }

    @Test
    public void testS3FileStoreWithSubPath() {
        testS3FileStorePath("testS3FileStoreWithSubPath");
    }

    private void testS3FileStorePath(String subPath) {
        try (HijackConfig ignored = new HijackConfig("S3_PATH_PREFIX", subPath)) {
            FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
            FileStore fs = fsMgr.allocFileStore(Constant.S3_FSKEY_FOR_CONFIG);
            String suffix = "MySuffix";
            FilePath fp = new FilePath(fs, suffix);

            String allocatedPath = fp.fullPath();
            String expectedPath;
            if (!Config.S3_PATH_PREFIX.isEmpty()) {
                expectedPath = String.format("s3://%s/%s/%s", Config.S3_BUCKET, Config.S3_PATH_PREFIX, suffix);
            } else {
                expectedPath = String.format("s3://%s/%s", Config.S3_BUCKET, suffix);
            }
            Assert.assertEquals(expectedPath, allocatedPath);
        }
    }

    @Test
    public void testFilePathSerializeAndDeserialize() {
        testFilePathSerializeAndDeserialize(Constant.S3_FSKEY_FOR_CONFIG);
        testFilePathSerializeAndDeserialize(Constant.HDFS_FSKEY_FOR_CONFIG);
        testFilePathSerializeAndDeserialize(Constant.AZURE_BLOB_FSKEY_FOR_CONFIG);
        testFilePathSerializeAndDeserialize(Constant.AZURE_ADLS2_FSKEY_FOR_CONFIG);
    }

    public void testFilePathSerializeAndDeserialize(String fsKey) {
        FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
        FileStore fs = fsMgr.allocFileStore(fsKey);
        String suffix = "MySuffix";
        FilePath fp = new FilePath(fs, suffix);
        // serialize to protobuf
        FilePathInfo info = fp.toProtobuf();
        // recreate from the protobuf
        FilePath newFp = FilePath.fromFullPath(fs, info.getFullPath());
        Assert.assertEquals(fp.fullPath(), newFp.fullPath());
        Assert.assertFalse(fs.isPartitionedPrefixEnabled());
        Assert.assertEquals(0, fs.numOfPartitionedPrefix());
        // compare the protobuf string
        Assert.assertEquals(fp.toProtobuf().toByteString(), newFp.toProtobuf().toByteString());
    }

    @Test
    public void testS3FileStoreLoadConfig() {
        try (HijackConfig ignored = new HijackConfig("S3_BUCKET", "")) {
            FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
            Assert.assertNull(fsMgr.loadS3FileStoreFromConfig());
            Assert.assertThrows(NotExistStarException.class, () -> fsMgr.allocFileStore(Constant.S3_FSKEY_FOR_CONFIG));
        }
        try (HijackConfig ignored = new HijackConfig("S3_BUCKET", "s3://a/b/c")) {
            Assert.assertThrows(InvalidArgumentStarException.class, () -> FileStoreMgr.createFileStoreMgrForTest(""));
        }
    }

    @Test
    public void testS3FileStoreMergeFrom() {
        S3FileStore s3fs = new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG,
                "test-bucket", "test-region", "test-endpoint", null, "");
        S3FileStore other = new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG,
                "test-bucket", "test-region1", "", new AwsDefaultCredential(), "");
        s3fs.mergeFrom(other);
        Assert.assertEquals("test-region1", s3fs.getRegion());
        Assert.assertEquals(AwsCredentialType.DEFAULT, s3fs.getCredential().type());

        Assert.assertFalse(s3fs.isPartitionedPrefixEnabled());
        Assert.assertEquals(0, s3fs.numOfPartitionedPrefix());
        Assert.assertEquals(0, s3fs.getPathStyleAccess());

        S3FileStore other2 = new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG,
                "test-bucket", "test-region1", "", new AwsDefaultCredential(), "", true, 32, 1);
        s3fs.mergeFrom(other2);
        Assert.assertTrue(s3fs.isPartitionedPrefixEnabled());
        Assert.assertEquals(32, s3fs.numOfPartitionedPrefix());
        Assert.assertEquals(1, s3fs.getPathStyleAccess());

        S3FileStore s3fs3 = new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG,
                "test-bucket", "test-region1", "", new AwsDefaultCredential(), "", true, 0, 2);

        Assert.assertTrue(s3fs3.isPartitionedPrefixEnabled());
        // set to default number of partitioned prefix
        Assert.assertEquals(256, s3fs3.numOfPartitionedPrefix());
        Assert.assertEquals(2, s3fs3.getPathStyleAccess());
    }

    @Test
    public void testS3FileStoreProtobuf() {
        AwsCredentialInfo awsCredentialInfo = AwsCredentialInfo.newBuilder()
                .setDefaultCredential(AwsDefaultCredentialInfo.newBuilder().build()).build();
        S3FileStoreInfo s3fs = S3FileStoreInfo.newBuilder()
                .setBucket("bucket").setPathPrefix("prefix").setCredential(awsCredentialInfo).build();
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsName("test-name")
                .setS3FsInfo(s3fs)
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.S3)
                .addAllLocations(new ArrayList<>()).build();
        FileStore s3FileStore = FileStore.fromProtobuf(info);
        Assert.assertEquals("test-fskey", s3FileStore.key());
        Assert.assertEquals("s3://bucket/prefix", s3FileStore.rootPath());

        FileStoreInfo info1 = s3FileStore.toProtobuf();
        S3FileStoreInfo s3fs1 = info1.getS3FsInfo();
        Assert.assertEquals(s3fs1.getCredential().getCredentialCase(),
                AwsCredentialInfo.CredentialCase.DEFAULT_CREDENTIAL);
        Assert.assertEquals(s3fs.getBucket(), s3fs1.getBucket());
        Assert.assertEquals(s3fs.getPathPrefix(), s3fs1.getPathPrefix());
        Assert.assertEquals(new ArrayList<>(Arrays.asList("s3://bucket/prefix")), info1.getLocationsList());

        FileStoreInfo info2 = s3FileStore.toDebugProtobuf();
        S3FileStoreInfo s3fs2 = info2.getS3FsInfo();
        Assert.assertEquals(s3fs2.getCredential().getCredentialCase(),
                AwsCredentialInfo.CredentialCase.CREDENTIAL_NOT_SET);
    }

    @Test
    public void testS3FileStoreProtobufWithPartitionedPrefix() {
        AwsCredentialInfo awsCredentialInfo = AwsCredentialInfo.newBuilder()
                .setDefaultCredential(AwsDefaultCredentialInfo.newBuilder().build()).build();
        S3FileStoreInfo s3fs = S3FileStoreInfo.newBuilder()
                .setBucket("bucket")
                .setPathPrefix("") // must be empty path prefix
                .setNumPartitionedPrefix(32)
                .setPartitionedPrefixEnabled(true)
                .setPathStyleAccess(2)
                .setCredential(awsCredentialInfo)
                .build();
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsName("test-name")
                .setS3FsInfo(s3fs)
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.S3)
                .addAllLocations(new ArrayList<>()).build();

        // Convert Protobuf to FileStore
        FileStore s3FileStore = FileStore.fromProtobuf(info);

        Assert.assertEquals("test-fskey", s3FileStore.key());
        Assert.assertEquals("s3://bucket", s3FileStore.rootPath());
        Assert.assertTrue(s3FileStore.isPartitionedPrefixEnabled());
        Assert.assertEquals(32, s3FileStore.numOfPartitionedPrefix());
        Assert.assertTrue(s3FileStore instanceof S3FileStore);
        Assert.assertEquals(2, ((S3FileStore) s3FileStore).getPathStyleAccess());

        // Convert FileStore to protobuf
        FileStoreInfo info1 = s3FileStore.toProtobuf();
        Assert.assertEquals(new ArrayList<>(Arrays.asList("s3://bucket")), info1.getLocationsList());
        // clear the auto-filled location field and compare
        FileStoreInfo info1shadow = info1.toBuilder().clearLocations().build();
        Assert.assertEquals(info.toString(), info1shadow.toString());

        S3FileStoreInfo s3fs1 = info1.getS3FsInfo();
        Assert.assertEquals(s3fs1.getCredential().getCredentialCase(),
                AwsCredentialInfo.CredentialCase.DEFAULT_CREDENTIAL);
        Assert.assertEquals(s3fs.getBucket(), s3fs1.getBucket());
        Assert.assertEquals(s3fs.getPathPrefix(), s3fs1.getPathPrefix());
        Assert.assertEquals(2, s3fs1.getPathStyleAccess());

        FileStoreInfo info2 = s3FileStore.toDebugProtobuf();
        S3FileStoreInfo s3fs2 = info2.getS3FsInfo();
        Assert.assertEquals(s3fs2.getCredential().getCredentialCase(),
                AwsCredentialInfo.CredentialCase.CREDENTIAL_NOT_SET);
    }

    @Test
    public void testS3FileStoreIsValid() {
        S3FileStore s3fs = new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG,
                "test-bucket", "test-region", "test-endpoint", null, "");
        Assert.assertFalse(s3fs.isValid());

        s3fs = new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG,
                "", "test-region", "test-endpoint", new AwsDefaultCredential(), "");
        Assert.assertFalse(s3fs.isValid());

        s3fs = new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG,
                "test-bucket", "test-region", "test-endpoint", new AwsDefaultCredential(), "");
        Assert.assertTrue(s3fs.isValid());

        s3fs = new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG,
                "test-bucket", "test-region", "test-endpoint", new AwsDefaultCredential(), "", true, 64, 0);
        Assert.assertTrue(s3fs.isValid());

        // path prefix should be empty
        s3fs = new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG,
                "test-bucket", "test-region", "test-endpoint", new AwsDefaultCredential(), "sub_path", true, 64, 0);
        Assert.assertFalse(s3fs.isValid());

        // numPartitionedPrefix should be larger than 0
        s3fs = new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG,
                "test-bucket", "test-region", "test-endpoint", new AwsDefaultCredential(), "", true, -5, 0);
        Assert.assertFalse(s3fs.isValid());

        Assert.assertThrows(InvalidArgumentStarException.class,
                () -> new S3FileStore(Constant.S3_FSKEY_FOR_CONFIG, Constant.S3_FSNAME_FOR_CONFIG,
                        "test-bucket", "test-region", "test-endpoint", new AwsDefaultCredential(), "", true, -5, 5));
    }

    @Test
    public void testHDFSFileStoreProtobuf() {
        HDFSFileStoreInfo hdfs = HDFSFileStoreInfo.newBuilder().setUrl("url").build();
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsType(FileStoreType.HDFS)
                .setFsName("test-name")
                .setHdfsFsInfo(hdfs)
                .setFsKey("test-fskey")
                .addAllLocations(new ArrayList<>()).build();
        FileStore hdfsFileStore = FileStore.fromProtobuf(info);
        Assert.assertEquals("test-fskey", hdfsFileStore.key());
        Assert.assertEquals("url/", hdfsFileStore.rootPath());

        FileStoreInfo info1 = hdfsFileStore.toProtobuf();
        HDFSFileStoreInfo hdfs1 = info1.getHdfsFsInfo();
        Assert.assertEquals(hdfs.getUrl(), hdfs1.getUrl());
        Assert.assertEquals(new ArrayList<>(Arrays.asList("url")), info1.getLocationsList());

        Map<String, String> configuration = new HashMap<>();
        configuration.put("abc", "cde");
        hdfs = HDFSFileStoreInfo.newBuilder().setUrl("url").setUsername("username")
                .putAllConfiguration(configuration).build();
        info = info.toBuilder().setHdfsFsInfo(hdfs).build();
        hdfsFileStore = FileStore.fromProtobuf(info);
        Assert.assertEquals("test-fskey", hdfsFileStore.key());
        Assert.assertEquals("url/", hdfsFileStore.rootPath());

        info1 = hdfsFileStore.toProtobuf();
        hdfs1 = info1.getHdfsFsInfo();
        Assert.assertEquals(hdfs.getUrl(), hdfs1.getUrl());
        Assert.assertEquals(new ArrayList<>(Arrays.asList("url")), info1.getLocationsList());
        Assert.assertEquals(hdfs.getUsername(), hdfs1.getUsername());
        Assert.assertEquals("cde", hdfs1.getConfiguration().get("abc"));

        info1 = hdfsFileStore.toDebugProtobuf();
        hdfs1 = info1.getHdfsFsInfo();
        Assert.assertEquals(hdfs.getUsername(), hdfs1.getUsername());
        Assert.assertEquals("cde", hdfs1.getConfiguration().get("abc"));
    }

    @Test
    public void testHDFSFileStoreIsValid() {
        HDFSFileStore hdfs = new HDFSFileStore(Constant.HDFS_FSKEY_FOR_CONFIG, Constant.HDFS_FSNAME_FOR_CONFIG, "");
        Assert.assertFalse(hdfs.isValid());

        hdfs = new HDFSFileStore(Constant.HDFS_FSKEY_FOR_CONFIG, Constant.HDFS_FSNAME_FOR_CONFIG, "url");
        Assert.assertTrue(hdfs.isValid());
    }

    @Test
    public void testAzBlobFileStoreLoadConfig() {
        try (HijackConfig ignored = new HijackConfig("AZURE_BLOB_ENDPOINT", "")) {
            FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
            Assert.assertNull(fsMgr.loadAzBlobFileStoreFromConfig());
            Assert.assertThrows(NotExistStarException.class, () -> fsMgr.allocFileStore(Constant.AZURE_BLOB_FSKEY_FOR_CONFIG));
        }

        try (HijackConfig ignored = new HijackConfig("AZURE_BLOB_PATH", "")) {
            FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
            fsMgr.loadAzBlobFileStoreFromConfig();
            Assert.assertThrows(NotExistStarException.class, () -> fsMgr.allocFileStore(Constant.AZURE_BLOB_FSKEY_FOR_CONFIG));
        }

        FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
        fsMgr.loadAzBlobFileStoreFromConfig();
        FileStore fs = fsMgr.allocFileStore("");
        Assert.assertNotNull(fs);
        FileStore newFs = FileStore.fromProtobuf(fs.toProtobuf());
        Assert.assertNotNull(newFs);
    }

    @Test
    public void testAzBlobFileStoreSerialize() {
        AzBlobCredential credential = new AzBlobCredential("test_key", "", "", "", "", "", "");
        AzBlobFileStore fs = new AzBlobFileStore(Constant.AZURE_BLOB_FSKEY_FOR_CONFIG,
                Constant.AZURE_BLOB_FSNAME_FOR_CONFIG, "test_endpoint", "test_path", credential);
        FileStoreInfo fsInfo = fs.toProtobuf();
        FileStore fileStore = FileStore.fromProtobuf(fsInfo);
        Assert.assertTrue(fileStore instanceof AzBlobFileStore);
        Assert.assertEquals(FileStoreType.AZBLOB, fileStore.type());
        Assert.assertEquals(fs.key(), fileStore.key());
    }

    @Test
    public void testAzBlobFileStoreProtobuf() {
        AzBlobCredentialInfo azcredential = AzBlobCredentialInfo.newBuilder().setSharedKey("aaa").build();
        AzBlobFileStoreInfo azblob = AzBlobFileStoreInfo.newBuilder().setPath("path").setCredential(azcredential).build();
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsName("test-name")
                .setAzblobFsInfo(azblob)
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.AZBLOB)
                .addAllLocations(new ArrayList<>()).build();
        FileStore azblobFileStore = AzBlobFileStore.fromProtobuf(info);
        Assert.assertEquals("test-fskey", azblobFileStore.key());
        Assert.assertEquals("azblob://path", azblobFileStore.rootPath());

        FileStoreInfo info1 = azblobFileStore.toProtobuf();
        AzBlobFileStoreInfo azblob1 = info1.getAzblobFsInfo();
        Assert.assertEquals(azblob.getPath(), azblob1.getPath());
        Assert.assertEquals(azblob.getCredential().getSharedKey(), "aaa");
        Assert.assertEquals(new ArrayList<>(Arrays.asList("azblob://path")), info1.getLocationsList());

        FileStoreInfo info2 = azblobFileStore.toDebugProtobuf();
        AzBlobFileStoreInfo azblob2 = info2.getAzblobFsInfo();
        Assert.assertEquals(azblob2.getCredential().getSharedKey(), "");
    }

    @Test
    public void testAzBlobFileStoreIsValid() {
        AzBlobFileStore fs = new AzBlobFileStore(Constant.AZURE_BLOB_FSKEY_FOR_CONFIG,
                Constant.AZURE_BLOB_FSNAME_FOR_CONFIG, "", "", null);
        Assert.assertFalse(fs.isValid());

        fs = new AzBlobFileStore(Constant.AZURE_BLOB_FSKEY_FOR_CONFIG,
                Constant.AZURE_BLOB_FSNAME_FOR_CONFIG, "test_endpoint", "", null);
        Assert.assertFalse(fs.isValid());

        fs = new AzBlobFileStore(Constant.AZURE_BLOB_FSKEY_FOR_CONFIG,
                Constant.AZURE_BLOB_FSNAME_FOR_CONFIG, "", "test_path", null);
        Assert.assertFalse(fs.isValid());

        fs = new AzBlobFileStore(Constant.AZURE_BLOB_FSKEY_FOR_CONFIG,
                Constant.AZURE_BLOB_FSNAME_FOR_CONFIG, "test_endpoint", "test_path", null);
        Assert.assertTrue(fs.isValid());
    }

    @Test
    public void testADLS2FileStoreLoadConfig() {
        try (HijackConfig ignored = new HijackConfig("AZURE_ADLS2_ENDPOINT", "")) {
            FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
            Assert.assertNull(fsMgr.loadADLS2FileStoreFromConfig());
            Assert.assertThrows(NotExistStarException.class, () -> fsMgr.allocFileStore(Constant.AZURE_ADLS2_FSKEY_FOR_CONFIG));
        }

        try (HijackConfig ignored = new HijackConfig("AZURE_ADLS2_PATH", "")) {
            FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
            fsMgr.loadADLS2FileStoreFromConfig();
            Assert.assertThrows(NotExistStarException.class, () -> fsMgr.allocFileStore(Constant.AZURE_ADLS2_FSKEY_FOR_CONFIG));
        }

        FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
        fsMgr.loadADLS2FileStoreFromConfig();
        FileStore fs = fsMgr.allocFileStore("");
        Assert.assertNotNull(fs);
        FileStore newFs = FileStore.fromProtobuf(fs.toProtobuf());
        Assert.assertNotNull(newFs);
    }

    @Test
    public void testADLS2FileStoreSerialize() {
        ADLS2Credential credential = new ADLS2Credential("test_key", "", "", "", "", "", "");
        ADLS2FileStore fs = new ADLS2FileStore(Constant.AZURE_ADLS2_FSKEY_FOR_CONFIG,
                Constant.AZURE_ADLS2_FSNAME_FOR_CONFIG, "test_endpoint", "test_path", credential);
        FileStoreInfo fsInfo = fs.toProtobuf();
        FileStore fileStore = FileStore.fromProtobuf(fsInfo);
        Assert.assertTrue(fileStore instanceof ADLS2FileStore);
        Assert.assertEquals(FileStoreType.ADLS2, fileStore.type());
        Assert.assertEquals(fs.key(), fileStore.key());
    }

    @Test
    public void testADLS2FileStoreProtobuf() {
        ADLS2CredentialInfo azcredential = ADLS2CredentialInfo.newBuilder().setSharedKey("aaa").build();
        ADLS2FileStoreInfo adls2 = ADLS2FileStoreInfo.newBuilder().setPath("path").setCredential(azcredential).build();
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsName("test-name")
                .setAdls2FsInfo(adls2)
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.ADLS2)
                .addAllLocations(new ArrayList<>()).build();
        FileStore adls2FileStore = ADLS2FileStore.fromProtobuf(info);
        Assert.assertEquals("test-fskey", adls2FileStore.key());
        Assert.assertEquals("adls2://path", adls2FileStore.rootPath());

        FileStoreInfo info1 = adls2FileStore.toProtobuf();
        ADLS2FileStoreInfo adls21 = info1.getAdls2FsInfo();
        Assert.assertEquals(adls2.getPath(), adls2.getPath());
        Assert.assertEquals(adls2.getCredential().getSharedKey(), "aaa");
        Assert.assertEquals(new ArrayList<>(Arrays.asList("adls2://path")), info1.getLocationsList());

        FileStoreInfo info2 = adls2FileStore.toDebugProtobuf();
        ADLS2FileStoreInfo adls22 = info2.getAdls2FsInfo();
        Assert.assertEquals(adls22.getCredential().getSharedKey(), "");
    }

    @Test
    public void testGSFileStoreLoadConfig() {
        try (HijackConfig ignored = new HijackConfig("GCP_GS_PATH", "")) {
            FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
            fsMgr.loadGSFileStoreFromConfig();
            Assert.assertThrows(NotExistStarException.class, () -> fsMgr.allocFileStore(Constant.GS_FSKEY_FOR_CONFIG));
        }

        FileStoreMgr fsMgr = FileStoreMgr.createFileStoreMgrForTest("");
        fsMgr.loadGSFileStoreFromConfig();
        FileStore fs = fsMgr.allocFileStore("");
        Assert.assertNotNull(fs);
        FileStore newFs = FileStore.fromProtobuf(fs.toProtobuf());
        Assert.assertNotNull(newFs);
    }

    @Test
    public void testGSFileStoreSerialize() {
        GSCredential credential = new GSCredential(false, "", "", "", "iuser");
        GSFileStore fs = new GSFileStore(Constant.GS_FSKEY_FOR_CONFIG,
                Constant.GS_FSNAME_FOR_CONFIG, "", "test_path", credential);
        FileStoreInfo fsInfo = fs.toProtobuf();
        FileStore fileStore = FileStore.fromProtobuf(fsInfo);
        Assert.assertTrue(fileStore instanceof GSFileStore);
        Assert.assertEquals(FileStoreType.GS, fileStore.type());
        Assert.assertEquals(fs.key(), fileStore.key());
        Assert.assertEquals(fs.rootPath(), fileStore.rootPath());
    }

    @Test
    public void testGSFileStoreProtobuf() {
        GSFileStoreInfo gs = GSFileStoreInfo.newBuilder().setPath("path").setUseComputeEngineServiceAccount(true).build();
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsName("test-name")
                .setGsFsInfo(gs)
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.GS)
                .addAllLocations(new ArrayList<>()).build();
        FileStore gsFileStore = GSFileStore.fromProtobuf(info);
        Assert.assertEquals("test-fskey", gsFileStore.key());
        Assert.assertEquals("gs://path", gsFileStore.rootPath());

        FileStoreInfo info1 = gsFileStore.toProtobuf();
        GSFileStoreInfo gs1 = info1.getGsFsInfo();
        Assert.assertEquals(gs.getPath(), gs1.getPath());
        Assert.assertEquals(new ArrayList<>(Arrays.asList("gs://path")), info1.getLocationsList());

        FileStoreInfo info2 = gsFileStore.toDebugProtobuf();
        GSFileStoreInfo gs2 = info2.getGsFsInfo();
        Assert.assertEquals(gs2.getUseComputeEngineServiceAccount(), true);
    }

    @Test
    public void testGSFileStoreIsValid() {
        GSFileStore fs = new GSFileStore(Constant.GS_FSKEY_FOR_CONFIG,
                Constant.GS_FSNAME_FOR_CONFIG, "", "", null);
        Assert.assertFalse(fs.isValid());

        fs = new GSFileStore(Constant.GS_FSKEY_FOR_CONFIG,
                Constant.GS_FSNAME_FOR_CONFIG, "test_endpoint", "", null);
        Assert.assertFalse(fs.isValid());

        fs = new GSFileStore(Constant.GS_FSKEY_FOR_CONFIG,
                Constant.GS_FSNAME_FOR_CONFIG, "", "test_path", null);
        Assert.assertTrue(fs.isValid());

        fs = new GSFileStore(Constant.GS_FSKEY_FOR_CONFIG,
                Constant.GS_FSNAME_FOR_CONFIG, "test_endpoint", "test_path", null);
        Assert.assertTrue(fs.isValid());
    }

    @Test
    public void testADLS2FileStoreIsValid() {
        ADLS2FileStore fs = new ADLS2FileStore(Constant.AZURE_ADLS2_FSKEY_FOR_CONFIG,
                Constant.AZURE_ADLS2_FSNAME_FOR_CONFIG, "", "", null);
        Assert.assertFalse(fs.isValid());

        fs = new ADLS2FileStore(Constant.AZURE_ADLS2_FSKEY_FOR_CONFIG,
                Constant.AZURE_ADLS2_FSNAME_FOR_CONFIG, "test_endpoint", "", null);
        Assert.assertFalse(fs.isValid());

        fs = new ADLS2FileStore(Constant.AZURE_ADLS2_FSKEY_FOR_CONFIG,
                Constant.AZURE_ADLS2_FSNAME_FOR_CONFIG, "", "test_path", null);
        Assert.assertFalse(fs.isValid());

        fs = new ADLS2FileStore(Constant.AZURE_ADLS2_FSKEY_FOR_CONFIG,
                Constant.AZURE_ADLS2_FSNAME_FOR_CONFIG, "test_endpoint", "test_path", null);
        Assert.assertTrue(fs.isValid());
    }

    @Test
    public void testInvalidFileStore() {
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsName("test-name")
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.INVALID)
                .addAllLocations(Arrays.asList("azblob://path")).build();
        FileStore invalidFileStore = FileStore.fromProtobuf(info);
        Assert.assertEquals("", invalidFileStore.rootPath());
        Assert.assertEquals(true, invalidFileStore.isValid());
        Assert.assertEquals(InvalidFileStore.INVALID_KEY_PREFIX + "_" + FileStoreType.INVALID, invalidFileStore.key());
        Assert.assertEquals(FileStoreType.INVALID, invalidFileStore.type());
    }

    @Test
    public void testAzblobFileStoreMergeFrom() {
        AzBlobCredentialInfo azcredential = AzBlobCredentialInfo.newBuilder().setSharedKey("aaa").build();
        AzBlobFileStoreInfo azblob = AzBlobFileStoreInfo.newBuilder().setPath("path").setCredential(azcredential).build();
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsName("test-name")
                .setAzblobFsInfo(azblob)
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.AZBLOB)
                .addAllLocations(new ArrayList<>()).build();

        { // update endpoint only
            FileStore azblobFileStore = AzBlobFileStore.fromProtobuf(info);
            Assert.assertEquals("test-fskey", azblobFileStore.key());
            Assert.assertEquals("azblob://path", azblobFileStore.rootPath());

            String endpoint = "new-endpoint";
            FileStore otherFileStore =
                    new AzBlobFileStore("test-fskey", "test-name", endpoint, "", AzBlobCredential.fromProtobuf(
                            AzBlobCredentialInfo.newBuilder().build()));

            AzBlobFileStoreInfo beforeUpdateInfo = azblobFileStore.toProtobuf().getAzblobFsInfo();
            azblobFileStore.mergeFrom(otherFileStore);
            AzBlobFileStoreInfo afterUpdateInfo = azblobFileStore.toProtobuf().getAzblobFsInfo();
            Assert.assertEquals(endpoint, afterUpdateInfo.getEndpoint());
            Assert.assertNotEquals(beforeUpdateInfo.getEndpoint(), afterUpdateInfo.getEndpoint());
            // reset the endpoint, both are equal
            beforeUpdateInfo = beforeUpdateInfo.toBuilder().clearEndpoint().build();
            afterUpdateInfo = afterUpdateInfo.toBuilder().clearEndpoint().build();
            Assert.assertEquals(beforeUpdateInfo, afterUpdateInfo);
        }
        { // update credential only
            FileStore azblobFileStore = AzBlobFileStore.fromProtobuf(info);

            AzBlobCredentialInfo newCredInfo = AzBlobCredentialInfo.newBuilder().setClientId("clientId").build();
            FileStore otherFileStore =
                    new AzBlobFileStore("test-fskey", "test-name", "", "", AzBlobCredential.fromProtobuf(newCredInfo));

            AzBlobFileStoreInfo beforeUpdateInfo = azblobFileStore.toProtobuf().getAzblobFsInfo();
            azblobFileStore.mergeFrom(otherFileStore);
            AzBlobFileStoreInfo afterUpdateInfo = azblobFileStore.toProtobuf().getAzblobFsInfo();

            Assert.assertNotEquals(beforeUpdateInfo.getCredential(), afterUpdateInfo.getCredential());
            Assert.assertEquals(afterUpdateInfo.getCredential(), newCredInfo);
            // reset the credential, both are equal
            beforeUpdateInfo = beforeUpdateInfo.toBuilder().clearCredential().build();
            afterUpdateInfo = afterUpdateInfo.toBuilder().clearCredential().build();
            Assert.assertEquals(beforeUpdateInfo, afterUpdateInfo);
        }
    }

    @Test
    public void testADLS2FileStoreMergeFrom() {
        ADLS2CredentialInfo azcredential = ADLS2CredentialInfo.newBuilder().setSharedKey("aaa").build();
        ADLS2FileStoreInfo adls2 = ADLS2FileStoreInfo.newBuilder().setPath("path").setCredential(azcredential)
                .build();
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsName("test-name")
                .setAdls2FsInfo(adls2)
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.ADLS2)
                .addAllLocations(new ArrayList<>()).build();

        { // update endpoint only
            FileStore adls2FileStore = ADLS2FileStore.fromProtobuf(info);
            Assert.assertEquals("test-fskey", adls2FileStore.key());
            Assert.assertEquals("adls2://path", adls2FileStore.rootPath());

            String endpoint = "new-endpoint";
            FileStore otherFileStore = new ADLS2FileStore("test-fskey", "test-name", endpoint, "",
                    ADLS2Credential.fromProtobuf(
                            ADLS2CredentialInfo.newBuilder().build()));

            ADLS2FileStoreInfo beforeUpdateInfo = adls2FileStore.toProtobuf().getAdls2FsInfo();
            adls2FileStore.mergeFrom(otherFileStore);
            ADLS2FileStoreInfo afterUpdateInfo = adls2FileStore.toProtobuf().getAdls2FsInfo();
            Assert.assertEquals(endpoint, afterUpdateInfo.getEndpoint());
            Assert.assertNotEquals(beforeUpdateInfo.getEndpoint(), afterUpdateInfo.getEndpoint());
            // reset the endpoint, both are equal
            beforeUpdateInfo = beforeUpdateInfo.toBuilder().clearEndpoint().build();
            afterUpdateInfo = afterUpdateInfo.toBuilder().clearEndpoint().build();
            Assert.assertEquals(beforeUpdateInfo, afterUpdateInfo);
        }
        { // update credential only
            FileStore adls2FileStore = ADLS2FileStore.fromProtobuf(info);

            ADLS2CredentialInfo newCredInfo = ADLS2CredentialInfo.newBuilder().setClientId("clientId").build();
            FileStore otherFileStore = new ADLS2FileStore("test-fskey", "test-name", "", "",
                    ADLS2Credential.fromProtobuf(newCredInfo));

            ADLS2FileStoreInfo beforeUpdateInfo = adls2FileStore.toProtobuf().getAdls2FsInfo();
            adls2FileStore.mergeFrom(otherFileStore);
            ADLS2FileStoreInfo afterUpdateInfo = adls2FileStore.toProtobuf().getAdls2FsInfo();

            Assert.assertNotEquals(beforeUpdateInfo.getCredential(), afterUpdateInfo.getCredential());
            Assert.assertEquals(afterUpdateInfo.getCredential(), newCredInfo);
            // reset the credential, both are equal
            beforeUpdateInfo = beforeUpdateInfo.toBuilder().clearCredential().build();
            afterUpdateInfo = afterUpdateInfo.toBuilder().clearCredential().build();
            Assert.assertEquals(beforeUpdateInfo, afterUpdateInfo);
        }
    }

    @Test
    public void testHdfsFileStoreMergeFrom() {
        HDFSFileStoreInfo hdfs = HDFSFileStoreInfo.newBuilder()
                .setUrl("url")
                .putConfiguration("haddoop.not.exist.configvar", "nothing")
                .build();
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsType(FileStoreType.HDFS)
                .setFsName("test-name")
                .setHdfsFsInfo(hdfs)
                .setFsKey("test-fskey")
                .addAllLocations(new ArrayList<>()).build();
        { // verify the FileStore object built from the FileStoreInfo
            FileStore hdfsFileStore = FileStore.fromProtobuf(info);
            Assert.assertEquals("test-fskey", hdfsFileStore.key());
            Assert.assertEquals("url/", hdfsFileStore.rootPath());
        }

        { // update username of the HdfsFileStore
            FileStore hdfsFileStore = FileStore.fromProtobuf(info);
            HDFSFileStoreInfo updatedInfo = HDFSFileStoreInfo.newBuilder()
                    .setUsername("username1").build();
            FileStore otherFileStore = FileStore.fromProtobuf(info.toBuilder().setHdfsFsInfo(updatedInfo).build());

            HDFSFileStoreInfo beforeUpdateInfo = hdfsFileStore.toProtobuf().getHdfsFsInfo();
            hdfsFileStore.mergeFrom(otherFileStore);
            HDFSFileStoreInfo afterUpdateInfo = hdfsFileStore.toProtobuf().getHdfsFsInfo();

            Assert.assertNotEquals(beforeUpdateInfo, afterUpdateInfo);
            Assert.assertEquals("username1", afterUpdateInfo.getUsername());
            // after clear the UserName field, the two infos are identical.
            beforeUpdateInfo = beforeUpdateInfo.toBuilder().clearUsername().build();
            afterUpdateInfo = afterUpdateInfo.toBuilder().clearUsername().build();
            Assert.assertEquals(beforeUpdateInfo, afterUpdateInfo);
        }

        { // update properties of the HdfsFileStore
            FileStore hdfsFileStore = FileStore.fromProtobuf(info);
            HDFSFileStoreInfo updatedInfo = HDFSFileStoreInfo.newBuilder()
                    .putConfiguration("hadoop.security", "simple")
                    .build();
            FileStore otherFileStore = FileStore.fromProtobuf(info.toBuilder().setHdfsFsInfo(updatedInfo).build());

            HDFSFileStoreInfo beforeUpdateInfo = hdfsFileStore.toProtobuf().getHdfsFsInfo();
            hdfsFileStore.mergeFrom(otherFileStore);
            HDFSFileStoreInfo afterUpdateInfo = hdfsFileStore.toProtobuf().getHdfsFsInfo();

            Assert.assertNotEquals(beforeUpdateInfo, afterUpdateInfo);
            Map<String, String> beforeMap = beforeUpdateInfo.getConfigurationMap();
            Assert.assertEquals(1L, beforeMap.size());
            Assert.assertEquals("nothing", beforeMap.get("haddoop.not.exist.configvar"));

            Map<String, String> afterMap = afterUpdateInfo.getConfigurationMap();
            Assert.assertEquals(2L, afterMap.size());
            Assert.assertEquals("nothing", afterMap.get("haddoop.not.exist.configvar"));
            Assert.assertEquals("simple", afterMap.get("hadoop.security"));

            // after clear the configuration field, the two infos are identical.
            beforeUpdateInfo = beforeUpdateInfo.toBuilder().clearConfiguration().build();
            afterUpdateInfo = afterUpdateInfo.toBuilder().clearConfiguration().build();
            Assert.assertEquals(beforeUpdateInfo, afterUpdateInfo);
        }
    }

    @Test
    public void testGSFileStoreMergeFrom() {
        GSFileStoreInfo gs = GSFileStoreInfo.newBuilder().setPath("path").setUseComputeEngineServiceAccount(true)
                .setImpersonation("iuser").build();
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsName("test-name")
                .setGsFsInfo(gs)
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.GS)
                .addAllLocations(new ArrayList<>()).build();

        { // update endpoint only
            FileStore gsFileStore = GSFileStore.fromProtobuf(info);
            Assert.assertEquals("test-fskey", gsFileStore.key());
            Assert.assertEquals("gs://path", gsFileStore.rootPath());

            String endpoint = "new-endpoint";
            FileStore otherFileStore =
                    new GSFileStore("test-fskey", "test-name", endpoint, "", new GSCredential(true, "", "", "",  "iuser"));

            GSFileStoreInfo beforeUpdateInfo = gsFileStore.toProtobuf().getGsFsInfo();
            gsFileStore.mergeFrom(otherFileStore);
            GSFileStoreInfo afterUpdateInfo = gsFileStore.toProtobuf().getGsFsInfo();
            Assert.assertEquals(endpoint, afterUpdateInfo.getEndpoint());
            Assert.assertNotEquals(beforeUpdateInfo.getEndpoint(), afterUpdateInfo.getEndpoint());
            // reset the endpoint, both are equal
            beforeUpdateInfo = beforeUpdateInfo.toBuilder().clearEndpoint().build();
            afterUpdateInfo = afterUpdateInfo.toBuilder().clearEndpoint().build();
            Assert.assertEquals(beforeUpdateInfo, afterUpdateInfo);
        }
        { // update credential only
            FileStore gsFileStore = GSFileStore.fromProtobuf(info);

            FileStore otherFileStore =
                    new GSFileStore("test-fskey", "test-name", "", "", new GSCredential(true, "", "", "",  "iuser2"));

            GSFileStoreInfo beforeUpdateInfo = gsFileStore.toProtobuf().getGsFsInfo();
            gsFileStore.mergeFrom(otherFileStore);
            GSFileStoreInfo afterUpdateInfo = gsFileStore.toProtobuf().getGsFsInfo();

            Assert.assertNotEquals(beforeUpdateInfo.getImpersonation(), afterUpdateInfo.getImpersonation());
            // reset the credential, both are equal
            beforeUpdateInfo = beforeUpdateInfo.toBuilder().clearImpersonation().build();
            afterUpdateInfo = afterUpdateInfo.toBuilder().clearImpersonation().build();
            Assert.assertEquals(beforeUpdateInfo, afterUpdateInfo);
        }
    }

    @Test
    public void testFileStoreProertiesMap() {
        AwsCredentialInfo awsCredentialInfo = AwsCredentialInfo.newBuilder()
                .setDefaultCredential(AwsDefaultCredentialInfo.newBuilder().build()).build();
        S3FileStoreInfo s3fs = S3FileStoreInfo.newBuilder()
                .setBucket("bucket").setPathPrefix("prefix").setCredential(awsCredentialInfo).build();
        Map<String, String> properties = new HashMap<>();
        properties.put("property_1", "value_1");
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsName("test-name")
                .setS3FsInfo(s3fs)
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.S3)
                .addAllLocations(new ArrayList<>())
                .putAllProperties(properties).build();

        // Convert Protobuf to FileStore
        FileStore s3FileStore = FileStore.fromProtobuf(info);
        Assert.assertNotNull(s3FileStore.getPropertiesMap());
        Assert.assertEquals(properties, s3FileStore.getPropertiesMap());
        Assert.assertEquals("value_1", s3FileStore.getPropertiesMap().get("property_1"));

        // Convert FileStore to protobuf
        FileStoreInfo info1 = s3FileStore.toProtobuf();
        Assert.assertNotNull(info1.getPropertiesMap());
        Assert.assertEquals(info.getPropertiesMap(), info1.getPropertiesMap());
        Assert.assertEquals("value_1", info1.getPropertiesMap().get("property_1"));
    }
}
