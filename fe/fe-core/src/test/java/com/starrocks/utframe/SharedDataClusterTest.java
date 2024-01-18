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

package com.starrocks.utframe;

import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsSimpleCredentialInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.cloudnative.storagevolume.StorageVolume;
import com.starrocks.cloudnative.storagevolume.StorageVolumeMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.SetDefaultStorageVolumeStmt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SharedDataClusterTest extends TestWithFeService {

    @Override
    protected void beforeCluster() {
        runMode = RunMode.SHARED_DATA;
    }

    @Test
    public void createStorageVolumeTestInSharedDataMode() throws Exception {
        // StorageVolume is a specific feature for shared-data mode
        String svName = "my_s3_volume";
        String svSql = "CREATE STORAGE VOLUME " + svName + " " +
                "TYPE = S3 " +
                "LOCATIONS = (\"s3://defaultbucket/test/\") " +
                "PROPERTIES " +
                "( " +
                "    \"aws.s3.region\" = \"us-west-2\", " +
                "    \"aws.s3.endpoint\" = \"https://s3.us-west-2.amazonaws.com\", " +
                "    \"aws.s3.use_aws_sdk_default_behavior\" = \"false\", " +
                "    \"aws.s3.use_instance_profile\" = \"false\", " +
                "    \"aws.s3.access_key\" = \"xxxxxxxxxx\", " +
                "    \"aws.s3.secret_key\" = \"yyyyyyyyyy\" " +
                ");";
        CreateStorageVolumeStmt stmt = (CreateStorageVolumeStmt) parseAndAnalyzeStmt(svSql);

        StorageVolumeMgr svMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();

        svMgr.createStorageVolume(stmt);
        StorageVolume sv = svMgr.getStorageVolumeByName(svName);
        Assertions.assertTrue(sv.getEnabled());
        StorageVolume defaultSV = svMgr.getDefaultStorageVolume();
        Assertions.assertNotEquals(defaultSV.getId(), sv.getId());

        FileStoreInfo fsInfo = sv.toFileStoreInfo();
        Assertions.assertEquals(FileStoreType.S3, fsInfo.getFsType());
        S3FileStoreInfo s3Info = fsInfo.getS3FsInfo();
        Assertions.assertEquals("https://s3.us-west-2.amazonaws.com", s3Info.getEndpoint());
        Assertions.assertEquals("us-west-2", s3Info.getRegion());
        Assertions.assertEquals(AwsCredentialInfo.CredentialCase.SIMPLE_CREDENTIAL,
                s3Info.getCredential().getCredentialCase());
        AwsSimpleCredentialInfo simpleCredentialInfo = s3Info.getCredential().getSimpleCredential();
        Assertions.assertEquals(simpleCredentialInfo.getAccessKey(), "xxxxxxxxxx");
        Assertions.assertEquals(simpleCredentialInfo.getAccessKeySecret(), "yyyyyyyyyy");

        // set the new storage volume as the default one
        String setDefaultSql = "SET " + svName + " AS DEFAULT STORAGE VOLUME;";
        SetDefaultStorageVolumeStmt setStmt = (SetDefaultStorageVolumeStmt) parseAndAnalyzeStmt(setDefaultSql);
        svMgr.setDefaultStorageVolume(setStmt);

        StorageVolume sv2 = svMgr.getDefaultStorageVolume();
        Assertions.assertEquals(sv2.getId(), sv.getId());
    }
}
