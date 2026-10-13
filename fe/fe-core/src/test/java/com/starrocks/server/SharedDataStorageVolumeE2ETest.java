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

package com.starrocks.server;

import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsWebIdentityCredentialInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.common.Config;
import com.starrocks.utframe.TestWithFeService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

public class SharedDataStorageVolumeE2ETest extends TestWithFeService {
    @Override
    protected void beforeCluster() {
        runMode = RunMode.SHARED_DATA;
    }

    @Override
    protected void runBeforeAll() {
        connectContext.setCurrentRoleIds(connectContext.getCurrentUserIdentity());
    }

    @Test
    public void testCreateWebIdentityStorageVolume() throws Exception {
        boolean oldValue = Config.enable_storage_volume_access_check;
        Config.enable_storage_volume_access_check = false;
        try {
            verifyWebIdentityStorageVolume("web_identity_volume", "", "");
            verifyWebIdentityStorageVolume("web_identity_assume_role_volume",
                    "arn:aws:iam::123456789012:role/data", "external_id");
        } finally {
            Config.enable_storage_volume_access_check = oldValue;
        }
    }

    private void verifyWebIdentityStorageVolume(String storageVolumeName, String iamRoleArn, String externalId)
            throws Exception {
        StringBuilder properties = new StringBuilder()
                .append("\"aws.s3.region\" = \"us-west-2\", ")
                .append("\"aws.s3.endpoint\" = \"https://s3.us-west-2.amazonaws.com\", ")
                .append("\"aws.s3.use_aws_sdk_default_behavior\" = \"false\", ")
                .append("\"aws.s3.use_instance_profile\" = \"false\", ")
                .append("\"aws.s3.use_web_identity_token_file\" = \"true\"");
        if (!iamRoleArn.isEmpty()) {
            properties.append(", \"aws.s3.iam_role_arn\" = \"").append(iamRoleArn).append("\"")
                    .append(", \"aws.s3.external_id\" = \"").append(externalId).append("\"");
        }

        String sql = "CREATE STORAGE VOLUME " + storageVolumeName + " " +
                "TYPE = S3 " +
                "LOCATIONS = (\"s3://web-identity-bucket/test/\") " +
                "PROPERTIES (" + properties + ");";
        connectContext.setQueryId(UUID.randomUUID());
        connectContext.executeSql(sql);
        Assertions.assertFalse(connectContext.getState().isError(), connectContext.getState().getErrorMessage());

        FileStoreInfo fileStoreInfo = GlobalStateMgr.getCurrentState().getStarOSAgent()
                .getFileStoreByName(storageVolumeName);
        Assertions.assertNotNull(fileStoreInfo);
        Assertions.assertEquals(FileStoreType.S3, fileStoreInfo.getFsType());
        Assertions.assertEquals("s3://web-identity-bucket/test/", fileStoreInfo.getLocations(0));

        S3FileStoreInfo s3Info = fileStoreInfo.getS3FsInfo();
        Assertions.assertEquals("us-west-2", s3Info.getRegion());
        Assertions.assertEquals("https://s3.us-west-2.amazonaws.com", s3Info.getEndpoint());
        Assertions.assertEquals(AwsCredentialInfo.CredentialCase.WEB_IDENTITY_CREDENTIAL,
                s3Info.getCredential().getCredentialCase());
        AwsWebIdentityCredentialInfo credential = s3Info.getCredential().getWebIdentityCredential();
        Assertions.assertEquals(iamRoleArn, credential.getIamRoleArn());
        Assertions.assertEquals(externalId, credential.getExternalId());
    }
}
