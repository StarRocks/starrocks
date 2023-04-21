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

import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.storagevolume.storageparams.S3StorageParams;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;

public class StorageVolumeMgrTest {
    @Test
    public void testStorageVolumeCRUD() {
        String svKey = "test";
        // create
        StorageVolumeMgr svm = new StorageVolumeMgr();
        List<String> locations = new ArrayList<>();
        locations.add("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        svm.createStorageVolume(svKey, "S3", locations, storageParams, true, "");
        Assert.assertEquals(true, svm.exists(svKey));
        StorageVolume sv = svm.getStorageVolume(svKey);
        S3StorageParams sp = (S3StorageParams) sv.getStorageParams();
        Assert.assertEquals("region", sp.getRegion());
        Assert.assertEquals("endpoint", sp.getEndpoint());

        // update
        storageParams.put(AWS_S3_REGION, "region1");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint1");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        svm.updateStorageVolume(svKey, storageParams, false, "test update", "test");
        sp = (S3StorageParams) sv.getStorageParams();
        Assert.assertEquals("region1", sp.getRegion());
        Assert.assertEquals("endpoint1", sp.getEndpoint());
        Assert.assertEquals("test update", sv.getComment());
        Assert.assertEquals(false, sv.getEnabled());
        Assert.assertEquals("test", svm.getDefaultSV());

        // remove
        svm.removeStorageVolume("test");
        Assert.assertFalse(svm.exists("test"));
    }
}
