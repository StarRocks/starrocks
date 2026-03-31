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

package com.starrocks.common.udf;

import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;

public class UDFDownloaderTest {


    @Test
    void testDownload2Local(@TempDir Path tempDir) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put(AWS_S3_REGION, "region");
        properties.put(AWS_S3_ENDPOINT, "endpoint");
        properties.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        CloudConfiguration cloudConfiguration =
                CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
        String remotePath = "s3://test-bucket/starrocks/udf/test.jar";
        String localPath = tempDir.resolve("test.jar").toString();
        Assertions.assertThrows(RuntimeException.class, () ->
                UDFDownloader.download2Local(remotePath, localPath, cloudConfiguration));
    }
}
