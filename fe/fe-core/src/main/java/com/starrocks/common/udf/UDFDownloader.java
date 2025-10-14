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

import com.starrocks.credential.CloudType;
import com.starrocks.storagevolume.StorageVolume;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class UDFDownloader {

    public static void download2Local(StorageVolume sv, String remotePath, String localPath) {
        try {
            doDownload(sv, remotePath, localPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to download S3 file to local: " + e.getMessage(), e);
        }
    }

    private static void doDownload(StorageVolume sv, String remotePath, String localPath)
            throws Exception {
        Path parentDir = Paths.get(localPath).getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
        }
        File localFile = new File(localPath);
        if (localFile.exists() && !localFile.delete()) {
            throw new RuntimeException("Failed to delete existing file: " + localPath);
        }
        if (sv.getCloudConfiguration().getCloudType() == CloudType.AWS) {
            S3StorageHandler s3StorageHandler = new S3StorageHandler(sv);
            s3StorageHandler.getObject(remotePath, localFile);
        }  else {
            throw new RuntimeException("Cloud type is not supported: " + sv.getCloudConfiguration().getCloudType());
        }
    }
}
