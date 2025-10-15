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

import com.starrocks.common.Status;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.thrift.TStatusCode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class UDFDownloader {

    private static final Logger LOG = LogManager.getLogger(UDFDownloader.class);

    public static void download2Local(StorageVolume sv, String remotePath, String localPath) {
        Status status = doDownload(sv, remotePath, localPath);
        if (status != Status.OK) {
            LOG.error(status.getErrorMsg());
            throw new RuntimeException(status.getErrorMsg());
        }
    }

    private static Status doDownload(StorageVolume sv, String remotePath, String localPath) {
        try {
            Path parentDir = Paths.get(localPath).getParent();
            if (parentDir != null && !Files.exists(parentDir)) {
                Files.createDirectories(parentDir);
            }
            File localFile = new File(localPath);
            if (localFile.exists() && !localFile.delete()) {
                String errMsg = String.format("Failed to delete existing local file %s", localFile);
                return new Status(new Status(TStatusCode.RUNTIME_ERROR, errMsg));
            }
            StorageHandler handler = StorageHandlerFactory.create(sv);
            handler.getObject(remotePath, localFile);
            return Status.OK;
        } catch (UnsupportedOperationException e) {
            return new Status(new Status(TStatusCode.RUNTIME_ERROR, e.getMessage()));
        } catch (Exception e) {
            String errMsg = String.format("Failed to download remote file %s as %s", remotePath, e.getMessage());
            return new Status(new Status(TStatusCode.RUNTIME_ERROR, errMsg));
        }
    }
}