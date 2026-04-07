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
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class UDFDownloader {

    private static final Logger LOG = LogManager.getLogger(UDFDownloader.class);

    public static void download2Local(String remotePath, String localPath,
                                      CloudConfiguration cloudConfiguration) throws IOException {
        Status status = doDownload(remotePath, localPath, cloudConfiguration);
        if (!status.ok()) {
            LOG.error(status.getErrorMsg());
            throw new RuntimeException(status.getErrorMsg());
        }
    }

    private static Status doDownload(String remotePath, String localPath, CloudConfiguration cloudConfiguration) {
        try (StorageHandler handler = StorageHandlerFactory.create(cloudConfiguration)) {
            handler.getObject(remotePath, localPath);
        } catch (UnsupportedOperationException e) {
            return new Status(new Status(TStatusCode.RUNTIME_ERROR, e.getMessage()));
        } catch (Exception e) {
            LOG.error("Failed to download remote file from {}", remotePath, e);
            String errMsg = String.format("Failed to download remote file from %s as %s", remotePath, e.getMessage());
            return new Status(new Status(TStatusCode.RUNTIME_ERROR, errMsg));
        }
        return Status.OK;
    }
}