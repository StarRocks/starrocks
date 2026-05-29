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

package com.starrocks.common.udf.impl;

import com.starrocks.common.udf.StorageHandler;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * StorageHandler for http://, https://, and file:// URLs using standard Java URL APIs.
 */
public class DefaultStorageHandler implements StorageHandler {

    @Override
    public InputStream openStream(String remotePath) throws Exception {
        return new URL(remotePath).openStream();
    }

    @Override
    public void getObject(String remotePath, String localPath) throws Exception {
        try (InputStream in = new URL(remotePath).openStream()) {
            Files.copy(in, Paths.get(localPath), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public void close() {
    }
}
