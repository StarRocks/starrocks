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

package com.starrocks.credential.azure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;

public class AzureStoragePathUtil {
    public static final Logger LOG = LogManager.getLogger(AzureStoragePathUtil.class);

    // Supported URI:
    // ADLS Gen2:
    // abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/<path>/<file_name>
    // ADLS Gen1:
    // ADLS Gen1's path don't need to be parsed, storage account and container is unnecessary for it.
    // adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>
    // Blob
    // wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>/<path>/<file_name>
    public static AzureStoragePath parseStoragePath(String path) {
        try {
            URI uri = new URI(path);
            String rawAuthority = uri.getRawAuthority();
            if (rawAuthority == null) {
                throw new URISyntaxException(path, "Illegal azure storage path");
            }
            String[] parts = uri.getRawAuthority().split("@");
            if (parts.length < 2) {
                throw new URISyntaxException(path, "Illegal azure storage path");
            }

            if (!path.contains(".blob.core.windows.net") && !path.contains(".dfs.core.windows.net")) {
                throw new URISyntaxException(path, "Illegal azure storage path");
            }

            String container = parts[0];
            if (container.isEmpty()) {
                throw new URISyntaxException(path, "Empty container name in azure storage path");
            }

            String[] leftParts = parts[1].split("\\.");
            String storageAccount = leftParts[0];
            if (storageAccount.isEmpty()) {
                throw new URISyntaxException(path, "Empty storage account in azure storage path");
            }
            return new AzureStoragePath(storageAccount, container);
        } catch (URISyntaxException exception) {
            LOG.warn(exception.getMessage());
        }
        // Return empty AzureStoragePath
        return new AzureStoragePath("", "");
    }
}
