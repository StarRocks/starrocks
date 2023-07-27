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

package com.starrocks.credential;

<<<<<<< HEAD
=======
import com.starrocks.catalog.JDBCResource;
import com.starrocks.credential.azure.AzureCloudConfigurationFactory;
import com.starrocks.credential.azure.AzureStoragePath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
>>>>>>> d1b75dd161 ([BugFix] Not showing password on show create JDBC catalog (#28059))
import java.util.Map;

public class CloudCredentialUtil {
    private static final String MASK_CLOUD_CREDENTIAL_WORDS = "******";

    public static void maskCloudCredential(Map<String, String> properties) {
        // aws.s3.access_key -> ******
        properties.computeIfPresent(CloudConfigurationConstants.AWS_S3_ACCESS_KEY,
                (k, v) -> replaceWithIndex(2, v.length() - 2, v, MASK_CLOUD_CREDENTIAL_WORDS));
        // aws.s3.secret_key -> ******
        properties.computeIfPresent(CloudConfigurationConstants.AWS_S3_SECRET_KEY,
                (k, v) -> replaceWithIndex(2, v.length() - 2, v, MASK_CLOUD_CREDENTIAL_WORDS));
        // aws.glue.access_key -> ******
        properties.computeIfPresent(CloudConfigurationConstants.AWS_GLUE_ACCESS_KEY,
                (k, v) -> replaceWithIndex(2, v.length() - 2, v, MASK_CLOUD_CREDENTIAL_WORDS));
        // aws.glue.secret_key -> ******
        properties.computeIfPresent(CloudConfigurationConstants.AWS_GLUE_SECRET_KEY,
                (k, v) -> replaceWithIndex(2, v.length() - 2, v, MASK_CLOUD_CREDENTIAL_WORDS));
    }

<<<<<<< HEAD
    public static String replaceWithIndex(int start, int end, String oldChar, String replaceChar) {
        if (start > end) {
            return MASK_CLOUD_CREDENTIAL_WORDS;
=======
    private static void doMask(Map<String, String> properties, String configKey) {
        // This key is only auxiliary authentication for Azure and does not need to be exposed.
        properties.remove(AzureCloudConfigurationFactory.AZURE_PATH_KEY);
        // Remove password of jdbc catalog
        properties.remove(JDBCResource.PASSWORD);

        // do mask
        properties.computeIfPresent(configKey, (key, value) -> {
            if (value.length() <= 4) {
                return MASK_CLOUD_CREDENTIAL_WORDS;
            } else {
                return new StringBuilder(value).
                        replace(2, value.length() - 2, MASK_CLOUD_CREDENTIAL_WORDS).
                        toString();
            }
        });
    }

    /**
     *  Supported URI:
     *  ADLS Gen2:
     *      abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>/<path>/<file_name>
     *  ADLS Gen1:
     *  ADLS Gen1's path don't need to be parsed, storage account and container is unnecessary for it.
     *      adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>
     *  Blob
     *      wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>/<path>/<file_name>
     */
    public static AzureStoragePath parseAzureStoragePath(String path) {
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
            LOG.debug(exception.getMessage());
>>>>>>> d1b75dd161 ([BugFix] Not showing password on show create JDBC catalog (#28059))
        }
        return String.valueOf(new StringBuilder(oldChar).replace(start, end, replaceChar));
    }
}