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

package com.starrocks.connector.iceberg.io;

import com.starrocks.common.StarRocksException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.InputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.ADLS_ENDPOINT;
import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.ADLS_SAS_TOKEN;
import static com.starrocks.credential.azure.AzureCloudConfigurationProvider.BLOB_ENDPOINT;
import static com.starrocks.credential.gcp.GCPCloudConfigurationProvider.ACCESS_TOKEN_PROVIDER_IMPL;
import static com.starrocks.credential.gcp.GCPCloudConfigurationProvider.GCS_ACCESS_TOKEN;

public class IcebergCachingFileIOTest {

    public void writeIcebergMetaTestFile() {
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter("/tmp/0001.metadata.json"));
            out.write("test iceberg metadata json file content");
            out.close();
        } catch (IOException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testNewInputFile() {
        writeIcebergMetaTestFile();
        String path = "file:/tmp/0001.metadata.json";

        // create iceberg cachingFileIO
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put("iceberg.catalog.type", "hive");
        cachingFileIO.initialize(icebergProperties);

        InputFile cachingFileIOInputFile = cachingFileIO.newInputFile(path);
        cachingFileIOInputFile.newStream();

        String cachingFileIOPath = cachingFileIOInputFile.location();
        Assertions.assertEquals(path, cachingFileIOPath);

        long cacheIOInputFileSize = cachingFileIOInputFile.getLength();
        Assertions.assertEquals(cacheIOInputFileSize, 39);
        cachingFileIO.deleteFile(path);
    }

    @Test
    public void testNewFileWithException() {
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        Map<String, String> icebergProperties = new HashMap<>();
        String key = ADLS_SAS_TOKEN + "account." + BLOB_ENDPOINT;
        icebergProperties.put(key, "sas_token");
        cachingFileIO.initialize(icebergProperties);

        String path = "file:/tmp/non_existent_file.json";
        Assertions.assertThrows(StarRocksConnectorException.class, () -> {
            cachingFileIO.newInputFile(path);
        });

        Assertions.assertThrows(StarRocksConnectorException.class, () -> {
            cachingFileIO.newOutputFile(path);
        });
    }

    @Test
    public void testBuildAzureConfFromProperties() throws StarRocksException {
        Map<String, String> properties = new HashMap<>();
        String key = ADLS_SAS_TOKEN + "account." + ADLS_ENDPOINT;
        String sasToken = "sas_token";
        properties.put(key, sasToken);
        String path = "abfss://container@account.dfs.core.windows.net/path/1/2";

        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        Configuration configuration = cachingFileIO.buildConfFromProperties(properties, path);

        String authType = configuration.get("fs.azure.account.auth.type.account." + ADLS_ENDPOINT);
        Assertions.assertEquals("SAS", authType);
        String token = configuration.get("fs.azure.sas.fixed.token.account." + ADLS_ENDPOINT);
        Assertions.assertEquals(sasToken, token);

        properties = new HashMap<>();
        key = ADLS_SAS_TOKEN + "account." + BLOB_ENDPOINT;
        sasToken = "blob_sas_token";
        properties.put(key, sasToken);
        path = "wasbs://container@account.blob.core.windows.net/path/1/2";

        cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        configuration = cachingFileIO.buildConfFromProperties(properties, path);

        token = configuration.get("fs.azure.sas.container.account." + BLOB_ENDPOINT);
        Assertions.assertEquals(sasToken, token);
    }

    @Test
    public void testBuildGCSConfFromProperties() throws StarRocksException {
        Map<String, String> properties = new HashMap<>();
        String accessToken = "access_token";
        properties.put(GCS_ACCESS_TOKEN, accessToken);
        String path = "gs://iceberg_gcp/iceberg_catalog/path/1/2";

        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        Configuration configuration = cachingFileIO.buildConfFromProperties(properties, path);
        String token = configuration.get("fs.gs.temporary.access.token");
        Assertions.assertEquals(accessToken, token);
        Assertions.assertEquals(ACCESS_TOKEN_PROVIDER_IMPL,
                configuration.get("fs.gs.auth.access.token.provider.impl"));
    }
}