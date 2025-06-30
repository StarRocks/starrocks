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

package com.starrocks.fs.azure;

import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AzBlobURITest {

    @Test
    public void testParse() throws StarRocksException {
        {
            // wasbs://container_name@account_name.blob.core.windows.net/blob_name
            String path = "wasbs://container_name@account_name.blob.core.windows.net/blob_name";
            AzBlobURI uri = AzBlobURI.parse(path);
            Assertions.assertEquals("wasbs", uri.getScheme());
            Assertions.assertEquals("account_name", uri.getAccount());
            Assertions.assertEquals("blob.core.windows.net", uri.getEndpointSuffix());
            Assertions.assertEquals("container_name", uri.getContainer());
            Assertions.assertEquals("blob_name", uri.getBlobPath());
            Assertions.assertEquals("wasbs://container_name@account_name.blob.core.windows.net/blob_name", uri.getBlobUri());
            Assertions.assertEquals(
                    "AzBlobURI{scheme='wasbs', account='account_name', endpointSuffix='blob.core.windows.net', " +
                            "container='container_name', blobPath='blob_name'}",
                    uri.toString());
        }

        {
            // wasb://xxx
            String path = "wasb://container_name@account_name.blob.core.windows.net/blob_name";
            AzBlobURI uri = AzBlobURI.parse(path);
            Assertions.assertEquals("wasb", uri.getScheme());
            Assertions.assertEquals("account_name", uri.getAccount());
            Assertions.assertEquals("blob.core.windows.net", uri.getEndpointSuffix());
            Assertions.assertEquals("container_name", uri.getContainer());
            Assertions.assertEquals("blob_name", uri.getBlobPath());
            Assertions.assertEquals("wasb://container_name@account_name.blob.core.windows.net/blob_name", uri.getBlobUri());
        }

        {
            // blob path: path/blob_name
            String path = "wasbs://container_name@account_name.blob.core.windows.net/path/blob_name";
            AzBlobURI uri = AzBlobURI.parse(path);
            Assertions.assertEquals("wasbs", uri.getScheme());
            Assertions.assertEquals("account_name", uri.getAccount());
            Assertions.assertEquals("blob.core.windows.net", uri.getEndpointSuffix());
            Assertions.assertEquals("container_name", uri.getContainer());
            Assertions.assertEquals("path/blob_name", uri.getBlobPath());
            Assertions.assertEquals("wasbs://container_name@account_name.blob.core.windows.net/path/blob_name", uri.getBlobUri());
        }

        {
            // blob path has wildcard
            String path = "wasbs://container_name@account_name.blob.core.windows.net/path/blob*";
            AzBlobURI uri = AzBlobURI.parse(path);
            Assertions.assertEquals("wasbs", uri.getScheme());
            Assertions.assertEquals("account_name", uri.getAccount());
            Assertions.assertEquals("blob.core.windows.net", uri.getEndpointSuffix());
            Assertions.assertEquals("container_name", uri.getContainer());
            Assertions.assertEquals("path/blob*", uri.getBlobPath());
            Assertions.assertEquals("wasbs://container_name@account_name.blob.core.windows.net/path/blob*", uri.getBlobUri());
        }

        {
            // no blob path
            String path = "wasbs://container_name@account_name.blob.core.windows.net";
            AzBlobURI uri = AzBlobURI.parse(path);
            Assertions.assertEquals("wasbs", uri.getScheme());
            Assertions.assertEquals("account_name", uri.getAccount());
            Assertions.assertEquals("blob.core.windows.net", uri.getEndpointSuffix());
            Assertions.assertEquals("container_name", uri.getContainer());
            Assertions.assertEquals("", uri.getBlobPath());
            Assertions.assertEquals("wasbs://container_name@account_name.blob.core.windows.net/", uri.getBlobUri());
        }

        {
            // invalid scheme
            String path = "https://container_name@account_name.blob.core.windows.net/blob_name";
            ExceptionChecker.expectThrows(StarRocksException.class, () -> AzBlobURI.parse(path));
        }

        {
            // invalid container format
            String path = "wasbs://account_name.blob.core.windows.net/container_name/blob_name";
            ExceptionChecker.expectThrows(StarRocksException.class, () -> AzBlobURI.parse(path));
        }
    }
}