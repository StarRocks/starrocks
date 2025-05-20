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

import com.starrocks.common.StarRocksException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// TODO: merge with AzureStoragePath?
public class AzBlobURI {
    private final String scheme;
    private final String account;
    private final String endpointSuffix;
    private final String container;
    private final String blobPath;

    private static final Pattern AZBLOB_URI_PATTERN =
            Pattern.compile("^(wasb[s]?)://([^@]+)@([^.]+)\\.([^/]+)(?:/(.*))?$");

    private static final String AZBLOB_URI_FORMAT = "%s://%s@%s.%s/%s";

    public AzBlobURI(String scheme, String account, String endpointSuffix, String container, String blobPath) {
        this.scheme = scheme;
        this.account = account;
        this.endpointSuffix = endpointSuffix;
        this.container = container;
        this.blobPath = blobPath;
    }

    public String getScheme() {
        return scheme;
    }

    public String getAccount() {
        return account;
    }

    public String getEndpointSuffix() {
        return endpointSuffix;
    }

    public String getContainer() {
        return container;
    }

    public String getBlobPath() {
        return blobPath;
    }

    // Parses an azure blob storage uri into its components.
    // Supports the following uri styles:
    //  1. HDFS-style with blob   : wasb[s]://${container_name}@${account_name}.blob.core.windows.net/${blob_name}
    //  2. HDFS-style without blob: wasb[s]://${container_name}@${account_name}.blob.core.windows.net
    public static AzBlobURI parse(String uri) throws StarRocksException {
        Matcher matcher = AZBLOB_URI_PATTERN.matcher(uri);
        if (!matcher.matches()) {
            throw new StarRocksException("Invalid wasb[s] URI format: " + uri);
        }

        String scheme = matcher.group(1);
        String container = matcher.group(2);
        String account = matcher.group(3);
        String endpointSuffix = matcher.group(4);
        String blobPath = matcher.group(5) == null ? "" : matcher.group(5);
        return new AzBlobURI(scheme, account, endpointSuffix, container, blobPath);
    }

    // Get hdfs style uri
    public String getBlobUri() {
        return String.format(AZBLOB_URI_FORMAT, scheme, container, account, endpointSuffix, blobPath);
    }

    @Override
    public String toString() {
        return "AzBlobURI{" +
                "scheme='" + scheme + '\'' +
                ", account='" + account + '\'' +
                ", endpointSuffix='" + endpointSuffix + '\'' +
                ", container='" + container + '\'' +
                ", blobPath='" + blobPath + '\'' +
                '}';
    }
}
