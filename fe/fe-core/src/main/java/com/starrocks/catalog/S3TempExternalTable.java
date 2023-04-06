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


package com.starrocks.catalog;

import com.google.common.base.Strings;
import com.starrocks.common.DdlException;

import java.util.Map;

public class S3TempExternalTable extends TempExternalTable {
    private static final String PROPERTY_ACCESS_KEY = "aws.s3.access_key";
    private static final String PROPERTY_SECRET_KEY = "aws.s3.secret_key";
    private static final String PROPERTY_REGION = "region";

    private String accessKey;
    private String secretKey;
    private String region;

    public S3TempExternalTable(Map<String, String> properties) throws DdlException {
        super(properties);
        parseProperties(properties);
    }

    private void parseProperties(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of external table");
        }

        accessKey = properties.get(PROPERTY_ACCESS_KEY);
        if (Strings.isNullOrEmpty(accessKey)) {
            throw new DdlException("path is null. Please add properties(path='xxx') when create table");
        }

        secretKey = properties.get(PROPERTY_SECRET_KEY);
        if (Strings.isNullOrEmpty(secretKey)) {
            throw new DdlException("format is null. Please add properties(format='xxx') when create table");
        }

        region = properties.get(PROPERTY_REGION);
        if (Strings.isNullOrEmpty(region)) {
            throw new DdlException("format is null. Please add properties(format='xxx') when create table");
        }
    }
}
