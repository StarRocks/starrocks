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

package com.starrocks.planner.lance;

import com.google.common.collect.Maps;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;

import java.util.Map;

public class LanceConfig {
    public static final String LANCE_FILE_SUFFIX = ".lance";
    public static final String PROP_RAW_OPTION_PREFIX = "lance.option.";

    private LanceConfig() {
    }

    public static Map<String, String> buildStorageOptions(Map<String, String> properties) {
        Map<String, String> storageOptions = Maps.newHashMap();
        if (properties == null) {
            return storageOptions;
        }

        putIfPresent(storageOptions, "aws_access_key_id",
                properties.get(CloudConfigurationConstants.AWS_S3_ACCESS_KEY));
        putIfPresent(storageOptions, "aws_secret_access_key",
                properties.get(CloudConfigurationConstants.AWS_S3_SECRET_KEY));
        putIfPresent(storageOptions, "aws_session_token",
                properties.get(CloudConfigurationConstants.AWS_S3_SESSION_TOKEN));
        putIfPresent(storageOptions, "aws_endpoint",
                properties.get(CloudConfigurationConstants.AWS_S3_ENDPOINT));
        putIfPresent(storageOptions, "aws_region",
                properties.get(CloudConfigurationConstants.AWS_S3_REGION));

        String pathStyle = properties.get(CloudConfigurationConstants.AWS_S3_ENABLE_PATH_STYLE_ACCESS);
        if (pathStyle != null) {
            storageOptions.put("aws_virtual_hosted_style_request",
                    Boolean.toString(!Boolean.parseBoolean(pathStyle)));
        }

        putIfPresent(storageOptions, "aws_access_key_id",
                properties.get(CloudConfigurationConstants.ALIYUN_OSS_ACCESS_KEY));
        putIfPresent(storageOptions, "aws_secret_access_key",
                properties.get(CloudConfigurationConstants.ALIYUN_OSS_SECRET_KEY));
        putIfPresent(storageOptions, "aws_endpoint",
                properties.get(CloudConfigurationConstants.ALIYUN_OSS_ENDPOINT));
        putIfPresent(storageOptions, "aws_session_token",
                properties.get(CloudConfigurationConstants.ALIYUN_OSS_STS_TOKEN));
        putIfPresent(storageOptions, "aws_region",
                properties.get(CloudConfigurationConstants.ALIYUN_OSS_REGION));

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(PROP_RAW_OPTION_PREFIX)) {
                storageOptions.put(entry.getKey().substring(PROP_RAW_OPTION_PREFIX.length()), entry.getValue());
            }
        }
        return storageOptions;
    }

    private static void putIfPresent(Map<String, String> target, String key, String value) {
        if (value != null && !value.isEmpty()) {
            target.put(key, value);
        }
    }
}
