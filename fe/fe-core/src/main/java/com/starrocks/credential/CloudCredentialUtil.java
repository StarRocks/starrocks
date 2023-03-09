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

    public static String replaceWithIndex(int start, int end, String oldChar, String replaceChar) {
        if (start > end) {
            return MASK_CLOUD_CREDENTIAL_WORDS;
        }
        return String.valueOf(new StringBuilder(oldChar).replace(start, end, replaceChar));
    }
}
