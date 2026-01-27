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

package com.starrocks.connector.share.credential;

import java.net.URI;

/**
 * Utility class for AWS credential-related operations.
 * This class provides shared functionality used across different modules.
 */
public class AwsCredentialUtil {
    public static final String HTTPS_SCHEME = "https://";

    /**
     * Checks if the given 'endpoint' contains a scheme. If not, the default HTTPS scheme is added.
     *
     * @param endpoint The endpoint string to be checked
     * @return The URI with the added scheme
     */
    public static URI ensureSchemeInEndpoint(String endpoint) {
        // Check if endpoint contains a valid scheme (contains "://")
        // This handles cases like "localhost:4566" where URI.create() incorrectly
        // parses the hostname as a scheme
        if (endpoint.contains("://")) {
            return URI.create(endpoint);
        }
        return URI.create(HTTPS_SCHEME + endpoint);
    }
}

