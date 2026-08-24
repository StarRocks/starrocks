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

package com.starrocks.authentication;

import com.starrocks.sql.analyzer.SemanticException;

import java.time.Duration;
import java.util.Map;

public class IometeSecurityIntegration extends SecurityIntegration {
    public static final String SERVER_URL = "server_url";
    public static final String DOMAIN = "domain";
    public static final String LAKEHOUSE = "lakehouse";
    public static final String CONNECT_TIMEOUT_MS = "connect_timeout_ms";
    public static final String REQUEST_TIMEOUT_MS = "request_timeout_ms";

    public IometeSecurityIntegration(String name, Map<String, String> propertyMap) {
        super(name, propertyMap);
    }

    @Override
    public AuthenticationProvider getAuthenticationProvider() {
        String serverUrl = required(SERVER_URL);
        String domain = required(DOMAIN);
        String lakehouse = required(LAKEHOUSE);
        Duration connectTimeout = Duration.ofMillis(parsePositiveLong(CONNECT_TIMEOUT_MS, 5000));
        Duration requestTimeout = Duration.ofMillis(parsePositiveLong(REQUEST_TIMEOUT_MS, 10000));
        return new IometeAuthenticationProvider(serverUrl, domain, lakehouse,
                connectTimeout, requestTimeout);
    }

    @Override
    public void checkProperty() throws SemanticException {
        required(SERVER_URL);
        required(DOMAIN);
        required(LAKEHOUSE);
    }

    private String required(String property) throws SemanticException {
        String value = propertyMap.get(property);
        if (value == null || value.isBlank()) {
            throw new SemanticException("missing required property: " + property);
        }
        return value;
    }

    private long parsePositiveLong(String property, long defaultValue) {
        String value = propertyMap.get(property);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        long parsed = Long.parseLong(value);
        if (parsed <= 0) {
            throw new IllegalArgumentException(property + " must be positive");
        }
        return parsed;
    }
}
