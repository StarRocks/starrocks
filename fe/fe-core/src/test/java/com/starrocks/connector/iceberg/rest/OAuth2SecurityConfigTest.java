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

package com.starrocks.connector.iceberg.rest;

import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.connector.iceberg.rest.IcebergRESTCatalog.Security.JWT;
import static com.starrocks.connector.iceberg.rest.IcebergRESTCatalog.Security.NONE;
import static com.starrocks.connector.iceberg.rest.IcebergRESTCatalog.Security.OAUTH2;

public class OAuth2SecurityConfigTest {

    @Test
    public void testBuild() {
        Map<String, String> properties = new HashMap<>();
        properties.put("security", "none");
        OAuth2SecurityConfig config = OAuth2SecurityConfigBuilder.build(properties);
        Assertions.assertEquals(NONE, config.getSecurity());

        properties = new HashMap<>();
        properties.put("security", "oaUth2");
        properties.put("oauth2.credential", "smith:cruise");
        properties.put("oauth2.scope", "PRINCIPAL");
        config = OAuth2SecurityConfigBuilder.build(properties);
        Assertions.assertEquals(OAUTH2, config.getSecurity());
        Assertions.assertEquals("smith:cruise", config.getCredential().get());
        Assertions.assertEquals("PRINCIPAL", config.getScope().get());
        // check default token refresh enabled
        Assertions.assertTrue(config.isTokenRefreshEnabled().isPresent());
        Assertions.assertTrue(config.isTokenRefreshEnabled().get());
        // check disabled token refresh
        properties.put("oauth2.token-refresh-enabled", "false");
        config = OAuth2SecurityConfigBuilder.build(properties);
        Assertions.assertTrue(config.isTokenRefreshEnabled().isPresent());
        Assertions.assertFalse(config.isTokenRefreshEnabled().get());

        properties = new HashMap<>();
        properties.put("security", "oaUth2");
        properties.put("oauth2.credential", "smith:cruise");
        properties.put("oauth2.token", "123456");
        properties.put("oauth2.scope", "PRINCIPAL");
        config = OAuth2SecurityConfigBuilder.build(properties);
        Assertions.assertEquals(NONE, config.getSecurity());
        
        // Test JWT security
        properties = new HashMap<>();
        properties.put("security", "jwt");
        properties.put("oauth2.token", "jwt-token-value");
        config = OAuth2SecurityConfigBuilder.build(properties);
        Assertions.assertEquals(JWT, config.getSecurity());
        Assertions.assertEquals("jwt-token-value", config.getToken().get());
    }

    @Test
    public void testProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("security", "oaUth2");
        properties.put("oauth2.credential", "smith:cruise");
        properties.put("oauth2.scope", "PRINCIPAL");
        properties.put("oauth2.server-uri", "http://localhost:8080");
        OAuth2SecurityConfig config = OAuth2SecurityConfigBuilder.build(properties);
        OAuth2SecurityProperties oauth2Properties = new OAuth2SecurityProperties(config);
        Assertions.assertEquals("smith:cruise", oauth2Properties.get().get(OAuth2Properties.CREDENTIAL));
        Assertions.assertEquals("PRINCIPAL", oauth2Properties.get().get(OAuth2Properties.SCOPE));
        Assertions.assertEquals("http://localhost:8080", oauth2Properties.get().get(OAuth2Properties.OAUTH2_SERVER_URI));

        properties = new HashMap<>();
        config = OAuth2SecurityConfigBuilder.build(properties);
        oauth2Properties = new OAuth2SecurityProperties(config);
        Assertions.assertEquals(0, oauth2Properties.get().size());
        
        // Test JWT properties
        properties = new HashMap<>();
        properties.put("security", "jwt");
        properties.put("oauth2.token", "jwt-token-value");
        properties.put("oauth2.audience", "test-audience");
        config = OAuth2SecurityConfigBuilder.build(properties);
        oauth2Properties = new OAuth2SecurityProperties(config);
        Assertions.assertEquals("jwt-token-value", oauth2Properties.get().get(OAuth2Properties.TOKEN));
        Assertions.assertEquals("test-audience", oauth2Properties.get().get(OAuth2Properties.AUDIENCE));
        // check token refresh disabled for JWT
        Assertions.assertEquals("false", oauth2Properties.get().get(OAuth2Properties.TOKEN_REFRESH_ENABLED));
    }
}