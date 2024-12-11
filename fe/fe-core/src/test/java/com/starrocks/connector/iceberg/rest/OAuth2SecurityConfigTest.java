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
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class OAuth2SecurityConfigTest {

    @Test
    public void testBuild() {
        Map<String, String> properties = new HashMap<>();
        properties.put("security", "none");
        OAuth2SecurityConfig config = OAuth2SecurityConfigBuilder.build(properties);
        Assert.assertEquals(SecurityEnum.NONE, config.getSecurity());

        properties = new HashMap<>();
        properties.put("security", "oaUth2");
        properties.put("oauth2.credential", "smith:cruise");
        properties.put("oauth2.scope", "PRINCIPAL");
        config = OAuth2SecurityConfigBuilder.build(properties);
        Assert.assertEquals(SecurityEnum.OAUTH2, config.getSecurity());
        Assert.assertEquals("smith:cruise", config.getCredential().get());
        Assert.assertEquals("PRINCIPAL", config.getScope().get());

        properties = new HashMap<>();
        properties.put("security", "oaUth2");
        properties.put("oauth2.credential", "smith:cruise");
        properties.put("oauth2.token", "123456");
        properties.put("oauth2.scope", "PRINCIPAL");
        config = OAuth2SecurityConfigBuilder.build(properties);
        Assert.assertEquals(SecurityEnum.NONE, config.getSecurity());
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
        Assert.assertEquals("smith:cruise", oauth2Properties.get().get(OAuth2Properties.CREDENTIAL));
        Assert.assertEquals("PRINCIPAL", oauth2Properties.get().get(OAuth2Properties.SCOPE));
        Assert.assertEquals("http://localhost:8080", oauth2Properties.get().get(OAuth2Properties.OAUTH2_SERVER_URI));

        properties = new HashMap<>();
        config = OAuth2SecurityConfigBuilder.build(properties);
        oauth2Properties = new OAuth2SecurityProperties(config);
        Assert.assertEquals(0, oauth2Properties.get().size());
    }
}
