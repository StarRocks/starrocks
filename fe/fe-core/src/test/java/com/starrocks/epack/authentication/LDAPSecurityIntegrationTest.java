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

package com.starrocks.epack.authentication;

import com.starrocks.common.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class LDAPSecurityIntegrationTest {

    @Test
    public void testGetHostAndPort() {
        Map<String, String> properties = new HashMap<>();
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_LDAP_CONN_URL, "ldap://127.0.0.1:123");
        LDAPSecurityIntegration integration = new LDAPSecurityIntegration("test", properties);

        Pair<String, Integer> hostAndPort = integration.getHostAndPort();
        Assert.assertEquals("127.0.0.1", hostAndPort.first);
        Assert.assertEquals(Integer.valueOf(123), hostAndPort.second);

        properties.clear();
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_LDAP_CONN_URL, "ldaps://127.0.0.1:123");
        hostAndPort = integration.getHostAndPort();
        Assert.assertEquals("127.0.0.1", hostAndPort.first);
        Assert.assertEquals(Integer.valueOf(123), hostAndPort.second);

        properties.clear();
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_LDAP_CONN_URL, "ldap://www.celerdata.com:123");
        hostAndPort = integration.getHostAndPort();
        Assert.assertEquals("www.celerdata.com", hostAndPort.first);
        Assert.assertEquals(Integer.valueOf(123), hostAndPort.second);

        properties.clear();
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_LDAP_CONN_URL, "ldap://www.celerdata.com:abc");
        hostAndPort = integration.getHostAndPort();
        Assert.assertNull(hostAndPort);

        properties.clear();
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_LDAP_CONN_URL, "www.celerdata.com:123");
        hostAndPort = integration.getHostAndPort();
        Assert.assertNull(hostAndPort);

        properties.clear();
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_LDAP_CONN_URL, "ldap://www.celerdata.com");
        hostAndPort = integration.getHostAndPort();
        Assert.assertNull(hostAndPort);

        properties.clear();
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_LDAP_CONN_URL, "ldap://admin-sandbox.celerdata.com:123");
        hostAndPort = integration.getHostAndPort();
        Assert.assertEquals("admin-sandbox.celerdata.com", hostAndPort.first);
        Assert.assertEquals(Integer.valueOf(123), hostAndPort.second);

        properties.clear();
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_LDAP_SERVER_HOST, "admin-sandbox.celerdata.com");
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_LDAP_SERVER_PORT, "123");
        hostAndPort = integration.getHostAndPort();
        Assert.assertEquals("admin-sandbox.celerdata.com", hostAndPort.first);
        Assert.assertEquals(Integer.valueOf(123), hostAndPort.second);

        properties.clear();
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_LDAP_SERVER_HOST, "admin-sandbox.celerdata.com");
        properties.put(LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_LDAP_SERVER_PORT, "abc");
        hostAndPort = integration.getHostAndPort();
        Assert.assertNull(hostAndPort);
    }
}
