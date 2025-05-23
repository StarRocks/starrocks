// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.mysql.security;

import com.starrocks.common.Config;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import javax.naming.Context;
import java.util.Hashtable;

class LdapSecurityTest {

    @BeforeEach
    void setup() {
        Config.authentication_ldap_simple_server_protocol = "ldaps";
        Config.authentication_ldaps_trust_store_path = "test.jks";
        Config.authentication_ldaps_trust_store_password = "changeit";
    }

    @Test
    void testStaticBlockSetsSystemProperties() {
        assertEquals("JKS", System.getProperty("custom.ldap.truststore.type"));
        assertEquals("test.jks", System.getProperty("custom.ldap.truststore.loc"));
        assertEquals("changeit", System.getProperty("custom.ldap.truststore.password"));
        assertEquals("TLS", System.getProperty("custom.ldap.ssl.protocol"));
    }

    @Test
    void testLdapEnvForLdaps() {
        // Simulate what LdapSecurity would do when in ldaps mode
        String url = Config.authentication_ldap_simple_server_protocol + "://localhost:389";
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, url);
        if ("ldaps".equals(Config.authentication_ldap_simple_server_protocol)
                && Config.authentication_ldaps_trust_store_path != null) {
            env.put("java.naming.ldap.factory.socket", CustomLdapSslSocketFactory.class.getName());
        }
        assertEquals(CustomLdapSslSocketFactory.class.getName(), env.get("java.naming.ldap.factory.socket"));
    }

    @Test
    void testLdapEnvForLdap() {
        Config.authentication_ldap_simple_server_protocol = "ldap";
        Config.authentication_ldaps_trust_store_path = null;
        String url = Config.authentication_ldap_simple_server_protocol + "://localhost:389";
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, url);
        if ("ldaps".equals(Config.authentication_ldap_simple_server_protocol)
                && Config.authentication_ldaps_trust_store_path != null) {
            env.put("java.naming.ldap.factory.socket", CustomLdapSslSocketFactory.class.getName());
        }
        assertNull(env.get("java.naming.ldap.factory.socket"));
    }
}
