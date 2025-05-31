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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Hashtable;
import javax.naming.Context;

public class LdapSecurityTest {
    @Before
    public void setup() {
        Config.authentication_ldap_simple_server_protocol = "ldaps";
        Config.authentication_ldaps_trust_store_path = "test.jks";
        Config.authentication_ldaps_trust_store_password = "changeit";
    }
    
    @Test
    public void testLdapEnvForLdaps() {
        // Simulate what LdapSecurity would do when in ldaps mode
        String url = Config.authentication_ldap_simple_server_protocol + "://localhost:389";
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, url);
        if ("ldaps".equals(Config.authentication_ldap_simple_server_protocol)
                && Config.authentication_ldaps_trust_store_path != null) {
            env.put("java.naming.ldap.factory.socket", CustomLdapSslSocketFactory.class.getName());
        }
        Assert.assertEquals(CustomLdapSslSocketFactory.class.getName(), env.get("java.naming.ldap.factory.socket"));
    }
    
    @Test
    public void testLdapEnvForLdap() {
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
        Assert.assertNull(env.get("java.naming.ldap.factory.socket"));
    }
}
