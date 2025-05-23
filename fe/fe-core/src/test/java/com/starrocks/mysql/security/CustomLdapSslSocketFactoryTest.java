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

import java.io.FileOutputStream;
import java.security.KeyStore;

import javax.net.SocketFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CustomLdapSslSocketFactoryTest {
    static String trustStorePath = "test-truststore.jks";
    static String trustStorePassword = "changeit";
    
    @BeforeClass
    public static void setupTrustStore() throws Exception {
        // Create an empty KeyStore for test purposes
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, trustStorePassword.toCharArray());
        try (FileOutputStream fos = new FileOutputStream(trustStorePath)) {
            ks.store(fos, trustStorePassword.toCharArray());
        }
        System.setProperty("custom.ldap.truststore.type", "JKS");
        System.setProperty("custom.ldap.truststore.loc", trustStorePath);
        System.setProperty("custom.ldap.truststore.password", trustStorePassword);
        System.setProperty("custom.ldap.ssl.protocol", "TLS");
    }
    
    @AfterClass
    public static void cleanup() {
        new java.io.File(trustStorePath).delete();
    }
    
    @Test
    public void testSingletonInstance() {
        SocketFactory factory1 = CustomLdapSslSocketFactory.getDefault();
        SocketFactory factory2 = CustomLdapSslSocketFactory.getDefault();
        assertSame(factory1, factory2);
    }
    
    @Test(expected = CustomLdapSslSocketFactoryException.class)
    public void testLoadTrustStoreWithInvalidPath() {
        System.setProperty("custom.ldap.truststore.loc", "invalid.jks");
        CustomLdapSslSocketFactory.getDefault();
    }
    
    @Test
    public void testLoadTrustStoreWithInvalidPathMessage() {
        System.setProperty("custom.ldap.truststore.loc", "invalid.jks");
        try {
            CustomLdapSslSocketFactory.getDefault();
        } catch (CustomLdapSslSocketFactoryException ex) {
            assertTrue(ex.getMessage().contains("Failed to create CustomSslSocketFactory"));
        }
    }
}
