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

package com.starrocks.common.util;

import com.starrocks.common.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Provider;
import java.security.Security;

public class SSLUtilTest {
    private static final String PASSWORD = "starrocks";
    private static final String TEST_PROVIDER_NAME = "StarRocksSSLUtilTestProvider";
    private static final String MISMATCH_PROVIDER_NAME = "StarRocksSSLUtilMismatchProvider";

    @TempDir
    public Path tempDir;

    @AfterEach
    public void tearDown() {
        Config.ssl_security_provider_class = "";
        Config.ssl_security_provider_name = "";
        Config.ssl_security_provider_path = "";
        Security.removeProvider(TEST_PROVIDER_NAME);
        Security.removeProvider(MISMATCH_PROVIDER_NAME);
    }

    @Test
    public void testLoadKeyStoreAutoDetectsPkcs12() throws Exception {
        Path keyStorePath = createKeyStore("PKCS12", "starrocks-keystore.p12");

        KeyStore keyStore = SSLUtil.loadKeyStore(keyStorePath.toString(), PASSWORD, "", "", "keystore");

        Assertions.assertNotNull(keyStore);
        Assertions.assertEquals(0, keyStore.size());
        Assertions.assertTrue("PKCS12".equalsIgnoreCase(keyStore.getType()));
    }

    @Test
    public void testLoadKeyStoreAutoDetectsJks() throws Exception {
        String compat = Security.getProperty("keystore.type.compat");
        Security.setProperty("keystore.type.compat", "false");
        try {
            Path keyStorePath = createKeyStore("JKS", "starrocks-keystore.jks");

            KeyStore keyStore = SSLUtil.loadKeyStore(keyStorePath.toString(), PASSWORD, "", "", "keystore");

            Assertions.assertNotNull(keyStore);
            Assertions.assertEquals(0, keyStore.size());
            Assertions.assertTrue("JKS".equalsIgnoreCase(keyStore.getType()));
        } finally {
            Security.setProperty("keystore.type.compat", compat == null ? "true" : compat);
        }
    }

    @Test
    public void testLoadKeyStoreWithExplicitType() throws Exception {
        Path keyStorePath = createKeyStore("PKCS12", "starrocks-keystore.p12");

        KeyStore keyStore = SSLUtil.loadKeyStore(keyStorePath.toString(), PASSWORD, "PKCS12", "", "keystore");

        Assertions.assertNotNull(keyStore);
        Assertions.assertEquals(0, keyStore.size());
    }

    @Test
    public void testLoadKeyStoreWithExplicitTypeReportsRootCause() {
        Path keyStorePath = tempDir.resolve("missing-keystore.p12");

        GeneralSecurityException e = Assertions.assertThrows(GeneralSecurityException.class,
                () -> SSLUtil.loadKeyStore(keyStorePath.toString(), PASSWORD, "PKCS12", "", "keystore"));

        Assertions.assertTrue(e.getMessage().contains("root cause: java.io.FileNotFoundException"));
    }

    @Test
    public void testLoadKeyStoreWithExplicitProvider() throws Exception {
        Path keyStorePath = createKeyStore("PKCS12", "starrocks-keystore.p12");
        String providerName = KeyStore.getInstance("PKCS12").getProvider().getName();

        KeyStore keyStore = SSLUtil.loadKeyStore(
                keyStorePath.toString(), PASSWORD, "PKCS12", providerName, "keystore");

        Assertions.assertNotNull(keyStore);
        Assertions.assertEquals(0, keyStore.size());
        Assertions.assertEquals(providerName, keyStore.getProvider().getName());
    }

    @Test
    public void testLoadKeyStoreRequiresTypeWhenProviderConfigured() throws Exception {
        Path keyStorePath = createKeyStore("PKCS12", "starrocks-keystore.p12");

        GeneralSecurityException e = Assertions.assertThrows(GeneralSecurityException.class,
                () -> SSLUtil.loadKeyStore(
                        keyStorePath.toString(), PASSWORD, "", "SUN", "keystore"));

        Assertions.assertTrue(e.getMessage().contains(
                "SSL keystore type must be set when SSL keystore provider 'SUN' is configured"));
    }

    @Test
    public void testLoadKeyStoreAutoDetectionReportsFailure() throws Exception {
        Path keyStorePath = createKeyStore("PKCS12", "starrocks-keystore.p12");

        GeneralSecurityException e = Assertions.assertThrows(GeneralSecurityException.class,
                () -> SSLUtil.loadKeyStore(keyStorePath.toString(), "wrong-password", "", "", "keystore"));

        Assertions.assertTrue(e.getMessage().contains("auto-detected type"));
        Assertions.assertNotNull(e.getCause());
        Assertions.assertTrue(e.getSuppressed().length > 0);
    }

    @Test
    public void testLoadKeyStoreAutoDetectionReportsRootCauseInMessage() {
        Path keyStorePath = tempDir.resolve("missing-keystore.p12");

        GeneralSecurityException e = Assertions.assertThrows(GeneralSecurityException.class,
                () -> SSLUtil.loadKeyStore(keyStorePath.toString(), PASSWORD, "", "", "keystore"));

        Assertions.assertTrue(e.getMessage().contains("java.io.FileNotFoundException"));
    }

    @Test
    public void testRegisterSecurityProviderSkipsExistingProvider() throws Exception {
        Security.addProvider(new TestProvider());
        Config.ssl_security_provider_class = "com.starrocks.common.util.NoSuchProvider";
        Config.ssl_security_provider_name = TEST_PROVIDER_NAME;

        SSLUtil.registerSecurityProviderIfNeeded(SSLUtilTest.class);

        Assertions.assertNotNull(Security.getProvider(TEST_PROVIDER_NAME));
    }

    @Test
    public void testRegisterSecurityProviderFromClasspath() throws Exception {
        Config.ssl_security_provider_class = TestProvider.class.getName();
        Config.ssl_security_provider_name = TEST_PROVIDER_NAME;

        SSLUtil.registerSecurityProviderIfNeeded(SSLUtilTest.class);

        Assertions.assertNotNull(Security.getProvider(TEST_PROVIDER_NAME));
    }

    @Test
    public void testRegisterSecurityProviderRejectsNonProviderClass() {
        Config.ssl_security_provider_class = NotProvider.class.getName();

        GeneralSecurityException e = Assertions.assertThrows(GeneralSecurityException.class,
                () -> SSLUtil.registerSecurityProviderIfNeeded(SSLUtilTest.class));

        Assertions.assertTrue(e.getMessage().contains("is not a java.security.Provider"));
    }

    @Test
    public void testRegisterSecurityProviderRejectsNameMismatch() {
        Config.ssl_security_provider_class = TestProvider.class.getName();
        Config.ssl_security_provider_name = MISMATCH_PROVIDER_NAME;

        GeneralSecurityException e = Assertions.assertThrows(GeneralSecurityException.class,
                () -> SSLUtil.registerSecurityProviderIfNeeded(SSLUtilTest.class));

        Assertions.assertTrue(e.getMessage().contains("does not match provider class name"));
    }

    private Path createKeyStore(String storeType, String fileName) throws Exception {
        KeyStore keyStore = KeyStore.getInstance(storeType);
        keyStore.load(null, PASSWORD.toCharArray());

        Path keyStorePath = tempDir.resolve(fileName);
        try (OutputStream out = Files.newOutputStream(keyStorePath)) {
            keyStore.store(out, PASSWORD.toCharArray());
        }
        return keyStorePath;
    }

    public static class TestProvider extends Provider {
        public TestProvider() {
            super(TEST_PROVIDER_NAME, "1.0", "Test SSL provider");
        }
    }

    public static class NotProvider {
    }
}
