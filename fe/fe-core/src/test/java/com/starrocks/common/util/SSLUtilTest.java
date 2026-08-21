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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

public class SSLUtilTest {
    private static final String PASSWORD = "starrocks";

    @TempDir
    public Path tempDir;

    @Test
    public void testLoadKeyStoreAutoDetectsPkcs12() throws Exception {
        Path keyStorePath = createKeyStore("PKCS12", "starrocks-keystore.p12");

        KeyStore keyStore = SSLUtil.loadKeyStore(keyStorePath.toString(), PASSWORD, "", "", "keystore");

        Assertions.assertNotNull(keyStore);
        Assertions.assertEquals(0, keyStore.size());
    }

    @Test
    public void testLoadKeyStoreAutoDetectsJks() throws Exception {
        Path keyStorePath = createKeyStore("JKS", "starrocks-keystore.jks");

        KeyStore keyStore = SSLUtil.loadKeyStore(keyStorePath.toString(), PASSWORD, "", "", "keystore");

        Assertions.assertNotNull(keyStore);
        Assertions.assertEquals(0, keyStore.size());
    }

    @Test
    public void testLoadKeyStoreWithExplicitType() throws Exception {
        Path keyStorePath = createKeyStore("PKCS12", "starrocks-keystore.p12");

        KeyStore keyStore = SSLUtil.loadKeyStore(keyStorePath.toString(), PASSWORD, "PKCS12", "", "keystore");

        Assertions.assertNotNull(keyStore);
        Assertions.assertEquals(0, keyStore.size());
    }

    @Test
    public void testLoadKeyStoreAutoDetectionReportsFailure() throws Exception {
        Path keyStorePath = createKeyStore("PKCS12", "starrocks-keystore.p12");

        GeneralSecurityException e = Assertions.assertThrows(GeneralSecurityException.class,
                () -> SSLUtil.loadKeyStore(keyStorePath.toString(), "wrong-password", "", "", "keystore"));

        Assertions.assertTrue(e.getMessage().contains("auto-detected type"));
        Assertions.assertTrue(e.getSuppressed().length > 0);
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
}
