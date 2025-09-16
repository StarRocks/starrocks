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

package com.starrocks.mysql;

import com.starrocks.common.Config;
import com.starrocks.http.SslUtil;
import com.starrocks.mysql.ssl.SSLContextLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.io.File;
import java.lang.reflect.Field;
import java.security.KeyStore;
import java.security.SecureRandom;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MysqlSslEngineConfigTest {
    private static void resetMysqlStatics() throws Exception {
        Field c = SSLContextLoader.class.getDeclaredField("sslContext");
        c.setAccessible(true);
        c.set(null, null);
        Field fc = SSLContextLoader.class.getDeclaredField("filteredCiphers");
        fc.setAccessible(true);
        fc.set(null, null);
    }

    @BeforeEach
    void setup() throws Exception {
        resetMysqlStatics();
        Config.ssl_keystore_password = "pass";
        Config.ssl_key_password = "pass";
        Config.ssl_truststore_location = "";
        Config.ssl_truststore_password = "";
        File f = File.createTempFile("keystore", ".jks");
        f.deleteOnExit();
        Config.ssl_keystore_location = f.getAbsolutePath();
    }

    @Test
    void jsseAllTls13RemovedSetsOnlyTls12OnEngine() throws Exception {
        String[] supported = {
                "TLS_AES_128_GCM_SHA256", "TLS_AES_256_GCM_SHA384", "TLS_CHACHA20_POLY1305_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
        };
        String[] filtered = {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"};

        KeyStore ks = mock(KeyStore.class);
        KeyManagerFactory kmf = mock(KeyManagerFactory.class);
        SSLContext ctx = mock(SSLContext.class);
        SSLEngine engine = mock(SSLEngine.class);

        SSLParameters paramsSupported = new SSLParameters();
        paramsSupported.setCipherSuites(supported);

        when(ctx.getSupportedSSLParameters()).thenReturn(paramsSupported);
        when(ctx.createSSLEngine()).thenReturn(engine);

        try (MockedStatic<KeyStore> ksStatic = mockStatic(KeyStore.class);
                MockedStatic<KeyManagerFactory> kmfStatic = mockStatic(KeyManagerFactory.class);
                MockedStatic<SSLContext> sslCtxStatic = mockStatic(SSLContext.class);
                MockedStatic<SslUtil> sslUtilStatic = mockStatic(SslUtil.class)) {

            ksStatic.when(() -> KeyStore.getInstance("JKS")).thenReturn(ks);
            kmfStatic.when(KeyManagerFactory::getDefaultAlgorithm).thenReturn("SunX509");
            kmfStatic.when(() -> KeyManagerFactory.getInstance("SunX509")).thenReturn(kmf);
            sslCtxStatic.when(() -> SSLContext.getInstance("TLSv1.2")).thenReturn(ctx);

            sslUtilStatic.when(() -> SslUtil.filterCipherSuites(supported)).thenReturn(filtered);

            SSLContextLoader.load();

            verify(ctx).init(eq(kmf.getKeyManagers()), isNull(), any(SecureRandom.class));
            sslUtilStatic.verify(() -> SslUtil.filterCipherSuites(supported));

            SSLContextLoader.newServerEngine();
            ArgumentCaptor<SSLParameters> cap = ArgumentCaptor.forClass(SSLParameters.class);
            verify(engine).setSSLParameters(cap.capture());
            assertArrayEquals(filtered, cap.getValue().getCipherSuites());
        }
    }

    @Test
    void jsseFilteredContainsTls13AndTls12SetsBoth() throws Exception {
        String[] supported = {
                "TLS_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
        };
        String[] filtered = {
                "TLS_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
        };

        KeyStore ks = mock(KeyStore.class);
        KeyManagerFactory kmf = mock(KeyManagerFactory.class);
        SSLContext ctx = mock(SSLContext.class);
        SSLEngine engine = mock(SSLEngine.class);
        SSLParameters paramsSupported = new SSLParameters();
        paramsSupported.setCipherSuites(supported);
        when(ctx.getSupportedSSLParameters()).thenReturn(paramsSupported);
        when(ctx.createSSLEngine()).thenReturn(engine);

        try (MockedStatic<KeyStore> ksStatic = mockStatic(KeyStore.class);
                MockedStatic<KeyManagerFactory> kmfStatic = mockStatic(KeyManagerFactory.class);
                MockedStatic<SSLContext> sslCtxStatic = mockStatic(SSLContext.class);
                MockedStatic<SslUtil> sslUtilStatic = mockStatic(SslUtil.class)) {

            ksStatic.when(() -> KeyStore.getInstance("JKS")).thenReturn(ks);
            kmfStatic.when(KeyManagerFactory::getDefaultAlgorithm).thenReturn("SunX509");
            kmfStatic.when(() -> KeyManagerFactory.getInstance("SunX509")).thenReturn(kmf);
            sslCtxStatic.when(() -> SSLContext.getInstance("TLSv1.2")).thenReturn(ctx);

            sslUtilStatic.when(() -> SslUtil.filterCipherSuites(supported)).thenReturn(filtered);

            SSLContextLoader.load();
            SSLContextLoader.newServerEngine();

            ArgumentCaptor<SSLParameters> cap = ArgumentCaptor.forClass(SSLParameters.class);
            verify(engine).setSSLParameters(cap.capture());
            assertArrayEquals(filtered, cap.getValue().getCipherSuites());
        }
    }

    @Test
    void jsseEmptyFilteredListSetsEmptyCipherArray() throws Exception {
        String[] supported = {"TLS_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"};
        String[] filtered = {};

        KeyStore ks = mock(KeyStore.class);
        KeyManagerFactory kmf = mock(KeyManagerFactory.class);
        SSLContext ctx = mock(SSLContext.class);
        SSLEngine engine = mock(SSLEngine.class);
        SSLParameters paramsSupported = new SSLParameters();
        paramsSupported.setCipherSuites(supported);
        when(ctx.getSupportedSSLParameters()).thenReturn(paramsSupported);
        when(ctx.createSSLEngine()).thenReturn(engine);

        try (MockedStatic<KeyStore> ksStatic = mockStatic(KeyStore.class);
                MockedStatic<KeyManagerFactory> kmfStatic = mockStatic(KeyManagerFactory.class);
                MockedStatic<SSLContext> sslCtxStatic = mockStatic(SSLContext.class);
                MockedStatic<SslUtil> sslUtilStatic = mockStatic(SslUtil.class)) {

            ksStatic.when(() -> KeyStore.getInstance("JKS")).thenReturn(ks);
            kmfStatic.when(KeyManagerFactory::getDefaultAlgorithm).thenReturn("SunX509");
            kmfStatic.when(() -> KeyManagerFactory.getInstance("SunX509")).thenReturn(kmf);
            sslCtxStatic.when(() -> SSLContext.getInstance("TLSv1.2")).thenReturn(ctx);

            sslUtilStatic.when(() -> SslUtil.filterCipherSuites(supported)).thenReturn(filtered);

            SSLContextLoader.load();
            SSLContextLoader.newServerEngine();

            ArgumentCaptor<SSLParameters> cap = ArgumentCaptor.forClass(SSLParameters.class);
            verify(engine).setSSLParameters(cap.capture());
            assertEquals(0, cap.getValue().getCipherSuites().length);
        }
    }

    @Test
    void jsseWithTruststoreUsesTrustManagersAndSetsFiltered() throws Exception {
        File ts = File.createTempFile("truststore", ".jks");
        ts.deleteOnExit();
        Config.ssl_truststore_location = ts.getAbsolutePath();
        Config.ssl_truststore_password = "tpass";

        String[] supported = {"TLS_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"};
        String[] filtered = {"TLS_AES_256_GCM_SHA384"};

        KeyStore ks = mock(KeyStore.class);
        KeyManagerFactory kmf = mock(KeyManagerFactory.class);
        SSLContext ctx = mock(SSLContext.class);
        SSLEngine engine = mock(SSLEngine.class);
        SSLParameters paramsSupported = new SSLParameters();
        paramsSupported.setCipherSuites(supported);
        when(ctx.getSupportedSSLParameters()).thenReturn(paramsSupported);
        when(ctx.createSSLEngine()).thenReturn(engine);

        KeyStore trustKs = mock(KeyStore.class);
        TrustManagerFactory tmf = mock(TrustManagerFactory.class);
        TrustManager[] tms = new TrustManager[] {mock(TrustManager.class)};
        when(tmf.getTrustManagers()).thenReturn(tms);

        try (MockedStatic<KeyStore> ksStatic = mockStatic(KeyStore.class);
                MockedStatic<KeyManagerFactory> kmfStatic = mockStatic(KeyManagerFactory.class);
                MockedStatic<TrustManagerFactory> tmfStatic = mockStatic(TrustManagerFactory.class);
                MockedStatic<SSLContext> sslCtxStatic = mockStatic(SSLContext.class);
                MockedStatic<SslUtil> sslUtilStatic = mockStatic(SslUtil.class)) {

            ksStatic.when(() -> KeyStore.getInstance("JKS"))
                    .thenReturn(ks)
                    .thenReturn(trustKs)
            ;
            kmfStatic.when(KeyManagerFactory::getDefaultAlgorithm).thenReturn("SunX509");
            kmfStatic.when(() -> KeyManagerFactory.getInstance("SunX509")).thenReturn(kmf);
            tmfStatic.when(TrustManagerFactory::getDefaultAlgorithm).thenReturn("PKIX");
            tmfStatic.when(() -> TrustManagerFactory.getInstance("PKIX")).thenReturn(tmf);
            sslCtxStatic.when(() -> SSLContext.getInstance("TLSv1.2")).thenReturn(ctx);

            sslUtilStatic.when(() -> SslUtil.filterCipherSuites(supported)).thenReturn(filtered);

            SSLContextLoader.load();

            verify(ctx).init(eq(kmf.getKeyManagers()), same(tms), any(SecureRandom.class));

            SSLContextLoader.newServerEngine();
            ArgumentCaptor<SSLParameters> cap = ArgumentCaptor.forClass(SSLParameters.class);
            verify(engine).setSSLParameters(cap.capture());
            assertArrayEquals(filtered, cap.getValue().getCipherSuites());
        }
    }
}