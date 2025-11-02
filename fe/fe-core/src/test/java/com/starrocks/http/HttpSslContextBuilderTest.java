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

package com.starrocks.http;

import com.starrocks.common.Config;
import com.starrocks.http.rest.HttpSSLContextLoader;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.io.File;
import java.lang.reflect.Field;
import java.security.KeyStore;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLServerSocketFactory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HttpSslContextBuilderTest {

    private static void resetHttpSslContext() throws Exception {
        Field f = HttpSSLContextLoader.class.getDeclaredField("initialized");
        f.setAccessible(true);
        f.set(null, false);
    }

    @BeforeEach
    void setup() throws Exception {
        resetHttpSslContext();
        Config.enable_https = true;
        Config.ssl_keystore_password = "pass";
        Config.ssl_key_password = "pass";
        File f = File.createTempFile("keystore", ".jks");
        f.deleteOnExit();
        Config.ssl_keystore_location = f.getAbsolutePath();
    }

    @Test
    void jdkSslFactoryPathFiltersAndApplies() throws Exception {
        String[] supported = {"TLS_AES_256_GCM_SHA384", "TLS_RSA_WITH_AES_128_CBC_SHA"};
        String[] filtered  = {"TLS_AES_256_GCM_SHA384"};

        KeyStore ks = mock(KeyStore.class);
        KeyManagerFactory kmf = mock(KeyManagerFactory.class);
        SSLServerSocketFactory ssf = mock(SSLServerSocketFactory.class);
        when(ssf.getSupportedCipherSuites()).thenReturn(supported);

        SslContextBuilder builder = mock(SslContextBuilder.class, RETURNS_SELF);
        SslContext ctx = mock(SslContext.class);
        when(builder.build()).thenReturn(ctx);

        try (MockedStatic<KeyStore> ksStatic = mockStatic(KeyStore.class);
                MockedStatic<KeyManagerFactory> kmfStatic = mockStatic(KeyManagerFactory.class);
                MockedStatic<SSLServerSocketFactory> ssfStatic = mockStatic(SSLServerSocketFactory.class);
                MockedStatic<SslUtil> sslUtilStatic = mockStatic(SslUtil.class);
                MockedStatic<SslContextBuilder> scbStatic = mockStatic(SslContextBuilder.class)) {

            ksStatic.when(() -> KeyStore.getInstance("JKS")).thenReturn(ks);
            doNothing().when(ks).load(any(), any());

            kmfStatic.when(KeyManagerFactory::getDefaultAlgorithm).thenReturn("SunX509");
            kmfStatic.when(() -> KeyManagerFactory.getInstance("SunX509")).thenReturn(kmf);
            doNothing().when(kmf).init(any(KeyStore.class), any());

            ssfStatic.when(SSLServerSocketFactory::getDefault).thenReturn(ssf);

            sslUtilStatic.when(() -> SslUtil.filterCipherSuites(supported)).thenReturn(filtered);

            scbStatic.when(() -> SslContextBuilder.forServer(kmf)).thenReturn(builder);

            SslContext out = HttpSSLContextLoader.getSslContext();

            assertSame(ctx, out);
            ArgumentCaptor<List<String>> cap = ArgumentCaptor.forClass(List.class);
            verify(builder).ciphers(cap.capture());
            assertArrayEquals(filtered, cap.getValue().toArray(new String[0]));
            sslUtilStatic.verify(() -> SslUtil.filterCipherSuites(supported));
            verify(builder).build();
        }
    }

    @Test
    void disabledHttpsNoCipherFiltering() throws Exception {
        resetHttpSslContext();
        Config.enable_https = false;
        Config.ssl_keystore_location = "whatever";

        try (MockedStatic<SslUtil> sslUtilStatic = mockStatic(SslUtil.class);
                MockedStatic<SSLServerSocketFactory> ssfStatic = mockStatic(SSLServerSocketFactory.class)) {

            sslUtilStatic.verifyNoInteractions();
            ssfStatic.verifyNoInteractions();
        }
    }
}