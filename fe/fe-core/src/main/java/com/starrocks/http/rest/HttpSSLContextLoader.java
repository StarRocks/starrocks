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

package com.starrocks.http.rest;

import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.http.SslUtil;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Arrays;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLServerSocketFactory;

public class HttpSSLContextLoader {
    private static SslContext sslContext;
    private static boolean initialized = false;

    public static synchronized SslContext getSslContext() throws Exception {
        if (!initialized) {
            if (Config.enable_https && !Strings.isNullOrEmpty(Config.ssl_keystore_location)) {
                sslContext = createSSLContext();
            }
            initialized = true;
        }
        return sslContext;
    }

    private static SslContext createSSLContext() throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (InputStream keyStoreIS = new FileInputStream(Config.ssl_keystore_location)) {
            if (Strings.isNullOrEmpty(Config.ssl_keystore_password)) {
                throw new IllegalArgumentException("The SSL keystore password cannot be null or empty.");
            }
            keyStore.load(keyStoreIS, Config.ssl_keystore_password.toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        if (Config.ssl_key_password == null) {
            throw new IllegalArgumentException("SSL key password (Config.ssl_key_password) cannot be null.");
        }
        kmf.init(keyStore, Config.ssl_key_password.toCharArray());

        String[] supportedCiphers = ((SSLServerSocketFactory) SSLServerSocketFactory.getDefault())
                .getSupportedCipherSuites();
        String[] filteredCiphers = SslUtil.filterCipherSuites(supportedCiphers);
        return SslContextBuilder.forServer(kmf).sslProvider(SslProvider.JDK).ciphers(Arrays.asList(filteredCiphers)).build();
    }
}
