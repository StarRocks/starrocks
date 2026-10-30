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

package com.starrocks.mysql.ssl;

import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.common.util.SSLUtil;
import com.starrocks.http.SslUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.KeyStore;
import java.security.SecureRandom;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class SSLContextLoader {
    private static final Logger LOG = LogManager.getLogger(SSLContextLoader.class);

    private static SSLContext sslContext;
    private static volatile String[] filteredCiphers;

    public static void load() throws Exception {
        if (!Strings.isNullOrEmpty(Config.ssl_keystore_location)) {
            sslContext = createSSLContext();
            filteredCiphers = filterCiphers(sslContext);
        }
    }

    public static SSLContext getSslContext() {
        return sslContext;
    }

    public static SSLEngine newServerEngine() {
        SSLEngine engine = sslContext.createSSLEngine();
        SSLParameters parameters = sslContext.getSupportedSSLParameters();
        parameters.setCipherSuites(filteredCiphers);
        engine.setSSLParameters(parameters);
        return engine;
    }

    private static SSLContext createSSLContext() throws Exception {
        SSLUtil.registerSecurityProviderIfNeeded(SSLContextLoader.class);

        KeyStore keyStore = SSLUtil.loadKeyStore(Config.ssl_keystore_location, Config.ssl_keystore_password,
                Config.ssl_keystore_type, Config.ssl_keystore_provider, "keystore");
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, Config.ssl_key_password.toCharArray());

        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        TrustManager[] trustManagers = null;
        if (!Strings.isNullOrEmpty(Config.ssl_truststore_location)) {
            trustManagers = createTrustManagers(Config.ssl_truststore_location, Config.ssl_truststore_password,
                    Config.ssl_truststore_type, Config.ssl_truststore_provider);
        }
        sslContext.init(kmf.getKeyManagers(), trustManagers, new SecureRandom());

        return sslContext;
    }

    private static String[] filterCiphers(SSLContext context) {
        String[] supportedCiphers = context.getSupportedSSLParameters().getCipherSuites();
        return SslUtil.filterCipherSuites(supportedCiphers);
    }

    private static TrustManager[] createTrustManagers(String filepath, String keystorePassword,
                                                      String storeType, String storeProvider) throws Exception {
        KeyStore trustStore = SSLUtil.loadKeyStore(filepath, keystorePassword, storeType, storeProvider, "truststore");
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        return trustFactory.getTrustManagers();
    }
}
