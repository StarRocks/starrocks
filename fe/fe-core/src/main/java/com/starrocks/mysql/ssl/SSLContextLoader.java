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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class SSLContextLoader {
    private static final Logger LOG = LogManager.getLogger(SSLContextLoader.class);

    private static SSLContext sslContext;

    private static ScheduledExecutorService scheduledExecutorService;

    private static int usingAutoRefreshInterval;

    private static String usingKeyStoreLocation;
    private static String usingKeyStorePassword;
    private static String usingKeyPassword;
    private static String usingTruststoreLocation;
    private static String usingTruststorePassword;

    private static String usingKeyStoreMD5;
    private static String usingTruststoreMD5;

    public static void load() throws Exception {
        if (!Strings.isNullOrEmpty(Config.ssl_keystore_location)) {
            sslContext = createSSLContext();
        }

        updateAutoRefreshInterval(Config.ssl_cert_auto_update_interval_s);
    }

    public static SSLContext getSslContext() {
        return sslContext;
    }

    public static int getUsingAutoRefreshInterval() {
        return usingAutoRefreshInterval;
    }

    public static void updateAutoRefreshInterval(int interval) {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdown();
        }

        if (interval > 0) {
            scheduledExecutorService = Executors.newScheduledThreadPool(1);
            scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    checkAndRefreshSSLContext();
                }
            }, interval, interval, TimeUnit.SECONDS);
        }

        usingAutoRefreshInterval = interval;
    }

    private static void checkAndRefreshSSLContext() {
        if (isConfigChanged() || isFileChanged()) {
            try {
                if (!Strings.isNullOrEmpty(Config.ssl_keystore_location)) {
                    sslContext = createSSLContext();
                    LOG.info("create a new ssl context successfully, " +
                                    "key store file: {}, md5: {}, trust store file: {}, md5: {}",
                            usingKeyStoreLocation, usingKeyStoreMD5, usingTruststoreLocation, usingTruststoreMD5);
                }
            } catch (Exception e) {
                LOG.warn("create ssl context failed", e);
            }
        }
    }

    private static SSLContext createSSLContext() throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (InputStream keyStoreIS = new FileInputStream(Config.ssl_keystore_location)) {
            keyStore.load(keyStoreIS, Config.ssl_keystore_password.toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, Config.ssl_key_password.toCharArray());

        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        TrustManager[] trustManagers = null;
        if (!Strings.isNullOrEmpty(Config.ssl_truststore_location)) {
            trustManagers = createTrustManagers(Config.ssl_truststore_location, Config.ssl_truststore_password);
        }
        sslContext.init(kmf.getKeyManagers(), trustManagers, new SecureRandom());

        updateUsingConfig();

        updateFileMD5();

        return sslContext;
    }

    private static TrustManager[] createTrustManagers(String filepath, String keystorePassword) throws Exception {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        try (InputStream trustStoreIS = new FileInputStream(filepath)) {
            trustStore.load(trustStoreIS, keystorePassword.toCharArray());
        }
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        return trustFactory.getTrustManagers();
    }

    private static void updateUsingConfig() {
        usingKeyStoreLocation = Config.ssl_keystore_location;
        usingKeyStorePassword = Config.ssl_keystore_password;
        usingKeyPassword = Config.ssl_key_password;
        usingTruststoreLocation = Config.ssl_truststore_location;
        usingTruststorePassword = Config.ssl_truststore_password;
    }

    private static void updateFileMD5() {
        try {
            usingKeyStoreMD5 = getFileMD5(usingKeyStoreLocation);
        } catch (Exception e) {
            LOG.warn("get md5 failed, file path: {}", usingKeyStoreLocation);
        }

        if (!Strings.isNullOrEmpty(usingTruststoreLocation)) {
            try {
                usingTruststoreMD5 = getFileMD5(usingTruststoreLocation);
            } catch (Exception e) {
                LOG.warn("get md5 failed, file path: {}", usingTruststoreLocation);
            }
        }
    }

    private static boolean isConfigChanged() {
        return !Objects.equals(usingKeyStoreLocation, Config.ssl_keystore_location)
                || !Objects.equals(usingKeyStorePassword, Config.ssl_keystore_password)
                || !Objects.equals(usingKeyPassword, Config.ssl_key_password)
                || !Objects.equals(usingTruststoreLocation, Config.ssl_truststore_location)
                || !Objects.equals(usingTruststorePassword, Config.ssl_truststore_password);
    }

    private static boolean isFileChanged() {
        if (!Strings.isNullOrEmpty(Config.ssl_keystore_location)) {
            String currentKeyStoreMD5;
            try {
                currentKeyStoreMD5 = getFileMD5(Config.ssl_keystore_location);
            } catch (Exception e) {
                LOG.warn("get md5 failed, file path: {}", Config.ssl_keystore_location);
                return false;
            }

            if (!Objects.equals(usingKeyStoreMD5, currentKeyStoreMD5)) {
                return true;
            }
        }

        if (!Strings.isNullOrEmpty(Config.ssl_truststore_location)) {
            String currentTruststoreMD5;
            try {
                currentTruststoreMD5 = getFileMD5(Config.ssl_keystore_location);
            } catch (Exception e) {
                LOG.warn("get md5 failed, file path: {}", Config.ssl_truststore_location);
                return false;
            }

            if (!Objects.equals(usingTruststoreMD5, currentTruststoreMD5)) {
                return true;
            }
        }

        return false;
    }

    private static String getFileMD5(String filePath) throws IOException, NoSuchAlgorithmException {
        try (FileInputStream fis = new FileInputStream(filePath)) {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] dataBytes = new byte[1024];

            int bytesRead;
            while ((bytesRead = fis.read(dataBytes)) != -1) {
                md.update(dataBytes, 0, bytesRead);
            }

            byte[] mdBytes = md.digest();

            StringBuilder sb = new StringBuilder();
            for (byte mdByte : mdBytes) {
                sb.append(Integer.toString((mdByte & 0xff) + 0x100, 16).substring(1));
            }
            return sb.toString();
        }
    }
}
