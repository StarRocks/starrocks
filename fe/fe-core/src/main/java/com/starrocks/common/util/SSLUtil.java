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

import com.google.common.base.Strings;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Provider;
import java.security.Security;

public final class SSLUtil {
    private static final Logger LOG = LogManager.getLogger(SSLUtil.class);

    private SSLUtil() {
    }

    public static KeyStore loadKeyStore(String filepath, String keystorePassword, String storeType,
                                        String storeProvider, String storeName) throws Exception {
        if (Strings.isNullOrEmpty(storeType)) {
            return loadKeyStoreAuto(filepath, keystorePassword, storeProvider, storeName);
        }
        return loadKeyStoreWithType(filepath, keystorePassword, storeType, storeProvider, storeName);
    }

    private static KeyStore loadKeyStoreAuto(String filepath, String keystorePassword, String storeProvider,
                                             String storeName) throws Exception {
        if (!Strings.isNullOrEmpty(storeProvider)) {
            throw new GeneralSecurityException(String.format(
                    "SSL %s type must be set when SSL %s provider '%s' is configured",
                    storeName, storeName, storeProvider));
        }

        String defaultType = KeyStore.getDefaultType();
        String[] candidateTypes = new String[] {defaultType, "JKS", "PKCS12"};
        GeneralSecurityException loadException = null;
        for (int i = 0; i < candidateTypes.length; i++) {
            String candidateType = candidateTypes[i];
            if (Strings.isNullOrEmpty(candidateType) || isDuplicatedCandidate(candidateTypes, i)) {
                continue;
            }
            try {
                return loadKeyStoreWithType(filepath, keystorePassword, candidateType, storeProvider, storeName);
            } catch (Exception e) {
                if (loadException == null) {
                    Throwable rootCause = getRootCause(e);
                    loadException = new GeneralSecurityException(String.format(
                            "Failed to load SSL %s file '%s' with auto-detected type, first failure: %s",
                            storeName, filepath, rootCause), e);
                } else {
                    loadException.addSuppressed(e);
                }
            }
        }
        if (loadException == null) {
            loadException = new GeneralSecurityException(String.format(
                    "Failed to load SSL %s file '%s' with auto-detected type: no candidate keystore types",
                    storeName, filepath));
        }
        throw loadException;
    }

    private static boolean isDuplicatedCandidate(String[] candidateTypes, int currentIndex) {
        for (int i = 0; i < currentIndex; i++) {
            if (candidateTypes[currentIndex].equalsIgnoreCase(candidateTypes[i])) {
                return true;
            }
        }
        return false;
    }

    private static KeyStore loadKeyStoreWithType(String filepath, String keystorePassword, String resolvedType,
                                                 String storeProvider, String storeName) throws Exception {
        try {
            KeyStore keyStore = Strings.isNullOrEmpty(storeProvider) ? KeyStore.getInstance(resolvedType) :
                    KeyStore.getInstance(resolvedType, storeProvider);
            try (InputStream keyStoreIS = new FileInputStream(filepath)) {
                keyStore.load(keyStoreIS, keystorePassword.toCharArray());
            }
            return keyStore;
        } catch (Exception e) {
            Throwable rootCause = getRootCause(e);
            throw new GeneralSecurityException(String.format(
                    "Failed to load SSL %s file '%s' with type '%s'%s, root cause: %s",
                    storeName, filepath, resolvedType,
                    Strings.isNullOrEmpty(storeProvider) ? "" : " and provider '" + storeProvider + "'",
                    rootCause), e);
        }
    }

    private static Throwable getRootCause(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    public static void registerSecurityProviderIfNeeded(Class<?> callerClass) throws Exception {
        if (Strings.isNullOrEmpty(Config.ssl_security_provider_class)) {
            return;
        }
        if (!Strings.isNullOrEmpty(Config.ssl_security_provider_name) &&
                Security.getProvider(Config.ssl_security_provider_name) != null) {
            return;
        }

        Class<?> providerClass;
        if (Strings.isNullOrEmpty(Config.ssl_security_provider_path)) {
            providerClass = Class.forName(Config.ssl_security_provider_class);
        } else {
            String[] paths = Config.ssl_security_provider_path.split(File.pathSeparator);
            URL[] urls = new URL[paths.length];
            for (int i = 0; i < paths.length; i++) {
                urls[i] = new File(paths[i]).toURI().toURL();
            }
            URLClassLoader classLoader = new URLClassLoader(urls, callerClass.getClassLoader());
            providerClass = Class.forName(Config.ssl_security_provider_class, true, classLoader);
        }

        Object providerObject = providerClass.getDeclaredConstructor().newInstance();
        if (!(providerObject instanceof Provider)) {
            throw new GeneralSecurityException("SSL security provider class " +
                    Config.ssl_security_provider_class + " is not a java.security.Provider");
        }
        Provider provider = (Provider) providerObject;
        if (!Strings.isNullOrEmpty(Config.ssl_security_provider_name) &&
                !Config.ssl_security_provider_name.equals(provider.getName())) {
            throw new GeneralSecurityException("Configured SSL security provider name " +
                    Config.ssl_security_provider_name + " does not match provider class name " + provider.getName());
        }
        Security.addProvider(provider);
        LOG.info("Registered SSL security provider {} from {}", provider.getName(),
                Strings.isNullOrEmpty(Config.ssl_security_provider_path) ? "classpath" :
                        Config.ssl_security_provider_path);
    }
}
