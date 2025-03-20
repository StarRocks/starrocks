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


package com.starrocks.mysql.security;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

public class CustomLdapSslSocketFactory extends SSLSocketFactory {
    private SSLSocketFactory sslSocketFactory;
    private static CustomLdapSslSocketFactory singletonCustomLdapSslSockFact;

    private CustomLdapSslSocketFactory()
            throws KeyManagementException, KeyStoreException, NoSuchAlgorithmException,
            CertificateException, IOException {
        sslSocketFactory = loadTrustStoreProgrammatically();
    }

    private static CustomLdapSslSocketFactory getSingletonInstance()
            throws KeyManagementException, KeyStoreException, NoSuchAlgorithmException,
            CertificateException, IOException {
        if (CustomLdapSslSocketFactory.singletonCustomLdapSslSockFact == null) {
            synchronized (CustomLdapSslSocketFactory.class) {
                if (CustomLdapSslSocketFactory.singletonCustomLdapSslSockFact == null) {
                    CustomLdapSslSocketFactory.singletonCustomLdapSslSockFact = new CustomLdapSslSocketFactory();
                }
            }
        }
        return CustomLdapSslSocketFactory.singletonCustomLdapSslSockFact;
    }

    public static SocketFactory getDefault() {
        CustomLdapSslSocketFactory customLdapSslSockFactory = null;
        try {
            customLdapSslSockFactory = CustomLdapSslSocketFactory.getSingletonInstance();
        } catch (Exception e) {
            throw new CustomLdapSslSocketFactoryException(
                    "Failed to create CustomSslSocketFactory. Exception: " + e.getClass().getSimpleName() + ". Reason: " +
                            e.getMessage(), e);
        }
        return customLdapSslSockFactory;
    }

    private SSLSocketFactory loadTrustStoreProgrammatically()
            throws KeyStoreException, IOException, NoSuchAlgorithmException,
            KeyManagementException, CertificateException {
        String trustStoreType = System.getProperty("custom.ldap.truststore.type");
        String trustStoreLoc = System.getProperty("custom.ldap.truststore.loc");
        char[] trustStorePasswordCharArr = System.getProperty("custom.ldap.truststore.password").toCharArray();
        String sslContextProtocol = System.getProperty("custom.ldap.ssl.protocol");

        KeyStore trustStore = KeyStore.getInstance(trustStoreType);
        try (BufferedInputStream bisTrustStore = new BufferedInputStream(new FileInputStream(trustStoreLoc))) {
            trustStore.load(bisTrustStore, trustStorePasswordCharArr);
        }
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        SSLContext sslContext = SSLContext.getInstance(sslContextProtocol);
        sslContext.init(null, trustFactory.getTrustManagers(), null);
        return sslContext.getSocketFactory();
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
        return sslSocketFactory.createSocket(s, host, port, autoClose);
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return sslSocketFactory.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return sslSocketFactory.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        return sslSocketFactory.createSocket(host, port);
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        return sslSocketFactory.createSocket(host, port);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
            throws IOException {
        return sslSocketFactory.createSocket(localHost, port, localHost, localPort);
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
            throws IOException {
        return sslSocketFactory.createSocket(address, port, localAddress, localPort);
    }
}
