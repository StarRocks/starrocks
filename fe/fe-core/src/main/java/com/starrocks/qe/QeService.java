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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/QeService.java

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

package com.starrocks.qe;

import com.google.common.base.Strings;
import com.starrocks.common.conf.Config;
import com.starrocks.mysql.MysqlServer;
import com.starrocks.mysql.nio.NMysqlServer;
import com.starrocks.mysql.ssl.SSLChannelImpClassLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class QeService {
    private static final Logger LOG = LogManager.getLogger(QeService.class);
    // MySQL protocol service
    private MysqlServer mysqlServer;

    public QeService(int port, boolean nioEnabled, ConnectScheduler scheduler) throws Exception {
        SSLContext sslContext = null;
        if (!Strings.isNullOrEmpty(Config.ssl_keystore_location)
                && SSLChannelImpClassLoader.loadSSLChannelImpClazz() != null) {
            sslContext = createSSLContext();
        }
        if (nioEnabled) {
            mysqlServer = new NMysqlServer(port, scheduler, sslContext);
        } else {
            mysqlServer = new MysqlServer(port, scheduler, sslContext);
        }
    }

    public void start() throws IOException {
        if (!mysqlServer.start()) {
            LOG.error("mysql server start failed");
            System.exit(-1);
        }
        LOG.info("QE service start.");
    }


    public MysqlServer getMysqlServer() {
        return mysqlServer;
    }

    public void setMysqlServer(MysqlServer mysqlServer) {
        this.mysqlServer = mysqlServer;
    }

    private SSLContext createSSLContext() throws Exception {
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
        return sslContext;
    }

    /**
     * Creates the trust managers required to initiate the {@link SSLContext}, using a JKS keystore as an input.
     *
     * @param filepath - the path to the JKS keystore.
     * @param keystorePassword - the keystore's password.
     * @return {@link TrustManager} array, that will be used to initiate the {@link SSLContext}.
     * @throws Exception
     */
    private TrustManager[] createTrustManagers(String filepath, String keystorePassword) throws Exception {
        KeyStore trustStore = KeyStore.getInstance("JKS");
        InputStream trustStoreIS = new FileInputStream(filepath);
        try {
            trustStore.load(trustStoreIS, keystorePassword.toCharArray());
        } finally {
            if (trustStoreIS != null) {
                trustStoreIS.close();
            }
        }
        TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustFactory.init(trustStore);
        return trustFactory.getTrustManagers();
    }
}

