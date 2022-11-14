// This file is made available under Elastic License 2.0.
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
import com.starrocks.common.Config;
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
        sslContext.init(kmf.getKeyManagers(), null, new SecureRandom());
        return sslContext;
    }
}

