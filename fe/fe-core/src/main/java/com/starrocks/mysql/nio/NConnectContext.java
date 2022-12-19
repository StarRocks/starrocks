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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/nio/NConnectContext.java

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
package com.starrocks.mysql.nio;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import org.xnio.StreamConnection;

import java.io.IOException;
import javax.net.ssl.SSLContext;

/**
 * connect context based on nio.
 */
public class NConnectContext extends ConnectContext {

    public NConnectContext(StreamConnection connection, SSLContext sslContext) {
        super();
        super.sslContext = sslContext;
        mysqlChannel = new NMysqlChannel(connection);
        remoteIP = mysqlChannel.getRemoteIp();
    }

    @Override
    public synchronized void cleanup() {
        if (closed) {
            return;
        }
        closed = true;
        mysqlChannel.close();
        returnRows = 0;
    }

    public void startAcceptQuery(ConnectProcessor connectProcessor) {
        ((NMysqlChannel) mysqlChannel).startAcceptQuery(this, connectProcessor);
    }

    public void suspendAcceptQuery() {
        ((NMysqlChannel) mysqlChannel).suspendAcceptQuery();
    }

    public void resumeAcceptQuery() {
        ((NMysqlChannel) mysqlChannel).resumeAcceptQuery();
    }

    public void stopAcceptQuery() throws IOException {
        ((NMysqlChannel) mysqlChannel).stopAcceptQuery();
    }
}
