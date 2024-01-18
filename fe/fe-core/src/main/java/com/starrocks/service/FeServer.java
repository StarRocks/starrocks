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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/service/FeServer.java

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

package com.starrocks.service;

import com.starrocks.common.thrift.ThriftServer;
import com.starrocks.thrift.FrontendService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TProcessor;

import java.io.IOException;

/**
 * StarRocks frontend thrift server
 */
public class FeServer {
    private static final Logger LOG = LogManager.getLogger(FeServer.class);

    private int port;
    private ThriftServer server;

    public FeServer(int port) {
        this.port = port;
    }

    public void start() {
        // setup frontend server
        TProcessor tprocessor = new FrontendService.Processor<FrontendService.Iface>(
                new FrontendServiceImpl(ExecuteEnv.getInstance()));
        server = new ThriftServer(port, tprocessor);
        try {
            server.start();
            LOG.info("thrift server started with port {}.", port);
        } catch (IOException e) {
            LOG.error("thrift server start failed with port {} , will exit", port, e);
            System.exit(-1);
        }
    }
}
