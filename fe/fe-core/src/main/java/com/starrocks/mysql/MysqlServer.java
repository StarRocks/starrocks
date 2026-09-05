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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/MysqlServer.java

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

package com.starrocks.mysql;

import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.NetUtils;
import com.starrocks.mysql.nio.AcceptListener;
import com.starrocks.qe.ConnectScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

// MySQL protocol network service
public class MysqlServer {
    private static final Logger LOG = LogManager.getLogger(MysqlServer.class);

    protected int port;
    protected volatile boolean running;
    private ConnectScheduler scheduler = null;
    private final XnioWorker xnioWorker;

    private final AcceptListener acceptListener;

    private AcceptingChannel<StreamConnection> server;

    // default task service.
    private final ExecutorService taskService = ThreadPoolManager
            .newDaemonCacheThreadPool(Config.max_mysql_service_task_threads_num, "starrocks-mysql-nio-pool", true);

    public MysqlServer(int port, ConnectScheduler connectScheduler) {
        this.port = port;
        this.xnioWorker = Xnio.getInstance().createWorkerBuilder()
                .setWorkerName("starrocks-mysql-nio")
                .setWorkerIoThreads(Config.mysql_service_io_threads_num)
                .setExternalExecutorService(taskService).build();
        // connectScheduler only used for idle check.
        this.acceptListener = new AcceptListener(connectScheduler);
    }

    // start MySQL protocol service
    // return true if success, otherwise false
    public boolean start() {
        try {
            OptionMap optionMap = OptionMap.builder()
                    .set(Options.TCP_NODELAY, true)
                    .set(Options.BACKLOG, Config.mysql_nio_backlog_num)
                    .set(Options.KEEP_ALIVE, Config.mysql_service_nio_enable_keep_alive)
                    .getMap();

            server = xnioWorker.createStreamConnectionServer(NetUtils.getSockAddrBasedOnCurrIpVersion(port),
                    acceptListener,
                    optionMap);
            server.resumeAccepts();
            running = true;
            LOG.info("Open mysql server success on {}", port);
            return true;
        } catch (IOException e) {
            LOG.warn("Open MySQL network service failed.", e);
            return false;
        }
    }

    public void stop() {
        if (running) {
            running = false;
            // close server channel, make accept throw exception
            try {
                server.close();
            } catch (IOException e) {
                LOG.warn("close server channel failed.", e);
            }
        }
    }


    public ConnectScheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(ConnectScheduler scheduler) {
        this.scheduler = scheduler;
    }
}
