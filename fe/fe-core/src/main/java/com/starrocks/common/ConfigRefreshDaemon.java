// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/backup/BackupHandler.java

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

package com.starrocks.common;

import com.starrocks.common.util.LeaderDaemon;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConfigRefreshDaemon extends LeaderDaemon {
    private static final int REFRESH_INTERVAL_MS = 10000;

    private final List<ConfigRefreshListener> listeners = new ArrayList<>();
    private final Lock lock = new ReentrantLock();

    public ConfigRefreshDaemon() {
        super("config-refresh-daemon", REFRESH_INTERVAL_MS);
    }

    @Override
    protected void runAfterCatalogReady() {
        lock.lock();
        try {
            for (ConfigRefreshListener listener : listeners) {
                listener.refresh();
            }
        } finally {
            lock.unlock();
        }
    }

    public void registerListener(ConfigRefreshListener listener) {
        lock.lock();
        try {
            listeners.add(listener);
        } finally {
            lock.unlock();
        }
    }
}
