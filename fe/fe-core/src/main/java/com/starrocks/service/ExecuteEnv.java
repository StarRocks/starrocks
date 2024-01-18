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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/service/ExecuteEnv.java

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

import com.starrocks.common.conf.Config;
import com.starrocks.qe.ConnectScheduler;

// Execute environment, used to save other module, need to singleton
public class ExecuteEnv {
    private static final ExecuteEnv INSTANCE = new ExecuteEnv();
    private ConnectScheduler scheduler;

    private ExecuteEnv() {
    }

    public static ExecuteEnv getInstance() {
        return INSTANCE;
    }

    public static void setup() {
        ExecuteEnv env = getInstance();
        env.scheduler = new ConnectScheduler(Config.qe_max_connection);
    }

    public ConnectScheduler getScheduler() {
        return scheduler;
    }
}
