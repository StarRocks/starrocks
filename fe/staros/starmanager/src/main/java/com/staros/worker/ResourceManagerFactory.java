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

package com.staros.worker;

import com.staros.util.Config;

public class ResourceManagerFactory {

    public static ResourceManager createResourceManager(WorkerManager manager) {
        String serverAddress = Config.RESOURCE_PROVISIONER_ADDRESS;
        if (Config.ENABLE_BUILTIN_RESOURCE_PROVISIONER_FOR_TEST) {
            serverAddress = String.format("%s:%d", Config.STARMGR_IP, Config.STARMGR_RPC_PORT);
        }
        if (!serverAddress.isEmpty()) {
            return new DefaultResourceManager(manager, serverAddress);
        } else {
            return new DummyResourceManager();
        }
    }
}
