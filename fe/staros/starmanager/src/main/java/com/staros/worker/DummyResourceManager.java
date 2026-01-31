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

import com.staros.proto.WorkerGroupSpec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DummyResourceManager implements ResourceManager {
    private static final Logger LOG = LogManager.getLogger(DummyResourceManager.class);

    @Override
    public void provisionResource(String serviceId, long groupId, WorkerGroupSpec spec, String owner) {
        LOG.info("DummyResourceManager::provisionResource called");
    }

    @Override
    public void alterResourceSpec(String serviceId, long groupId, WorkerGroupSpec spec) {
        LOG.info("DummyResourceManager::alterResourceSpec called");
    }

    @Override
    public void releaseResource(String serviceId, long groupId) {
        LOG.info("DummyResourceManager::releaseResource called");
    }

    @Override
    public boolean isValidSpec(WorkerGroupSpec spec) {
        return true;
    }

    @Override
    public void stop() {
        // nothing to do
    }
}
