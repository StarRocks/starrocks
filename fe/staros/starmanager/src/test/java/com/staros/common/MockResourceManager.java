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

package com.staros.common;

import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerGroupState;
import com.staros.worker.ResourceManager;
import com.staros.worker.Worker;
import com.staros.worker.WorkerManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MockResourceManager implements ResourceManager {
    private static final Logger LOG = LogManager.getLogger(MockResourceManager.class);

    private boolean autoProvision = false;
    private final WorkerManager workerManager;
    private Map<String, Map<String, String>> specMap;

    public MockResourceManager(WorkerManager workerManager) {
        String resourceFile = "worker_group_spec_for_test.json";
        this.workerManager = workerManager;
        try {
            String jsonString = Resources.toString(Resources.getResource(resourceFile), StandardCharsets.UTF_8);
            this.specMap = new Gson().fromJson(jsonString, new TypeToken<Map<String, Map<String, String>>>(){}.getType());
        } catch (IOException exception) {
            LOG.warn("Fail to load resource file: {}", resourceFile, exception);
        }
    }

    void setAutoProvision(boolean autoProvision) {
        this.autoProvision = autoProvision;
    }

    @Override
    public void provisionResource(String serviceId, long groupId, WorkerGroupSpec spec, String owner) {
        if (!preconditionCheck(spec)) {
            return;
        }
        int nNodes = Integer.parseInt(specMap.get(spec.getSize()).get("num_of_nodes"));
        List<String> nodes = new ArrayList<>();
        for (int i = 0; i < nNodes; ++i) {
            nodes.add(TestHelper.generateMockWorkerIpAddress());
        }
        if (!nodes.isEmpty()) {
            List<Long> workerIds = workerManager.addWorkers(serviceId, groupId, nodes);
            workerIds.forEach(x -> workerManager.getWorker(x).updateLastSeenTime(System.currentTimeMillis()));
            waitUntilAllWorkersOnline(workerIds);
        }
        workerManager.getWorkerGroup(serviceId, groupId).updateState(WorkerGroupState.READY);
    }

    @Override
    public void alterResourceSpec(String serviceId, long groupId, WorkerGroupSpec spec) {
        if (!preconditionCheck(spec)) {
            return;
        }
        int nNodes = Integer.parseInt(specMap.get(spec.getSize()).get("num_of_nodes"));
        List<Long> existIds = workerManager.getWorkerGroup(serviceId, groupId).getAllWorkerIds(false);
        if (existIds.size() == nNodes) {
            return;
        } else if (existIds.size() > nNodes) { // scale-in
            int index = existIds.size();
            while (index-- > nNodes) {
                workerManager.removeWorker(serviceId, groupId, existIds.get(index));
            }
        } else { // scale-out
            List<String> newNodes = new ArrayList<>();
            for (int i = existIds.size(); i < nNodes; ++i) {
                newNodes.add(TestHelper.generateMockWorkerIpAddress());
            }
            if (!newNodes.isEmpty()) {
                List<Long> workerIds = workerManager.addWorkers(serviceId, groupId, newNodes);
                workerIds.forEach(x -> workerManager.getWorker(x).updateLastSeenTime(System.currentTimeMillis()));
                waitUntilAllWorkersOnline(workerIds);
            }
        }
        workerManager.getWorkerGroup(serviceId, groupId).updateState(WorkerGroupState.READY);
    }

    @Override
    public void releaseResource(String serviceId, long groupId) {
        // do nothing
    }

    @Override
    public boolean isValidSpec(WorkerGroupSpec spec) {
        return specMap.containsKey(spec.getSize());
    }

    private void waitUntilAllWorkersOnline(List<Long> workerIds) {
        Awaitility.await().pollInterval(5, TimeUnit.MILLISECONDS)
                .atMost(2, TimeUnit.SECONDS)
                .until(() -> workerIds.stream().map(workerManager::getWorker).allMatch(Worker::isAlive));
    }

    @Override
    public void stop() {
        // do nothing
    }

    private boolean preconditionCheck(WorkerGroupSpec spec) {
        if (!autoProvision) {
            LOG.info("MockResourceManager::autoProvision is disabled!");
            return false;
        }
        if (!isValidSpec(spec)) {
            LOG.warn("Unknown spec size: {}", spec.getSize());
            return false;
        }
        return true;
    }
}
