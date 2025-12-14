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

package com.staros.provisioner;

import com.google.gson.Gson;
import com.staros.exception.AlreadyExistsStarException;
import com.staros.exception.NotExistStarException;
import com.staros.exception.ResourceExhaustedStarException;
import com.staros.proto.NodeInfo;
import com.staros.util.Config;
import com.staros.util.LockCloseable;
import io.grpc.BindableService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class StarProvisionServer {
    private static final Logger LOG = LogManager.getLogger(StarProvisionServer.class);

    private static class Node {
        private final String host;

        public Node(String host) {
            this.host = host;
        }

        public String getHost() {
            return this.host;
        }
    }

    /**
     * Helper class to serialize/deserialize key data of StarProvisionServer
     */
    private static class Serializer {
        private final List<Node> freeNodes;
        private final Map<String, List<Node>> assignedPools;

        public Serializer() {
            this.freeNodes = new ArrayList<>();
            this.assignedPools = new HashMap<>();
        }

        public Serializer(List<Node> freeNodes, Map<String, List<Node>> assignedPools) {
            this.freeNodes = freeNodes;
            this.assignedPools = assignedPools;
        }

        public String toGson() {
            return new Gson().toJson(this);
        }

        public static Serializer fromGson(Reader reader) {
            return new Gson().fromJson(reader, Serializer.class);
        }
    }

    private List<Node> freeNodes;
    private Map<String, List<Node>> assignedPools;
    private final ReentrantLock lock;
    private final String persistFile;

    public StarProvisionServer() {
        this.freeNodes = new ArrayList<>();
        this.assignedPools = new HashMap<>();
        this.lock = new ReentrantLock();
        String dataFile = "";
        if (!Config.BUILTIN_PROVISION_SERVER_DATA_DIR.isEmpty()) {
            dataFile = String.format("%s/provisioner.dat", Config.BUILTIN_PROVISION_SERVER_DATA_DIR);
        }
        this.persistFile = dataFile;
        if (!this.persistFile.isEmpty()) {
            loadData();
        }
    }

    public static List<BindableService> getServices(StarProvisionServer server) {
        List<BindableService> services = new ArrayList<>();
        services.add(new StarProvisionService(server));
        services.add(new StarProvisionManageService(server));
        return services;
    }

    public void processAddNodeRequest(String host) {
        try (LockCloseable ignored = new LockCloseable(lock)) {
            if (freeNodes.stream().anyMatch(x -> x.getHost().equals(host))) {
                throw new AlreadyExistsStarException("Host:{} already exists!", host);
            }
            assignedPools.forEach((key, val) -> {
                if (val.stream().anyMatch(x -> x.getHost().equals(host))) {
                    throw new AlreadyExistsStarException("Host:{} already exists!", host);
                }
            });
            Node node = new Node(host);
            freeNodes.add(node);
            dumpData();
        }
    }

    public List<NodeInfo> processProvisionResourceRequest(String name, int numOfNodes) {
        try (LockCloseable ignored = new LockCloseable(lock)) {
            if (assignedPools.containsKey(name)) {
                throw new AlreadyExistsStarException("PoolName:{} already exists!", name);
            }
            if (freeNodes.size() < numOfNodes) {
                throw new ResourceExhaustedStarException("Not enough resource to meet requirement.");
            }
            List<Node> newPool = new ArrayList<>();
            for (int i = 0; i < numOfNodes; ++i) {
                newPool.add(freeNodes.get(i));
            }
            assignedPools.put(name, newPool);
            freeNodes.removeAll(newPool);
            dumpData();
            return newPool.stream().map(x -> NodeInfo.newBuilder().setHost(x.getHost()).build()).collect(Collectors.toList());
        }
    }

    public List<NodeInfo> processGetResourceRequest(String name) {
        try (LockCloseable ignored = new LockCloseable(lock)) {
            if (!assignedPools.containsKey(name)) {
                throw new NotExistStarException("PoolName:{} not exist!", name);
            }
            return assignedPools.get(name).stream().map(x -> NodeInfo.newBuilder().setHost(x.getHost()).build())
                    .collect(Collectors.toList());
        }
    }

    public List<NodeInfo> processScaleResourceRequest(String name, int numOfNodes) {
        try (LockCloseable ignored = new LockCloseable(lock)) {
            if (!assignedPools.containsKey(name)) {
                throw new NotExistStarException("PoolName:{} not exist!", name);
            }
            List<Node> pool = assignedPools.get(name);
            if (pool.size() < numOfNodes) { // scale out
                int diff = numOfNodes - pool.size();
                if (freeNodes.size() < diff) {
                    throw new ResourceExhaustedStarException("Not enough resource for scale pool:{}", name);
                }
                while (diff-- > 0) {
                    pool.add(freeNodes.remove(0));
                }
            } else if (pool.size() > numOfNodes) {
                int diff = pool.size() - numOfNodes;
                while (diff-- > 0) {
                    freeNodes.add(pool.remove(pool.size() - 1));
                }
            }
            dumpData();
            return pool.stream().map(x -> NodeInfo.newBuilder().setHost(x.getHost()).build())
                    .collect(Collectors.toList());
        }
    }

    public void processDeleteResourceRequest(String name) {
        try (LockCloseable ignored = new LockCloseable(lock)) {
            if (assignedPools.containsKey(name)) {
                freeNodes.addAll(assignedPools.get(name));
                assignedPools.remove(name);
                dumpData();
            }
        }
    }

    public void clear() {
        try (LockCloseable ignored = new LockCloseable(lock)) {
            assignedPools.clear();
            freeNodes.clear();
        }
    }

    public long getFreeNodeCount() {
        return freeNodes.size();
    }

    public long getAssignedPoolsSize() {
        return assignedPools.size();
    }

    private void dumpData() {
        if (persistFile.isEmpty()) {
            return;
        }
        try (LockCloseable ignored = new LockCloseable(lock)) {
            Writer writer = Files.newBufferedWriter(Paths.get(persistFile));
            String str = new Serializer(freeNodes, assignedPools).toGson();
            writer.write(str);
            writer.close();
        } catch (Exception exception) {
            LOG.warn("Fail to dump data to file:{}, {}", persistFile, exception.toString());
        }
    }

    private void loadData() {
        if (persistFile.isEmpty()) {
            LOG.info("persist file is empty. Skip load data");
            return;
        }
        try (LockCloseable ignored = new LockCloseable(lock)) {
            Path path = Paths.get(persistFile);
            if (!Files.exists(path)) {
                LOG.info("{} not exist, skip loading data", persistFile);
                return;
            }
            Reader reader = Files.newBufferedReader(path);
            Serializer serializer = Serializer.fromGson(reader);
            reader.close();
            // Copy freeNodes and assignedPools
            this.freeNodes.clear();
            this.assignedPools.clear();
            this.freeNodes.addAll(serializer.freeNodes);
            this.assignedPools.putAll(serializer.assignedPools);
        } catch (Exception exception) {
            LOG.warn("Excepted when load meta from persistent file:{}, {}", persistFile, exception.toString());
        }
    }
}
