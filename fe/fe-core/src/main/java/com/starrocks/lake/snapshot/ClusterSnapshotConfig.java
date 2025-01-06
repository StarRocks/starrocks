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

package com.starrocks.lake.snapshot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterSnapshotConfig {
    private static final Logger LOG = LogManager.getLogger(ClusterSnapshotConfig.class);

    public static class ClusterSnapshot {
        @JsonProperty("storage_volume_name")
        private String storageVolumeName;

        private StorageVolume storageVolume;

        @JsonProperty("cluster_service_id")
        private String clusterServiceId;

        @JsonProperty("cluster_snapshot_name")
        private String clusterSnapshotName;

        public String getStorageVolumeName() {
            return storageVolumeName;
        }

        public void setStorageVolumeName(String storageVolumeName) {
            this.storageVolumeName = storageVolumeName;
        }

        public StorageVolume getStorageVolume() {
            return storageVolume;
        }

        public void setStorageVolume(StorageVolume storageVolume) {
            this.storageVolume = storageVolume;
        }

        public String getClusterServiceId() {
            return clusterServiceId;
        }

        public void setClusterServiceId(String clusterServiceId) {
            this.clusterServiceId = clusterServiceId;
        }

        public String getClusterSnapshotName() {
            return clusterSnapshotName;
        }

        public void setClusterSnapshotName(String clusterSnapshotName) {
            this.clusterSnapshotName = clusterSnapshotName;
        }
    }

    public static class Frontend {
        public static enum FrontendType {
            FOLLOWER,
            OBSERVER;

            @JsonCreator
            public static FrontendType forValue(String value) {
                return FrontendType.valueOf(value.toUpperCase());
            }

            @JsonValue
            public String toValue() {
                return name().toLowerCase();
            }
        }

        @JsonProperty("host")
        private String host;

        @JsonProperty("edit_log_port")
        private int editLogPort;

        @JsonProperty("type")
        private FrontendType type = FrontendType.FOLLOWER;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getEditLogPort() {
            return editLogPort;
        }

        public void setEditLogPort(int editLogPort) {
            this.editLogPort = editLogPort;
        }

        public FrontendType getType() {
            return type;
        }

        public void setType(FrontendType type) {
            this.type = type;
        }

        public boolean isFollower() {
            return this.type == FrontendType.FOLLOWER;
        }

        public boolean isObserver() {
            return this.type == FrontendType.OBSERVER;
        }

        @Override
        public String toString() {
            return "Frontend [host=" + host + ", editLogPort=" + editLogPort + ", type=" + type + "]";
        }
    }

    public static class ComputeNode {
        @JsonProperty("host")
        private String host;

        @JsonProperty("heartbeat_service_port")
        private int heartbeatServicePort;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getHeartbeatServicePort() {
            return heartbeatServicePort;
        }

        public void setHeartbeatServicePort(int heartbeatServicePort) {
            this.heartbeatServicePort = heartbeatServicePort;
        }

        @Override
        public String toString() {
            return "ComputeNode [host=" + host + ", heartbeatServicePort=" + heartbeatServicePort + "]";
        }
    }

    public static class StorageVolume {
        private static class PropertiesDeserializer extends JsonDeserializer<Map<String, String>> {

            @Override
            public Map<String, String> deserialize(JsonParser parser, DeserializationContext context)
                    throws IOException, JsonProcessingException {
                ObjectMapper mapper = (ObjectMapper) parser.getCodec();
                List<Map<String, String>> list = mapper.readValue(parser,
                        new TypeReference<List<Map<String, String>>>() {
                        });

                Map<String, String> properties = new HashMap<>();
                for (Map<String, String> entry : list) {
                    String key = entry.get("key");
                    String value = entry.get("value");
                    if (key == null || key.isEmpty() || value == null || value.isEmpty()) {
                        throw new JsonProcessingException("Missing 'key' or 'value' in properties entry",
                                parser.getTokenLocation()) {
                        };
                    }
                    properties.put(key, value);
                }
                return properties;
            }
        }

        @JsonProperty("name")
        private String name;

        @JsonProperty("type")
        private String type;

        @JsonProperty("location")
        private String location;

        @JsonProperty("comment")
        private String comment;

        @JsonProperty("properties")
        @JsonDeserialize(using = PropertiesDeserializer.class)
        private Map<String, String> properties;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }
    }

    @JsonProperty("cluster_snapshot")
    private ClusterSnapshot clusterSnapshot;

    @JsonProperty("frontends")
    private List<Frontend> frontends;

    @JsonProperty("compute_nodes")
    private List<ComputeNode> computeNodes;

    @JsonProperty("storage_volumes")
    private List<StorageVolume> storageVolumes;

    public ClusterSnapshot getClusterSnapshot() {
        return clusterSnapshot;
    }

    public List<Frontend> getFrontends() {
        return frontends;
    }

    public List<ComputeNode> getComputeNodes() {
        return computeNodes;
    }

    public List<StorageVolume> getStorageVolumes() {
        return storageVolumes;
    }

    private void onLoad() {
        if (clusterSnapshot != null) {
            Preconditions.checkNotNull(storageVolumes,
                    "Storage volume " + clusterSnapshot.getStorageVolumeName() + " not found");

            StorageVolume storageVolume = storageVolumes.stream()
                    .filter(sv -> sv.getName().equals(clusterSnapshot.getStorageVolumeName()))
                    .findFirst()
                    .orElse(null);

            Preconditions.checkNotNull(storageVolume,
                    "Storage volume " + clusterSnapshot.getStorageVolumeName() + " not found");

            clusterSnapshot.setStorageVolume(storageVolume);
        }
    }

    public static ClusterSnapshotConfig load(String clusterSnapshotYamlFile) {
        try {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            ClusterSnapshotConfig config = objectMapper.readValue(new File(clusterSnapshotYamlFile),
                    ClusterSnapshotConfig.class);
            if (config == null) {
                // Empty config file
                config = new ClusterSnapshotConfig();
            }
            config.onLoad();
            return config;
        } catch (Exception e) {
            LOG.warn("Failed to load cluster snapshot config {} ", clusterSnapshotYamlFile, e);
            throw new RuntimeException(e);
        }
    }
}
