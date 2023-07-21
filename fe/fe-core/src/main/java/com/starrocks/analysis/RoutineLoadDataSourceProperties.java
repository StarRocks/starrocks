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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/RoutineLoadDataSourceProperties.java

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

package com.starrocks.analysis;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.load.routineload.LoadDataSourceType;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.parser.NodePosition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RoutineLoadDataSourceProperties implements ParseNode {

    private static final ImmutableSet<String> CONFIGURABLE_KAFKA_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY)
            .add(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY)
            .add(CreateRoutineLoadStmt.CONFLUENT_SCHEMA_REGISTRY_URL)
            .build();

    private static final ImmutableSet<String> CONFIGURABLE_PULSAR_PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY)
            .add(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY)
            .build();

    @SerializedName(value = "type")
    private String type = "KAFKA";
    // origin properties, no need to persist
    private Map<String, String> properties = Maps.newHashMap();

    @SerializedName(value = "kafkaPartitionOffsets")
    private List<Pair<Integer, Long>> kafkaPartitionOffsets = Lists.newArrayList();
    @SerializedName(value = "customKafkaProperties")
    private Map<String, String> customKafkaProperties = Maps.newHashMap();

    @SerializedName(value = "pulsarPartitionInitialPositions")
    private List<Pair<String, Long>> pulsarPartitionInitialPositions = Lists.newArrayList();
    @SerializedName(value = "customPulsarProperties")
    private Map<String, String> customPulsarProperties = Maps.newHashMap();
    @SerializedName(value = "confluentSchemaRegistryUrl")
    private String confluentSchemaRegistryUrl;

    private final NodePosition pos;

    public RoutineLoadDataSourceProperties() {
        // empty
        pos = NodePosition.ZERO;
    }

    public RoutineLoadDataSourceProperties(String type, Map<String, String> properties) {
        this(type, properties, NodePosition.ZERO);
    }

    public RoutineLoadDataSourceProperties(String type, Map<String, String> properties, NodePosition pos) {
        this.pos = pos;
        this.type = type.toUpperCase();
        this.properties = properties;
    }

    public void analyze() throws AnalysisException {
        checkDataSourceProperties();
    }

    public boolean hasAnalyzedProperties() {
        if (type.equals("KAFKA")) {
            return !kafkaPartitionOffsets.isEmpty() || !customKafkaProperties.isEmpty();
        } else if (type.equals("PULSAR")) {
            return !pulsarPartitionInitialPositions.isEmpty() || !customPulsarProperties.isEmpty();
        } else {
            return false;
        }
    }

    public String getType() {
        return type;
    }

    public String getConfluentSchemaRegistryUrl() {
        return confluentSchemaRegistryUrl;
    }

    public List<Pair<Integer, Long>> getKafkaPartitionOffsets() {
        return kafkaPartitionOffsets;
    }

    public Map<String, String> getCustomKafkaProperties() {
        return customKafkaProperties;
    }

    public List<Pair<String, Long>> getPulsarPartitionInitialPositions() {
        return pulsarPartitionInitialPositions;
    }

    public Map<String, String> getCustomPulsarProperties() {
        return customPulsarProperties;
    }

    private void checkDataSourceProperties() throws AnalysisException {
        LoadDataSourceType sourceType;
        try {
            sourceType = LoadDataSourceType.valueOf(type);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("routine load job does not support this type " + type);
        }
        switch (sourceType) {
            case KAFKA:
                checkKafkaProperties();
                break;
            case PULSAR:
                checkPulsarProperties();
                break;
            default:
                break;
        }
    }

    private void checkKafkaProperties() throws AnalysisException {
        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !CONFIGURABLE_KAFKA_PROPERTIES_SET.contains(entity)).filter(
                entity -> !entity.startsWith("property.") && !entity.startsWith("confluent.")).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid kafka custom property");
        }

        // check partitions
        final String kafkaPartitionsString = properties.get(CreateRoutineLoadStmt.KAFKA_PARTITIONS_PROPERTY);
        if (kafkaPartitionsString != null) {
            if (!properties.containsKey(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY)) {
                throw new AnalysisException("Partition and offset must be specified at the same time");
            }

            CreateRoutineLoadStmt.analyzeKafkaPartitionProperty(kafkaPartitionsString, Maps.newHashMap(),
                    kafkaPartitionOffsets);
        } else {
            if (properties.containsKey(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY)) {
                throw new AnalysisException("Missing kafka partition info");
            }
        }

        // check offset
        String kafkaOffsetsString = properties.get(CreateRoutineLoadStmt.KAFKA_OFFSETS_PROPERTY);
        if (kafkaOffsetsString != null) {
            CreateRoutineLoadStmt.analyzeKafkaOffsetProperty(kafkaOffsetsString, kafkaPartitionOffsets);
        }

        // check custom properties
        CreateRoutineLoadStmt.analyzeKafkaCustomProperties(properties, customKafkaProperties);

        if (properties.containsKey(CreateRoutineLoadStmt.CONFLUENT_SCHEMA_REGISTRY_URL)) {
            confluentSchemaRegistryUrl = properties.get(CreateRoutineLoadStmt.CONFLUENT_SCHEMA_REGISTRY_URL);
        }
    }

    private void checkPulsarProperties() throws AnalysisException {
        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !CONFIGURABLE_PULSAR_PROPERTIES_SET.contains(entity)).filter(
                entity -> !entity.startsWith("property.")).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid pulsar custom property");
        }

        // check custom properties
        CreateRoutineLoadStmt.analyzePulsarCustomProperties(properties, customPulsarProperties);

        // check partitions
        List<String> pulsarPartitions = Lists.newArrayList();
        final String pulsarPartitionsString = properties.get(CreateRoutineLoadStmt.PULSAR_PARTITIONS_PROPERTY);
        if (pulsarPartitionsString != null) {
            if (!properties.containsKey(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY) &&
                    !customPulsarProperties.containsKey(CreateRoutineLoadStmt.PULSAR_DEFAULT_INITIAL_POSITION)) {
                throw new AnalysisException("Partition and [default]position must be specified at the same time");
            }
            CreateRoutineLoadStmt.analyzePulsarPartitionProperty(pulsarPartitionsString,
                    customPulsarProperties, pulsarPartitions, pulsarPartitionInitialPositions);
        } else {
            if (properties.containsKey(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY)) {
                throw new AnalysisException("Missing pulsar partition info");
            }
        }

        // check position
        String pulsarPositionsString = properties.get(CreateRoutineLoadStmt.PULSAR_INITIAL_POSITIONS_PROPERTY);
        if (pulsarPositionsString != null) {
            CreateRoutineLoadStmt.analyzePulsarPositionProperty(pulsarPositionsString,
                    pulsarPartitions, pulsarPartitionInitialPositions);
        }
    }

    @Override
    public String toString() {
        if (!hasAnalyzedProperties()) {
            return "empty";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("type: ").append(type);
        if (type.equals("KAFKA")) {
            sb.append(", kafka partition offsets: ").append(kafkaPartitionOffsets);
            sb.append(", custom properties: ").append(customKafkaProperties);
        } else if (type.equals("PULSAR")) {
            if (!pulsarPartitionInitialPositions.isEmpty()) {
                sb.append(", pulsar partition initial positions: ").append(pulsarPartitionInitialPositions);
            }
            sb.append(", custom properties: ").append(customPulsarProperties);
        }
        return sb.toString();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
