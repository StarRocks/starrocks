// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/KafkaUtil.java

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

package com.starrocks.common.util;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class KafkaUtil {
    private static final Logger LOG = LogManager.getLogger(KafkaUtil.class);

    private static final KafkaAPI kafkaAPI = new KafkaAPI();

    public static List<Integer> getAllKafkaPartitions(String brokerList, String topic) throws UserException {
        return kafkaAPI.getAllKafkaPartitions(brokerList, topic);
    }

    // latest offset is (the latest existing message offset + 1)
    public static Map<Integer, Long> getEndOffsets(String brokerList, String topic,
                                                      List<Integer> partitions) throws UserException {
        return kafkaAPI.getEndOffsets(brokerList, topic, partitions);
    }

    public static Map<Integer, Long> getBeginningOffsets(String brokerList, String topic,
                                                         List<Integer> partitions) throws UserException {
        return kafkaAPI.getBeginningOffsets(brokerList, topic,  partitions);
    }

    static class KafkaAPI {
        public List<Integer> getAllKafkaPartitions(String brokerList, String topic)
                throws UserException {
            // create request
            final Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            final Consumer<String, String> consumer = new KafkaConsumer<>(props);

            List<Integer> partitionIDs = new ArrayList<>();
            final List<PartitionInfo> partitions = consumer.partitionsFor(topic, 
                    Duration.ofSeconds(Config.routine_load_kafka_timeout_second));

            for (PartitionInfo partition : partitions) {
                partitionIDs.add(partition.partition());
            }

            consumer.close();

            return partitionIDs;
        }

        public Map<Integer, Long> getEndOffsets(String brokerList, String topic,
                                                List<Integer> partitions) throws UserException {

            final Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());  
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());  

            final Consumer<String, String> consumer = new KafkaConsumer<>(props);

            ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
            for (Integer partitionID : partitions) {
                topicPartitions.add(new TopicPartition(topic, partitionID));
            }
            final Map<TopicPartition, Long> partitionNndOffsets = consumer.endOffsets(topicPartitions, 
                    Duration.ofSeconds(Config.routine_load_kafka_timeout_second));

            final Map<Integer, Long> endOffsets = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : partitionNndOffsets.entrySet()) {
                endOffsets.put(entry.getKey().partition(), entry.getValue());
            }

            consumer.close();
            return endOffsets;
        }

        public Map<Integer, Long> getBeginningOffsets(String brokerList, String topic,
                                                List<Integer> partitions) throws UserException {

            final Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            final Consumer<String, String> consumer = new KafkaConsumer<>(props);

            ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
            for (Integer partitionID : partitions) {
                topicPartitions.add(new TopicPartition(topic, partitionID));
            }
            final Map<TopicPartition, Long> partitionNndOffsets = consumer.beginningOffsets(topicPartitions, 
                    Duration.ofSeconds(Config.routine_load_kafka_timeout_second));

            final Map<Integer, Long> beginningOffsets = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> entry : partitionNndOffsets.entrySet()) {
                beginningOffsets.put(entry.getKey().partition(), entry.getValue());
            }

            consumer.close();
            return beginningOffsets;
        }

    }
}

