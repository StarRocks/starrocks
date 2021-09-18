// This file is made available under Elastic License 2.0.
package com.starrocks.common.profile;

import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Properties;

/**
 * @Author ikaruga4600
 * @Date 2021/9/15 16:53
 */
public class KafkaProfileStorage extends AsyncProfileStorage {

    private static final Logger LOG = LogManager.getLogger(KafkaProfileStorage.class);
    private final KafkaProducer<String, String> producer;

    KafkaProfileStorage() {
        Properties props = new Properties();
        props.put("bootstrap.servers", Config.profile_storage_kafka_servers);
        props.put("producer.type", "async");
        props.put("compression.type", "lz4");
        props.put("linger.ms", 1000);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    @Override
    protected void persist(List<Pair<String, String>> profiles) {
        for (Pair<String, String> profile : profiles) {
            producer.send(new ProducerRecord<>(Config.profile_storage_kafka_topic,
                            profile.first, profile.second), (metadata, exp) -> {
                        if (exp != null) {
                            LOG.warn("profile persist failed", exp);
                        }
                    }
            );
        }
    }

}
