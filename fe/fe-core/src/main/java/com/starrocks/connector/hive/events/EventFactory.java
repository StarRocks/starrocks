// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.events;

import com.starrocks.connector.hive.CacheUpdateProcessor;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;

import java.util.List;

/**
 * Factory interface to generate a {@link MetastoreEvent} from a {@link NotificationEvent} object.
 */
public interface EventFactory {

    /**
     * Generates a {@link MetastoreEvent} representing {@link NotificationEvent}
     *
     * @param hmsEvent  the event as received from Hive Metastore.
     * @param metaCache the cached instance of this event that needs to be updated.
     * @return {@link MetastoreEvent} representing hmsEvent.
     * @throws MetastoreNotificationException If the hmsEvent information cannot be parsed.
     */
    List<MetastoreEvent> get(NotificationEvent hmsEvent,
<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/EventFactory.java
                             CacheUpdateProcessor cacheProcessor,
                             String catalogName) throws MetastoreNotificationException;
=======
                             CacheUpdateProcessor metaCache,
                             HiveTable table, String catalogNames) throws MetastoreNotificationException;
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/EventFactory.java
}
