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
     * @param cacheProcessor the cache update process instance to update catalog level cache.
     * @return {@link MetastoreEvent} representing hmsEvent.
     * @throws MetastoreNotificationException If the hmsEvent information cannot be parsed.
     */
    List<MetastoreEvent> get(NotificationEvent hmsEvent,
                             CacheUpdateProcessor cacheProcessor,
                             String catalogName) throws MetastoreNotificationException;
}
