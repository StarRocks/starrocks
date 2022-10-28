// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.events;

import com.starrocks.connector.hive.CacheUpdateProcessor;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.spark_project.guava.collect.Lists;

import java.util.List;

/**
 * An event type which is ignored. Useful for unsupported metastore event types
 */
public class IgnoredEvent extends MetastoreEvent {
<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/IgnoredEvent.java
    protected IgnoredEvent(NotificationEvent event, CacheUpdateProcessor cacheProcessor, String catalogName) {
        super(event, cacheProcessor, catalogName);
    }

    private static List<MetastoreEvent> getEvents(NotificationEvent event,
                                                  CacheUpdateProcessor cacheProcessor, String catalogName) {
        return Lists.newArrayList(new IgnoredEvent(event, cacheProcessor, catalogName));
=======
    protected IgnoredEvent(NotificationEvent event, CacheUpdateProcessor metaCache, String catalogName) {
        super(event, metaCache, catalogName);
    }

    private static List<MetastoreEvent> getEvents(NotificationEvent event, CacheUpdateProcessor metaCache, String catalogName) {
        return Lists.newArrayList(new IgnoredEvent(event, metaCache, catalogName));
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/IgnoredEvent.java
    }

    @Override
    public void process() {
        debugLog("Ignoring unknown event type " + metastoreNotificationEvent.getEventType());
    }
}
