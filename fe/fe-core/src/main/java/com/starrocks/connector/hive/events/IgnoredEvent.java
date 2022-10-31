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
    protected IgnoredEvent(NotificationEvent event, CacheUpdateProcessor metaCache) {
        super(event, metaCache);
    }

    private static List<MetastoreEvent> getEvents(NotificationEvent event, CacheUpdateProcessor metaCache) {
        return Lists.newArrayList(new IgnoredEvent(event, metaCache));
    }

    @Override
    public void process() {
        debugLog("Ignoring unknown event type " + metastoreNotificationEvent.getEventType());
    }
}
