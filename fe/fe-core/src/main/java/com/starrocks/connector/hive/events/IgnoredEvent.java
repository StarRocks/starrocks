// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.events;

import com.google.common.collect.Lists;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;

import java.util.List;

/**
 * An event type which is ignored. Useful for unsupported metastore event types
 */
public class IgnoredEvent extends MetastoreEvent {
    protected IgnoredEvent(NotificationEvent event, CacheUpdateProcessor cacheProcessor, String catalogName) {
        super(event, cacheProcessor, catalogName);
    }

    private static List<MetastoreEvent> getEvents(NotificationEvent event,
                                                  CacheUpdateProcessor cacheProcessor, String catalogName) {
        return Lists.newArrayList(new IgnoredEvent(event, cacheProcessor, catalogName));
    }

    @Override
    public void process() {
        debugLog("Ignoring unknown event type " + metastoreNotificationEvent.getEventType());
    }
}
