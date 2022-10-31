// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;

import java.util.List;

/**
 * MetastoreEvent for CREATE_TABLE event type
 */
public class CreateTableEvent extends MetastoreTableEvent {
    public static final String CREATE_TABLE_EVENT_TYPE = "CREATE_TABLE";

    public static List<MetastoreEvent> getEvents(NotificationEvent event, CacheUpdateProcessor metaCache) {
        return Lists.newArrayList(new CreateTableEvent(event, metaCache));
    }

    private CreateTableEvent(NotificationEvent event, CacheUpdateProcessor metaCache)
            throws MetastoreNotificationException {
        super(event, metaCache);
        Preconditions.checkArgument(MetastoreEventType.CREATE_TABLE.equals(getEventType()));
        Preconditions.checkNotNull(MetastoreEventType.CREATE_TABLE, debugString("Event message is null"));
        CreateTableMessage createTableMessage =
                MetastoreEventsProcessor.getMessageDeserializer().getCreateTableMessage(event.getMessage());

        try {
            hmsTbl = createTableMessage.getTableObj();
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Unable to deserialize the event message"), e);
        }
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        throw new UnsupportedOperationException("Unsupported event type: " + getEventType());
    }
}
