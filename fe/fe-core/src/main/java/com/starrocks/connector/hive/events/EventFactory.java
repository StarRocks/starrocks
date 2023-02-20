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
