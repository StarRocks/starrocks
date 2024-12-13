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

import com.google.common.collect.Lists;
<<<<<<< HEAD
import com.starrocks.connector.hive.CacheUpdateProcessor;
=======
import com.starrocks.connector.hive.HiveCacheUpdateProcessor;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import org.apache.hadoop.hive.metastore.api.NotificationEvent;

import java.util.List;

/**
 * An event type which is ignored. Useful for unsupported metastore event types
 */
public class IgnoredEvent extends MetastoreEvent {
<<<<<<< HEAD
    protected IgnoredEvent(NotificationEvent event, CacheUpdateProcessor cacheProcessor, String catalogName) {
=======
    protected IgnoredEvent(NotificationEvent event, HiveCacheUpdateProcessor cacheProcessor, String catalogName) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        super(event, cacheProcessor, catalogName);
    }

    private static List<MetastoreEvent> getEvents(NotificationEvent event,
<<<<<<< HEAD
                                                  CacheUpdateProcessor cacheProcessor, String catalogName) {
=======
                                                  HiveCacheUpdateProcessor cacheProcessor, String catalogName) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return Lists.newArrayList(new IgnoredEvent(event, cacheProcessor, catalogName));
    }

    @Override
    public void process() {
        debugLog("Ignoring unknown event type " + metastoreNotificationEvent.getEventType());
    }
}
