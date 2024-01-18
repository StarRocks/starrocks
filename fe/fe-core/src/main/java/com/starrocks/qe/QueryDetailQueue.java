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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/QueryDetailQueue.java

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

package com.starrocks.qe;

import com.google.common.collect.Lists;
import com.starrocks.common.conf.Config;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

// Queue of QueryDetail.
// It's used to collect queries for monitor.
public class QueryDetailQueue {
    private static final ConcurrentLinkedDeque<QueryDetail> TOTAL_QUERIES = new ConcurrentLinkedDeque<>();
    private static final ScheduledExecutorService SCHEDULED = Executors.newSingleThreadScheduledExecutor();

    private static final AtomicLong LATEST_MS = new AtomicLong();
    private static final AtomicLong LATEST_MS_CNT = new AtomicLong();

    static {
        SCHEDULED.scheduleAtFixedRate(QueryDetailQueue::removeExpiredQueryDetails, 0, 5, TimeUnit.SECONDS);
    }

    public static void addQueryDetail(QueryDetail queryDetail) {
        queryDetail.setEventTime(getCurrentTimeNS());
        TOTAL_QUERIES.addLast(queryDetail);
    }

    private static void removeExpiredQueryDetails() {
        long deleteTime = getCurrentTimeNS() - Config.query_detail_cache_time_nanosecond;

        while (!TOTAL_QUERIES.isEmpty() && TOTAL_QUERIES.peekFirst().getEventTime() < deleteTime) {
            TOTAL_QUERIES.pollFirst();
        }
    }

    public static List<QueryDetail> getQueryDetailsAfterTime(long eventTime) {
        List<QueryDetail> results = Lists.newArrayList();
        for (QueryDetail queryDetail : TOTAL_QUERIES) {
            if (queryDetail.getEventTime() > eventTime) {
                results.add(queryDetail);
            }
        }
        return results;
    }

    public static long getTotalQueriesCount() {
        return TOTAL_QUERIES.size();
    }

    private static long getCurrentTimeNS() {
        long ms = System.currentTimeMillis();
        if (ms == LATEST_MS.get()) {
            return ms * 1000000 + LATEST_MS_CNT.incrementAndGet();
        } else {
            LATEST_MS.set(ms);
            LATEST_MS_CNT.set(0);
            return ms * 1000000;
        }
    }
}
