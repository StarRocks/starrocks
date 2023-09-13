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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

// Queue of QueryDetail.
// It's used to collect queries for monitor.
public class QueryDetailQueue {
    private static final LinkedList<QueryDetail> TOTAL_QUERIES = new LinkedList<QueryDetail>();

    //starrocks-manager pull queries every 1 second
    //metrics calculate query latency every 15 second
    //do not set cacheTime lower than these time
    private static final long CACHE_TIME_NS = 30000000000L;
    private static long latestMS;
    private static long latestMSCnt;

    public static synchronized void addAndRemoveTimeoutQueryDetail(QueryDetail queryDetail) {
        //set event time here to guarantee order
        long now = getCurrentTimeNS();
        queryDetail.setEventTime(now);
        TOTAL_QUERIES.add(queryDetail);

        Iterator<QueryDetail> it = TOTAL_QUERIES.iterator();
        long deleteTime = now - CACHE_TIME_NS;
        while (it.hasNext()) {
            QueryDetail detail = it.next();
            if (detail.getEventTime() < deleteTime) {
                it.remove();
            } else {
                break;
            }
        }
    }

    public static synchronized List<QueryDetail> getQueryDetailsAfterTime(long eventTime) {
        List<QueryDetail> results = Lists.newArrayList();
        for (QueryDetail queryDetail : TOTAL_QUERIES) {
            if (queryDetail.getEventTime() > eventTime) {
                results.add(queryDetail);
            }
        }
        return results;
    }

    public static synchronized long getTotalQueriesCount() {
        return TOTAL_QUERIES.size();
    }

    //must get lock before call
    //NOTICE: this is not precise nano seconds, but good enough to make eventTime in order and unique
    private static long getCurrentTimeNS() {
        long ms = System.currentTimeMillis();
        if (ms == latestMS) {
            latestMSCnt++;
            return ms * 1000000 + latestMSCnt;
        } else {
            latestMS = ms;
            latestMSCnt = 0;
            return ms * 1000000;
        }
    }
}
