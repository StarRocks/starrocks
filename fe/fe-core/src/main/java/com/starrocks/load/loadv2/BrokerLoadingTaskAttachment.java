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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/BrokerLoadingTaskAttachment.java

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

package com.starrocks.load.loadv2;

import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;

import java.util.List;
import java.util.Map;

public class BrokerLoadingTaskAttachment extends TaskAttachment {

    private final Map<String, String> counters;
    private final String trackingUrl;
    private final List<TabletCommitInfo> commitInfoList;
    private final List<TabletFailInfo> failInfoList;
    private final List<String> rejectedRecordPaths;
    private final long writeDurationMs;

    public BrokerLoadingTaskAttachment(long taskId, Map<String, String> counters, String trackingUrl,
                                       List<TabletCommitInfo> commitInfoList, List<TabletFailInfo> failInfoList,
                                       List<String> rejectedRecordPaths,
                                       long writeDurationMs) {
        super(taskId);
        this.trackingUrl = trackingUrl;
        this.counters = counters;
        this.commitInfoList = commitInfoList;
        this.failInfoList = failInfoList;
        this.rejectedRecordPaths = rejectedRecordPaths;
        this.writeDurationMs = writeDurationMs;
    }

    public String getCounter(String key) {
        return counters.get(key);
    }

    public Map<String, String> getCounters() {
        return counters;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public List<TabletCommitInfo> getCommitInfoList() {
        return commitInfoList;
    }

    public List<TabletFailInfo> getFailInfoList() {
        return failInfoList;
    }

    public List<String> getRejectedRecordPaths() {
        return rejectedRecordPaths;
    }

    public long getWriteDurationMs() {
        return writeDurationMs;
    }

    @Override
    public String toString() {
        return "BrokerLoadingTxnCommitAttachment " + counters.toString();
    }
}
