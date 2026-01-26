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

package com.starrocks.metric;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.NoopMetricRegistry;
import com.codahale.metrics.Snapshot;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;

public class LeaderAwareHistogramMetric extends HistogramMetric {

    private static final Histogram NOOP_HISTOGRAM = new NoopMetricRegistry().histogram("noop");

    private volatile boolean isLeader;

    public LeaderAwareHistogramMetric(String name) {
        super(name);
        this.isLeader = GlobalStateMgr.getCurrentState().isLeader();
        addLabel(getLeaderLabel(isLeader));
    }

    @Override
    public Snapshot getSnapshot() {
        updateLeaderLabel();
        return isLeader ? super.getSnapshot() : NOOP_HISTOGRAM.getSnapshot();
    }

    @Override
    public long getCount() {
        updateLeaderLabel();
        return isLeader ? super.getCount() : 0;
    }

    private void updateLeaderLabel() {
        boolean leader = GlobalStateMgr.getCurrentState().isLeader();
        if (isLeader == leader) {
            return;
        }
        isLeader = leader;
        for (int i = 0; i < labels.size(); i++) {
            MetricLabel label = labels.get(i);
            if (label.getKey().equals(FeConstants.METRIC_LABEL_IS_LEADER)) {
                labels.set(i, getLeaderLabel(leader));
                break;
            }
        }
    }

    private MetricLabel getLeaderLabel(boolean isLeader) {
        return new MetricLabel(FeConstants.METRIC_LABEL_IS_LEADER, String.valueOf(isLeader));
    }
}
