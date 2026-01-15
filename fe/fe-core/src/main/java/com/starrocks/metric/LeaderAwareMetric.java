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

import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;

/**
 * LeaderAwareMetric automatically adds a label to distinguish
 * between metrics reported by the leader and follower FE in the cluster.
 *
 * The label {is_leader="true|false"} is based on current node role.
 */
public abstract class LeaderAwareMetric<T> extends Metric<T> {
    private boolean isLeader;

    public LeaderAwareMetric(String name, MetricType type, MetricUnit unit, String description) {
        super(name, type, unit, description);
        isLeader = GlobalStateMgr.getCurrentState().isLeader();
        addLabel(getLabel(isLeader));
    }

    @Override
    public T getValue() {
        boolean leader = GlobalStateMgr.getCurrentState().isLeader();
        if (isLeader != leader) {
            addLabel(getLabel(leader));
            isLeader = leader;
        }
        return isLeader ? getValueLeader() : getValueNonLeader();
    }

    public abstract T getValueNonLeader();

    public abstract T getValueLeader();

    private static MetricLabel getLabel(boolean isLeader) {
        return new MetricLabel(FeConstants.METRIC_LABEL_IS_LEADER, String.valueOf(isLeader));
    }
}

