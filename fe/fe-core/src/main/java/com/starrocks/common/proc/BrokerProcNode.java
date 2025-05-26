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

package com.starrocks.common.proc;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Map;

public class BrokerProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> BROKER_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("IP").add("Port").add("Alive")
            .add("LastStartTime").add("LastUpdateTime").add("ErrMsg")
            .build();

    @Override
    public ProcResult fetchResult() {
        BaseProcResult result = new BaseProcResult();
        result.setNames(BROKER_PROC_NODE_TITLE_NAMES);

        BrokerMgr brokerMgr = GlobalStateMgr.getCurrentState().getBrokerMgr();
        Map<String, ArrayListMultimap<String, FsBroker>> brokersMap = brokerMgr.getBrokersMap();

        for (Map.Entry<String, ArrayListMultimap<String, FsBroker>> entry : brokersMap.entrySet()) {
            String brokerName = entry.getKey();
            for (FsBroker broker : entry.getValue().values()) {
                List<String> row = Lists.newArrayList();
                row.add(brokerName);
                row.add(broker.ip);
                row.add(String.valueOf(broker.port));
                row.add(String.valueOf(broker.isAlive));
                row.add(TimeUtils.longToTimeString(broker.lastStartTime));
                row.add(TimeUtils.longToTimeString(broker.lastUpdateTime));
                row.add(broker.heartbeatErrMsg);
                result.addRow(row);
            }
        }

        return result;
    }
}