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


package com.staros.starlet;

import com.staros.proto.AddShardInfo;
import com.staros.proto.AddShardRequest;
import com.staros.proto.RemoveShardRequest;
import com.staros.proto.WorkerState;
import com.staros.worker.Worker;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// this class is used for unit test
public class MockStarletAgent extends StarletAgent {
    private static final Logger LOG = LogManager.getLogger(MockStarletAgent.class);

    private Worker worker;
    private Map<Long, AddShardInfo> shards; // <shardId, AddShardInfo>

    public MockStarletAgent() {
        worker = null;
        shards = new HashMap<>();
    }

    public synchronized int getShardCount() {
        return shards.size();
    }

    @Override
    public void setWorker(Worker w) {
        worker = w;
    }

    /*
     * @return Pair<success, stateChanged>
     */
    @Override
    public synchronized Pair<Boolean, Boolean> heartbeat() {
        worker.setState(WorkerState.ON);
        return Pair.of(true, false);
    }

    @Override
    public synchronized void addShard(AddShardRequest request) {
        List<AddShardInfo> infos = request.getShardInfoList();
        for (AddShardInfo info : infos) {
            shards.put(info.getShardId(), info.toBuilder().build());
        }
    }

    @Override
    public synchronized void removeShard(RemoveShardRequest request) {
        List<Long> shardIds = request.getShardIdsList();
        for (Long shardId : shardIds) {
            shards.remove(shardId);
        }
    }

    public Map<Long, AddShardInfo> getAllShards() {
        return shards;
    }
}
