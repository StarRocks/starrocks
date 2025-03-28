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

package com.starrocks.qe.scheduler.dag;

import com.starrocks.common.StarRocksException;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.Deployer;
import com.starrocks.rpc.RpcException;
import com.starrocks.thrift.TUniqueId;

public interface ExecutionSchedule {
    void prepareSchedule(Coordinator coordinator, Deployer deployer, ExecutionDAG dag);

    void schedule(Coordinator.ScheduleOption option) throws RpcException, StarRocksException;

    void tryScheduleNextTurn(TUniqueId fragmentInstanceId) throws RpcException, StarRocksException;
    void cancel();
}
