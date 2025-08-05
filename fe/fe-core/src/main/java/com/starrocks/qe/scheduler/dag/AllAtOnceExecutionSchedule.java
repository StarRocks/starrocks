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

import com.starrocks.common.UserException;
import com.starrocks.qe.scheduler.Deployer;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.rpc.RpcException;
import com.starrocks.thrift.TUniqueId;

import java.util.List;

// all at once execution schedule only schedule once.
public class AllAtOnceExecutionSchedule implements ExecutionSchedule {
    private Deployer deployer;
    private ExecutionDAG dag;

    @Override
    public void prepareSchedule(Deployer deployer, ExecutionDAG dag) {
        this.deployer = deployer;
        this.dag = dag;
    }

    @Override
    public void schedule() throws RpcException, UserException {
        for (List<ExecutionFragment> executionFragments : dag.getFragmentsInTopologicalOrderFromRoot()) {
            final DeployState deployState = deployer.createFragmentExecStates(executionFragments);
            deployer.deployFragments(deployState);
        }
    }

    public void tryScheduleNextTurn(TUniqueId fragmentInstanceId) {
    }
}
