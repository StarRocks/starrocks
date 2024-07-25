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

package com.starrocks.qe.scheduler.slot;

import com.google.common.collect.ImmutableList;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;

import java.util.ArrayList;
import java.util.List;

public class DeployState {
    // Divide requests of fragments in the current group to three stages.
    // - stage 1, the request with RF coordinator + descTable.
    // - stage 2, the first request to a host, which need send descTable.
    // - stage 3, the non-first requests to a host, which needn't send descTable.
    private final List<List<FragmentInstanceExecState>> threeStageExecutionsToDeploy =
            ImmutableList.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());

    public List<List<FragmentInstanceExecState>> getThreeStageExecutionsToDeploy() {
        return threeStageExecutionsToDeploy;
    }

}
