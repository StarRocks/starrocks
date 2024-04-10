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

package com.starrocks.consistency;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.server.GlobalStateMgr;

public class MetaRecoveryProdDir implements ProcNodeInterface {
    public static final ImmutableList<String> META_RECOVER_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Database")
            .add("Table")
            .add("Partition")
            .add("PartitionId")
            .add("Reason")
            .add("Advise")
            .build();

    @Override
    public ProcResult fetchResult() {
        BaseProcResult result = new BaseProcResult();
        result.setNames(META_RECOVER_PROC_NODE_TITLE_NAMES);
        GlobalStateMgr.getCurrentState().getMetaRecoveryDaemon().fetchProcNodeResult(result);
        return result;
    }
}
