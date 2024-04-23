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

package com.starrocks.warehouse;

import com.google.common.collect.ImmutableList;
import com.starrocks.lake.StarOSAgent;

import java.util.ArrayList;
import java.util.List;

public class DefaultWarehouse extends Warehouse {

    private static final List<Long> WORKER_GROUP_ID_LIST;

    public DefaultWarehouse(long id, String name) {
        super(id, name, "An internal warehouse init after FE is ready");
    }

    static {
        List<Long> workerGroupIdList = new ArrayList<>();
        workerGroupIdList.add(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
        WORKER_GROUP_ID_LIST = ImmutableList.copyOf(workerGroupIdList);
    }

    @Override
    public List<Long> getWorkerGroupIds() {
        return WORKER_GROUP_ID_LIST;
    }
}
