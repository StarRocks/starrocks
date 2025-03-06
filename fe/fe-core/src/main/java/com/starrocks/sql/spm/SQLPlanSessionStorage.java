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
package com.starrocks.sql.spm;

import com.google.common.collect.Lists;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.stream.Collectors;

class SQLPlanSessionStorage implements SQLPlanStorage {
    private final List<BaselinePlan> baselinePlans = Lists.newArrayList();

    public List<BaselinePlan> getAllBaselines() {
        return baselinePlans;
    }

    public void storeBaselinePlan(BaselinePlan plan) {
        plan.setId(GlobalStateMgr.getCurrentState().getNextId());
        baselinePlans.add(plan);
    }

    public List<BaselinePlan> findBaselinePlan(String sqlDigest, long hash) {
        return baselinePlans.stream().filter(p -> p.getBindSqlHash() == hash && p.getBindSqlDigest().equals(sqlDigest))
                .collect(Collectors.toList());
    }

    public void dropBaselinePlan(long baseLineId) {
        baselinePlans.removeIf(p -> p.getId() == baseLineId);
    }

    public void dropAllBaselinePlans() {
        baselinePlans.clear();
    }

}
