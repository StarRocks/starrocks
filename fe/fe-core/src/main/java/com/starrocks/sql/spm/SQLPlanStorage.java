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

import java.util.List;

public interface SQLPlanStorage {
    static SQLPlanStorage create(boolean isGlobal) {
        return isGlobal ? new SQLPlanGlobalStorage() : new SQLPlanSessionStorage();
    }

    List<BaselinePlan> getAllBaselines();

    void storeBaselinePlan(BaselinePlan plan);

    List<BaselinePlan> findBaselinePlan(String sqlDigest, long hash);

    void dropBaselinePlan(long baseLineId);

    // for ut test use
    void dropAllBaselinePlans();

    default void replayBaselinePlan(BaselinePlan plan, boolean isCreate) {}
}
