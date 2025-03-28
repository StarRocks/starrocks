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

package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;

import java.util.Objects;

public class MaterializedViewWrapper {
    private final MaterializedView mv;
    private final int level;
    private final MvPlanContext mvPlanContext;

    public MaterializedViewWrapper(MaterializedView mv, int level) {
        this.mv = mv;
        this.level = level;
        this.mvPlanContext = null;
    }

    public MaterializedViewWrapper(MaterializedView mv, int level, MvPlanContext mvPlanContext) {
        this.mv = mv;
        this.level = level;
        this.mvPlanContext = mvPlanContext;
    }

    public MaterializedView getMV() {
        return mv;
    }

    public int getLevel() {
        return level;
    }

    public MvPlanContext getMvPlanContext() {
        return mvPlanContext;
    }

    public static MaterializedViewWrapper create(MaterializedView mv, int level) {
        return new MaterializedViewWrapper(mv, level);
    }

    public static MaterializedViewWrapper create(MaterializedView mv, int level, MvPlanContext mvPlanContext) {
        return new MaterializedViewWrapper(mv, level, mvPlanContext);
    }

    @Override
    public int hashCode() {
        int result = mv.hashCode();
        result = 31 * result + level;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MaterializedViewWrapper mvWrapper = (MaterializedViewWrapper) o;
        if (level != mvWrapper.level) {
            return false;
        }
        if (!mv.equals(mvWrapper.mv)) {
            return false;
        }
        return Objects.equals(mvPlanContext, mvWrapper.mvPlanContext);
    }

    @Override
    public String toString() {
        return "MVWrapper{" +
                "mv=" + mv +
                ", level=" + level +
                '}';
    }
}
