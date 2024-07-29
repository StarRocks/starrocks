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

package com.starrocks.sql.optimizer.rule.transformation.materialization.compensation;

import java.util.List;

/**
 * To support mv compensation(transparent) rewrite, we need to compensate some partitions from the defined query of mv
 * if some of mv's base tables have updated/refreshed.
 * </p>
 * There are some differences for different types of tables to compensate:
 * - OlapTable, partition ids that are already updated.
 * - ExternalTable, partition keys that are already changed.
 * @param <T>
 */
public abstract class BaseCompensation<T> {
    private List<T> compensations;

    public BaseCompensation(List<T> compensations) {
        this.compensations = compensations;
    }

    public List<T> getCompensations() {
        return compensations;
    }

    @Override
    public abstract String toString();
}
