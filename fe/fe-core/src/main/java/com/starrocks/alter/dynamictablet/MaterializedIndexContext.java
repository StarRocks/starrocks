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

package com.starrocks.alter.dynamictablet;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedIndex;

/*
 * MaterializedIndexContext saves the context during tablet splitting or merging for a materialized index
 */
public class MaterializedIndexContext {

    @SerializedName(value = "materializedIndex")
    protected final MaterializedIndex materializedIndex;

    @SerializedName(value = "dynamicTablets")
    protected final DynamicTablets dynamicTablets;

    public MaterializedIndexContext(MaterializedIndex materializedIndex, DynamicTablets dynamicTablets) {
        this.materializedIndex = materializedIndex;
        this.dynamicTablets = dynamicTablets;
    }

    public MaterializedIndex getMaterializedIndex() {
        return materializedIndex;
    }

    public DynamicTablets getDynamicTablets() {
        return dynamicTablets;
    }

    public long getParallelTablets() {
        return dynamicTablets.getParallelTablets();
    }
}
