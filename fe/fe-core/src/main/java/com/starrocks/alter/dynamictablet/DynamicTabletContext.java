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

import java.util.Map;

/*
 * DynamicTabletContext saves the context during tablet splitting or merging for a physical partition
 */
public class DynamicTabletContext {
    @SerializedName(value = "commitVersion")
    protected long commitVersion = 0;

    @SerializedName(value = "indexIdToDynamicTablets")
    protected final Map<Long, DynamicTablets> indexIdToDynamicTablets;

    public DynamicTabletContext(Map<Long, DynamicTablets> indexIdToDynamicTablets) {
        this.indexIdToDynamicTablets = indexIdToDynamicTablets;
    }

    public long getCommitVersion() {
        return commitVersion;
    }

    public void setCommitVersion(long commitVersion) {
        this.commitVersion = commitVersion;
    }

    public Map<Long, DynamicTablets> getIndexIdToDynamicTablets() {
        return indexIdToDynamicTablets;
    }

    public long getParallelTablets() {
        long parallelTablets = 0;
        for (DynamicTablets dynamicTablets : indexIdToDynamicTablets.values()) {
            parallelTablets += dynamicTablets.getParallelTablets();
        }
        return parallelTablets;
    }
}
