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

package com.starrocks.sql.optimizer;

public class ScanOptimzeOption {
    private boolean canUseAnyColumn;
    private boolean canUseMinMaxCountOpt;
    private boolean usePartitionColumnValueOnly;

    public void setCanUseAnyColumn(boolean v) {
        canUseAnyColumn = v;
    }

    public void setCanUseMinMaxCountOpt(boolean v) {
        canUseMinMaxCountOpt = v;
    }

    public boolean getCanUseAnyColumn() {
        return canUseAnyColumn;
    }

    public boolean getCanUseMinMaxCountOpt() {
        return canUseMinMaxCountOpt;
    }

    public void setUsePartitionColumnValueOnly(boolean v) {
        this.usePartitionColumnValueOnly = v;
    }

    public boolean getUsePartitionColumnValueOnly() {
        return usePartitionColumnValueOnly;
    }

    public ScanOptimzeOption copy() {
        ScanOptimzeOption opt = new ScanOptimzeOption();
        opt.canUseAnyColumn = this.canUseAnyColumn;
        opt.canUseMinMaxCountOpt = this.canUseMinMaxCountOpt;
        opt.usePartitionColumnValueOnly = this.usePartitionColumnValueOnly;
        return opt;
    }
}
