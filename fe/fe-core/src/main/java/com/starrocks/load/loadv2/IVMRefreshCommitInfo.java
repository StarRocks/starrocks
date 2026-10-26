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

package com.starrocks.load.loadv2;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.common.tvr.TvrVersionRange;

import java.util.Map;

/**
 * What an IVM refresh records on the transaction it commits with, so the window it consumed rides the
 * same journal record as the data it applied: a node that never sees the follow-up refresh-scheme
 * record can still promote the bookmark on replay.
 *
 * <p>An instance stays in memory until its {@code TransactionState} is evicted, and that gate is a
 * count rather than a duration -- {@code DatabaseTransactionMgr.removeExpiredTxns} drops the oldest
 * final-status states once a database exceeds {@code label_keep_max_num} -- so the footprint is
 * bounded by that number times the base table count, and does not grow with refresh frequency.
 */
public class IVMRefreshCommitInfo {
    /** Per base table, the end of the window this refresh consumed. */
    @SerializedName("committedTvrMap")
    private final Map<BaseTableInfo, TvrVersionRange> committedTvrMap;

    public IVMRefreshCommitInfo(Map<BaseTableInfo, TvrVersionRange> committedTvrMap) {
        this.committedTvrMap = committedTvrMap;
    }

    public Map<BaseTableInfo, TvrVersionRange> getCommittedTvrMap() {
        return committedTvrMap;
    }
}
