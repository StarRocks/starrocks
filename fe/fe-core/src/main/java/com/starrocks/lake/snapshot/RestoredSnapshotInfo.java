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

package com.starrocks.lake.snapshot;

// save the infomation parsed from the cluster snapshot when start in restore mode
public class RestoredSnapshotInfo {
    private String restoredSnapshotName;
    private long feJournalId;
    private long starMgrJournalId;

    public RestoredSnapshotInfo(String restoredSnapshotName, long feJournalId, long starMgrJournalId) {
        this.restoredSnapshotName = restoredSnapshotName;
        this.feJournalId = feJournalId;
        this.starMgrJournalId = starMgrJournalId;
    }

    public String getRestoredSnapshotName() {
        return restoredSnapshotName;
    }

    public long getRestoredFEJournalIds() {
        return feJournalId;
    }

    public long getRestoredStarMgrJournalIds() {
        return starMgrJournalId;
    }
}
