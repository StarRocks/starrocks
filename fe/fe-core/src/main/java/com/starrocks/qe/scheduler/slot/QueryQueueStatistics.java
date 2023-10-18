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

package com.starrocks.qe.scheduler.slot;

public class QueryQueueStatistics {
    private int totalQueries = 0;
    private int admittingQueries = 0;
    private int pendingByGlobalResourceQueries = 0;
    private int pendingByGlobalSlotQueries = 0;
    private int pendingByGroupResourceQueries = 0;
    private int pendingByGroupSlotQueries = 0;

    public void reset(int totalQueries) {
        this.totalQueries = totalQueries;
        this.admittingQueries = 0;
        this.pendingByGlobalResourceQueries = 0;
        this.pendingByGlobalSlotQueries = 0;
        this.pendingByGroupResourceQueries = 0;
        this.pendingByGroupSlotQueries = 0;
    }

    public void finalizeStats() {
        pendingByGlobalSlotQueries += totalQueries - admittingQueries -
                pendingByGlobalResourceQueries - pendingByGlobalSlotQueries -
                pendingByGroupResourceQueries - pendingByGroupSlotQueries;
    }

    public int getTotalQueries() {
        return totalQueries;
    }

    public int getAdmittingQueries() {
        return admittingQueries;
    }

    public int getPendingByGlobalResourceQueries() {
        return pendingByGlobalResourceQueries;
    }

    public int getPendingByGlobalSlotQueries() {
        return pendingByGlobalSlotQueries;
    }

    public int getPendingByGroupResourceQueries() {
        return pendingByGroupResourceQueries;
    }

    public int getPendingByGroupSlotQueries() {
        return pendingByGroupSlotQueries;
    }

    public void incrAdmittingQueries(int admittingQueries) {
        this.admittingQueries += admittingQueries;
    }

    public void incrPendingByGlobalResourceQueries(int pendingByGlobalResourceQueries) {
        this.pendingByGlobalResourceQueries += pendingByGlobalResourceQueries;
    }

    public void incrPendingByGlobalSlotQueries(int pendingByGlobalSlotQueries) {
        this.pendingByGlobalSlotQueries += pendingByGlobalSlotQueries;
    }

    public void incrPendingByGroupResourceQueries(int pendingByGroupResourceQueries) {
        this.pendingByGroupResourceQueries += pendingByGroupResourceQueries;
    }

    public void incrPendingByGroupSlotQueries(int pendingByGroupSlotQueries) {
        this.pendingByGroupSlotQueries += pendingByGroupSlotQueries;
    }
}
