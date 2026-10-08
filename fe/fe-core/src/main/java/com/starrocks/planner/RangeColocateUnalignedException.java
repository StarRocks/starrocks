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

package com.starrocks.planner;

/**
 * Raised at scheduling time when a range-colocate scan's tablet-to-bucket assignment, built while planning, no longer
 * matches the colocate group's current layout: a tablet split or merge published between planning and scheduling, or
 * the group is mid-realignment. Planning the statement again yields a plan that either uses the current tablets or
 * falls back to a shuffle; when the initial scheduling pass raises it, nothing has been deployed yet, so the query
 * retry loop re-plans instead of failing (see DefaultCoordinator#startScheduling and ExecuteExceptionHandler). If
 * the layout is still unaligned when the statement is re-planned, the retries fail with the same error.
 */
public class RangeColocateUnalignedException extends IllegalStateException {
    public RangeColocateUnalignedException(String message) {
        super(message);
    }
}
