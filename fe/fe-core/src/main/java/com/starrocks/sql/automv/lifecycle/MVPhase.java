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

package com.starrocks.sql.automv.lifecycle;

public enum MVPhase {
    // means embryo/nourishment/birth/bath: A MV is recommended but can not serving users queries.
    MP_CRADLE,
    // means crown-and-belt/assuming-office: A MV try to serving users queries, however, it is a intern
    // MV, if it can accelerate the queries, then it becomes tenured MV; else it would becomes retired.
    MP_INTERN,
    // means throne: A MV is testified to be good enough to serving online for long time.
    MP_TENURED,
    // means decline/illness: A MV is testified to be not good enough, a MV stay in this phase to exceed
    // long time, would be killed and put into grave;
    MP_RETIRED,
    // means death/grave: A MV is so poor in performance that it is kicked offline, it's resource would be
    // recycled.
    MP_GRAVE,
    // means extinction: A MV is disappear permanently.
    MP_EXTINCTION,
}
