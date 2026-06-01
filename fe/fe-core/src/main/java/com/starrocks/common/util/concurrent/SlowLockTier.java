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

package com.starrocks.common.util.concurrent;

/**
 * Verbosity tier for a single slow-lock log event. Higher tiers carry more diagnostic detail and
 * cost more to produce, so they are throttled more strictly; when a tier is throttled the decision
 * degrades to the next cheaper tier. If every tier (including the L3 breadcrumb) is still within
 * its interval the event is suppressed — degradation bounds the worst-case silence to one
 * breadcrumb interval during sustained contention, it does not guarantee a line per event.
 */
public enum SlowLockTier {
    /** Full lock info JSON plus owner / current-thread stack traces. Strictest throttle. */
    L1_STACK_INFO,
    /** Full lock info JSON, no stack traces. Looser throttle. */
    L2_INFO,
    /** A single plain-text warn line, no JSON and no stack. Loosest throttle — the floor. */
    L3_BREADCRUMB
}
