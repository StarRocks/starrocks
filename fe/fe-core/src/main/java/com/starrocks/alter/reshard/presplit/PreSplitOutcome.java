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

package com.starrocks.alter.reshard.presplit;

/**
 * Outcome of {@link TabletPreSplitCoordinator#maybeAct}. Sealed so callers
 * cannot silently miss a new case when later stages add planning/submission
 * outcomes.
 */
public sealed interface PreSplitOutcome
        permits PreSplitOutcome.Skipped, PreSplitOutcome.Eligible, PreSplitOutcome.Finished {

    /** Eligibility check failed or a recoverable pipeline failure occurred; load proceeds against the original tablet. */
    record Skipped(SkipReason reason) implements PreSplitOutcome { }

    /** All eligibility checks passed. Returned by the eligibility-only entry point. */
    record Eligible() implements PreSplitOutcome { }

    /** Pre-split completed: the reshard job was submitted and reached the FINISHED state. */
    record Finished() implements PreSplitOutcome { }
}
