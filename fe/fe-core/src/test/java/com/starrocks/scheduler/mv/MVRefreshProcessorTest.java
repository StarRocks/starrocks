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

package com.starrocks.scheduler.mv;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MVRefreshProcessorTest {

    @Test
    public void aRunThatStartsItsOwnJobLeadsTheBatch() {
        assertTrue(MVRefreshProcessor.isBatchLeadRun("run-1", "run-1"));
    }

    /**
     * A pct fallback can span dozens of batches, every one of them a fresh task run that records no
     * reason of its own and then persists the refresh scheme. Treating one as a lead would overwrite
     * the reason the lead run wrote with an empty one, losing why the view fell back at all.
     */
    @Test
    public void aLaterBatchOfTheSameJobDoesNot() {
        assertFalse(MVRefreshProcessor.isBatchLeadRun("run-1", "run-7"));
    }

    @Test
    public void aRunWithNoBatchLeadsIt() {
        assertTrue(MVRefreshProcessor.isBatchLeadRun(null, "run-1"));
    }
}
