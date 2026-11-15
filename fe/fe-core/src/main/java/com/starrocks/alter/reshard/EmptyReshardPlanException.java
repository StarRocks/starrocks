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

package com.starrocks.alter.reshard;

import com.starrocks.common.StarRocksException;

/**
 * The plan for this table came out empty: nothing about the current layout and configuration asks for
 * a split. Distinct from the failures that merely mean "not now" -- an exhausted parallel-tablet
 * budget, a table another job currently owns -- because only this one is deterministic. Re-running it
 * on an unchanged layout produces the same empty plan, so the caller may latch it; the others must
 * stay retryable.
 */
public class EmptyReshardPlanException extends StarRocksException {
    public EmptyReshardPlanException(String msg) {
        super(msg);
    }
}
