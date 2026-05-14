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

import com.starrocks.common.StarRocksException;

/**
 * Signal raised by {@link ParquetMetadataSampler#tryPlan} that means
 * "retry with Tier 2". Distinct from a plain {@link StarRocksException}
 * thrown by Tier 1, which means "sampling failed, skip pre-split".
 */
public final class Tier1UnavailableException extends StarRocksException {
    private static final long serialVersionUID = 1L;

    public Tier1UnavailableException(String reason) {
        super(reason);
    }
}
