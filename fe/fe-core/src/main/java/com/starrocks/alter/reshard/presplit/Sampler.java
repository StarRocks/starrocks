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
 * Pluggable input sampler for Sample-Based Tablet Pre-Split. Returns a
 * non-null {@link SampleSet} on success (possibly {@link SampleSet#EMPTY});
 * throws when sampling cannot proceed. The coordinator converts a thrown
 * exception into an eligibility-skip — loads never fail because sampling did.
 */
@FunctionalInterface
public interface Sampler {
    SampleSet sample(SampleRequest request) throws StarRocksException;
}
