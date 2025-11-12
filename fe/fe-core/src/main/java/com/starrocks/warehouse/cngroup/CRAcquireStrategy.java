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

package com.starrocks.warehouse.cngroup;

/**
 * {@code CRAcquireStrategy} is to mark the strategy to get the compute resource from the warehouse.
 */
public enum CRAcquireStrategy {
    STANDARD, // default strategy
    RANDOM, // round-robin strategy
    LOCAL_FIRST; // local first strategy

    public static CRAcquireStrategy fromString(String value) {
        try {
            return CRAcquireStrategy.valueOf(value.trim().toUpperCase());
        } catch (Exception e) {
            return STANDARD;
        }
    }
}
