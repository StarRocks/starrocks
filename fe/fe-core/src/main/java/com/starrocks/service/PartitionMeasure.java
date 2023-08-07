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

package com.starrocks.service;

public class PartitionMeasure {

    public final long interval;
    public final String granularity;

    public PartitionMeasure(long interval, String granularity) {
        this.interval = interval;
        this.granularity = granularity;
    }

    public long getInterval() {
        return interval;
    }

    public String getGranularity() {
        return granularity;
    }
}
