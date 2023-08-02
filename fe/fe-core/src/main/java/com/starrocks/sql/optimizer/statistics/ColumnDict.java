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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;

public final class ColumnDict {
    private final ImmutableMap<ByteBuffer, Integer> dict;
    private final long collectedVersionTime;
    private long versionTime;

    public ColumnDict(ImmutableMap<ByteBuffer, Integer> dict, long versionTime) {
        Preconditions.checkState(dict.size() > 0 && dict.size() <= 256,
                "dict size %s is illegal", dict.size());
        this.dict = dict;
        this.collectedVersionTime = versionTime;
        this.versionTime = versionTime;
    }

    public ImmutableMap<ByteBuffer, Integer> getDict() {
        return dict;
    }

    public long getVersionTime() {
        return versionTime;
    }

    public long getCollectedVersionTime() {
        return collectedVersionTime;
    }

    void updateVersionTime(long versionTime) {
        this.versionTime = versionTime;
    }
}