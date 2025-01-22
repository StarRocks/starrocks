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
    // olap table use time info as version info.
    // table on lake use num as version, collectedVersion means historical version num,
    // while version means version in current period.
    private final long collectedVersion;
    private long version;

    public ColumnDict(ImmutableMap<ByteBuffer, Integer> dict, long version) {
        Preconditions.checkState(dict.size() > 0 && dict.size() <= 256,
                "dict size %s is illegal", dict.size());
        this.dict = dict;
        this.collectedVersion = version;
        this.version = version;
    }

    public ColumnDict(ImmutableMap<ByteBuffer, Integer> dict, long collectedVersion, long version) {
        this.dict = dict;
        this.collectedVersion = collectedVersion;
        this.version = version;
    }

    public ImmutableMap<ByteBuffer, Integer> getDict() {
        return dict;
    }

    public long getVersion() {
        return version;
    }

    public long getCollectedVersion() {
        return collectedVersion;
    }

    public int getDictSize() {
        return dict.size();
    }

    void updateVersion(long version) {
        this.version = version;
    }
}