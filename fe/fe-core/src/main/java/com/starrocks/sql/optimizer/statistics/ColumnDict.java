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
import java.util.Map;
import java.util.TreeSet;

public final class ColumnDict {
    private final ImmutableMap<ByteBuffer, Integer> dict;
    // for olap table, version info is time
    // for table on lake, version info is version num
    private final long collectedVersion;
    private long version;

    public ColumnDict(ImmutableMap<ByteBuffer, Integer> dict, long version) {
        Preconditions.checkState(dict.size() > 0 && dict.size() <= 256,
                "dict size %s is illegal", dict.size());
        this.dict = dict;
        this.collectedVersion = version;
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