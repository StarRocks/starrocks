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


package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32;

public class HashDistributionKey {
    private static final Logger LOG = LogManager.getLogger(PartitionKey.class);
    private List<LiteralExpr> keys;
    private List<Type> types;

    public HashDistributionKey() {
        keys = Lists.newArrayList();
        types = Lists.newArrayList();
    }

    public long getHashValue() {
        CRC32 hashValue = new CRC32();
        int i = 0;
        for (LiteralExpr expr : keys) {
            ByteBuffer buffer = expr.getHashValue(types.get(i));
            hashValue.update(buffer.array(), 0, buffer.limit());
            i++;
        }
        return hashValue.getValue();
    }

    public void pushColumn(LiteralExpr keyValue, Type keyType) {
        keys.add(keyValue);
        types.add(keyType);
    }

    public void popColumn() {
        keys.remove(keys.size() - 1);
        types.remove(types.size() - 1);
    }
}
