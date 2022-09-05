// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
